# Copyright 2021 Yurii Shestakov <yuriis@nvidia.com>
#

"""
Shows the functionality of exec using a Busybox container.
Spawns a new POD on each command to be executed
Creates a pool of workers for each POD
"""

import sys
import time
import yaml
import os
import os.path
import signal
from argparse import ArgumentParser
from kubernetes import config
from kubernetes import client
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
import kubernetes.client.exceptions
# from pprint import pprint
import queue
import random
import threading
import string

class JobLogger(object):
    def __init__(self, log_path, job_name):
        super().__init__()
        self.log_path = log_path
        # assert os.path.exists(log_path) and os.path.isdir(log_path)
        self.fn = os.path.join(log_path, "{}.log".format(job_name))
        self.fo = open(self.fn, 'w')

    def __del__(self):
        if self.fo is not None:
            self.close()

    def close(self):
        assert self.fo is not None
        if self.fo is not None:
            self.fo.close()
            self.fo = None

    def append_stdout(self, out):
        assert self.fo is not None
        self.fo.write(out)

    def append_stderr(self, out):
        assert self.fo is not None
        self.fo.write(out)

    def log_exception(self, exc):
        assert self.fo is not None
        self.fo.write("-E-: {}\n".format(exc))

    def set_command(self, command):
        assert self.fo is not None
        self.fo.write("-I-: {}\n".format(command))

    def set_exitcode(self, rc):
        assert self.fo is not None
        self.fo.write("-X-: {:d}\n".format(rc))


class Worker(threading.Thread):

    def __init__(self, stop_ev, task_q, done_q):
        super().__init__()
        self.stop_ev = stop_ev
        self.task_q = task_q
        self.done_q = done_q
        # self._tid = threading.get_ident()

    def worker(self, cmd, idx):
        """
        This method should be overwritten in the derived class
        """
        time.sleep(5)

    @property
    def tid(self):
        tid = threading.get_ident()
        return tid

    def run(self):
        # looping until event is published
        while not self.stop_ev.is_set():
            res = None
            idx = None
            try:
                (idx, cmd) = self.task_q.get(block=False) # , timeout=0.5
                res = cmd  # DUMMY RES
                print("Worker.run: {} << [{}] {}".format(self.tid, idx, cmd))
                if idx is None:
                    break
                res = self.worker(cmd, idx)
                self.done_q.put((idx, res))
            except queue.Empty:
                continue
            except Exception as ex:
                print("Worker.run: {} . e: {}".format(self.tid, ex))
                self.done_q.put((idx, ex))
                break
        print("Worker.run: {} << q".format(self.tid))


class KubeWorker(Worker):
    POD_TIMEOUT_M = 90   # pod timeout (minutes)

    def __init__(self, stop_ev, task_q, done_q,
                 pod_name, image, namespace, node_selector,
                 log_path='.', verbose=False):
        super().__init__(stop_ev, task_q, done_q)
        self.pod_name = pod_name
        self.image = image
        self.namespace = namespace
        self.node_selector = node_selector
        self.log_path = log_path
        self.verbose = verbose
        self.log = None

    @property
    def api_instance(self):
        return core_v1_api.CoreV1Api()

    @property
    def tid(self):
        return self.pod_name

    def pod_exists(self, name):
        "Check that pod exists by name"
        while not self.stop_ev.is_set():
            try:
                resp = self.api_instance.read_namespaced_pod(
                    name=name, namespace=self.namespace)
                if resp.status.phase in ('Terminating'):
                    time.sleep(1)
                    continue
                print("%s: pod_exists: %s" % (name, resp.status.phase))
                return True

            except ApiException as e:
                if e.status == 404:
                    return False
                print("%s: APIERR:pod_exists: %s: %s| %s" % (
                    name, e.status, e.reason, e.body))
                if e.status == 0:
                    # return False
                    continue
                raise e
        return not self.stop_ev.is_set()

    def spawn_pod(self, name):
        # name is a container(pod?) name to spawn
        while self.pod_exists(name):
            self.delete_pod(name, True)
            time.sleep(1)
        if self.stop_ev.is_set():
            return False
        # print("Pod %s does not exist. Creating it..." % name)
        if self.verbose:
            print("%s: spawn_pod..." % name)
        pod_tout_5m = int(self.POD_TIMEOUT_M/1.0 + 0.5)
        pod_cmd = ('seq 1 {} |while read i ;'
                   'do echo -n "$i of {}: ";date; '
                   'test -e /tmp/.exit && exit 0 ; sleep 60; done'
                   ''.format(pod_tout_5m, pod_tout_5m))
        pod_manifest = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': name
            },
            'spec': {
                'terminationGracePeriodSeconds': 3,
                'restartPolicy': 'Never',
                'nodeSelector': self.node_selector,
                'containers': [{
                    'image': self.image,
                    'name': name,
                    "args": [ "/bin/sh", "-c", pod_cmd ]
                }]
            }
        }
        resp = self.api_instance.create_namespaced_pod(
            body=pod_manifest, namespace=self.namespace)
        while True:
            resp = self.api_instance.read_namespaced_pod(
                name=name, namespace=self.namespace)
            if resp.status.phase != 'Pending':
                break
            if self.stop_ev.is_set():
                return False
            time.sleep(1)
        return True

    def exec_my_command(self, cmd, name):
        # Calling exec and waiting for response
        print("{}: EXEC: {}".format(name, cmd))
        exec_command = [ '/bin/sh', '-c', cmd ]
        resp = stream(
            self.api_instance.connect_get_namespaced_pod_exec,
            name, self.namespace, command=exec_command,
            stderr=True, stdin=True,
            stdout=True, tty=False,
            _preload_content=False)
        while resp.is_open() and not self.stop_ev.is_set():
            resp.update(timeout=1)
            if resp.peek_stdout():
                # print("%s: STDOUT: %s" % (name, resp.read_stdout()), end='')
                self.log.append_stderr(resp.read_stdout())
            if resp.peek_stderr():
                # print("%s: STDERR: %s" % (name, resp.read_stderr()), end=''),
                self.log.append_stderr(resp.read_stderr())
        if not self.stop_ev.is_set():
            resp.run_forever(timeout=5)
            rc = resp.returncode
            resp.close()
        else:
            rc = -1
        # print("{}: returncode: {:d}".format(name, rc))
        return rc

    def delete_pod(self, name, foreground=False):
        if self.verbose:
            print("%s: delete_pod ..." % name)
        if foreground:
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5
            )
        else:
            body=client.V1DeleteOptions()
        try:
            resp = self.api_instance.delete_namespaced_pod(
                name, self.namespace, body=body)
        except ApiException as e:
            if e.status == 0:
                return
            msg = "{}:ERR:delete_pod/API:{}:{}".format(name, e.status, e.reason)
            print(msg, file=sys.stderr)
            self.log.log_exception(msg)
        except Exception as exc:
            msg = "{}:ERR:delete_pod: {}".format(name, repr(exc))
            print(msg, file=sys.stderr)
            self.log.log_exception(msg)

    def worker(self, cmd, cmd_idx):
        """
        This worker spawns a pod for the each new task
        """
        pod_name = "{}-{:03d}".format(self.pod_name, cmd_idx)
        exit_code = None
        has_pod = False
        try:
            self.log = JobLogger(self.log_path, pod_name)
            if not self.spawn_pod(pod_name):
                # terminate worker if can't spawn a pod
                return None
            has_pod = True
            self.log.set_command(cmd)
            exit_code = self.exec_my_command(cmd, pod_name)
            self.log.set_exitcode(exit_code)
        except kubernetes.client.exceptions.ApiException as e:
            msg = "%s:w3/APIERR: %s: %s" % (pod_name, e.reason, e.body)
            print(msg, file=sys.stderr)
            self.log.log_exception(msg)
        except KeyboardInterrupt as e:
            msg = "%s:w3/SIGINT: %s" % (pod_name, e)
            print(msg, file=sys.stderr)
            self.log.log_exception(msg)
        finally:
            # now, delete the pod
            if has_pod:
                try:
                    self.delete_pod(pod_name, True)
                except kubernetes.client.exceptions.ApiException as e:
                    print("%s:w3/APIERR: %s" % (pod_name, e.reason),
                          file=sys.stderr)
            if self.log is not None:
                self.log.close()
        return exit_code


class PodScheduler(object):

    def __init__(self, pod_name, image, tasks, node_selector,
            namespace='default', workers_num=50, log_path='.', verbose=False):
        random_str = ''.join(random.choice(string.ascii_lowercase) for x in range(5))
        self.pod_name = "{}-{}".format( pod_name, random_str)
        self.image = image
        self.tasks = tasks
        self.node_selector = node_selector
        self.namespace = namespace
        self.workers_num = workers_num
        self.log_path = log_path
        self.verbose = verbose
        self.stop_ev = threading.Event()
        self.workers = None

    def sig_handler(self, signum, frame):
        """
        The signal handler is called only in the context of main thread
        """
        tid = threading.get_ident()
        print('sig_handler(signum {})(tid {})'.format(signum, tid),
              file=sys.stderr)
        self.stop_ev.set()

    def set_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)   # Ctrl-C

    def res_collector(self, done_q, n):
        """
        Collects results of execution from the done_q
        Checks `stop_ev` event to exit immediatly
        """
        # blocking get until event is published
        c = 0
        while not self.stop_ev.is_set() and c < n:
            try:
                (idx, res) = done_q.get(block=False, timeout=0.5)
                print("res_collector: [{}] {}".format(idx, res))
                c += 1
            except queue.Empty:
                continue
        print("res_collector: done")

    def run_scheduler(self):
        self.set_signal_handlers()
        task_q = queue.Queue()
        done_q = queue.Queue()
        workers_num = self.workers_num
        n_tasks = len(self.tasks)
        if n_tasks < workers_num:
            workers_num = n_tasks
        threads = [KubeWorker(self.stop_ev, task_q, done_q,
                              "{}-{:03d}".format(self.pod_name, i),
                              self.image, self.namespace,
                              self.node_selector,
                              self.log_path, self.verbose)
                          for i in range(workers_num)]
        self.workers = threads
        for t in threads:
            t.setDaemon(True)
            t.start()
        for (idx, cmd) in enumerate(self.tasks):
            task_q.put((idx, cmd))
        for _ in threads:  # signal workers to quit
            task_q.put((None, None))
        self.res_collector(done_q, n_tasks)
        for t in threads:  # wait until workers exit
            t.join()


def main(args):
    # load job definition from the job file
    with open(args.job, 'r') as fi:
        job = yaml.safe_load(fi)
    config.load_kube_config()
    c = Configuration()
    assert os.path.exists(args.log_path) and os.path.isdir(args.log_path)
    sch = PodScheduler(
        pod_name=job['name'],
        image=job['image'],
        tasks=job['tasks'],
        node_selector=job.get('nodeSelector'),
        namespace=job.get('ns', 'default'),
        workers_num=job.get('workers_num', 50),
        log_path=args.log_path,
        verbose=args.verbose
    )
    sch.run_scheduler()
    print("main: done")


if __name__ == '__main__':
    parser = ArgumentParser(prog='pod_spawn')
    parser.add_argument('-j', dest='job', required=True,
                        help='Input job name')
    parser.add_argument('-v', dest='verbose', action='store_true',
                        help='Verbose output')
    parser.add_argument('-l', dest='log_path', default='.',
                        help='Verbose output')
    args = parser.parse_args()
    main(args)
