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
import signal
from argparse import ArgumentParser
from kubernetes import config
from kubernetes import client
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
import kubernetes.client.exceptions
from pprint import pprint
import queue
import random
import threading
import inspect
import ctypes
import types


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
                 pod_name, image, namespace, node_selector):
        super().__init__(stop_ev, task_q, done_q)
        self.pod_name = pod_name
        self.image = image
        self.namespace = namespace
        self.node_selector = node_selector

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
                print("%s: STDOUT: %s" % (name, resp.read_stdout()), end='')
            if resp.peek_stderr():
                print("%s: STDERR: %s" % (name, resp.read_stderr()), end=''),
        if not self.stop_ev.is_set():
            resp.run_forever(timeout=5)
            rc = resp.returncode
            resp.close()
        else:
            rc = -1
        print("{}: returncode: {:d}".format(name, rc))
        return rc

    def delete_pod(self, name, foreground=False):
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
            print("{}:ERR:delete_pod/API:{}:{}".format(name, e.status, e.reason))
        except Exception as exc:
            print("{}:ERR:delete_pod: {}".format(name, repr(exc)))

    def worker(self, cmd, cmd_idx):
        """
        This worker spawns a pod for the each new task
        """
        pod_name = "{}-{:03d}".format(self.pod_name, cmd_idx)
        exit_code = None
        has_pod = False
        try:
            if not self.spawn_pod(pod_name):
                # terminate worker if can't spawn a pod
                return None
            has_pod = True
            exit_code = self.exec_my_command(cmd, pod_name)
            # self._send_signal(pod_name, 15)
        except kubernetes.client.exceptions.ApiException as e:
            print("%s:w3/APIERR: %s: %s" % (pod_name, e.reason, e.body),
                  file=sys.stderr)
        except KeyboardInterrupt as e:
            print("%s:w3/SIGINT: %s" % (pod_name, e), file=sys.stderr)
        finally:
            # now, delete the pod
            if has_pod:
                try:
                    self.delete_pod(pod_name, True)
                except kubernetes.client.exceptions.ApiException as e:
                    print("%s:w3/APIERR: %s" % (pod_name, e.reason),
                          file=sys.stderr)

        return exit_code


class PodScheduler(object):

    def __init__(self, pod_name, image, tasks, node_selector,
            namespace='default', workers_num=50, multiply=1):
        self.pod_name = pod_name
        self.image = image
        self.tasks = tasks
        self.node_selector = node_selector
        self.namespace = namespace
        self.workers_num = workers_num
        self.multiply = multiply
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
                              self.node_selector)
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
    sch = PodScheduler(
        pod_name=job['name'],
        image=job['image'],
        tasks=job['tasks'],
        node_selector=job.get('nodeSelector'),
        namespace=job.get('ns', 'default'),
        workers_num=job.get('workers_num', 50)
    )
    sch.run_scheduler()
    print("main: done")


if __name__ == '__main__':
    parser = ArgumentParser(prog='pod_spawn')
    parser.add_argument('-j', dest='job', required=True,
                        help='Input job name')
    args = parser.parse_args()
    main(args)
