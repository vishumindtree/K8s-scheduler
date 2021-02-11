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
from multiprocessing import Process, Queue, current_process
from pprint import pprint
import concurrent.futures


class PodScheduler(object):
    POD_TIMEOUT_M = 7   # pod timeout (minutes)

    def __init__(self, name, image, tasks, node_selector,
            namespace='default', workers_num=50, multiply=1):
        self.name = name
        self.image = image
        self.tasks = tasks
        self.node_selector = node_selector
        self.namespace = namespace
        self.workers_num = workers_num
        self.multiply = multiply

    @property
    def api_instance(self):
        return core_v1_api.CoreV1Api()

    def pod_exists(self, name):
        "Check that pod exists by name"
        while True:
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
        return True

    def spawn_pod(self, name):
        # name is a container(pod?) name to spawn
        while self.pod_exists(name):
            self.delete_pod(name, True)
            time.sleep(1)
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
            time.sleep(1)
        return True

    def _send_signal(self, name, signal):
        # exec_command = ['/bin/kill', ('-%d' % signal),  '1']
        exec_command = ['/bin/touch', '/tmp/.exit']
        resp = stream(
            self.api_instance.connect_get_namespaced_pod_exec,
            name, self.namespace, command=exec_command,
            stderr=False, stdin=False,
            stdout=False, tty=False,
            _preload_content=False)
        resp.run_forever(timeout=2)
        rc = resp.returncode
        return rc

    def exec_my_command(self, cmd, name):
        # Calling exec and waiting for response
        print("{}: EXEC {}".format(name, cmd))
        exec_command = [ '/bin/sh', '-c', cmd ]
        resp = stream(
            self.api_instance.connect_get_namespaced_pod_exec,
            name, self.namespace, command=exec_command,
            stderr=True, stdin=True,
            stdout=True, tty=False,
            _preload_content=False)
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                print("%s: STDOUT: %s" % (name, resp.read_stdout()))
            if resp.peek_stderr():
                print("%s: STDERR: %s" % (name, resp.read_stderr()))
        resp.run_forever(timeout=5)
        rc = resp.returncode
        print("{}: returncode: {:d}".format(name, rc))
        resp.close()
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

    def worker3(self, cmd, pod_idx):
        """
        This worker spawns a pod for the each new task
        """
        pod_name = "{}-{:03d}".format(self.name, pod_idx)
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
            print("%s:w3/APIERR: %s" % (pod_name, e.reason), file=sys.stderr)
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

    def sig_handler(self, signum, frame):
        print('Signal handler called with signal', signum, file=sys.stderr)
        raise OSError("Let's stop")

    def set_signal_handlers(self):
        # signal.signal(signal.SIGTERM, self.sig_handler)
        # signal.signal(signal.SIGINT, self.sig_handler)   # Ctrl-C
        pass

    def run_scheduler(self):
        self.set_signal_handlers()
        if self.node_selector is None:
            self.node_selector = {
                'beta.kubernetes.io/os': 'linux',
                'kubernetes.io/arch': 'amd64'
                # ,'kubernetes.io/hostname': 'swx-snake-01'
           }

        j_num = len(self.tasks)
        if j_num < self.workers_num:
            self.workers_num = j_num
        # Submit tasks
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.workers_num) as executor:
            future_to_res = {
                executor.submit(self.worker3, task, i): (task, i)
                for i, task in enumerate(self.tasks)
            }
            for future in concurrent.futures.as_completed(future_to_res):
                cmd, pod_idx = future_to_res[future]
                pod_name = "{}-{:03d}".format(self.name, pod_idx)
                try:
                    rc = future.result()
                except kubernetes.client.exceptions.ApiException as e:
                    print("%s:APIERR: %s" % (pod_name, cmd, e.reason),
                          file=sys.stderr)
                except Exception as exc:
                    print('%s:%r generated an exception: %r' %
                          (pod_name, cmd, exc), file=sys.stderr)
                else:
                    print('%s:%r => %s' % (pod_name, cmd, rc))


def main(args):
    # load job definition from the job file
    with open(args.job, 'r') as fi:
        job = yaml.safe_load(fi)
    config.load_kube_config()
    c = Configuration()
    sch = PodScheduler(
        name=job['name'],
        image=job['image'],
        tasks=job['tasks'],
        node_selector=job.get('nodeSelector'),
        namespace=job.get('ns', 'default'),
        workers_num=job.get('workers_num', 50)
    )
    sch.run_scheduler()


if __name__ == '__main__':
    parser = ArgumentParser(prog='pod_spawn')
    parser.add_argument('-j', dest='job', required=True,
                        help='Input job name')
    args = parser.parse_args()
    main(args)
