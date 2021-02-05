# Copyright 2021 Yurii Shestakov <yuriis@nvidia.com>
#

"""
Shows the functionality of exec using a Busybox container.
Spawns a new POD on each command to be executed
Creates a pool of workers for each POD
"""

import time
import yaml
from kubernetes import config
from kubernetes import client
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from argparse import ArgumentParser
from multiprocessing import Process, Queue, current_process
from pprint import pprint


def spawn_pod(api_instance, name, nsel, image='busybox', ns='default'):
    # name is a container(pod?) name to spawn
    resp = None
    try:
        resp = api_instance.read_namespaced_pod(name=name,
                                                namespace=ns)
    except ApiException as e:
        if e.status != 404:
            print("ERR: %s" % e)
            return False

    if resp:
        return True
    print("Pod %s does not exist. Creating it..." % name)
    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': name
        },
        'spec': {
            'nodeSelector': nsel,
            'containers': [{
                'image': image,
                'name': name,
                "args": [
                    "/bin/sh",
                    "-c",
                    "while true;do date;sleep 5; done"
                ]
            }]
        }
    }
    # pprint(pod_manifest)
    resp = api_instance.create_namespaced_pod(body=pod_manifest, namespace=ns)
    while True:
        resp = api_instance.read_namespaced_pod(name=name, namespace=ns)
        if resp.status.phase != 'Pending':
            break
        time.sleep(1)
    return True


def exec_my_command(api_instance, cmd, name, ns):
    # Calling exec and waiting for response
    print("{}: EXEC {}".format(name, cmd))
    exec_command = [ '/bin/sh', '-c', cmd ]
    resp = stream(api_instance.connect_get_namespaced_pod_exec,
                  name, ns, command=exec_command,
                  stderr=True, stdin=True,
                  stdout=True, tty=False,
                  _preload_content=False)
    while resp.is_open():
        resp.update(timeout=1)
        if resp.peek_stdout():
            print("STDOUT: %s" % resp.read_stdout())
        if resp.peek_stderr():
            print("STDERR: %s" % resp.read_stderr())
    resp.run_forever(timeout=5)
    rc = resp.returncode
    print("returncode: {:d}".format(rc))
    resp.close()
    return rc

#
# Function run by worker processes
#

def worker(api_instance, name, image, ns, nsel, input_q, output_q):
    if not spawn_pod(api_instance, name, nsel, image, ns):
        return
    for cmd in iter(input_q.get, 'STOP'):
        exit_code = exec_my_command(api_instance, cmd, name, ns)
        output_q.put((cmd, exit_code))
    # now, delete the pod
    body = client.V1DeleteOptions()
    resp = api_instance.delete_namespaced_pod(name, ns, body=body)
    print("delete_namespaced_pod: {}".format(name))
    # pprint(resp)


def worker2(api_instance, name, image, ns, nsel, input_q, output_q):

    for (n, cmd) in enumerate(iter(input_q.get, 'STOP')):
        pod_name = "{}-{:04d}".format(name, n)
        if not spawn_pod(api_instance, pod_name, nsel, image, ns):
            return
        exit_code = exec_my_command(api_instance, cmd, pod_name, ns)
        output_q.put((cmd, exit_code))
        # now, delete the pod
        body = client.V1DeleteOptions()
        resp = api_instance.delete_namespaced_pod(pod_name, ns, body=body)


def scheduler(job):
    NUMBER_OF_PROCESSES = job['workers_num']

    # Create queues
    task_q = Queue()
    done_q = Queue()

    # Submit tasks
    M = int(job.get('multiply', 1))
    j_num = len(job['tasks']) * M
    if j_num < NUMBER_OF_PROCESSES:
        NUMBER_OF_PROCESSES = j_num
    for i in range(M):
        for task in job['tasks']:
            task_q.put(task)

    if 'nodeSelector' in job:
        nsel = job['nodeSelector']
    else:
        nsel = { 'beta.kubernetes.io/os': 'linux',
                 'kubernetes.io/arch': 'amd64'
               # ,'kubernetes.io/hostname': 'swx-snake-01'
                }
    # Start worker processes
    for i in range(NUMBER_OF_PROCESSES):
        api_instance = core_v1_api.CoreV1Api()
        Process(target=worker2,
                args=(api_instance, "{}-{:02d}".format(job['name'], i),
                      job['image'], job.get('ns', 'default'),
                      nsel, task_q, done_q)
                ).start()

    # Tell child processes to stop
    for i in range(NUMBER_OF_PROCESSES):
        task_q.put('STOP')
    # Get and print results
    print('Unordered results:')
    for i in range(j_num):
        print('\t', done_q.get())


def main(args):
    # load job definition from the job file
    with open(args.job, 'r') as fi:
        job = yaml.safe_load(fi)
    config.load_kube_config()
    c = Configuration()
    scheduler(job)


if __name__ == '__main__':
    parser = ArgumentParser(prog='pod_spawn')
    parser.add_argument('-j', dest='job', required=True,
                        help='Input job name')
    args = parser.parse_args()
    main(args)
