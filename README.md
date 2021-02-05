# Python scheduler for Kubernetes

The goal of this project -- provide a framework to run large amount of tests (programs) 
in the Kubernetes cluster as part of Jenkins job
Initially we assumed that Jenkins by itself could schedule 1000x of tasks. But it failed.


## Checkout kubernetes-client

First of all, you need to download, build and install `kubernetes-client`

    git clone git@github.com:kubernetes-client/python.git k8s-python
    cd k8s-python
    git checkout v17.14.0a1
    python3 setup.py build
    sudo python3 setup.py install


## Setup kube config

Please check you have `~/.kube/config` and `kubectl get nodes` works for you

## Run a test job

First, inspect the example job  file like `job0.yaml`

    workers_num: 200
    image: harbor.mellanox.com/swx-storage/x86_64/busybox
    name: pod-spawn-test
    multiply: 1
    nodeSelector:
        beta.kubernetes.io/os: linux
        kubernetes.io/arch: amd64
    tasks:
        - "pwd"
        - "date"
        - "sleep 1; echo OK"
        - "ls -l /bin/*sh"
        - "id"
        - "false"


And try to run it on your machine:

    $ python3 pod_spawn_n.py -j job0.yaml


## Some results

The above test job is used to exstimate overhead of the scheduler and the approach to run each command in a new container.
Having  number of workers set to 100-200 and number of tasks about 6000, I got the whole batch executed in 24 minutes


    $ time python3 pod_spawn_n.py  -j job6000.yaml
    ...
    real    24m23.999s
    user    11m59.474s
    sys     0m27.964s

So, it takes about 0.225-0.244 seconds to spawn a new container (pod), execute very simple command and destroy the pod.


