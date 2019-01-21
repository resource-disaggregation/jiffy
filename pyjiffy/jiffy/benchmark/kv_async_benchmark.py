from __future__ import print_function

import logging
import time
from multiprocessing import Process, Barrier

from jiffy import JiffyClient


class Pipeline(object):
    def __init__(self, client, op_name):
        self.client = client
        self.client_op = getattr(self.client, "multi_{}".format(op_name))
        self.args = []
        setattr(self, op_name, self.op)

    def op(self, *args):
        self.args.extend(args)

    def execute(self):
        self.client_op(self.args)


def make_workload(path, off, count, client):
    logging.info("Reading %d ops of workload from %s at offset %d" % (count, path, off))
    pipelines = {"get": Pipeline(client, "get"),
                 "put": Pipeline(client, "put"),
                 "update": Pipeline(client, "update"),
                 "remove": Pipeline(client, "remove")}

    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()[int(off):int(off + count)]]
        workload = [[getattr(pipelines[x[0]], x[0]), x[1:]] for x in ops]
    logging.info("Read %d ops of workload from %s at offset %d" % (len(workload), path, off))

    return pipelines, workload


def load_and_run_workload(barrier, workload_path, workload_off, d_host, d_port, l_port, data_path,
                          n_ops, max_async):
    client = JiffyClient(d_host, d_port, l_port)
    kv = client.open(data_path)
    pipelines, workload = make_workload(workload_path, workload_off, n_ops, kv)
    logging.info("[Process] Loaded data for process.")

    barrier.wait()
    logging.info("[Process] Starting benchmark...")

    ops = 0
    begin = time.time()
    while ops < len(workload):
        workload[ops][0](*workload[ops][1])
        ops += 1
        if ops % max_async == 0:
            for k in pipelines:
                pipelines[k].execute()

    end = time.time()

    print(float(ops) / (end - begin))


def run_async_kv_benchmark(d_host, d_port, l_port, data_path, workload_path, workload_off=0, n_ops=100000, n_procs=1,
                           max_async=10000):
    barrier = Barrier(n_procs)
    benchmark = [Process(target=load_and_run_workload,
                         args=(barrier, workload_path, workload_off + i * (n_ops / n_procs), d_host, d_port, l_port,
                               data_path, int(n_ops / n_procs), max_async,))
                 for i in range(n_procs)]

    for b in benchmark:
        b.start()

    for b in benchmark:
        b.join()

    logging.info("[Master] Benchmark complete.")
