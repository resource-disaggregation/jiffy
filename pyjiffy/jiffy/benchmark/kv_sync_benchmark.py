from __future__ import print_function

import logging
import time
from multiprocessing import Process, Barrier

from jiffy import JiffyClient


def make_workload(path, off, count, client):
    logging.info("Reading %d ops of workload from %s at offset %d" % (count, path, off))
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()[int(off):int(off + count)]]
        workload = [[getattr(client, x[0]), x[1:]] for x in ops]

    logging.info("Read %d ops of workload from %s at offset %d" % (len(workload), path, off))
    return workload


def load_and_run_workload(barrier, workload_path, workload_off, d_host, d_port, l_port, data_path, n_ops):
    client = JiffyClient(d_host, d_port, l_port)
    kv = client.open(data_path)
    workload = make_workload(workload_path, workload_off, n_ops, kv)
    logging.info("[Process] Loaded data for process.")

    barrier.wait()
    logging.info("[Process] Starting benchmark...")

    ops = 0
    begin = time.time()
    while ops < len(workload):
        workload[ops][0](*workload[ops][1])
        ops += 1
    end = time.time()

    print(float(ops) / (end - begin))


def run_sync_kv_throughput_benchmark(d_host, d_port, l_port, data_path, workload_path, workload_off=0, n_ops=100000,
                                     n_procs=1):
    barrier = Barrier(n_procs)
    benchmark = [Process(target=load_and_run_workload,
                         args=(barrier, workload_path, workload_off + i * (n_ops / n_procs), d_host,
                               d_port, l_port, data_path, int(n_ops / n_procs),))
                 for i in range(n_procs)]

    for b in benchmark:
        b.start()

    for b in benchmark:
        b.join()

    logging.info("[Master] Benchmark complete.")


def run_sync_kv_latency_benchmark(d_host, d_port, l_port, data_path, workload_path, workload_off=0, n_ops=100000):
    client = JiffyClient(d_host, d_port, l_port)
    kv = client.open(data_path)
    workload = make_workload(workload_path, workload_off, n_ops, kv)

    ops = 0
    while ops < len(workload):
        begin = time.time()
        workload[ops][0](*workload[ops][1])
        tot = time.time() - begin
        print("%f" % (tot * 1e6))
        ops += 1
