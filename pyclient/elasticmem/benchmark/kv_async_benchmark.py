import logging

import time
from multiprocessing import Process, Condition, Value

import sys

from elasticmem import ElasticMemClient


def make_workload(path, off, count, client):
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()[off:(off + count)]]
        workload = [[getattr(client, "send_" + x[0]), x[1:]] for x in ops]

    return workload


def load_and_run_workload(n_load, load_cv, start_cv, workload_path, workload_off, d_host, d_port, l_port, data_path,
                          n_ops, n_procs, max_async):
    client = ElasticMemClient(d_host, d_port, l_port)
    kv = client.open(data_path)
    workload = make_workload(workload_path, workload_off, n_ops, kv)

    with load_cv:
        n_load.value += 1
        logging.info("[Process] Loaded data for process.")
        if n_load.value == n_procs:
            print >> sys.stderr, "[Process] All processes completed loading, notifying master..."
            load_cv.notify()

    with start_cv:
        logging.info("[Process] Waiting for master to start...")
        start_cv.wait()

    logging.info("[Process] Starting benchmark...")

    ops = 0
    begin = time.time()
    op_seqs = []
    while ops < len(workload):
        op_seqs.append(workload[ops][0](*workload[ops][1]))
        ops += 1
        if ops % max_async == 0:
            kv.recv_responses(op_seqs)
            del op_seqs[:]
    kv.recv_responses(op_seqs)
    del op_seqs[:]
    end = time.time()

    print float(ops) / (end - begin)


def run_async_kv_benchmark(d_host, d_port, l_port, data_path, workload_path, workload_off=0, n_ops=100000, n_procs=1,
                           max_async=10000):
    load_cv = Condition()
    start_cv = Condition()
    n_load = Value('i', 0)
    benchmark = [Process(target=load_and_run_workload,
                         args=(n_load, load_cv, start_cv, workload_path, workload_off + i * (n_ops / n_procs), d_host,
                               d_port, l_port, data_path, int(n_ops / n_procs), n_procs, max_async,))
                 for i in range(n_procs)]

    for b in benchmark:
        b.start()

    logging.info("[Master] Waiting for processes to load data...")
    with load_cv:
        load_cv.wait()

    logging.info("[Master] Notifying processes to start...")
    with start_cv:
        start_cv.notify_all()

    for b in benchmark:
        b.join()
    logging.info("[Master] Benchmark complete.")
