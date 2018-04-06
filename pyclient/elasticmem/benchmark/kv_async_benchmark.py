import logging
import time
from multiprocessing import Process, Barrier

from elasticmem import ElasticMemClient


def make_workload(path, off, count, client):
    logging.info("Reading %d ops of workload from %s at offset %d" % (count, path, off))
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()[int(off):int(off + count)]]
        workload = [[getattr(client, "send_" + x[0]), x[1:]] for x in ops]
    logging.info("Read %d ops of workload from %s at offset %d" % (len(workload), path, off))

    return workload


def load_and_run_workload(barrier, workload_path, workload_off, d_host, d_port, l_port, data_path,
                          n_ops, max_async):
    client = ElasticMemClient(d_host, d_port, l_port)
    kv = client.open(data_path)
    workload = make_workload(workload_path, workload_off, n_ops, kv)
    logging.info("[Process] Loaded data for process.")

    barrier.wait()
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
