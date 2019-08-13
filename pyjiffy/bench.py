import sys

from jiffy import JiffyClient
from jiffy.benchmark.kv_async_benchmark import run_async_kv_benchmark
from jiffy.benchmark.kv_sync_benchmark import run_sync_kv_latency_benchmark, run_sync_kv_throughput_benchmark


def benchmark_sync():
    d_host = sys.argv[1]
    w_path = sys.argv[2]
    path = "/test"
    em = JiffyClient(d_host)
    em.create_hash_table(path, '/tmp')
    off = 0
    for i in [1, 2, 4, 8, 16, 32]:
        stdout_backup = sys.stdout
        with open('load_' + str(i), 'w') as f:
            sys.stdout = f
            run_sync_kv_throughput_benchmark(d_host, 9090, 9091, path, w_path + '/load', off, 100000, i)
        off += 100000
        sys.stdout = stdout_backup

    stdout_backup = sys.stdout
    with open('load_latency', 'w') as f:
        sys.stdout = f
        run_sync_kv_latency_benchmark(d_host, 9090, 9091, path, w_path + '/load', off, 400000)
    off += 100000
    sys.stdout = stdout_backup

    for i in [1, 2, 4, 8, 16, 32]:
        for workload in ["workloada", "workloadb", "workloadc", "workloadd"]:
            stdout_backup = sys.stdout
            with open(workload + '_' + str(i), 'w') as f:
                sys.stdout = f
                run_sync_kv_throughput_benchmark(d_host, 9090, 9091, path, w_path + '/' + workload, 0, 100000, i)
            sys.stdout = stdout_backup
    em.remove(path)


def benchmark_async():
    d_host = sys.argv[1]
    w_path = sys.argv[2]
    path = "/test"
    em = JiffyClient(d_host)
    em.create_hash_table(path, '/tmp')
    off = 0
    for i in [1, 2, 4, 8, 16, 32]:
        stdout_backup = sys.stdout
        with open('async_load_' + str(i), 'w') as f:
            sys.stdout = f
            run_async_kv_benchmark(d_host, 9090, 9091, path, w_path + '/load', off, 100000, 1, i)
        off += 100000
        sys.stdout = stdout_backup

    for i in [1, 2, 4, 8, 16, 32]:
        for workload in ["workloada", "workloadb", "workloadc", "workloadd"]:
            stdout_backup = sys.stdout
            with open('async_' + workload + '_' + str(i), 'w') as f:
                sys.stdout = f
                run_async_kv_benchmark(d_host, 9090, 9091, path, w_path + '/' + workload, 0, 100000, 1, i)
            sys.stdout = stdout_backup
    em.remove(path)


def benchmark_chain():
    d_host = sys.argv[1]
    w_path = sys.argv[2]
    chain_length = int(sys.argv[3])
    path = "/test"
    em = JiffyClient(d_host)
    em.create_hash_table(path, '/tmp', 1, chain_length)
    off = 0
    print("Write throughput")
    for i in [1, 2, 4, 8, 16, 32]:
        print("i={}".format(i))
        stdout_backup = sys.stdout
        with open('chain_write_' + str(i) + "_" + str(chain_length), 'w') as f:
            sys.stdout = f
            run_sync_kv_throughput_benchmark(d_host, 9090, 9091, path, w_path + '/load', off, 10000, i)
        off += 10000
        sys.stdout = stdout_backup

    print("Write latency")
    stdout_backup = sys.stdout
    with open('chain_write_latency_' + str(chain_length), 'w') as f:
        sys.stdout = f
        run_sync_kv_latency_benchmark(d_host, 9090, 9091, path, w_path + '/load', off, 10000)
    off += 10000
    sys.stdout = stdout_backup

    off = 0
    print("Read throughput")
    for i in [1, 2, 4, 8, 16, 32]:
        print("i={}".format(i))
        stdout_backup = sys.stdout
        with open('chain_read_' + str(i) + '_' + str(chain_length), 'w') as f:
            sys.stdout = f
            run_sync_kv_throughput_benchmark(d_host, 9090, 9091, path, w_path + '/workloadc', off, 10000, i)
        sys.stdout = stdout_backup
    em.remove(path)

    print("Read latency")
    stdout_backup = sys.stdout
    with open('chain_read_latency_' + str(chain_length), 'w') as f:
        sys.stdout = f
        run_sync_kv_latency_benchmark(d_host, 9090, 9091, path, w_path + '/workloadc', off, 10000)
    sys.stdout = stdout_backup


if __name__ == "__main__":
    benchmark_chain()
