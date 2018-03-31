import threading

import time


class Counter:
    def __init__(self):
        self.value = 0

    def __call__(self, result):
        self.value += 1

    def __int__(self):
        return self.value

    def __float__(self):
        return float(self.value)


def make_workload(path, client):
    counters = {"get_async": Counter(),
                "put_async": Counter(),
                "remove_async": Counter(),
                "update_async": Counter()}
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()]
        workload = [[getattr(client, x[0]), ([] if x[0] == "wait" else x[1:] + [counters.get(x[0])])] for x in ops]
    return workload, counters


class WorkloadRunner(threading.Thread):
    def __init__(self, workload, client):
        super(WorkloadRunner, self).__init__()
        self.client = client
        self.workload = workload
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        i = 0
        while i < len(self.workload):
            self.workload[i][0](*self.workload[i][1])
            i += 1
        self.client.wait()
        self._stop_event.set()

    def wait(self):
        self._stop_event.wait()


def run_async_kv_benchmark(path, client):
    workload, counters = make_workload(path, client)
    benchmark = WorkloadRunner(workload, client)
    start = time.clock()
    benchmark.start()
    benchmark.wait()
    end = time.clock()
    # Compute throughput
    elapsed = (end - start)
    tot_ops = 0.0
    for key, value in counters.iteritems():
        tot_ops += float(value)
    throughput = tot_ops / elapsed
    return throughput, counters
