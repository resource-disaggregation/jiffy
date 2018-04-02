import threading

import time


def make_workload(path, client):
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()]
        workload = [[getattr(client, x[0]), x[1:]] for x in ops]

    return workload


class WorkloadRunner(threading.Thread):
    def __init__(self, workload, client):
        super(WorkloadRunner, self).__init__()
        self.client = client
        self.workload = workload
        self.ops = 0
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        while self.ops < len(self.workload):
            self.workload[self.ops][0](*self.workload[self.ops][1])
            self.ops += 1

        self._stop_event.set()

    def wait(self):
        self._stop_event.wait()


def run_sync_kv_benchmark(path, client):
    workload = make_workload(path, client)
    benchmark = WorkloadRunner(workload, client)
    start = time.clock()
    benchmark.start()
    benchmark.wait()
    end = time.clock()
    return float(benchmark.ops) / (end - start)
