import threading

import time


def make_workload(path, client):
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()]
        workload = [[getattr(client, x[0]), x[1:]] for x in ops]

    return workload


class WorkloadRunner(threading.Thread):
    def __init__(self, i, workload_path, client):
        super(WorkloadRunner, self).__init__()
        self.id = i
        self.begin = 0.0
        self.end = 0.0
        self.client = client
        self.workload = make_workload(workload_path, client)
        self.ops = 0
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        self.begin = time.clock()
        while self.ops < len(self.workload):
            self.workload[self.ops][0](*self.workload[self.ops][1])
            self.ops += 1
        self.end = time.clock()
        self._stop_event.set()

    def wait(self):
        self._stop_event.wait()

    def throughput(self):
        return float(self.ops) / (self.end - self.begin)


def run_sync_kv_benchmark(workload_path, client, data_path, num_threads=1):
    benchmark = [WorkloadRunner(i, workload_path, client.open(data_path, False)) for i in range(num_threads)]
    for b in benchmark:
        b.start()
    for b in benchmark:
        b.wait()
    return [b.throughput() for b in benchmark]
