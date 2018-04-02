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
    counters = {"get": Counter(),
                "put": Counter(),
                "remove": Counter(),
                "update": Counter()}

    def op_name(base_name):
        return base_name if base_name == "wait" else base_name + "_async"

    def op_args(base_name, args):
        return [] if base_name == "wait" else args + [counters.get(base_name)]

    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()]
        if ops[-1][0] != 'wait':
            ops.append(['wait'])

        workload = [[getattr(client, op_name(x[0])), op_args(x[0], x[1:])] for x in ops]

    return workload, counters


class WorkloadRunner(threading.Thread):
    def __init__(self, workload, client, max_async=10000):
        super(WorkloadRunner, self).__init__()
        self.client = client
        self.workload = workload
        self.max_async = max_async
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        i = 0

        while i < len(self.workload):
            self.workload[i][0](*self.workload[i][1])
            i += 1
            if i % self.max_async == 0:
                self.client.wait()

        self._stop_event.set()

    def wait(self):
        self._stop_event.wait()


def run_async_kv_benchmark(path, client, max_async=10000):
    workload, counters = make_workload(path, client)
    benchmark = WorkloadRunner(workload, client, max_async)
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
