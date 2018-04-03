import threading

import time

from elasticmem import RemoveMode


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
    def __init__(self, i, workload_path, client, num_ops, max_async=10000):
        super(WorkloadRunner, self).__init__()
        self.id = i
        self.begin = 0.0
        self.end = 0.0
        self.client = client
        self.workload, self.counters = make_workload(workload_path, client)
        self.num_ops = min(num_ops, len(self.workload))
        self.max_async = max_async
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        i = 0
        self.begin = time.clock()
        while i < self.num_ops:
            self.workload[i][0](*self.workload[i][1])
            i += 1
            if i % self.max_async == 0:
                self.client.wait()
        self.end = time.clock()
        self._stop_event.set()

    def wait(self):
        self._stop_event.wait()

    def throughput(self):
        tot_ops = 0.0
        for key, value in self.counters.iteritems():
            tot_ops += float(value)
        return tot_ops / (self.end - self.begin)


def run_async_kv_benchmark(workload_path, client, data_path, num_ops=100000, num_threads=1, max_async=10000):
    create_file = not client.fs.exists(data_path)  # Create the file if it does not exist
    if create_file:
        print "Creating file %s" % data_path
        client.create(data_path, '/tmp')

    benchmark = [WorkloadRunner(i, workload_path, client.open(data_path, False), int(num_ops / num_threads), max_async)
                 for i in range(num_threads)]
    for b in benchmark:
        b.start()
    for b in benchmark:
        b.wait()

    if create_file:  # if we created the file, we should clean up
        print "Removing file %s" % data_path
        client.remove(data_path, RemoveMode.delete)

    return [b.throughput() for b in benchmark]
