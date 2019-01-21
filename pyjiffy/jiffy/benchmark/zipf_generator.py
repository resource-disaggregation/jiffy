import bisect
import random
import math
from functools import reduce

import numpy


class ZipfGenerator(object):
    def __init__(self, alpha, max_val, n):
        tmp = [1. / (math.pow(float(i), alpha)) for i in range(1, max_val + 1)]
        zeta = reduce(lambda sums, x: sums + [sums[-1] + x], tmp, [0])
        self.zeta_dist = [x / zeta[-1] for x in zeta]
        self.n = n
        self.i = 0

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.i == self.n:
            raise StopIteration
        self.i += 1
        return bisect.bisect(self.zeta_dist, random.random()) - 1


def gen_zipf(alpha, max_val, n):
    tmp = numpy.power(numpy.arange(1, max_val + 1), -alpha)
    zeta = numpy.r_[0.0, numpy.cumsum(tmp)]
    zeta_dist = [x / zeta[-1] for x in zeta]
    v = numpy.searchsorted(zeta_dist, numpy.random.random(n))
    samples = [t - 1 for t in v]
    return samples
