from collections import Counter

from elasticmem import ElasticMemClient, RemoveMode, logging
from elasticmem.benchmark.zipf_generator import gen_zipf
from elasticmem.kv import crc


class Distribution:
    uniform = 0
    zipf = 1


def zipf_keys(theta, num_buckets, num_keys):
    bucket_size = int(65536 / num_buckets)
    dist = Counter(gen_zipf(theta, num_buckets, num_keys))
    i = 0
    keys = []
    while len(keys) != num_keys:
        key = crc.crc16(i.to_bytes(8, byteorder='little'))
        bucket_key = int(key / bucket_size)
        if bucket_key in dist:
            dist[bucket_key] -= 1
            keys.append(key)
            if dist[bucket_key] == 0:
                del dist[bucket_key]
        i += 1
    return keys


def run_scale_workload(d_host='127.0.0.1', d_port=9090, l_port=9091, data_path='/data/test', n_ops=100000,
                       value_size=102400, skew=0.0):
    client = ElasticMemClient(d_host, d_port, l_port)
    kv = client.open_or_create(data_path, '/tmp')
    value = bytes(value_size)
    keys = zipf_keys(skew, 512, n_ops)
    logging.info("Generated {} keys".format(len(keys)))
    for key in keys:
        kv.put(key, value)
    for key in keys:
        kv.remove(key)
    client.remove(data_path, RemoveMode.delete)
