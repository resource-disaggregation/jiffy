from elasticmem import ElasticMemClient, RemoveMode


def run_load(d_host='127.0.0.1', d_port=9090, l_port=9091, data_path='/data/test', n_ops=100000, value_size=102400):
    client = ElasticMemClient(d_host, d_port, l_port)
    kv = client.create(data_path, '/tmp')
    value = bytes(value_size)
    for i in range(n_ops):
        key = i.to_bytes(8, byteorder='little')
        kv.put(key, value)
    client.remove(data_path, RemoveMode.delete)
