# Workload driver program
# python3 driver.py 127.0.0.1 9090 9091 92 1 /home/midhul/snowflake_demands_nogaps10.pickle $((4 * 1024)) /home/midhul/nfs/jiffy_dump

import time
import os
import sys
from jiffy import JiffyClient
from multiprocessing import Process, Queue
import pickle
import math
import datetime

# Deterministic mapping from filename to worker
def map_file_to_worker(filename, num_workers):
    return abs(hash(filename)) % num_workers

def worker(q, resq, dir_host, dir_porta, dir_portb, block_size, backing_path):
    # Initialize
    # Connect the directory server with the corresponding port numbers
    client = JiffyClient(dir_host, dir_porta, dir_portb)
    buf = 'a' * block_size
    in_jiffy = {}
    jiffy_create_ts = {}
    lat_sum = 0
    lat_count = 0
    total_block_time = 0
    jiffy_blocks = 0
    persistent_blocks = 0
    print('Worker connected')

    while True:
        task = q.get()
        if task is None:
            resq.put({'lat_sum': lat_sum, 'lat_count': lat_count, 'total_block_time': total_block_time, 'jiffy_blocks': jiffy_blocks, 'persistent_blocks': persistent_blocks})
            print('Worker exiting')
            break
        filename = task['filename']
        if task['op'] == 'put':
            jiffy_write = True
            try:
                f = client.create_file(filename, 'local:/' + backing_path)
                jiffy_create_ts[filename] = datetime.datetime.now()
                start_time = datetime.datetime.now()
                f.write(buf)
                elapsed = datetime.datetime.now() - start_time
                lat_sum += elapsed.total_seconds()
                lat_count += 1
                jiffy_blocks += 1
                print('Wrote to jiffy')
            except Exception as e:
                print('Write to jiffy failed')
                jiffy_write = False
                if filename in jiffy_create_ts:
                    del jiffy_create_ts[filename]

            in_jiffy[filename] = jiffy_write
            
            if not jiffy_write:
                # Write to persistent storage
                f = open(backing_path + filename, 'w')
                start_time = datetime.datetime.now()
                f.write(buf)
                f.flush()
                os.fsync(f.fileno())
                f.close()
                elapsed = datetime.datetime.now() - start_time
                lat_sum += elapsed.total_seconds()
                lat_count += 1
                persistent_blocks += 1
                print('Wrote to persistent storage')

        if task['op'] == 'remove':
            if in_jiffy[filename]:
                try:
                    client.remove(filename)
                    duration_used = datetime.datetime.now() - jiffy_create_ts[filename]
                    total_block_time += duration_used.total_seconds()
                    print('Removed from jiffy')
                except:
                    print('Remove from jiffy failed')
                del jiffy_create_ts[filename]
            
            del in_jiffy[filename]
        
    return

def get_demands(filename, scale_factor, t):
    with open(filename, 'rb') as handle:
        norm_d = pickle.load(handle)

    ret = []
    
    for i in range(len(norm_d[t])):
        ret.append(math.ceil(norm_d[t][i] * scale_factor))

    return ret

if __name__ == "__main__":
    dir_host = sys.argv[1]
    dir_porta = int(sys.argv[2])
    dir_portb = int(sys.argv[3])
    block_size = int(sys.argv[7])
    backing_path = sys.argv[8]
    tenant_id = sys.argv[4]
    para = int(sys.argv[5])
    demands = get_demands(sys.argv[6], 100, tenant_id)
    # demands = [1000, 0, 0, 0, 0]

    if not os.path.exists('%s/%s' % (backing_path, tenant_id)):
        os.makedirs('%s/%s' % (backing_path, tenant_id))

    # Create queues
    queues = []
    for i in range(para):
        queues.append(Queue())

    results = Queue()
    
    # Create workers
    workers = []
    for i in range(para):
        p = Process(target=worker, args=(queues[i], results, dir_host, dir_porta, dir_portb, block_size, backing_path))
        workers.append(p)

    # Start workers
    for i in range(para):
        workers[i].start()
    print('Started workers')

    time_start = time.time()

    cur_files = []
    cur_demand = 0
    file_seq = 0
    for e in range(len(demands)):
        if demands[e] > cur_demand:
            # Create files and write
            for i in range(demands[e] - cur_demand):
                filename = '/%s/block%d.txt' % (tenant_id, file_seq)
                file_seq += 1
                cur_files.append(filename)
                wid = map_file_to_worker(filename, para)
                queues[wid].put({'op': 'put', 'filename': filename})

        elif demands[e] < cur_demand:
            # Remove files
            for i in range(cur_demand - demands[e]):
                filename = cur_files.pop(0)
                wid = map_file_to_worker(filename, para)
                queues[wid].put({'op': 'remove', 'filename': filename})
        
        cur_demand = demands[e]
        assert len(cur_files) == cur_demand
        time.sleep(1)

    for i in range(para):
        queues[i].put(None)

    # Wait for worker to finish
    for i in range(para):
        queues[i].close()
        queues[i].join_thread()
        workers[i].join()

    # Get stats
    lat_sum = 0
    lat_count = 0
    total_block_time = 0
    jiffy_blocks = 0
    persistent_blocks = 0
    for i in range(para):
        res = results.get()
        lat_sum += res['lat_sum']
        lat_count += res['lat_count']
        total_block_time += res['total_block_time']
        jiffy_blocks += res['jiffy_blocks']
        persistent_blocks += res['persistent_blocks']

    print('Average latency: ' + str(float(lat_sum)/lat_count))
    print('Total block time: ' + str(total_block_time))
    print('Jiffy blocks: ' + str(jiffy_blocks))
    print('Persistent blocks: ' + str(persistent_blocks))

    time_end = time.time()
    print("Execution time")
    print(time_end -  time_start)


    
