# Workload driver program
# python3 driver.py 127.0.0.1 9090 9091 a 1

import time
import os
import sys
from jiffy import JiffyClient
from multiprocessing import Process, Queue

# Deterministic mapping from filename to worker
def map_file_to_worker(filename, num_workers):
    return abs(hash(filename)) % num_workers

def worker(q, dir_host, dir_porta, dir_portb, block_size, backing_path):
    # Initialize
    # Connect the directory server with the corresponding port numbers
    client = JiffyClient(dir_host, dir_porta, dir_portb)
    buf = 'a' * block_size
    in_jiffy = {}
    print('Worker connected')

    while True:
        task = q.get()
        if task is None:
            print('Worker exiting')
            break
        filename = task['filename']
        if task['op'] == 'put':
            jiffy_write = True
            try:
                f = client.create_file(filename, 'local:/' + backing_path)
                f.write(buf)
                print('Wrote to jiffy')
            except Exception as e:
                print('Write to jiffy failed')
                jiffy_write = False

            in_jiffy[filename] = jiffy_write
            
            if not jiffy_write:
                # Write to persistent storage
                f = open(backing_path + filename, 'w')
                f.write(buf)
                f.flush()
                os.fsync(f.fileno())
                f.close()
                print('Wrote to persistent storage')

        if task['op'] == 'remove':
            if in_jiffy[filename]:
                try:
                    client.remove(filename)
                    print('Removed from jiffy')
                except:
                    print('Remove from jiffy failed')
            
            del in_jiffy[filename]
        
    return

if __name__ == "__main__":
    dir_host = sys.argv[1]
    dir_porta = int(sys.argv[2])
    dir_portb = int(sys.argv[3])
    block_size = 2 * 1024 * 1024
    backing_path = '/home/midhul/jiffy_dump'
    tenant_id = sys.argv[4]
    para = int(sys.argv[5])
    demands = [1000, 0, 0, 0, 0]

    if not os.path.exists('%s/%s' % (backing_path, tenant_id)):
        os.makedirs('%s/%s' % (backing_path, tenant_id))

    # Create queues
    queues = []
    for i in range(para):
        queues.append(Queue())
    
    # Create workers
    workers = []
    for i in range(para):
        p = Process(target=worker, args=(queues[i], dir_host, dir_porta, dir_portb, block_size, backing_path))
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

    time_end = time.time()
    print("Execution time")
    print(time_end -  time_start)


    
