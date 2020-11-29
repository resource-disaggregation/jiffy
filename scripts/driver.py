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
import queue
import boto3
import uuid

# Deterministic mapping from filename to worker
def map_file_to_worker(filename, num_workers):
    return abs(hash(filename)) % num_workers

# def run_monitor(q, dir_host, dir_porta, dir_portb, tenant_id):
#     client = JiffyClient(dir_host, dir_porta, dir_portb)
#     print('Monitor connected')

#     exit_flag = False
#     cur_jiffy_blocks = 0

#     while True:
#         extra_persistent_blocks = 0
#         while True:
#             try:
#                 stat = q.get_nowait()
#                 if stat is None:
#                     exit_flag = True
#                 elif stat == 'put_jiffy':
#                     cur_jiffy_blocks += 1
#                 elif stat == 'remove_jiffy':
#                     cur_jiffy_blocks -= 1
#                 elif stat == 'put_persistent':
#                     extra_persistent_blocks += 1
#                 # Ignore remove_persistent 
#             except queue.Empty:
#                 break

#         if exit_flag:
#             print('Monitor exiting')
#             break
#         adv_demand = cur_jiffy_blocks + extra_persistent_blocks
#         assert adv_demand >= 0
#         client.fs.add_tags('advertise_demand', {'tenant_id': tenant_id, 'demand': str(adv_demand)})
#         time.sleep(1)


def worker(q, resq, monitor_q, dir_host, dir_porta, dir_portb, block_size, backing_path):
    # Initialize
    # Connect the directory server with the corresponding port numbers
    monitor_q.cancel_join_thread()
    client = JiffyClient(dir_host, dir_porta, dir_portb)
    s3 = boto3.client('s3')
    buf = 'a' * block_size
    in_jiffy = {}
    jiffy_create_ts = {}
    jiffy_fd = {}
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
            # monitor_queue.put('inc_demand')
            jiffy_write = True
            try:
                start_time = datetime.datetime.now()
                f = client.create_file(filename, 'local:/' + backing_path)
                elapsed = datetime.datetime.now() - start_time
                print('Create time: ' + str(elapsed.total_seconds()))
                jiffy_create_ts[filename] = datetime.datetime.now()
                jiffy_fd[filename] = f
                start_time = datetime.datetime.now()
                f.write(buf)
                elapsed = datetime.datetime.now() - start_time
                lat_sum += elapsed.total_seconds()
                lat_count += 1
                jiffy_blocks += 1
                # monitor_queue.put('put_jiffy')
                print('Wrote to jiffy ' + str(elapsed.total_seconds()))
            except Exception as e:
                print('Write to jiffy failed')
                jiffy_write = False
                if filename in jiffy_create_ts:
                    del jiffy_create_ts[filename]
                if filename in jiffy_fd:
                    del jiffy_fd[filename]

            in_jiffy[filename] = jiffy_write
            
            if not jiffy_write:
                # Write to persistent storage
                # monitor_queue.put('put_persistent')
                # f = open(backing_path + filename, 'w')
                start_time = datetime.datetime.now()
                # f.write(buf)
                # f.flush()
                # os.fsync(f.fileno())
                # f.close()
                resp = s3.put_object(Bucket=backing_path, Key=uuid.uuid4().hex + '/' + filename, Body=buf)
                if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception('S3 write failed')
                elapsed = datetime.datetime.now() - start_time
                lat_sum += elapsed.total_seconds()
                lat_count += 1
                persistent_blocks += 1
                print('Wrote to persistent storage ' + str(elapsed.total_seconds()))
            
            monitor_q.put('put_complete')

        if task['op'] == 'remove':
            # monitor_q.put('dec_demand')
            if in_jiffy[filename]:
                # monitor_q.put('remove_jiffy')
                try:
                    jiffy_fd[filename].clear()
                    client.remove(filename)
                    duration_used = datetime.datetime.now() - jiffy_create_ts[filename]
                    total_block_time += duration_used.total_seconds()
                    print('Removed from jiffy')
                except:
                    print('Remove from jiffy failed')
                del jiffy_create_ts[filename]
                del jiffy_fd[filename]
            # else:
            #     monitor_q.put('remove_persistent')
            
            del in_jiffy[filename]
            monitor_q.put('remove_complete')
        
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
    fair_share = 100
    demands = get_demands(sys.argv[6], fair_share, tenant_id) + [0]
    #demands = [1000, 0, 0, 0, 0]

    if not os.path.exists('%s/%s' % (backing_path, tenant_id)):
        os.makedirs('%s/%s' % (backing_path, tenant_id))

    # Create queues
    queues = []
    for i in range(para):
        queues.append(Queue())

    results = Queue()
    monitor_queue = Queue()

    # Create monitor
    # monitor = Process(target=run_monitor, args=(monitor_queue, dir_host, dir_porta, dir_portb, tenant_id))

    # Start monitor
    # monitor.start()
    client = JiffyClient(dir_host, dir_porta, dir_portb)
    print('Monitor connected')
    
    # Create workers
    workers = []
    for i in range(para):
        p = Process(target=worker, args=(queues[i], results, monitor_queue, dir_host, dir_porta, dir_portb, block_size, backing_path))
        workers.append(p)

    # Start workers
    for i in range(para):
        workers[i].start()
    print('Started workers')

    time_start = time.time()

    cur_files = []
    cur_demand = 0
    file_seq = 0
    inflight_puts = 0
    for e in range(len(demands)):
        # Log queue sizes
        total_left_over = 0
        for i in range(para):
            # print('Queue this epoch: ' + str(num_queued))
            # print('Queue size: ' + str(queues[i].qsize()))
            # print('Left over: ' + str(queues[i].qsize() - num_queued))
            total_left_over += queues[i].qsize()
        print('Left over: ' + str(total_left_over))

        # Advertise demand
        # Use previous epoch demand, taking into account carry-over
        while True:
            try:
                stat = monitor_queue.get_nowait()
                if stat == 'put_complete':
                    inflight_puts -= 1
            except queue.Empty:
                break
        
        # adv_demand = cur_demand + inflight_puts
        adv_demand = cur_demand
        assert adv_demand >= 0
        print('Avertising demand: ' + str(adv_demand))
        client.fs.add_tags('advertise_demand', {'tenant_id': tenant_id, 'demand': str(adv_demand)})

        num_queued = 0
        if demands[e] > cur_demand:
            # Create files and write
            for i in range(demands[e] - cur_demand):
                filename = '/%s/block%d.txt' % (tenant_id, file_seq)
                file_seq += 1
                cur_files.append(filename)
                wid = map_file_to_worker(filename, para)
                queues[wid].put({'op': 'put', 'filename': filename})
                num_queued += 1
                inflight_puts += 1

        elif demands[e] < cur_demand:
            # Remove files
            for i in range(cur_demand - demands[e]):
                filename = cur_files.pop(0)
                wid = map_file_to_worker(filename, para)
                queues[wid].put({'op': 'remove', 'filename': filename})
                num_queued += 1
        
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

    # monitor_queue.put(None)
    # Wait for monitor to exit
    monitor_queue.close()
    # monitor_queue.join_thread()
    # monitor.join()

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


    
