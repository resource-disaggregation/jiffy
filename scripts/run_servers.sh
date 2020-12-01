# ./run_servers.sh test 127.0.0.1 random 2 100 1 $((128*1024)) 4 1000

function cleanup() {
    killall directoryd;
    killall storaged;
    echo "Cleaned up";
}

trap cleanup EXIT


cleanup;

config=$1
host=$2
# set large lease period
alloc=$3
num_tenants=$4
init_credits=$5
num_storaged=$6
block_size=$7
num_blocks=$8
algo_interval=$9

pids=()

# Start directory server
JIFFY_DIRECTORY_HOST=$host JIFFY_LEASE_PERIOD_MS=999999999 ../build/directory/directoryd --alloc $alloc -n $num_tenants --init_credits $init_credits --algo_interval $algo_interval > ~/karma-eval/$config.dir.log 2>&1 &
pids+=($!);
echo "Launched directory server";

sleep 5;

# Start storage servers
base_port=9090
for ((i = 0 ; i < $num_storaged ; i++)); do
    cur_port=$(($base_port + 10*$i))
    cur_blocks=$(($num_blocks/$num_storaged))
    if (( $i < $(($num_blocks%$num_storaged)) )); then
        cur_blocks=$(($cur_blocks+1))
    fi
    JIFFY_DIRECTORY_HOST=$host JIFFY_STORAGE_HOST=$host JIFFY_STORAGE_MGMT_PORT=$(($cur_port+3)) JIFFY_STORAGE_SCALING_PORT=$(($cur_port+4)) JIFFY_STORAGE_SERVICE_PORT=$(($cur_port+5)) JIFFY_STORAGE_NUM_BLOCKS=$cur_blocks JIFFY_STORAGE_NUM_BLOCK_GROUPS=1 JIFFY_BLOCK_CAPACITY=$block_size ../build/storage/storaged > ~/karma-eval/$config.storage$i.log 2>&1 &
    pids+=($!);
    echo "Launched storage server $i";
done

for pid in ${pids[*]}; do
    wait $pid
done


