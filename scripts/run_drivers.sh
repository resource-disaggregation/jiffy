# ./run_drivers.sh karma-tenants10-dur15-c100 128.84.155.69 9090 9091 /home/midhul/snowflake_demands_nogaps10_15min.pickle $((128 * 1024)) /home/midhul/nfs/jiffy_dump 1

function cleanup() {
    killall python3;
    echo "Cleaned up";
}

trap cleanup EXIT

cleanup;

config=$1
dir_host=$2
dir_porta=$3
dir_portb=$4
trace_file=$5
block_size=$6
backing_path=$7
num_threads=$8
tenant_ids=('92' '45' '14' '79' '93' '1' '54' '96' '44' '76')

pids=()

for tenant in "${tenant_ids[@]}"
do
    python3 -u driver.py $dir_host $dir_porta $dir_portb $tenant $num_threads $trace_file $block_size $backing_path > ~/karma-eval/$config.tenant$tenant.txt 2>&1 &
    pids+=($!);
    echo "Launched tenant$tenant";
done

for pid in ${pids[*]}; do
    wait $pid
done

echo "Done"
