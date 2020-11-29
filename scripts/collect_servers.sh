config=$1

echo "TODO: Fix util computation"
avg_util=$(cat ~/karma-eval/$config.dir.log | grep -i 'utilization' | head -n $((900*100)) awk '{sum += $6} END {print sum}')

sync_latency=$(cat ~/karma-eval/$config.storage*.log | grep 'dirty block in' | awk '{sum += $10} END {print sum/NR}')
num_syncs=$(cat ~/karma-eval/$config.storage*.log | grep 'dirty block in' | wc -l)

echo "Utilization: " $avg_util
echo "Sync latency: " $sync_latency
echo "Num syncs: " $num_syncs

