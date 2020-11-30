config=$1
num_epochs=$2
num_blocks=$3

# echo "TODO: Fix util computation"

avg_util=$(cat ~/karma-eval/$config.dir.log | grep -i 'epoch used blocks' | head -n $num_epochs | awk -v n=$num_blocks '{sum += $8} END {print (sum/NR)/n}')
sync_latency=$(cat ~/karma-eval/$config.storage*.log | grep 'dirty block in' | awk '{sum += $10} END {print sum/NR}')
num_syncs=$(cat ~/karma-eval/$config.storage*.log | grep 'dirty block in' | wc -l)

echo "Utilization: " $avg_util
echo "Sync latency: " $sync_latency
echo "Num syncs: " $num_syncs

