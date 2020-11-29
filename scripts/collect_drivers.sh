config=$1

avg_lat=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'average latency' | awk '{sum += $3;} END {print sum/NR}')
jiffy_lat=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'wrote to jiffy' | awk '{sum+=$4} END {print sum/NR}')
persistent_lat=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'wrote to persistent' | awk '{sum += $5} END {print sum/NR}')
jiffy_blocks=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'jiffy blocks' | awk '{sum += $3} END {print sum}')
persistent_blocks=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'persistent blocks' | awk '{sum += $3} END {print sum}')


echo "Average latency: " $avg_lat
echo "Jiffy latency: " $jiffy_lat
echo "Persistent latency: " $persistent_lat
echo "Jiffy blocks: " $jiffy_blocks
echo "Persistent blocks: " $persistent_blocks