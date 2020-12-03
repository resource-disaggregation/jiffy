config=$1

cat ~/karma-eval/$config.tenant*.txt | grep -i '\[alt\]' > ~/karma-eval/$config.alt.txt
cat ~/karma-eval/$config.tenant*.txt | grep -i '\[selfish\]' > ~/karma-eval/$config.selfish.txt
cat ~/karma-eval/$config.tenant*.txt | grep -i -e '\[selfish\]' -e '\[alt\]' > ~/karma-eval/$config.all.txt

function print_stats() {
    # avg_lat=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'average latency' | awk '{sum += $3;} END {print sum/NR}')
    lat_sum=$(cat $1 | grep -i 'latency sum' | awk '{sum += $5;} END {print sum}')
    lat_count=$(cat $1 | grep -i 'latency count' | awk '{sum += $5;} END {print sum}')
    jiffy_lat=$(cat $1 | grep -i 'wrote to jiffy' | awk '{sum+=$6} END {print sum/NR}')
    persistent_lat=$(cat $1 | grep -i 'wrote to persistent' | awk '{sum += $7} END {print sum/NR}')
    jiffy_blocks=$(cat $1 | grep -i 'jiffy blocks' | awk '{sum += $5} END {print sum}')
    persistent_blocks=$(cat $1 | grep -i 'persistent blocks' | awk '{sum += $5} END {print sum}')

    echo "Average latency: " $(echo - | awk -v sum=$lat_sum -v count=$lat_count '{print sum/count;}')
    echo "Jiffy latency: " $jiffy_lat
    echo "Persistent latency: " $persistent_lat
    echo "Jiffy blocks: " $jiffy_blocks
    echo "Persistent blocks: " $persistent_blocks
}

echo "All tenants"
print_stats ~/karma-eval/$config.all.txt

echo "Altruistic tenants"
print_stats ~/karma-eval/$config.alt.txt

echo "Selfish tenants"
print_stats ~/karma-eval/$config.selfish.txt


