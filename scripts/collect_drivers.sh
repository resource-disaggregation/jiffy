config=$1
tenants_file=$2

mapfile -t tenant_ids < $tenants_file;

rm ~/karma-eval/$config.dump.txt
for tenant in "${tenant_ids[@]}"
do
    cat ~/karma-eval/$config.tenant$tenant.txt | grep -i -e '\[selfish\]' -e '\[alt\]' -e 'wrote to jiffy' -e 'wrote to persistent' >> ~/karma-eval/$config.dump.txt
done

# cat ~/karma-eval/$config.tenant*.txt | grep -i '\[alt\]' > ~/karma-eval/$config.alt.txt
# cat ~/karma-eval/$config.tenant*.txt | grep -i '\[selfish\]' > ~/karma-eval/$config.selfish.txt
# cat ~/karma-eval/$config.tenant*.txt | grep -i -e '\[selfish\]' -e '\[alt\]' > ~/karma-eval/$config.all.txt

function print_stats() {
    # avg_lat=$(cat ~/karma-eval/$config.tenant*.txt | grep -i 'average latency' | awk '{sum += $3;} END {print sum/NR}')
    lat_sum=$(cat $1 | grep -i 'latency sum' | awk '{sum += $5;} END {print sum}')
    lat_count=$(cat $1 | grep -i 'latency count' | awk '{sum += $5;} END {print sum}')
    jiffy_blocks=$(cat $1 | grep -i 'jiffy blocks' | awk '{sum += $5} END {print sum}')
    persistent_blocks=$(cat $1 | grep -i 'persistent blocks' | awk '{sum += $5} END {print sum}')

    echo "Average latency: " $(echo - | awk -v sum=$lat_sum -v count=$lat_count '{print sum/count;}')
    echo "Jiffy blocks: " $jiffy_blocks
    echo "Persistent blocks: " $persistent_blocks
}

echo "Tenant stats"
echo "-------------"
print_stats ~/karma-eval/$config.dump.txt
jiffy_lat=$(cat ~/karma-eval/$config.dump.txt  | grep -i 'wrote to jiffy' | awk '{sum+=$4} END {print sum/NR}')
persistent_lat=$(cat ~/karma-eval/$config.dump.txt  | grep -i 'wrote to persistent' | awk '{sum += $5} END {print sum/NR}')
echo "Jiffy latency: " $jiffy_lat
echo "Persistent latency: " $persistent_lat

# echo "Altruistic tenants"
# print_stats ~/karma-eval/$config.alt.txt

# echo "Selfish tenants"
# print_stats ~/karma-eval/$config.selfish.txt


