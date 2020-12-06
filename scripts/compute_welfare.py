# Compute welfare of tenants from directoryd log

# 2020-12-04 23:30:51 INFO jiffy::directory::karma_block_allocator::compute_allocations() Demands: 79 98 64 100 88 2 16 0 90 84 87 100 59 1 47 2 53 0 42 100 61 4 46 2737 51 1 55 648 37 1 80 100 76 1 39 100 56 1908 34 0 50 0 68 2 13 100 18 28 91 207 60 980 11 1 31 73 57 100 3 427 40 25 8 81 92 1 28 0 70 209 6 100 15 100 20 1 44 4 5 33 72 0 32 3 23 82 27 59 78 29 89 42 75 0 69 1 73 24 65 4 83 100 74 100 63 1 97 1 99 1 94 59 95 100 93 100 4 452 2 1 81 42 62 100 45 39 85 100 21 100 33 100 84 45 10 100 14 30 24 79 35 94 22 87 30 50 98 1 41 0 26 37 48 3 52 100 1 310 12 100 49 100 58 3 36 97 7 80 19 100 86 100 9 100 0 139 17 1 82 17 38 179 54 100 77 100 66 100 25 100 29 1 43 13 67 100 71 100 96 140 Allocs: 69 1 94 59 65 4 89 42 75 0 78 29 73 24 83 100 74 100 72 0 63 1 99 1 95 100 97 1 62 100 93 100 27 59 57 100 32 3 23 82 5 33 44 4 6 100 20 1 15 100 70 209 28 0 92 1 8 81 40 25 3 427 31 73 11 1 91 100 60 100 13 100 18 28 21 100 4 452 2 1 81 42 33 100 84 45 10 100 14 30 24 79 35 94 22 87 98 1 41 0 26 37 48 3 52 100 1 310 12 100 7 80 36 97 49 100 58 3 19 100 9 100 86 100 30 50 0 139 17 1 82 17 38 179 54 100 77 100 25 100 66 100 29 1 43 13 67 100 71 100 96 140 45 39 85 100 68 2 50 0 34 0 56 100 39 100 76 1 80 100 37 1 55 100 51 1 46 100 61 4 42 100 53 0 47 2 59 1 87 100 90 84 16 0 88 2 64 100 79 98 Credits: 69 101624 94 103547 65 109228 89 106878 75 106877 78 105320 73 110009 83 106978 74 103902 72 84076 63 109506 99 115006 95 106201 97 105635 62 105091 93 0 27 106435 57 92030 32 108956 23 104607 5 100465 44 106878 6 90186 20 115005 15 100835 70 32970 28 84094 92 113419 8 99990 40 101035 3 49578 31 72150 11 111278 91 0 60 0 13 98070 18 106878 21 95254 4 46870 2 54256 81 106878 33 105881 84 107899 10 93243 14 91868 24 94657 35 105749 22 101083 98 106877 41 99646 26 98731 48 40314 52 91132 1 68453 12 90262 7 93363 36 102285 49 96655 58 106878 19 92448 9 91779 86 92091 30 107005 0 98569 17 32670 82 80541 38 84287 54 104212 77 95851 25 96854 66 104507 29 101737 43 109425 67 100065 71 101057 96 84937 45 94193 85 102081 68 102207 50 106877 34 106564 56 0 39 92072 76 109538 80 96791 37 107706 55 0 51 99645 46 0 61 114686 42 96568 53 91700 47 21532 59 106877 87 102199 90 80750 16 110001 88 106878 64 104332 79 95797 oracle_demands: 69 0 94 0 65 0 89 0 75 0 78 0 73 0 83 0 74 0 72 0 63 0 99 0 95 0 97 0 62 0 93 90 27 0 57 0 32 0 23 0 5 0 44 0 6 0 20 0 15 0 70 209 28 0 92 0 8 0 40 0 3 0 31 0 11 0 91 207 60 980 13 0 18 0 21 0 4 0 2 0 81 0 33 0 84 0 10 0 14 0 24 0 35 0 22 0 98 0 41 0 26 0 48 0 52 0 1 210 12 0 7 0 36 0 49 0 58 0 19 0 9 0 86 0 30 0 0 0 17 0 82 0 38 79 54 0 77 0 25 0 66 0 29 0 43 0 67 0 71 0 96 0 45 0 85 0 68 0 50 0 34 0 56 1908 39 0 76 0 80 0 37 0 55 648 51 0 46 2728 61 0 42 0 53 0 47 0 59 0 87 0 90 0 16 0 88 0 64 0 79 0
# 2020-12-04 23:30:51 INFO jiffy::directory::karma_block_allocator::compute_allocations() Tenant used blocks: 69 0 94 0 65 0 89 0 75 0 78 0 73 0 83 0 74 0 72 0 63 0 99 0 95 0 97 0 62 0 93 100 27 0 57 0 32 0 23 0 5 0 44 0 6 0 20 0 15 0 70 100 28 0 92 0 8 0 40 0 3 0 31 0 11 0 91 100 60 100 13 0 18 0 21 0 4 0 2 0 81 0 33 0 84 0 10 0 14 0 24 0 35 0 22 0 98 0 41 0 26 0 48 0 52 0 1 13 12 0 7 0 36 0 49 0 58 0 19 0 9 0 86 0 30 0 0 0 17 0 82 0 38 100 54 0 77 0 25 0 66 0 29 0 43 0 67 0 71 0 96 0 45 0 85 0 68 0 50 0 34 0 56 100 39 0 76 0 80 0 37 0 55 100 51 0 46 100 61 0 42 0 53 0 47 0 59 0 87 0 90 0 16 0 88 0 64 0 79 0

import sys

log_file = sys.argv[1]
num_tenants = int(sys.argv[2])
num_epochs = int(sys.argv[3])
tenants_file = sys.argv[4]

tenant_subset = []
f = open(tenants_file, 'r')
for line in f:
    if line.strip() == '':
        continue
    tenant_subset.append(line.strip())
f.close()

print('Set of tenants: ' + str(len(tenant_subset)))

blocks_used = {}
oracle_demands = {}

begin = False

f = open(log_file, 'r')
epochs = 0
for line in f:
    if epochs >= num_epochs:
        break
    if 'oracle_demand' in line:
        pos = line.find('oracle_demand')
        elems = line[pos:].strip().split(' ')[1:]
        assert len(elems) >= 2 * num_tenants
        for i in range(num_tenants):
            if not elems[2*i] in oracle_demands:
                oracle_demands[elems[2*i]] = []
            oracle_demands[elems[2*i]].append(int(elems[2*i + 1]))
        begin = True

    elif 'Tenant used blocks' in line:
        if not begin:
            continue
        pos = line.find('Tenant used blocks')
        elems = line[pos:].strip().split(' ')[3:]
        assert len(elems) >= 2 * num_tenants
        for i in range(num_tenants):
            if not elems[2*i] in blocks_used:
                blocks_used[elems[2*i]] = []
            blocks_used[elems[2*i]].append(int(elems[2*i + 1]))
        epochs += 1

f.close()

# welfare = {}
# for t in blocks_used:
#     welfare[t] = 0
#     for i in range(num_epochs):
#         if oracle_demands[t][i] > 0:
#             welfare[t] += min(1.0, float(blocks_used[t][i]) / float(oracle_demands[t][i]))
#         else:
#             welfare[t] += 1.0
#     welfare[t] /= num_epochs

welfare = {}
agg_blocks_used = {}
agg_oracle_demand = {}
for t in blocks_used:
    agg_blocks_used[t] = sum([blocks_used[t][i] for i in range(num_epochs)])
    agg_oracle_demand[t] = sum([oracle_demands[t][i] for i in range(num_epochs)])
    welfare[t] = min(1.0, sum([blocks_used[t][i] for i in range(num_epochs)])/sum([oracle_demands[t][i] for i in range(num_epochs)]))

for t in tenant_subset:
    print(t + ' ' + str(welfare[t]) + ' ' + str(agg_blocks_used[t]) + ' ' + str(agg_oracle_demand[t]))

avg_welfare = sum([welfare[t] for t in tenant_subset]) / len(tenant_subset)
print('Avg welfare: ' + str(avg_welfare))

