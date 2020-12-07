import random

num_tenants = 100

tenants = [str(i) for i in range(100)]
setname = 'set3'

def write_to_file(filename, arr):
    f = open(filename, 'w')
    first = True
    for x in arr:
        if not first:
            f.write('\n')
        else:
            first = False
        f.write(x)
    f.close()

for alt_frac in [0, 25, 50, 75, 100]:
    alt = tenant_sample = random.sample(tenants, alt_frac)
    selfish = [x for x in tenants if x not in alt]
    write_to_file('../' + setname + '/alt%d-alt.txt' % (alt_frac), alt)
    write_to_file('../' + setname + '/alt%d-selfish.txt' % (alt_frac), selfish)
