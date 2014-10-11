#!/usr/bin/env python3
from expyrimenter import SSH, Executor


# Suppose we have a cluster with hostnames vm0, vm1 and vm2
# (and their clock are synchronized)
cluster = ['hdpd%d' % i for i in range(0, 3)]

# Let's run the command below in all VMs:
cmd = 'echo "$(date +%S) Hello by $(hostname)"'

# Blocking version
print('Serial execution\n================')
for vm in cluster:
    output = SSH(vm, cmd, stdout=True).run()
    print(output)

# Create a pool for parallel execution
pool = Executor()

# Non-blocking version
print('\nParallel execution\n==================')
for vm in cluster:
    ssh = SSH(vm, cmd, stdout=True, stderr=False)
    pool.run(ssh)

# Block until all parallel calls are done
# and then print the results
pool.wait()
for result in pool.results:
    print(result)
