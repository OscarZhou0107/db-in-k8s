import matplotlib.pyplot as plt
import numpy as np

# load the files 
timestamp = np.loadtxt('avg_timestamps.txt')
avg_latency = np.loadtxt('avg_latency.txt')
avg_throughput = np.loadtxt('avg_throughtput.txt')

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10,5))

timestamp -= timestamp[0]
timestamp /= 1000

# Latency graph
ax1.plot(timestamp, avg_latency)
ax1.set_title('Average Latency')
ax1.set_xlabel('Timestamp (second)')
ax1.set_ylabel('Latency (second)')

# Throughtput graph
ax2.plot(timestamp, avg_throughput)
ax2.set_title('Average Throughput')
ax2.set_xlabel('Timestamp (second)')
ax2.set_ylabel('Throughput (queries/sec)')

plt.subplots_adjust(wspace=0.3)
plt.savefig('runtime_latency_throughput.png', dpi=300)