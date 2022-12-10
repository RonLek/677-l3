from datetime import datetime
import matplotlib.pyplot as plt

cache_average = []
cacheless_average = []
peers = [6, 7, 8]

def calculate_average(filename, c):
    with open(c + '/peers/' + filename, 'r') as f:
        data = f.readlines()
    
    start = datetime.strptime(data[0].split()[1], "%H:%M:%S.%f")
    end = datetime.strptime(data[-1].split()[1], "%H:%M:%S.%f")
    duration = end - start
    return (len(data) * 5 )/ duration.total_seconds()
    
filenames = ['6peers.txt', '7peers.txt', '8peers.txt']
for filename in filenames:
    cache_average.append(calculate_average(filename, 'cache'))
    cacheless_average.append(calculate_average(filename, 'cacheless'))

print(cache_average)
print(cacheless_average)

plt.plot(peers, cache_average, label='Cache')
plt.plot(peers, cacheless_average, label='Cacheless')
plt.ylabel('Average Throughput (average goods per second)')
plt.xlabel('Number of Peers')
plt.title('Average Throughput vs Number of Peers')
plt.legend(loc = 'upper right')
plt.savefig('exp1_peers_plot.png')
