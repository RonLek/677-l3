from datetime import datetime
import matplotlib.pyplot as plt

cache_average = []
cacheless_average = []
traders = [1, 2, 3]

def calculate_average(filename, c):
    with open(c + '/traders/' + filename, 'r') as f:
        data = f.readlines()
    
    start = datetime.strptime(data[0].split()[1], "%H:%M:%S.%f")
    end = datetime.strptime(data[-1].split()[1], "%H:%M:%S.%f")
    duration = end - start
    return (len(data) * 5 )/ duration.total_seconds()
    
filenames = ['1trader.txt', '2traders.txt', '3traders.txt']
for filename in filenames:
    cache_average.append(calculate_average(filename, 'cache'))
    cacheless_average.append(calculate_average(filename, 'cacheless'))

print(cache_average)
print(cacheless_average)

plt.plot(traders, cache_average, label='Cache')
plt.plot(traders, cacheless_average, label='Cacheless')
plt.ylabel('Average Throughput (average goods per second)')
plt.xlabel('Number of Traders')
plt.title('Average Throughput vs Number of Traders')
plt.legend(loc = 'upper right')
plt.savefig('exp1_traders_plot.png')

# print("Average response time: ", sum(res)/len(res))
# print("Max response time: ", max(res))
# print("Min response time: ", min(res))