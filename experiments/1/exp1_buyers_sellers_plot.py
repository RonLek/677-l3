from datetime import datetime
import matplotlib.pyplot as plt

cache_average = []
cacheless_average = []
traders = [1, 2, 3]

def calculate_average(filename, c):
    with open(c + '/buyer_seller/' + filename, 'r') as f:
        data = f.readlines()
    
    start = datetime.strptime(data[0].split()[1], "%H:%M:%S.%f")
    end = datetime.strptime(data[-1].split()[1], "%H:%M:%S.%f")
    duration = end - start
    return (len(data) * 5 )/ duration.total_seconds()
    
filenames_cache = ['1buyer_2sellers.txt', '1buyer_4sellers.txt', '1buyers_3sellers.txt', '2buyers_3sellers.txt', '4buyers_2sellers.txt']
filenames_cacheless = ['2buyers_1seller.txt', '2buyers_2sellers.txt', '2buyers_3sellers.txt', '3buyers_3sellers.txt', '4buyers_1seller.txt']
x_cache = [1/2, 1/4, 1/3, 2/3, 4/2]
x_cacheless = [2/1, 2/2, 2/3, 3/3, 4/1]

for filename in filenames_cache:
    cache_average.append(calculate_average(filename, 'cache'))

for filename in filenames_cacheless:
    cacheless_average.append(calculate_average(filename, 'cacheless'))

print(cache_average)
print(cacheless_average)

plt.plot(x_cache, cache_average, label='Cache')
plt.plot(x_cacheless, cacheless_average, label='Cacheless')
plt.ylabel('Average Throughput (average goods per second)')
plt.xlabel('Buyers/Sellers Ratio')
plt.title('Average Throughput vs Buyers/Sellers Ratio')
plt.legend(loc = 'upper right')
plt.savefig('exp1_buyers_sellers_plot.png')
