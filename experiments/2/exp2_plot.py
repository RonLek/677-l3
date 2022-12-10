from datetime import datetime
import matplotlib.pyplot as plt

percentage = []
def overselling_percentage(filename):
    none = 0
    total = 0
    with open(filename, 'r') as f:
        for line in f:
            if 'none' in line:
                none += 1
            total += 1

    return none/total
    
filenames = ['1buyer_3sellers.txt', '1buyer_4sellers.txt', '3buyers_1seller.txt']
x = [1/3, 1/4, 3/1]

for filename in filenames:
    percentage.append(overselling_percentage(filename))

print(percentage)

plt.plot(x, percentage)
plt.ylabel('Overselling Percentage')
plt.xlabel('Buyers/Sellers Ratio')
plt.title('Overselling Percentage vs Buyers/Sellers Ratio')
plt.savefig('exp2_buyers_sellers_plot.png')
