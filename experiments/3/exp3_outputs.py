from datetime import datetime

retired = False
sold_before_retire = 0
sold_after_retire = 0
start = ''
end = ''
before_retire_throughput = 0
after_retire_throughput = 0
with open("exp3_outputs.txt") as f:
    for line in f:
        if line.split()[2] == 'retired':
            retired = True
            before_retire_throughput = sold_before_retire / (datetime.strptime(end, "%H:%M:%S.%f") - datetime.strptime(start, "%H:%M:%S.%f")).total_seconds()
            start = ''
            continue

        if not retired:
            sold_before_retire += 1
            if start == '':
                start = line.split()[1]
            end = line.split()[1]

        if retired:
            sold_after_retire += 1
            if start == '':
                start = line.split()[1]
            end = line.split()[1]

after_retire_throughput = sold_after_retire / (datetime.strptime(end, "%H:%M:%S.%f") - datetime.strptime(start, "%H:%M:%S.%f")).total_seconds()
print("Before retire throughput: ", before_retire_throughput)
print("After retire throughput: ", after_retire_throughput)
