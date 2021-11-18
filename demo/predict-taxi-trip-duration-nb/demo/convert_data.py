import sys
import time, datetime
i = 0
for line in sys.stdin:
    if i == 0:
        i+=1
        print(line.strip())
        continue
    arr = line.strip().split(",")
    arr[2] = str(int(time.mktime(time.strptime(arr[2], "%Y-%m-%d %H:%M:%S"))) * 1000)
    arr[3] = str(int(time.mktime(time.strptime(arr[3], "%Y-%m-%d %H:%M:%S"))) * 1000)
    print(",".join(arr))

