import pandas as pd
import sys

path = sys.argv[1]

train_data = pd.read_parquet(path+"/train/train.parquet")
val_data = pd.read_parquet(path+"/val/val.parquet")
test_data = pd.read_parquet(path+"/test/test.parquet")

print(train_data['Label'].size)
print(val_data['Label'].size)
print(test_data['Label'].size)

total = train_data.append(val_data)
total = total.append(test_data)
del total['Label']
table_size = total.apply(lambda x: x.nunique(), axis = 0)

print("table size array: ")
print(*table_size.array, sep=',')
