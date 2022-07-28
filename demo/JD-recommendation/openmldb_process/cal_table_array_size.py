import pandas as pd
import sys

train_data = pd.read_parquet("/home/gtest/work/openmldb/new_process/out/train/train.parquet")
val_data = pd.read_parquet("/home/gtest/work/openmldb/new_process/out/val/val.parquet")
test_data = pd.read_parquet("/home/gtest/work/openmldb/new_process/out/test/test.parquet")

print(train_data['Label'].size)
print(val_data['Label'].size)
print(test_data['Label'].size)

total = train_data.append(val_data)
total = total.append(test_data)
del total['Label']
table_size = total.apply(lambda x: x.nunique(), axis = 0)

print("table size array: ")
print(*table_size.array, sep=',')
