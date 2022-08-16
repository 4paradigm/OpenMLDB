import pandas as pd
import sys

path = sys.argv[1]

train_data = pd.read_parquet(path+"/train/train.parquet")
val_data = pd.read_parquet(path+"/val/val.parquet")
test_data = pd.read_parquet(path+"/test/test.parquet")
total = pd.concat([train_data,val_data],  ignore_index=True)
total = pd.concat([total,test_data],  ignore_index=True)
del total['Label']
table_size = total.apply(lambda x: x.nunique(), axis = 0)

print("table size array: ")
print(*table_size.array, sep=',')
