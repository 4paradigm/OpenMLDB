# 基于csv db测试 sql执行引擎


## db 文件目录结构

```
db_root_dir
     |
      \db1
        |
         \table1
            |
             \index
            |
             \schema
            |
             \data.csv
```

index 文件内容
```
index_name first_key second_key
```
second_key目前只支持int64_t

schema文件

```
col1 varchar
col2 int64
```

data.csv文件内容

```
col1,col2
xxxx,123
```

## 执行sql

```
../../build/src/vm/csv_db --db_dir=./db_dir --db=db1 --query="select col1, col2, col3  from table1;"  2>/dev/null
run select col1, col2, col3  from table1;
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| str  | stt  | 11   |
| ss   | 3    | 12   |
| ff   | g    | 13   |
| ff   | g    | 14   |
| ff   | g    | 15   |
+------+------+------+

5 row in set
```

