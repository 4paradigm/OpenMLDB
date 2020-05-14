### 用例描述
测试窗口多列求和

### 创建测试数据
```bash
sh create_data.sh
```

### 创建sql脚本
```bash
# 100列窗口求和, 窗口尺寸=5
python create_sql_script.py --offset 5 --cols 100 multi_column_sum_100.sql
```

### 运行FeSQL测试
```
fesql-spark --sql multi_column_sum.sql --input "t1=DATA_PATH"
```

### 运行SparkSQL对比测试
```
fesql-spark --spark-sql --sql multi_column_sum_100.sql --input "t1=DATA_PATH"
```
