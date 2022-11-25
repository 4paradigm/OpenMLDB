# 自动特征工程

我们支持自动特征工程，可以根据源数据自动选择top特征，最终生成特征抽取SQL。你可以直接使用该SQL作为特征抽取脚本。

## 使用方法

```
git clone https://github.com/4paradigm/OpenMLDB.git
cd python/openmldb_autofe
pip install .
openmldb_autofe <yaml_path>
```

## yaml配置

更详细的配置可以参考[AutoFE测试配置](https://github.com/4paradigm/OpenMLDB/tree/main/python/openmldb_autofe/tests/test.yaml)

必需配置如下所示：
```
apiserver: 127.0.0.1:9080 # 我们使用apiserver来连接OpenMLDB
db: demo_db # AutoFE特征选择期间使用的数据库名
tables:
  - table: t1
    schema: "id string, vendor_id int, ..., trip_duration int" # 表schema
    file_path: file://... # AutoFE特征选择需要进行特征抽取，所以需要数据

  - table: t2
    ...

main_table: t1 # 单表时设置为该表，多表时需要指定一张主表
label: trip_duration # 主表的标签列

windows:
  - name: w1 # 主表时间窗口配置
    partition_by: vendor_id
    order_by: pickup_datetime
    window_type: rows_range
    start: 1d PRECEDING
    end: CURRENT ROW

  - name: w2 # 多表时间窗口配置， 目前UNION只支持同schema的表UION
    union: t2
    partition_by: vendor_id
    order_by: pickup_datetime
    window_type: rows_range
    start: 1d PRECEDING
    end: CURRENT ROW

# offline_feature_path: # 可不配置，默认写入file:///tmp/autofe_offline_feature。如果OpenMLDB是分布式部署的，必须保证此路径能够被taskmanager和autofe主程序读写。

topk: 10 # 选择最优的特征个数
```
