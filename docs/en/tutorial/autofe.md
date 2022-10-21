# AutoFE

AutoFe support to choose top features from dataset, and generate the SQL for top features. You can use the SQL to be the feature extraction script.

## Usage

```
git clone https://github.com/4paradigm/OpenMLDB.git
cd python/openmldb_autofe
pip install .
openmldb_autofe <yaml_path>
```

## yaml config

More detail example is in 配[AutoFE test yaml](https://github.com/4paradigm/OpenMLDB/tree/main/python/openmldb_autofe/tests/test.yaml)

The required options are shown below：
```
apiserver: 127.0.0.1:9080 # we use apiserver to connect OpenMLDB
db: demo_db # the db name when AutoFE do feature selection
tables:
  - table: t1
    schema: "id string, vendor_id int, ..., trip_duration int" # 表schema
    file_path: file://... # AutoFE feature selection will use the real feature, so we need data

  - table: t2
    ...

main_table: t1 # set it if only one table; set a main table when multi tables
label: trip_duration # the label column in main table

windows:
  - name: w1 # main table time window
    partition_by: vendor_id
    order_by: pickup_datetime
    window_type: rows_range
    start: 1d PRECEDING
    end: CURRENT ROW

  - name: w2 # union time window, UNION only supports the same schema tables now
    union: t2
    partition_by: vendor_id
    order_by: pickup_datetime
    window_type: rows_range
    start: 1d PRECEDING
    end: CURRENT ROW

# offline_feature_path: # write to file:///tmp/autofe_offline_feature if not set. If OpenMLDB cluster is distributed, you should ensure that taskmanager and autofe progress can read the path

topk: 10 # the num of top features we selected
```
