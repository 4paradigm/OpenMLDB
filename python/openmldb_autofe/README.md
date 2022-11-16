# OpenMLDB AutoFE

## Install

```
cd python/openmldb_autofe
pip install .
```

## Usage

```
openmldb_autofe <yaml_path>
```

yaml settings hints:
1. apiserver
1. table settings: tables, main_table, label, windows
1. table file_path & offline_feature_path: hdfs is recommended. If you want to use `file://`, the taskmanager must be local, `file_path` & `offline_feature_path` must in the machine which the taskmanager running on.

## Dev
```
export PYTHONNOUSERSITE=1; conda env create -f environment.yml # make env clean
conda activate autofe-dev
python -m site # to check, won't have `.local` libs
python autofe/autofe.py tests/test.yaml
```

## Guide

OpenMLDB AutoFE steps:
1. generate time features, translate them to one sql(`OpenMLDBSQLGenerator`)
1. use the sql to query OpenMLDB(include creating tables,loading data, extracting feature), get extracted features, you can check it in `<offline_feature_path>/first_features`
1. use `first_features` to train(`AutoXTrain`), get top k features
1. `OpenMLDBSQLGenerator` traslate the top k features to one sql(`final_sql`)

### Boundary

1. Union table features: `WINDOW UNION` can't support different schemas now. If you need, convert tables to the same schema. e.g.
```
# t1: id, mid, ts, purchase
t1 add col card(all 0)
# t2: mid, card, ts, purchase
t2 add col id(all 0)
t1 & t2 can union partition by mid order by ts
```
1. SQL can't remove unused windows. If user set 2 windows, SQL will have 2 windows even no feature use them.
