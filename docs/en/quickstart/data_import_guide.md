# Data Import Quickstart
 
There are 2 versions of OpenMLDB: the standalone version and cluster version.
- For standalone version, datasets are all stored in the memory. Only [`LOAD DATA`](../reference/sql/dml/LOAD_DATA_STATEMENT.md) can be used to import data in this mode.
- For the cluster version, datasets are stored separately in the offline and online storage engines. Offline and online ends don't share the data.

This tutorial will focus on the data import methods of cluster version.

## Data Import Methods of Cluster Version

### 1 Offline Import (`LOAD DATA`)

- OpenMLDB doesn't have its specialized offline storage engine, but it requires user to specify the offline storage path, that is modifying the configuration option of taskmanager: `offline.data.prefix`. You can use third-party storage engines, like local directory, HDFS, s3 to configure.
- There is only one way to import data offline: using [`LOAD DATA` command](../reference/sql/dml/LOAD_DATA_STATEMENT.md). Hard copy will be adopted as default.
- OpenMLDB will copy the original data to the path of `offline.data.prefix` by default. The files of `csv` and `parquet` format are supported. 
- `LOAD DATA` with a soft link is also supported, you can use the option `deep_copy=false` to configure. Only the storage path of the datasets will be saved in OpenMLDB in a soft link. Both the `csv` and `parquet` files are supported as well.


```{note}
If the offline path of the table is a soft link, OpenMLDB doesn't support appending data to the table as it doesn't have write access to the files in the **soft link path**. You can overwrire the offline path of the table. If the path has been overwritten, the data in the original directory will not be removed, only the directory in the OpenMLDB will change.
```

### 2 Online Import

The [online modes](../tutorial/modes.md) of OpenMLDB cluster version provide online storage engine (stored in memory). Only **hard copy** can be used in online import.

#### 2.1 `LOAD DATA`

[`LOAD DATA` command](../reference/sql/dml/LOAD_DATA_STATEMENT.md) can be used in **Online Request** and **Online Preview** mode to load `csv` files and `parquet` files.

#### 2.2 Stream

Data can be loaded from `Pulsar`, `Kafka` and `RocketMQ ` as well, see the following links for detail.
- [Pulsar Connector](../use_case/pulsar_connector_demo.md)
- [Kafka Connector](../use_case/kafka_connector_demo.md)
- [RocketMQ Connector](../zh/use_case/rocketmq_connector.md)

## Note

The [openmldb-import tool](../tutorial/data_import.md) can be used for bulk load, importing the data quickly into the standalone or the online storage of cluster version.

The bulk load tool is still in development. There are some restrictions for usage:
1. Only `csv` files can be loaded.
2. The tool is supported only on a single machine. The requirement for the memory of the single machine is high and maybe the memory should be larger than the size of the data to be imported.

