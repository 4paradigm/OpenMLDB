# 数据快速导入
 
OpenMLDB分为单机版和集群版。单机版中，数据仅保存在内存中，导入方式仅支持使用[`LOAD DATA`](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md)命令。 集群版中，数据可以分别存在离线端和在线端。离线端和在线端数据不共享。

本文着重介绍集群版的数据导入方式。

## 集群版数据导入

### 1 离线导入（ `LOAD DATA`）

- OpenMLDB本身不提供离线存储引擎，但需要指定离线存储的地址，即修改taskmanager配置项`offline.data.prefix`，可以是本地目录、hdfs、s3等存储介质。

- **导入方式只有一个**，离线模式下执行[`LOAD DATA`](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md)。默认为硬拷贝导入。

- 默认情况下，OpenMLDB会将源数据拷贝到`offline.data.prefix`目录中。支持读取的文件格式有csv和parquet。

- `LOAD DATA`还支持软链接模式，通过option`deep_copy=false`来配置。软链接导入时，只会将源数据地址写入OpenMLDB表信息中，不会做硬拷贝。也支持csv和parquet两种文件格式。


```{note}
表的离线数据地址如果是软链接，OpenMLDB 不支持追加（append）数据到该表离线存储中，因为我们无权修**软链接目录**下的数据。OpenMLDB 仅支持覆盖（overwrite）该表的离线数据地址。如果覆盖了，原软链接目录下的数据也不会被删除，仅仅是OpenMLDB中丢弃了该原始目录。
```

### 2 在线导入

OpenMLDB集群版的在线模式提供自建的在线存储引擎（保存于内存中）。因此，在线导入**只有硬拷贝**。

#### 2.1 `LOAD DATA`

在**在线预览模式**和**在线请求模式**下执行 [`LOAD DATA`命令](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md)，支持读取csv和parquet格式的文件。

#### 2.2 流式导入（stream）

OpenMLDB支持从`Pulsar`， `Kafka` 和 `RocketMQ `在线导入数据，详见
- [Pulsar Connector](../use_case/pulsar_connector_demo.md)
- [Kafka Connector](../use_case/kafka_connector_demo.md)
- [RocketMQ Connector](../use_case/rocketmq_connector.md)

## 补充

OpenMLDB的[openmldb-import工具](../tutorial/data_import.md)，提供了bulk load的导入方式，可以将数据快速导入到单机版或集群版的在线存储。

但该工具仍处于开发中，因此，目前有诸多限制。包括：
1. 仅支持导入本地的csv多文件。
1. 仅支持单机运行工具，单机内存要求较高，可能需要大于“导入数据的总量”。

