# 数据导入快速指南

## 简介
首先，OpenMLDB分为单机版和集群版。

单机版中，数据仅保存在内存中，导入方式仅支持sql `LOAD DATA`。

集群版中，数据分别存在离线端和在线端，数据不共享。

下面着重介绍集群版的数据导入方式。

## 集群版数据导入

### 离线导入 `LOAD DATA`

OpenMLDB本身不提供离线存储引擎，但需要指定离线存储的地址，即taskmanager配置项`offline.data.prefix`，可以是本地目录、hdfs、s3等存储介质。

**导入方式只有一个**，`offline`执行模式下的sql `LOAD DATA`，默认为硬拷贝导入。

默认情况下，OpenMLDB会将源数据拷贝写入到`offline.data.prefix`目录中。支持读取的文件格式为csv和parquet。

`LOAD DATA`还支持软链接模式，通过option`deep_copy=false`来配置。软链接导入时，只会将源数据地址写入OpenMLDB表信息中，不会做硬拷贝。支持csv和parquet两种文件格式。

注意，表的离线数据地址如果是软链接，OpenMLDB不支持追加（append）数据到该表离线存储中，因为我们无权修改“软链接目录”的数据，仅支持覆盖（overwrite）该表的离线数据地址。如果覆盖了，原软链接目录的数据也不会被删除，仅仅是OpenMLDB丢弃了软链接目录。

### 在线导入

OpenMLDB集群的在线端，提供自建的在线存储引擎（保存于内存中）。因此，在线导入**只有硬拷贝**。

#### `LOAD DATA`

在`online`执行模式下执行sql `LOAD DATA`，读取支持文件格式为csv和parquet。

#### stream

在线还支持流式导入，支持从pulsar导入数据到OpenMLDB在线，参考[Pulsar Connector：接入实时数据流](../use_case/pulsar_openmldb_connector_demo.md)。

## 补充

OpenMLDB的[openmldb-import工具](../tutorial/data_import.md)，提供了bulk load的导入方式，可以将数据快速导入到单机版或集群在线存储。

但该工具仍处于开发中，因此，目前有诸多限制。包括：
1. 仅支持导入本地的csv多文件。
1. 仅支持单机运行工具，单机内存要求较高，可能需要大于“导入数据的总量”。

如果满足条件并希望最快速度导入，可以使用该导入工具。
