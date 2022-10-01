# Data Import Quickstart
 
There are 2 versions of OpenMLDB: the standalone version and cluster version.
- For standalone version, datasets are all saved in the memory. Only [`LOAD DATA`](../reference/sql/dml/LOAD_DATA_STATEMENT.md) can be used to import data in this mode.
- For cluster version, datasets can be saved offline or online. Offline and online ends don't share the data.

This tutorial will focus on the data import methods of cluster version.

## Data Import Methods of Cluster Version

### 1 Offline Import (`LOAD DATA`)

- OpenMLDB本身不提供离线存储引擎，但需要指定离线存储的地址，即修改taskmanager配置项`offline.data.prefix`，可以是本地目录、hdfs、s3等存储介质。

- **导入方式只有一个**，离线模式下执行[`LOAD DATA`](../reference/sql/dml/LOAD_DATA_STATEMENT.md)。默认为硬拷贝导入。

- 默认情况下，OpenMLDB会将源数据拷贝到`offline.data.prefix`目录中。支持读取的文件格式有csv和parquet。

- `LOAD DATA`还支持软链接模式，通过option`deep_copy=false`来配置。软链接导入时，只会将源数据地址写入OpenMLDB表信息中，不会做硬拷贝。也支持csv和parquet两种文件格式。


```{note}
表的离线数据地址如果是软链接，OpenMLDB 不支持追加（append）数据到该表离线存储中，因为我们无权修**软链接目录**下的数据。OpenMLDB 仅支持覆盖（overwrite）该表的离线数据地址。如果覆盖了，原软链接目录下的数据也不会被删除，仅仅是OpenMLDB中丢弃了该原始目录。
If the offline path of the table is soft link, OpenMLDB doesn't support appending data to the table as it doesn't have write access to the files in the **soft link path**.
```

### 2 Online Import

The online modes of OpenMLDB cluster version provide online storage engine (saved in memory). Only **hard copy** can be used in online import.

#### 2.1 `LOAD DATA`

[`LOAD DATA` command](../reference/sql/dml/LOAD_DATA_STATEMENT.md) can be used in **Online Request** and **Online Preview** mode to load `csv` files and `parquet` files.

#### 2.2 Stream

Data can be loaded from `pulsar` as well, see [Pulsar Connector](../use_case/pulsar_connector_demo.md) for detail.

## Note

The [openmldb-import tool](../tutorial/data_import.md) can be used for bulk load, importing the data quickly into the standalone or the online storage of cluster version.

The bulk load tool is still in development. There are some restrictions for usage:
1. Only `csv` files can be loaded.
2. The tool is supported only on a single machine. The requirement for the memory of the single machine is high and maybe the memory should be larger than the size of the data to be imported.

