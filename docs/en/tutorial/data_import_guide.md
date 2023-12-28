# Quick Data Import

OpenMLDB comes in two versions: Standalone and Cluster. In the standalone version, data is exclusively stored in memory, and data import is limited to the use of the [`LOAD DATA`](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) command. In the cluster version, data can be stored separately on both offline and online platforms, with no sharing of data between the two.

This article focuses on data import methods in the cluster version.

## Data Import in Cluster Version

### Offline Import（ `LOAD DATA`）

- OpenMLDB itself does not provide an offline storage engine, but it requires specifying the address for offline storage. This is done by modifying the taskmanager configuration `offline.data.prefix`, which can point to various storage media such as local directories, HDFS, S3, and others.
- **There is only one data import method available**: Executing [`LOAD DATA`](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) in offline mode. The default method is hard copy import.
- By default, OpenMLDB copies the source data to the `offline.data.prefix` directory. Supported file formats for reading include CSV and Parquet.
- `LOAD DATA` also supports a soft link mode, which can be configured using the `deep_copy=false` option. When importing soft links, only the source data address is recorded in the OpenMLDB table information, and no hard copy is created. This mode also supports two file formats: CSV and Parquet.


```{note}
If the offline data address of the table is a soft link, OpenMLDB does not support appending data to the offline storage of the table. This limitation arises because OpenMLDB lacks the authorization to modify data within the **soft link** directory. OpenMLDB exclusively supports overwriting the offline data address of the table. When overwritten, the data in the original soft link directory will not be deleted; only the soft link will be discarded.
```

### Online Import

The online mode of the OpenMLDB cluster version incorporates a self-built online storage engine, which is memory-based. Consequently, online data import is **only hard copy**.

#### `LOAD DATA`

To perform online data import, execute the [`LOAD DATA` Command](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) in either **Online Preview Mode** or **Online Request Mode**. This process supports reading files in CSV and Parquet formats.

#### Stream Import

OpenMLDB offers support for online data import from sources such as `Pulsar`, `Kafka`, and `RocketMQ`, with details outlined in:

- [Pulsar Connector](../integration/online_datasources/pulsar_connector_demo)
- [Kafka Connector](../integration/online_datasources/kafka_connector_demo)
- [RocketMQ Connector](../integration/online_datasources/rocketmq_connector)

## In Addition

The OpenMLDB [openmldb-import tool](../tutorial/data_import.md) offers a bulk load import method that enables swift data import into online storage for both standalone and clustered versions.

The tool is currently in the development phase and thus comes with several limitations, including:
1. It only supports the import of multiple local CSV files.
2. The tool supports only standalone operation and requires high memory resources, potentially exceeding the "total amount of imported data."
