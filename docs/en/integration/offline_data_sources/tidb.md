# TiDB

## Introduction

[TiDB](https://docs.pingcap.com/) is an open-source distributed relational database with key features including horizontal scaling, high availability suitable for financial use, real-time HTAP, cloud-native architecture, and compatibility with MySQL 5.7 protocol and ecosystem. OpenMLDB supports the use of TiDB as an offline storage engine for importing data and exporting feature computation data.

## Usage

### Installation

[OpenMLDB Spark Distribution](../../tutorial/openmldbspark_distribution.md) v0.8.5 and later versions utilize the TiSpark tool to interact with TiDB. The current release includes TiSpark 3.1.x dependencies (`tispark-assembly-3.2_2.12-3.1.5.jar`, `mysql-connector-java-8.0.29.jar`). If your TiSpark version doesn't match your TiDB version, refer to the [TiSpark documentation](https://docs.pingcap.com/tidb/stable/tispark-overview) for compatible dependencies to add to Spark's classpath/jars.


### Configuration

You need to add TiDB configurations to Spark configurations in two ways:

- taskmanager.properties(.template): Add TiDB configurations to the `spark.default.conf` property, then restart the taskmanager.
- CLI: Add this configuration to the ini conf and start CLI using `--spark_conf`, refer to [Client Spark Configuration File](../../reference/client_config/client_spark_config.md).

For details on TiDB configurations for TiSpark, refer to [TiSpark Configuration](https://docs.pingcap.com/tidb/stable/tispark-overview#tispark-configurations).

For example, configuration in `taskmanager.properties(.template)`:

```properties
spark.default.conf=spark.sql.extensions=org.apache.spark.sql.TiExtensions;spark.sql.catalog.tidb_catalog=org.apache.spark.sql.catalyst.catalog.TiCatalog;spark.sql.catalog.tidb_catalog.pd.addresses=127.0.0.1:2379;spark.tispark.pd.addresses=127.0.0.1:2379;spark.sql.tidb.addr=127.0.0.1;spark.sql.tidb.port=4000;spark.sql.tidb.user=root;spark.sql.tidb.password=root;
```

Once either configuration is successful, access TiDB tables using the format `tidb_catalog.<db_name>.<table_name>`. If you prefer not to use `tidb_catalog`, you can set `spark.sql.catalog.default=tidb_catalog` in the configuration. This allows accessing TiDB tables using the format `<db_name>.<table_name>`.

## Data Format

TiDB schema reference can be found at [TiDB Schema](https://docs.pingcap.com/tidb/stable/data-type-overview). Currently, only the following TiDB data formats are supported:

| OpenMLDB Data Format | TiDB Data Format |
|----------------------|------------------|
| BOOL                 | BOOL             |
| SMALLINT             | Not supported    |
| INT                  | Not supported    |
| BIGINT               | BIGINT           |
| FLOAT                | FLOAT            |
| DOUBLE               | DOUBLE           |
| DATE                 | DATE             |
| TIMESTAMP            | TIMESTAMP        |
| STRING               | VARCHAR(M)       |

## Importing TiDB Data into OpenMLDB

Importing data from TiDB sources is supported through the `LOAD DATA INFILE` API, using the specific URI interface format `tidb://tidb_catalog.[db].[table]` to import data from TiDB. Note:

- Both offline and online engines can import TiDB data sources.
- TiDB import supports symbolic links, which can reduce hard copying and ensure that OpenMLDB always reads the latest data from TiDB. To enable soft link data import, use the parameter `deep_copy=false`.
- The `OPTIONS` parameter only supports `deep_copy`, `mode`, and `sql`.

For example:

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
```

Data loading also supports using SQL statements to filter specific data from TiDB data tables. Note that the SQL must conform to SparkSQL syntax, and the data table is the registered table name without the `tidb://` prefix.

For example:

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE tidb_catalog.db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM tidb_catalog.db1.t1 where key=\"foo\"')
```

## Exporting OpenMLDB Data to TiDB

Exporting data from OpenMLDB to TiDB sources is supported through the `SELECT INTO` API, using the specific URI interface format `tidb://tidb_catalog.[db].[table]` to export data to the TiDB data warehouse. Note:

- The database and table must already exist. Currently, automatic creation of non-existent databases or tables is not supported.
- Only the export mode `mode` is effective in the `OPTIONS` parameter. Other parameters are not effective, and the current parameter is mandatory.

For example:

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'tidb://tidb_catalog.db1.t1' options(mode='append');
```