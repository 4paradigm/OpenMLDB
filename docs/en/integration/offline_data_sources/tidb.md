# TiDB

## Introduction

[TiDB](https://docs.pingcap.com/) is an open-source distributed relational database with key features including horizontal scaling, high availability suitable for financial use, real-time HTAP, cloud-native architecture, and compatibility with MySQL 5.7 protocol and ecosystem. OpenMLDB supports the use of TiDB as an offline storage engine for importing data and exporting feature computation data.

## Usage

### Installation

The current version utilizes TiSpark for interacting with the TiDB database. To get started, download the necessary dependencies for TiSpark 3.1.x (`tispark-assembly-3.2_2.12-3.1.5.jar` and `mysql-connector-java-8.0.29.jar`). If the TiSpark version is not compatible with your current TiDB version, refer to the [TiSpark documentation](https://docs.pingcap.com/tidb/stable/tispark-overview) for downloading the corresponding TiSpark dependencies. Then, add them to the Spark classpath/jars.


### Configuration

You need to add TiDB configurations to Spark configurations. There are two ways to do so:

- taskmanager.properties(.template): Add TiDB configurations to the `spark.default.conf` property, then restart the taskmanager.
- CLI: Add this configuration to the ini conf and start CLI using `--spark_conf`, refer to [Client Spark Configuration File](../../reference/client_config/client_spark_config.md).

For details on TiDB configurations for TiSpark, refer to [TiSpark Configuration](https://docs.pingcap.com/tidb/stable/tispark-overview#tispark-configurations).

For example, configuration in `taskmanager.properties(.template)`:

```properties
spark.default.conf=spark.sql.extensions=org.apache.spark.sql.TiExtensions;spark.sql.catalog.tidb_catalog=org.apache.spark.sql.catalyst.catalog.TiCatalog;spark.sql.catalog.tidb_catalog.pd.addresses=127.0.0.1:2379;spark.tispark.pd.addresses=127.0.0.1:2379;spark.sql.tidb.addr=127.0.0.1;spark.sql.tidb.port=4000;spark.sql.tidb.user=root;spark.sql.tidb.password=root;
```

Once either configuration is successful, access TiDB tables using the format `tidb_catalog.<db_name>.<table_name>`. If you do not want to add the catalog name prefix of tidb, you can set `spark.sql.catalog.default=tidb_catalog` in the configuration. This allows accessing TiDB tables using the format `<db_name>.<table_name>`.

## Data Format

TiDB schema reference can be found at [TiDB Schema](https://docs.pingcap.com/tidb/stable/data-type-overview). Currently, only the following TiDB data formats are supported:

| OpenMLDB Data Format | TiDB Data Format |
|----------------------|------------------|
| BOOL                 | BOOL             |
| SMALLINT             | SMALLINT         |
| INT                  | INT              |
| BIGINT               | BIGINT           |
| FLOAT                | FLOAT            |
| DOUBLE               | DOUBLE           |
| DATE                 | DATE             |
| TIMESTAMP            | DATETIME         |
| TIMESTAMP            | TIMESTAMP        |
| STRING               | VARCHAR(M)       |

Tip: Asymmetric integer conversion will be affected by the value range. Please try to refer to the above data types for mapping.

## Importing TiDB Data into OpenMLDB

Importing data from TiDB sources is supported through the [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) API, using the specific URI interface format `tidb://tidb_catalog.[db].[table]` to import data from TiDB. Note:

- Both offline and online engines can import TiDB data sources.
- TiDB import supports symbolic links, which can reduce hard copying and ensure that OpenMLDB always reads the latest data from TiDB. To enable soft link data import, use the parameter `deep_copy=false`.
- TiDB supports parameter `skip_cvt` in `@@execute_mode='online'` mode: whether to skip field type conversion, the default is `false`, if it is `true`, field type conversion and strict schema checking will be performed , if it is `false`, there will be no conversion and schema checking actions, and the performance will be better, but there may be errors such as type overflow, which requires manual inspection.
- The `OPTIONS` parameter only supports `deep_copy`, `mode`, `sql` , and `skip_cvt` .

For example:

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
```

Data loading also supports using SQL statements to filter specific data from TiDB data tables. Note that the SQL must conform to SparkSQL syntax, and the data table is the registered table name without the `tidb://` prefix.

For example:

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE tidb_catalog.db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM tidb_catalog.db1.t1 where key=\"foo\"')
```

## Exporting OpenMLDB Offline Engine Data to TiDB

Exporting data from OpenMLDB to TiDB sources is supported through the [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md) API, using the specific URI interface format `tidb://tidb_catalog.[db].[table]` to export data to the TiDB data warehouse. Note:

- The offline engine can support exporting TiDB data sources, but the online engine does not yet support it.
- The database and table must already exist. Currently, automatic creation of non-existent databases or tables is not supported.
- The `OPTIONS` parameter is only valid for `mode='append'`. Other parameters as `overwrite` and `errorifexists` are invalid. This is because the current version of TiSpark does not support them. If TiSpark supports them in future versions, you can upgrade for compatibility.

For example:

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'tidb://tidb_catalog.db1.t1' options(mode='append');
```
