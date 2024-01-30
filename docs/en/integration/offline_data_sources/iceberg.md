# Iceberg

## Introduction

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink, Hive and Impala using a high-performance table format that works just like a SQL table. OpenMLDB supports the use of Iceberg as an offline storage engine for importing data and exporting feature computation data.

## Configuration

### Installation

For users employing [The OpenMLDB Spark Distribution Version](../../tutorial/openmldbspark_distribution.md), specifically v0.8.5 and newer iterations, the essential Iceberg 1.4.3 dependencies are already integrated. If you are working with an alternative Spark distribution or different iceberg version, you can download the corresponding Iceberg dependencies from the [Iceberg release](https://iceberg.apache.org/releases/) and add them to the Spark classpath/jars. For example, if you are using OpenMLDB Spark, you should download `x.x.x Spark 3.2_12 runtime Jar`(x.x.x is iceberg version) and add it to `jars/` in Spark home.

### Configuration

You should add the catalog configuration to the Spark configuration. This can be accomplished in two ways:

- taskmanager.properties(.template): Include iceberg configs within the `spark.default.conf` configuration item, followed by restarting the taskmanager.
- CLI: Integrate this configuration directive into ini conf and use `--spark_conf` when start CLI. Please refer to [Client Spark Configuration](../../reference/client_config/client_spark_config.md).

Iceberg config details can be found in [Iceberg Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/).

For example, set hive catalog in `taskmanager.properties(.template)`:

```properties
spark.default.conf=spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog;spark.sql.catalog.hive_prod.type=hive;spark.sql.catalog.hive_prod.uri=thrift://metastore-host:port
```

If you need to create iceberg tables, you also need to configure `spark.sql.catalog.hive_prod.warehouse`.

Set hadoop catalog:

```properties
spark.default.conf=spark.sql.catalog.hadoop_prod=org.apache.iceberg.hadoop.HadoopCatalog;spark.sql.catalog.hadoop_prod.type=hadoop;spark.sql.catalog.hadoop_prod.warehouse=hdfs://hadoop-namenode:port/warehouse
```

Set rest catalog:

```properties
spark.default.conf=spark.sql.catalog.rest_prod=org.apache.iceberg.spark.SparkCatalog;spark.sql.catalog.rest_prod.catalog-impl=org.apache.iceberg.rest.RESTCatalog;spark.sql.catalog.rest_prod.uri=http://iceberg-rest:8181/
```

The full configuration of the iceberg catalog see [Iceberg Catalog Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/).

### Debug Information

When you import data from Iceberg, you can check the task log to confirm whether the task read the source data.
```
INFO ReaderImpl: Reading ORC rows from
```
TODO

## Data Format

Iceberg schema see [Iceberg Schema](https://iceberg.apache.org/spec/#schema). Currently, it only supports the following Iceberg data format:

| OpenMLDB Data Format | Iceberg Data Format |
| -------------------- | ------------------- |
| BOOL                 | bool                |
| INT                  | int                 |
| BIGINT               | long                |
| FLOAT                | float               |
| DOUBLE               | double              |
| DATE                 | date                |
| TIMESTAMP            | timestamp           |
| STRING               | string              |

## Import Iceberg Data to OpenMLDB

Importing data from Iceberg sources is facilitated through the API [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md). This operation employs a specialized URI format, `hive://[db].table`, to seamlessly import data from Iceberg. Here are some important considerations:

- Both offline and online engines are capable of importing data from Iceberg sources.
- The Iceberg data import feature supports soft connections. This approach minimizes the need for redundant data copies and ensures that OpenMLDB can access Iceberg's most up-to-date data at any given time. To activate the soft link mechanism for data import, utilize the `deep_copy=false` parameter.
- The `OPTIONS` parameter offers three valid settings: `deep_copy`, `mode` and `sql`.

For example, load data from Iceberg configured as hive catalog: 

```sql
LOAD DATA INFILE 'iceberg://hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
-- or
LOAD DATA INFILE 'hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false, format='iceberg');
```

The data loading process also supports using SQL queries to filter specific data from Hive tables. It's important to note that the SQL syntax must comply with SparkSQL standards. The table name used should be the registered name without the `iceberg://` prefix.

For example:

```sql
LOAD DATA INFILE 'iceberg://hive_prod.db1.t1' INTO TABLE db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM hive_prod.db1.t1 where key=\"foo\"')
```

## Export OpenMLDB Data to Iceberg

Exporting data to Hive sources is facilitated through the API [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md), which employs a distinct URI format, `iceberg://[catalog].[db].table`, to seamlessly transfer data to the Iceberg data warehouse. Here are some key considerations:

- If you omit specifying a database name, the default database name used will be `default_Db`. TODO?
- When a database name is explicitly provided, it's imperative that the database already exists. Currently, the system does not support the automatic creation of non-existent databases.
- In the event that the designated Hive table name is absent, the system will automatically generate a table with the corresponding name within the Hive environment.
- The `OPTIONS` parameter exclusively takes effect within the export mode of `mode`. Other parameters do not exert any influence.

For example: 

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'iceberg://hive_prod.db1.t1';
```
