# Iceberg

## Introduction

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink, Hive and Impala using a high-performance table format that works just like a SQL table. OpenMLDB supports the use of Iceberg as an offline storage engine for importing data and exporting feature computation data.

## Usage

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

```{warning}
The Hive catalog refers to the storage of Iceberg metadata in Hive. This catalog can only read Iceberg tables and cannot read tables in other formats in Hive. It does not have the full capabilities of Hive.
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

After any successful configuration, Iceberg tables can be accessed using the format `<catalog_name>.<db_name>.<table_name>`. If you don't want to use `<catalog_name>`, you can set `spark.sql.catalog.default=<catalog_name>` in the configuration. Alternatively, you can add `spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog` and `spark.sql.catalog.spark_catalog.type=hive` to merge the Iceberg catalog into the Spark catalog (non-Iceberg tables will still exist in the Spark catalog). This way, Iceberg tables can be accessed using the format `<db_name>.<table_name>`.

#### Hive Session Catalog

If you need to merge Hive's EXTERNAL tables (ACID tables are currently not readable, see [Hive](./hive.md) for details) and Iceberg tables into the same catalog, you can use the Session Catalog mode. Note that this configuration can only be applied to the default Spark catalog spark_catalog and cannot create other catalogs.

```properties
spark.default.conf=spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog;spark.sql.catalog.spark_catalog.type=hive;spark.sql.catalog.spark_catalog.uri=thrift://metastore-host:port
```

For example, if your SQL is written based on Hive, Hive tables and Iceberg tables are only differentiated by the database name and table name. Without a Session Catalog, if you only configure spark.sql.catalogImplementation=hive, you can read Hive tables, but you will not be able to read Iceberg tables (metadata is readable, data is not readable) unless you modify the configuration of the Iceberg table with ALTER TABLE hive_prod.nyc.taxis SET TBLPROPERTIES ('engine.hive.enabled'='true');. If you don't modify the Iceberg configuration and only add a regular Iceberg catalog, you will need to add the catalog name to all Iceberg tables in order to read them in OpenMLDB. However, if you use the Session Catalog, you can read both Hive and Iceberg databases, tables, and their data in the spark_catalog.

### Debug Information

When you import data from Iceberg, you can check the task log to confirm whether the task read the source data.

```log
24/01/30 09:01:05 INFO SharedState: Setting hive.metastore.warehouse.dir ('hdfs://namenode:19000/user/hive/warehouse') to the value of spark.sql.warehouse.dir.
24/01/30 09:01:05 INFO SharedState: Warehouse path is 'hdfs://namenode:19000/user/hive/warehouse'.
...
24/01/30 09:01:06 INFO HiveUtils: Initializing HiveMetastoreConnection version 2.3.9 using Spark classes.
24/01/30 09:01:06 INFO HiveClientImpl: Warehouse location for Hive client (version 2.3.9) is hdfs://namenode:19000/user/hive/warehouse
24/01/30 09:01:06 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
24/01/30 09:01:06 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
24/01/30 09:01:06 INFO HiveMetaStore: 0: Opening raw store with implementation class:org.apache.hadoop.hive.metastore.ObjectStore
24/01/30 09:01:06 INFO ObjectStore: ObjectStore, initialize called
24/01/30 09:01:06 INFO Persistence: Property hive.metastore.integral.jdo.pushdown unknown - will be ignored
24/01/30 09:01:06 INFO Persistence: Property datanucleus.cache.level2 unknown - will be ignored
24/01/30 09:01:07 INFO ObjectStore: Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes="Table,StorageDescriptor,SerDeInfo,Partition,Database,Type,FieldSchema,Order"
24/01/30 09:01:07 INFO MetaStoreDirectSql: Using direct SQL, underlying DB is POSTGRES
24/01/30 09:01:07 INFO ObjectStore: Initialized ObjectStore
24/01/30 09:01:08 INFO HiveMetaStore: Added admin role in metastore
24/01/30 09:01:08 INFO HiveMetaStore: Added public role in metastore
24/01/30 09:01:08 INFO HiveMetaStore: No user is added in admin role, since config is empty
24/01/30 09:01:08 INFO HiveMetaStore: 0: get_database: default
```

When exporting to Iceberg, you can check the task log for information similar to the following:

```log
24/01/30 09:57:29 INFO AtomicReplaceTableAsSelectExec: Start processing data source write support: IcebergBatchWrite(table=nyc.taxis_out, format=PARQUET). The input RDD has 1 partitions.
...
24/01/30 09:57:31 INFO AtomicReplaceTableAsSelectExec: Data source write support IcebergBatchWrite(table=nyc.taxis_out, format=PARQUET) committed.
...
24/01/30 09:57:31 INFO HiveTableOperations: Committed to table hive_prod.nyc.taxis_out with the new metadata location hdfs://namenode:19000/user/hive/iceberg_storage/nyc.db/taxis_out/metadata/00001-038d8b81-04a6-4a19-bb83-275eb4664937.metadata.json
24/01/30 09:57:31 INFO BaseMetastoreTableOperations: Successfully committed to table hive_prod.nyc.taxis_out in 224 ms
```

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

Exporting data to Iceberg sources is facilitated through the API [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md), which employs a distinct URI format, `iceberg://[catalog].[db].table`, to seamlessly transfer data to the Iceberg data warehouse. Here are some key considerations:

- If you omit specifying Iceberg database name, the default database used in Iceberg will be `default`.
- When Iceberg database name is explicitly provided, it's imperative that the database already exists. Currently, the system does not support the automatic creation of non-existent databases.
- In the event that the designated Iceberg table name is absent, the system will automatically generate a table with the corresponding name within the Hive environment.
- The `OPTIONS` parameter exclusively takes effect within the export mode of `mode`. Other parameters do not exert any influence.

For example:

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'iceberg://hive_prod.db1.t1';
```
