# Iceberg

## 简介

[Apache Iceberg](https://iceberg.apache.org/) 是一个开源的大数据表格格式。Iceberg可以在Spark、Trino、PrestoDB、Flink、Hive和Impala等计算引擎中添加表格，使用高性能的表格格式，就像SQL表格一样。OpenMLDB 支持使用 Iceberg 作为离线存储引擎，导入数据和导出特征计算数据。

## 使用

### 安装

[OpenMLDB Spark 发行版](../../tutorial/openmldbspark_distribution.md) v0.8.5 及以上版本均已经包含 Iceberg 1.4.3 依赖。如果你需要与其他iceberg版本或者其他Spark发行版一起使用，你可以从[Iceberg release](https://iceberg.apache.org/releases/)下载对应的Iceberg依赖，并将其添加到Spark的classpath/jars中。例如，如果你使用的是OpenMLDB Spark，你应该下载`x.x.x Spark 3.2_12 runtime Jar`(x.x.x is iceberg version)并将其添加到Spark home的`jars/`中。

### 配置

你需要将catalog配置添加到Spark配置中。有两种方式：

- taskmanager.properties(.template): 在配置项 `spark.default.conf` 中加入Iceberg配置，随后重启taskmanager。
- CLI: 在 ini conf 中加入此配置项，并使用`--spark_conf`启动CLI，参考[客户端Spark配置文件](../../reference/client_config/client_spark_config.md)。

Iceberg配置详情参考[Iceberg Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)。

例如，在`taskmanager.properties(.template)`中设置hive catalog：

```properties
spark.default.conf=spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog;spark.sql.catalog.hive_prod.type=hive;spark.sql.catalog.hive_prod.uri=thrift://metastore-host:port
```

```{warning}
Hive catalog仅仅是指Iceberg元数据存于Hive，此catalog也只能读取Iceberg表，不能读取Hive其他格式的表，并没有完整的Hive能力。
```

如果需要创建iceberg表，还需要配置`spark.sql.catalog.hive_prod.warehouse`。

设置 hadoop catalog：

```properties
spark.default.conf=spark.sql.catalog.hadoop_prod=org.apache.iceberg.hadoop.HadoopCatalog;spark.sql.catalog.hadoop_prod.type=hadoop;spark.sql.catalog.hadoop_prod.warehouse=hdfs://hadoop-namenode:port/warehouse
```

设置 rest catalog：

```properties
spark.default.conf=spark.sql.catalog.rest_prod=org.apache.iceberg.spark.SparkCatalog;spark.sql.catalog.rest_prod.catalog-impl=org.apache.iceberg.rest.RESTCatalog;spark.sql.catalog.rest_prod.uri=http://iceberg-rest:8181/
```

Iceberg catalog的完整配置参考[Iceberg Catalog Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)。

任一配置成功后，均使用`<catalog_name>.<db_name>.<table_name>`的格式访问Iceberg表。如果不想使用`<catalog_name>`，可以在配置中设置`spark.sql.catalog.default=<catalog_name>`。也可添加`spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog`，`spark.sql.catalog.spark_catalog.type=hive`，让iceberg catalog合入spark catalog中（非iceberg表仍然存在于spark catalog中），这样可以使用`<db_name>.<table_name>`的格式访问Iceberg表。

#### Hive Session Catalog

如果你需要将Hive中的EXTERNAL表（ACID表目前不可读，详情见[Hive](./hive.md)）和Iceberg表合入同一个catalog中，可以使用Session Catalog模式。注意，此配置只可以配置Spark的默认catalog `spark_catalog`，不可以新建立其他catalog。

```properties
spark.default.conf=spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog;spark.sql.catalog.spark_catalog.type=hive;spark.sql.catalog.spark_catalog.uri=thrift://metastore-host:port
```

例如，你的SQL是基于Hive编写的，Hive表和Iceberg表实际仅用库名表名区分。在无Session Catalog时，仅配置了`spark.sql.catalogImplementation=hive`，你可以读取Hive表，但你将无法读取Iceberg表（元数据可读，数据不可读），除非修改Iceberg表的配置`ALTER TABLE hive_prod.nyc.taxis SET TBLPROPERTIES ('engine.hive.enabled'='true');`。如果不修改Iceberg配置，只增加前面的某一种普通iceberg catalog，你就需要给所有的Iceberg表加上catalog名，才能在OpenMLDB中读取到Iceberg表。而使用Session Catalog的话，你可以在`spark_catalog`中同时读取到Hive和Iceberg的库表及其数据。

### 调试信息

成功连接Iceberg Hive Catalog后，你可以在日志中看到类似以下的信息：

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

导出到Iceberg时，你可以检查任务日志，应该有类似以下的信息：

```log
24/01/30 09:57:29 INFO AtomicReplaceTableAsSelectExec: Start processing data source write support: IcebergBatchWrite(table=nyc.taxis_out, format=PARQUET). The input RDD has 1 partitions.
...
24/01/30 09:57:31 INFO AtomicReplaceTableAsSelectExec: Data source write support IcebergBatchWrite(table=nyc.taxis_out, format=PARQUET) committed.
...
24/01/30 09:57:31 INFO HiveTableOperations: Committed to table hive_prod.nyc.taxis_out with the new metadata location hdfs://namenode:19000/user/hive/iceberg_storage/nyc.db/taxis_out/metadata/00001-038d8b81-04a6-4a19-bb83-275eb4664937.metadata.json
24/01/30 09:57:31 INFO BaseMetastoreTableOperations: Successfully committed to table hive_prod.nyc.taxis_out in 224 ms
```

## 数据格式

Iceberg schema参考[Iceberg Schema](https://iceberg.apache.org/spec/#schema)。目前，仅支持以下Iceberg数据格式：

| OpenMLDB 数据格式 | Iceberg 数据格式 |
| ----------------- | ---------------- |
| BOOL              | bool             |
| INT               | int              |
| BIGINT            | long             |
| FLOAT             | float            |
| DOUBLE            | double           |
| DATE              | date             |
| TIMESTAMP         | timestamp        |
| STRING            | string           |

## 导入 Iceberg 数据到 OpenMLDB

从 Iceberg 表导入数据，需要使用 [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) 语句。这个语句使用特殊的 URI 格式 `hive://[db].table`，可以无缝地从 Iceberg 导入数据。以下是一些重要的注意事项：

- 离线引擎和在线引擎都可以从 Iceberg 表导入数据。
- 离线导入支持软链接，但是在线导入不支持软链接。使用软链接时，需要在导入OPTIONS中指定 `deep_copy=false`。
- Iceberg 表导入只有三个参数有效： `deep_copy`, `mode` and `sql`。其他格式参数`delimiter`，`quote`等均无效。

例如，通过Iceberg Hive Catalog导入数据：

```sql
LOAD DATA INFILE 'iceberg://hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
-- or
LOAD DATA INFILE 'hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false, format='iceberg');
```

数据导入支持`sql`参数，筛选出表种的特定数据进行导入，注意 SQL 必须符合 SparkSQL 语法，数据表为注册后的表名，不带 `iceberg://` 前缀。

```sql
LOAD DATA INFILE 'iceberg://hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false, sql='select * from t1 where id > 100');
```

## 导出 OpenMLDB 数据到 Iceberg

从 OpenMLDB 导出数据到 Iceberg 表，需要使用 [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md) 语句，这个语句使用特殊的 URI 格式 `iceberg://[db].table`，可以无缝地导出数据到 Iceberg 表。以下是一些重要的注意事项：

- 如果不指定Iceberg数据库名字，则会使用Iceberg默认数据库`default`
- 如果指定Iceberg数据库名字，则该数据库必须已经存在，目前不支持对于不存在的数据库进行自动创建
- 如果指定的Iceberg表名不存在，则会在 Iceberg 内自动创建对应名字的表
- `OPTIONS` 参数只有导出模式`mode`生效，其他参数均不生效

举例：

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'iceberg://hive_prod.db1.t1';
```
