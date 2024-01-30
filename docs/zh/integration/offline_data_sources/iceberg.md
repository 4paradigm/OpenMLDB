# Iceberg

## 简介

[Apache Iceberg](https://iceberg.apache.org/) 是一个开源的大数据表格格式。Iceberg可以在Spark、Trino、PrestoDB、Flink、Hive和Impala等计算引擎中添加表格，使用高性能的表格格式，就像SQL表格一样。OpenMLDB 支持使用 Iceberg 作为离线存储引擎，导入数据和导出特征计算数据。

## 配置

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

### 调试信息

当你从Iceberg导入数据时，你可以检查任务日志，确认任务是否读取了源数据。

```
INFO ReaderImpl: Reading ORC rows from
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

- 如果不指定数据库名字，则会使用默认数据库名字 `default_db` TODO
- 如果指定数据库名字，则该数据库必须已经存在，目前不支持对于不存在的数据库进行自动创建
- 如果指定的Hive表名不存在，则会在 Hive 内自动创建对应名字的表
- `OPTIONS` 参数只有导出模式`mode`生效，其他参数均不生效

举例：

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'iceberg://hive_prod.db1.t1';
```
