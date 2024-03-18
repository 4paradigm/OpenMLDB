# TiDB

## 简介

[TiDB](https://docs.pingcap.com/zh/) 是一款开源分布式关系型数据库，支持水平扩缩容、金融级高可用、实时 HTAP、云原生的分布式数据库、兼容 MySQL 5.7 协议和 MySQL 生态等重要特性。OpenMLDB 支持使用 TiDB 作为离线存储引擎，用于读取和导出特征计算的数据。

## 使用

### 安装

[OpenMLDB Spark 发行版](../../tutorial/openmldbspark_distribution.md) v0.8.5 及以上版本使用了TiSpark工具来操作TiDB数据库， 当前版本已包含 TiSpark 3.1.x 依赖（tispark-assembly-3.2_2.12-3.1.5.jarh、mysql-connector-java-8.0.29.jar）。如果TiSpark版本不兼容现有的TiDB版本，你可以从[TiSpark文档](https://docs.pingcap.com/zh/tidb/stable/tispark-overview)查找下载对应的TiSpark依赖，并将其添加到Spark的classpath/jars中。


### 配置

你需要将TiDB配置添加到Spark配置中。有两种方式：

- taskmanager.properties(.template): 在配置项 `spark.default.conf` 中加入TiDB配置，随后重启taskmanager。
- CLI: 在 ini conf 中加入此配置项，并使用`--spark_conf`启动CLI，参考[客户端Spark配置文件](../../reference/client_config/client_spark_config.md)。

TiDB关于TiSpark的配置详情参考[TiSpark Configuration](https://docs.pingcap.com/zh/tidb/stable/tispark-overview#tispark-%E9%85%8D%E7%BD%AE)。

例如，在`taskmanager.properties(.template)`中的配置：

```properties
spark.default.conf=spark.sql.extensions=org.apache.spark.sql.TiExtensions;spark.sql.catalog.tidb_catalog=org.apache.spark.sql.catalyst.catalog.TiCatalog;spark.sql.catalog.tidb_catalog.pd.addresses=127.0.0.1:2379;spark.tispark.pd.addresses=127.0.0.1:2379;spark.sql.tidb.addr=127.0.0.1;spark.sql.tidb.port=4000;spark.sql.tidb.user=root;spark.sql.tidb.password=root;
```

任一配置成功后，均使用`tidb_catalog.<db_name>.<table_name>`的格式访问TiDB表。如果不想使用`tidb_catalog`，可以在配置中设置`spark.sql.catalog.default=tidb_catalog`。这样可以使用`<db_name>.<table_name>`的格式访问TiDB表。

## 数据格式

TiDB schema参考[TiDB Schema](https://docs.pingcap.com/zh/tidb/stable/data-type-overview)。目前，仅支持以下TiDB数据格式：

| OpenMLDB 数据格式 | TiDB 数据格式  |
| ----------------- |------------|
| BOOL              | BOOL       |
| INT               | 暂不支持       |
| BIGINT            | BIGINT     |
| FLOAT             | FLOAT      |
| DOUBLE            | DOUBLE     |
| DATE              | DATE       |
| TIMESTAMP         | TIMESTAMP  |
| STRING            | VARCHAR(M) |

## 导入 TiDB 数据到 OpenMLDB

对于 TiDB 数据源的导入是通过 API [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) 进行支持，通过使用特定的 URI 接口 `tidb://tidb_catalog.[db].[table]` 的格式进行导入 TiDB 内的数据。注意：

- 离线和在线引擎均可以导入 TiDB 数据源
- TiDB 导入支持软连接，可以减少硬拷贝并且保证 OpenMLDB 随时读取到 TiDB 的最新数据。启用软链接方式进行数据导入：使用参数 `deep_copy=false`
- `OPTIONS` 参数仅有 `deep_copy` 、`mode` 和 `sql` 有效

举例：

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
```

加载数据还支持使用 SQL 语句筛选 TiDB 数据表特定数据，注意 SQL 必须符合 SparkSQL 语法，数据表为注册后的表名，不带 `tidb://` 前缀。

举例：

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE tidb_catalog.db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM tidb_catalog.db1.t1 where key=\"foo\"')
```

## 导出 OpenMLDB 数据到 TiDB

对于 TiDB 数据源的导出是通过 API [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md) 进行支持，通过使用特定的 URI 接口 `tidb://tidb_catalog.[db].[table]` 的格式进行导出到 TiDB 数仓。注意：

- 数据库和数据表必须已经存在，目前不支持对于不存在的数据库或数据表进行自动创建
- `OPTIONS` 参数只有导出模式`mode`生效，其他参数均不生效，当前参数为必填项

举例：

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'tidb://tidb_catalog.db1.t1' options(mode='append');
```
