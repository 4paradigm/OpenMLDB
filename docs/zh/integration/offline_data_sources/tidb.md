# TiDB

## 简介

[TiDB](https://docs.pingcap.com/zh/) 是一款开源分布式关系型数据库，支持水平扩缩容、金融级高可用、实时 HTAP、云原生的分布式数据库、兼容 MySQL 5.7 协议和 MySQL 生态等重要特性。OpenMLDB 支持使用 TiDB 作为离线存储引擎，用于读取和导出特征计算的数据。

## 使用

### 安装

当前版本使用TiSpark来操作TiDB数据库， 需要先下载 TiSpark 3.1.x 的相关依赖（`tispark-assembly-3.2_2.12-3.1.5.jar`、`mysql-connector-java-8.0.29.jar`）。如果TiSpark版本不兼容现有的TiDB版本，可以在[TiSpark文档](https://docs.pingcap.com/zh/tidb/stable/tispark-overview)查找下载对应的TiSpark依赖，然后将其添加到Spark的classpath/jars中。


### 配置

你需要将TiDB配置添加到Spark配置中。有两种方式：

- taskmanager.properties(.template): 在配置项 `spark.default.conf` 中加入TiDB配置，随后重启taskmanager。
- CLI: 在 ini conf 中加入此配置项，并使用`--spark_conf`启动CLI，参考[客户端Spark配置文件](../../reference/client_config/client_spark_config.md)。

TiDB关于TiSpark的配置详情参考[TiSpark Configuration](https://docs.pingcap.com/zh/tidb/stable/tispark-overview#tispark-%E9%85%8D%E7%BD%AE)。

例如，在`taskmanager.properties(.template)`中的配置：

```properties
spark.default.conf=spark.sql.extensions=org.apache.spark.sql.TiExtensions;spark.sql.catalog.tidb_catalog=org.apache.spark.sql.catalyst.catalog.TiCatalog;spark.sql.catalog.tidb_catalog.pd.addresses=127.0.0.1:2379;spark.tispark.pd.addresses=127.0.0.1:2379;spark.sql.tidb.addr=127.0.0.1;spark.sql.tidb.port=4000;spark.sql.tidb.user=root;spark.sql.tidb.password=root;
```

任一配置成功后，均使用`tidb_catalog.<db_name>.<table_name>`的格式访问TiDB表。如果不想添加tidb的catalog名称前缀，可以在配置中设置`spark.sql.catalog.default=tidb_catalog`。这样可以使用`<db_name>.<table_name>`的格式访问TiDB表。

## 数据格式

TiDB schema参考[TiDB Schema](https://docs.pingcap.com/zh/tidb/stable/data-type-overview)。目前，仅支持以下TiDB数据格式：

| OpenMLDB 数据格式 | TiDB 数据格式  |
| ----------------- |------------|
| BOOL              | BOOL       |
| SMALLINT          | SMALLINT   |
| INT               | INT        |
| BIGINT            | BIGINT     |
| FLOAT             | FLOAT      |
| DOUBLE            | DOUBLE     |
| DATE              | DATE       |
| TIMESTAMP         | DATETIME   |
| TIMESTAMP         | TIMESTAMP  |
| STRING            | VARCHAR(M) |

提示：不对称的整型转换会被取值范围影响，请尽量参考以上数据类型进行映射。

## 导入 TiDB 数据到 OpenMLDB

对于 TiDB 数据源的导入是通过 API [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md) 进行支持，通过使用特定的 URI 接口 `tidb://tidb_catalog.[db].[table]` 的格式进行导入 TiDB 内的数据。注意：

- 离线和在线引擎均可以导入 TiDB 数据源
- TiDB 导入支持软连接，可以减少硬拷贝并且保证 OpenMLDB 随时读取到 TiDB 的最新数据。启用软链接方式进行数据导入：使用参数 `deep_copy=false`
- TiDB 在`@@execute_mode='online'`模式下支持参数`skip_cvt`：是否跳过字段类型转换，默认为`false`，如果为`true`则会进行字段类型转换以及严格的schema检查，如果为`false`则没有转换以及schema检查动作，性能更好一些，但可能存在类型溢出等错误，需要人工检查。
- `OPTIONS` 参数仅有 `deep_copy` 、`mode` 、`sql` 和 `skip_cvt` 有效

举例：

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
```

加载数据还支持使用 SQL 语句筛选 TiDB 数据表特定数据，注意 SQL 必须符合 SparkSQL 语法，数据表为注册后的表名，不带 `tidb://` 前缀。

举例：

```sql
LOAD DATA INFILE 'tidb://tidb_catalog.db1.t1' INTO TABLE tidb_catalog.db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM tidb_catalog.db1.t1 where key=\"foo\"')
```

## 导出 OpenMLDB 离线引擎数据到 TiDB

对于 TiDB 数据源的导出是通过 API [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md) 进行支持，通过使用特定的 URI 接口 `tidb://tidb_catalog.[db].[table]` 的格式进行导出到 TiDB 数仓。注意：

- 离线引擎可以支持导出 TiDB 数据源，在线引擎还不支持
- 数据库和数据表必须已经存在，目前不支持对于不存在的数据库或数据表进行自动创建
- `OPTIONS` 参数仅`mode='append'`有效，其他参数`overwrite`、`errorifexists`均无效，这是由于TiSpark当前版本不支持，如果TiSpark后续版本支持可以进行升级兼容。

举例：

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'tidb://tidb_catalog.db1.t1' options(mode='append');
```
