# Iceberg

## Introduction

[Apache Iceberg](https://iceberg.apache.org/) is a table format that offers a host of features, including schema evolution, partitioning, and metadata management. OpenMLDB supports the use of Iceberg as an offline storage engine for reading and exporting feature computation data

## Configuration

### Installation

For users employing [The OpenMLDB Spark Distribution Version](../../tutorial/openmldbspark_distribution.md), specifically v0.8.5 and newer iterations, the essential Iceberg dependencies are already integrated. If you are working with an alternative Spark distribution or different iceberg version, you can download the corresponding Iceberg dependencies from the [Iceberg release](https://iceberg.apache.org/releases/) and add them to the Spark classpath/jars. For example, if you are using OpenMLDB Spark, you should download `x.x.x Spark 3.2_12 runtime Jar`(x.x.x is iceberg version) and add it to `jars/` in Spark home.

### Configuration

You should add the catalog configuration to the Spark configuration. This can be accomplished in two ways:

- taskmanager.properties(.template): Include iceberg configs within the `spark.default.conf` configuration item, followed by restarting the taskmanager.
- CLI: Integrate this configuration directive into ini conf and use `--spark_conf` when start CLI. Please refer to [Client Spark Configuration](../../reference/client_config/client_spark_config.md).

Iceberg config details can be found in [Iceberg Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/).

For example, set hive catalog in `taskmanager.properties(.template)`:

```properties
spark.default.conf=spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog;spark.sql.catalog.hive_prod.type=hive;spark.sql.catalog.hive_prod.uri=thrift://metastore-host:port
```

### Debug Information



## Data Format

Currently, it only supports the following Hive data format:

| OpenMLDB Data Format | Hive Data Format |
| -------------------- | ---------------- |
| BOOL                 | BOOL             |
| SMALLINT             | SMALLINT         |
| INT                  | INT              |
| BIGINT               | BIGINT           |
| FLOAT                | FLOAT            |
| DOUBLE               | DOUBLE           |
| DATE                 | DATE             |
| TIMESTAMP            | TIMESTAMP        |
| STRING               | STRING           |

## Quickly Create Tables Through the `LIKE` Syntax TODO

We offer the convenience of utilizing the `LIKE` syntax to facilitate the creation of tables with identical schemas in OpenMLDB, leveraging existing Hive tables. This is demonstrated in the example below.


```sql
CREATE TABLE db1.t1 LIKE HIVE 'hive://hive_db.t1';
-- SUCCEED
```

It's worth noting that there are certain known issues associated with using the `LIKE` syntax for creating tables based on Hive shortcuts:

- When employing the default timeout configuration via the command line, the table creation process might exhibit a timeout message despite the execution being successful. The final outcome can be verified by utilizing the `SHOW TABLES` command. If you need to adjust the timeout duration, refer to [Adjusting Configuration](../../openmldb_sql/ddl/SET_STATEMENT.md#offline-commands-configuration-details).
- Should the Hive table contain column constraints (such as `NOT NULL`), these particular constraints won't be incorporated into the newly created table.

## Import Iceberg Data to OpenMLDB

Importing data from Iceberg sources is facilitated through the API [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md). This operation employs a specialized URI format, `hive://[db].table`, to seamlessly import data from Hive. Here are some important considerations:

- Both offline and online engines are capable of importing data from Hive sources.
- The Hive data import feature supports soft connections. This approach minimizes the need for redundant data copies and ensures that OpenMLDB can access Hive's most up-to-date data at any given time. To activate the soft link mechanism for data import, utilize the `deep_copy=false` parameter.
- The `OPTIONS` parameter offers two valid settings: `deep_copy`, `mode` and `sql`.

For example, load data from iceberg configured as hive catalog: 

```sql
LOAD DATA INFILE 'iceberg://hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
-- or
LOAD DATA INFILE 'hive_prod.db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false, format='iceberg');
```

The data loading process also supports using SQL queries to filter specific data from Hive tables. It's important to note that the SQL syntax must comply with SparkSQL standards. The table name used should be the registered name without the `hive://` prefix.

For example:

```sql
LOAD DATA INFILE 'iceberg://hive_prod.db1.t1' INTO TABLE db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM hive_prod.db1.t1 where key=\"foo\"')
```

## Export OpenMLDB Data to Iceberg

Exporting data to Hive sources is facilitated through the API [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md), which employs a distinct URI format, `iceberg://[catalog].[db].table`, to seamlessly transfer data to the Hive data warehouse. Here are some key considerations:

- If you omit specifying a database name, the default database name used will be `default_Db`. TODO?
- When a database name is explicitly provided, it's imperative that the database already exists. Currently, the system does not support the automatic creation of non-existent databases.
- In the event that the designated Hive table name is absent, the system will automatically generate a table with the corresponding name within the Hive environment.
- The `OPTIONS` parameter exclusively takes effect within the export mode of `mode`. Other parameters do not exert any influence.

For example: 

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'iceberg://hive_prod.db1.t1';
```
