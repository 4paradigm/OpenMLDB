# Hive

## Introduction

[Apache Hive](https://hive.apache.org/) is a widely utilized data warehouse tool that serves as a cornerstone in data management. OpenMLDB extends its capabilities by offering seamless import and export functionalities for Hive as a data warehousing solution. While Hive primarily caters to offline data warehousing needs, it can also function as a pivotal data source for online data ingestion during the initialization phase of online engines.

## Configuration

### Installation

For users employing [The OpenMLDB Spark Distribution Version](../../tutorial/openmldbspark_distribution.md), specifically v0.6.7 and newer iterations, the essential Hive dependencies are already integrated. However, if you are working with an alternative Spark distribution, you can follow these steps for installation.

```{note}
Should you opt not to utilize Hive support and refrain from incorporating Hive dependency packages into your Spark dependencies, it becomes imperative to insert `enable.hive.support=false` within the taskmanager configuration. Failing to do so may lead to errors within the Job execution process due to the unavailability of Hive-related classes.
```

1. Execute the following command in Spark to compile Hive dependencies

```bash
./build/mvn -Pyarn -Phive -Phive-thriftserver -DskipTests clean package
```

2. After successfully executed, the dependent package is located in the directory `assembly/target/scala-xx/jars`

2. Add all dependent packages to Spark's class path.

### Configuration

At present, OpenMLDB exclusively supports utilizing metastore services for establishing connections to Hive. You can adopt either of the two provided configuration methods to access the Hive data source:

- Using the `spark.conf` Approach: You can set up `spark.hadoop.hive.metastore.uris` within the Spark configuration. This can be accomplished in two ways:
  - taskmanager.properties: Include `spark.hadoop.hive.metastore.uris=thrift://...` within the `spark.default.conf` configuration item, followed by restarting the taskmanager.
  - CLI (Command Line Interface): Integrate this configuration directive into the ini configuration and utilize `--spark_conf` while initiating the CLI. [Client Spark Configuration File](../../reference/client_config/client_spark_config.md) can guide this process.
- hive-site.xml: Alternatively, you can configure `hive.metastore.uris` within the `hive-site.xml` file. Place this configuration file within the `conf/` directory of the Spark home. If the `HADOOP_CONF_DIR` environment variable is already set, you can also position the configuration file there. For instance:

```xml
<configuration>
	<property>
		<name>hive.metastore.uris</name>
		<!--Make sure that <value> points to the Hive Metastore URI in your cluster -->
		<value>thrift://localhost:9083</value>
		<description>URI for client to contact metastore server</description>
	</property>
</configuration>
```

Apart from configuring the Hive connection, it is crucial to provide the necessary permissions to the initial users (both OS users and groups) of the TaskManager for create, read, and write operations within Hive. Additionally, Read, Write, and Execute permissions should be granted to the HDFS path associated with the Hive table.

Insufficient permissions might lead to encountering the following error:

```
org.apache.hadoop.security.AccessControlException: Permission denied: user=xx, access=xxx, inode="xxx":xxx:supergroup:drwxr-xr-x
```

In such cases, the error points to a lack of authorization for accessing the HDFS path linked to the Hive table. The remedy involves assigning Read, Write, and Execute permissions to the concerned user to ensure proper access to the HDFS path.

```{seealso}
If you have any inquiries, kindly ascertain the permission management approach implemented within your Hive cluster. For guidance, you can consult the [Permission Management] (https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Authorization#LanguageManualAuthorization-OverviewofAuthorizationModes) documentation to gain an understanding of the various authorization modes available.
```

### Debug Information

Verify whether the task is connected to the appropriate Hive cluster by examining the task log. Here's how you can proceed:

- Check for the entry `INFO HiveConf:` in the log, which will indicate the Hive configuration file that was utilized. If you require further information about the loading process, you can review the Spark logs.
- When connecting to the Hive metastore, there should be a log entry similar to `INFO metastore: Trying to connect to metastore with URI`. A successful connection will be denoted by a log entry reading `INFO metastore: Connected to metastore.`

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

## Quickly Create Tables Through the `LIKE` Syntax

We offer the convenience of utilizing the `LIKE` syntax to facilitate the creation of tables with identical schemas in OpenMLDB, leveraging existing Hive tables. This is demonstrated in the example below.


```sql
CREATE TABLE db1.t1 LIKE HIVE 'hive://hive_db.t1';
-- SUCCEED
```

It's worth noting that there are certain known issues associated with using the `LIKE` syntax for creating tables based on Hive shortcuts:

- When employing the default timeout configuration via the command line, the table creation process might exhibit a timeout message despite the execution being successful. The final outcome can be verified by utilizing the `SHOW TABLES` command. If you need to adjust the timeout duration, refer to [Adjusting Configuration](../../openmldb_sql/ddl/SET_STATEMENT.md#offline-command-configuration-details).
- Should the Hive table contain column constraints (such as `NOT NULL`), these particular constraints won't be incorporated into the newly created table.

## Import Hive Data to OpenMLDB

Importing data from Hive sources is facilitated through the API [`LOAD DATA INFILE`](../../openmldb_sql/dml/LOAD_DATA_STATEMENT.md). This operation employs a specialized URI format, `hive://[db].table`, to seamlessly import data from Hive. Here are some important considerations:

- Both offline and online engines are capable of importing data from Hive sources.
- The Hive data import feature supports soft connections. This approach minimizes the need for redundant data copies and ensures that OpenMLDB can access Hive's most up-to-date data at any given time. To activate the soft link mechanism for data import, utilize the `deep_copy=false` parameter.
- The `OPTIONS` parameter offers two valid settings: `deep_copy` and `mode`.

For example: 

```sql
LOAD DATA INFILE 'hive://db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
```

## Export OpenMLDB Data to Hive

Exporting data to Hive sources is facilitated through the API [`SELECT INTO`](../../openmldb_sql/dql/SELECT_INTO_STATEMENT.md), which employs a distinct URI format, `hive://[db].table`, to seamlessly transfer data to the Hive data warehouse. Here are some key considerations:

- If you omit specifying a database name, the default database name used will be `default_Db`.
- When a database name is explicitly provided, it's imperative that the database already exists. Currently, the system does not support the automatic creation of non-existent databases.
- In the event that the designated Hive table name is absent, the system will automatically generate a table with the corresponding name within the Hive environment.
- The `OPTIONS` parameter exclusively takes effect within the export mode of `mode`. Other parameters do not exert any influence.

For example: 

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'hive://db1.t1';
```

