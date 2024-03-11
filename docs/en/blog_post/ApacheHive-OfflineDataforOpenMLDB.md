
# Apache Hive — Offline Data for OpenMLDB

![](https://cdn-images-1.medium.com/max/3336/1*1JN5tOgSx1-mKihnRxlXXA.png)

The [Apache Hive](https://hive.apache.org/)™ is a distributed, fault-tolerant data warehouse system that enables analytics at a massive scale and facilitates reading, writing, and managing petabytes of data residing in distributed storage using SQL. OpenMLDB extends its capabilities by offering seamless import and export functionalities for Hive as a data warehousing solution. While Hive is primarily used as an offline data source, it can also function as a data source for online data ingestion during the initialization phase of online engines.

Note that currently, only reading and writing to non-ACID tables (EXTERNAL tables) in Hive is supported. ACID tables (Full ACID or insert-only tables, i.e., MANAGED tables) are not supported at the moment.

## OpenMLDB Deployment

You can refer to the official documentation for [deployment](https://openmldb.ai/docs/en/main/deploy/install_deploy.html). An easier way is to deploy with an official docker image, as described in [Quickstart](https://openmldb.ai/docs/en/main/quickstart/openmldb_quickstart.html).

In addition, you will also need Spark, please refer to [OpenMLDB Spark Distribution](https://openmldb.ai/docs/en/main/tutorial/openmldbspark_distribution.html).

## Hive-OpenMLDB Integration

### Installation

For users employing [OpenMLDB Spark Distribution Version](https://openmldb.ai/docs/en/main/tutorial/openmldbspark_distribution.html), specifically v0.6.7 and newer iterations, the essential Hive dependencies are already integrated.

However, if you are working with an alternative Spark distribution, you can follow these steps for installation.

* Execute the following command in Spark to compile Hive dependencies
```

    ./build/mvn -Pyarn -Phive -Phive-thriftserver -DskipTests clean package
```    

* After successfully executed, the dependent package is located in the directory `assembly/target/scala-xx/jars`

* Add all dependent packages to Spark’s class path.

### Configuration

At present, OpenMLDB exclusively supports utilizing metastore services for establishing connections to Hive. You can adopt either of the two provided configuration methods to access the Hive data source. To set up a simple HIVE environment, configuring `hive.metastore.uris` will suffice. However, in the production environment when HIVE configurations are required, configurations through `hive-site.xml` is recommended.

* Using the `spark.conf` Approach: You can set up `spark.hadoop.hive.metastore.uris` within the Spark configuration. This can be accomplished in two ways:

a. taskmanager.properties: Include `spark.hadoop.hive.metastore.uris=thrift://...` within the `spark.default.conf` configuration item, followed by restarting the taskmanager.

b. CLI: Integrate this configuration directive into ini conf and use `--spark_conf` when start CLI. Please refer to [Client Spark Configuration](https://openmldb.ai/docs/en/main/reference/client_config/client_spark_config.html).

* `hive-site.xml`: You can configure `hive.metastore.uris` within the `hive-site.xml` file. Place this configuration file within the `conf/` directory of the Spark home. If the `HADOOP_CONF_DIR` environment variable is already set, you can also position the configuration file there. For instance:

```
    <configuration>
      <property>
        <name>hive.metastore.uris</name>
         <!--Make sure that <value> points to the Hive Metastore URI in your cluster -->
         <value>thrift://localhost:9083</value>
         <description>URI for client to contact metastore server</description>
      </property>
    </configuration>
```

Apart from configuring the Hive connection, it is crucial to provide the necessary permissions to the initial users (both OS users and groups) of the TaskManager for Read/Write operations within Hive. Additionally, Read/Write/Execute permissions should be granted to the HDFS path associated with the Hive table.

### Check

Verify whether the task is connected to the appropriate Hive cluster by examining the task log. Here’s how you can proceed:

* `INFO HiveConf`: indicates the Hive configuration file that was utilized. If you require further information about the loading process, you can review the Spark logs.

* When connecting to the Hive metastore, there should be a log entry similar to `INFO metastore: Trying to connect to metastore with URI`. A successful connection will be denoted by a log entry reading `INFO metastore: Connected to metastore`.

## Usage

### Table Creation with `LIKE`

You can use LIKE syntax to create tables, leveraging existing Hive tables, with identical schemas in OpenMLDB.
```
    CREATE TABLE db1.t1 LIKE HIVE 'hive://hive_db.t1';
    -- SUCCEED
```
### Import Hive Data to OpenMLDB

Importing data from Hive sources is done through the API [LOAD DATA INFILE](https://openmldb.ai/docs/en/main/openmldb_sql/dml/LOAD_DATA_STATEMENT.html). This operation employs a specialized URI format, `hive://[db].table`, to seamlessly import data from Hive.
```
    LOAD DATA INFILE 'hive://db1.t1' INTO TABLE t1 OPTIONS(deep_copy=false);
```
The data loading process also supports using SQL queries to filter specific data from Hive tables. The table name used should be the registered name without the `hive://` prefix.
```
    LOAD DATA INFILE 'hive://db1.t1' INTO TABLE db1.t1 OPTIONS(deep_copy=true, sql='SELECT * FROM db1.t1 where key=\"foo\"')
```
### Export OpenMLDB Data to Hive

Exporting data to Hive sources is done through the API [SELECT INTO](https://openmldb.ai/docs/en/main/openmldb_sql/dql/SELECT_INTO_STATEMENT.html), which employs a distinct URI `hive://[db].table`, to seamlessly transfer data to Hive.
```
    SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'hive://db1.t1';
```
### Summary

This is a brief guide for integration of Hive offline data source with OpenMLDB to best facilitate your application needs. For more details, you can check the official documentation on [Hive integration](https://openmldb.ai/docs/en/main/integration/offline_data_sources/hive.html).

OpenMLDB community has recently released [FeatInsight](https://github.com/4paradigm/FeatInsight), a sophisticated feature store service, leveraging OpenMLDB for efficient feature computation, management, and orchestration. The service is available for trial at [http://152.136.144.33/](http://152.136.144.33/). Contact us for a user ID and password to gain access!


**For more information on OpenMLDB:**
* Official website: [https://openmldb.ai/](https://openmldb.ai/)
* GitHub: [https://github.com/4paradigm/OpenMLDB](https://github.com/4paradigm/OpenMLDB)
* Documentation: [https://openmldb.ai/docs/en/](https://openmldb.ai/docs/en/)
* Join us on [**Slack**](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)!
