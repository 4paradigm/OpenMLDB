# Getting Started Guide

As OpenMLDB is a distributed system with multiple modes and rich client options, beginners may have many questions or encounter issues during their initial usage. This guide aims to explain how to diagnose and debug problems, and how to provide effective information to technical support when needed.

## Error Diagnosis

During the use of OpenMLDB, besides SQL syntax errors, other error messages may not be straightforward but could be related to the cluster's state. Therefore, error diagnosis should **first confirm the cluster's status**. When encountering an error, use the one-click diagnosis tool first, as it provides a comprehensive and intuitive diagnosis report. If this tool cannot be used, manually execute `SHOW COMPONENTS;` and `SHOW TABLE STATUS LIKE '%';` to provide partial information.

The report will display the status of the cluster components, online tables, and provide instructions on how to fix the issues. Follow the instructions in the report for resolution, details can be found in [One-click Inspect](../maintain/diagnose.md#oneclick-inspect).

```bash
openmldb_tool inspect [-c=0.0.0.0:2181/openmldb]
```

It's important to note that since offline storage is only read during the execution of offline jobs and offline jobs are not a continuous state, one-click diagnosis can only display TaskManager component status and cannot diagnose offline storage or offline job execution errors. For offline job diagnosis, refer to [Offline SQL Execution](#considerations-for-cluster-offline-sql-execution).

If the diagnosis report indicates that the cluster is healthy but the problem persists, please provide the error details and the diagnosis report to us.

## Creating OpenMLDB and Connecting

Firstly, we recommend beginners who are not familiar with distributed multi-process management to use Docker to create OpenMLDB for quick and easy setup. Once you become familiar with the components of OpenMLDB, you can then attempt a distributed deployment.

Refer to [Quick Start](./openmldb_quickstart.md) for creating OpenMLDB with Docker. Note that there are two versions in the documentation, a single-node version, and a cluster version. Make sure to choose the appropriate version and avoid mixing them.

A successful start involves being able to connect to the OpenMLDB server using the CLI (`/work/openmldb/bin/openmldb`) and executing `show components;` to view the running status of OpenMLDB server components. It is recommended to use the [diagnostic tool](../maintain/diagnose.md), execute `status` and `inspect` to obtain more reliable diagnostic results.

If the CLI cannot connect to OpenMLDB, first confirm that the processes are running correctly. You can use `ps f|grep bin/openmldb` to confirm the nameserver and tabletserver processes. For a cluster, also confirm the zk service using `ps f | grep zoo.cfg` and the task manager process using `ps f | grep TaskManagerServer`.

If all service processes are running, but CLI still fails to connect to the server, check the CLI parameters. If the issue persists, contact us and provide the CLI error information.

```{seealso}
If we also need the configuration and logs of the OpenMLDB server, you can use the diagnostic tool to obtain them, as described in the [next section](#providing-configuration-and-logs-for-technical-support).
```

### Deployment Tool Explanation

Make sure you are using the correct deployment method. Typically, we consider two deployment methods, as mentioned in the [Installation and Deployment](../deploy/install_deploy.md) documentation. The "one-click deployment" is also known as the `sbin` deployment method. Configuration file modifications and log file locations are closely related to the deployment method, so it is essential to be accurate.

- `sbin` Deployment:
  The `sbin` deployment involves deploying (copying installation packages to various nodes) and then starting components. It's important to note that the deployment node is the one where the `sbin` is executed, and it's preferable not to change the directory during operations. The deployment copies to the directories of the nodes specified in `conf/hosts`, and these directories on the nodes are referred to as the running directories.

  Configuration files are generated from template files during the deploy phase. Therefore, if you need to modify the configuration of a specific component, do not modify it directly on the node; modify the template file during deployment. If you do not perform a deploy operation, you can modify the configuration file (non-template) directly in the node's running directory, but be cautious, as deploy will override the changes.

  In case of any unexpected situations, the final configuration files in the running directories of the component nodes (specified in `conf/hosts`) are authoritative. View the running directory with `conf/hosts`, for example, `localhost:10921 /tmp/openmldb/tablet-1` indicates that the running directory for tablet1 is `/tmp/openmldb/tablet-1`. If you are unable to resolve the issue yourself, provide the final configuration files to us.

  Log files for `sbin` are located in the running directory of the component nodes. TaskManager logs are typically in `<dir>/taskmanager/bin/logs`, and logs for other components are in `<dir>/logs`. If there are specific configurations, refer to the configuration items.

- Manual Deployment:
  Manual deployment involves using the scripts `bin/start.sh` and configuration files in the `conf/` directory (non-template files). The only thing to note is that `spark.home` can be empty, in which case it will read the `SPARK_HOME` environment variable. If you suspect issues with the environment variable causing TaskManager startup failures, it is recommended to specify the `spark.home` configuration.

  Log files for manually deployed components are located in the directory where `bin/start.sh` is run. TaskManager logs are typically in `<dir>/taskmanager/bin/logs`, and logs for other components are in `<dir>/logs`. If there are specific configurations, refer to the configuration items.

## Operations

- As mentioned earlier, `inspect` can help us check the cluster's status, and if there are issues, the `recoverdata` tool can be used. However, this is a post-repair method, and under normal circumstances, we should avoid such problems through correct operational methods. When bringing nodes online or offline, **do not actively kill all tablets and then restart**. It is recommended to use the scale-in/scale-out method for operations; details can be found in [Scaling](../maintain/scale.md).
  - If you believe that existing data is not important, and you need to bring nodes online or offline quickly, you can restart the nodes directly. The `stop-all.sh` and `start-all.sh` scripts are used for quick rebuilding of the cluster and may result in the failure of online table data recovery; **recovery is not guaranteed**.

- Various components may also encounter complex issues such as network jitter and slow nodes during long-term service. Please enable [monitoring](../maintain/monitoring.md). If the monitoring shows that the server side is behaving normally, suspect client or network issues. If there are delays or low QPS on the server side, provide relevant monitoring charts to us.

## Understanding Storage

OpenMLDB separates online and offline storage and computation. Therefore, you need to be clear whether you are importing or querying data in online mode or offline mode.

Offline storage is in locations like HDFS. If you suspect that there is missing data imported into offline storage, you need to check the storage location for further inspection.

For online storage, consider the following points:
- Online storage has a concept of TTL. If you import data and find that the data is missing, it might have been evicted due to expiration.
- To check the number of records in online storage, use `select count(*) from t1`, or use `show table status`. Avoid using `select * from t1` for checking, as it might truncate data. Such queries are only suitable for data preview.

For guidance on designing your data flow, refer to [Common Architecture Integration Methods for Real-time Decision Systems with OpenMLDB](../tutorial/app_arch.md).

### Online Tables

Online tables reside in memory and are also backed up on disk. You can check the number of records in online tables using `select count(*) from t1`, or view the table status using `show table status` (there may be some delay; wait for a moment before checking).

Online tables can have multiple indexes, and you can view them using `desc <table>`. When writing a record, each index stores a copy, but the classification and ordering of records in each index may differ. Due to the TTL eviction mechanism in indexes, the data volume in different indexes may not be the same. The results of `select count(*) from t1` and `show table status` represent the data volume of the first index and do not necessarily reflect the data volume in other indexes. The SQL query uses the index selected by the SQL Engine as the optimal index, and you can check this using the physical plan of the SQL.

When creating a table, you can specify indexes or omit them. If not specified, a default index is created. For the default index, which does not have a `ts` column (using the current time as the sorting column, referred to as the time index), the data will never be evicted. You can use it as a reference to check if the data volume is accurate. However, such an index consumes too much memory and cannot be deleted (deletion support is planned for the future). You can use the NS Client to modify the TTL to evict data and reduce memory usage.

The time index (index without `ts`) also affects `PutIfAbsent` imports. If your data import may fail midway, and there is no other way to delete or deduplicate, and you want to use `PutIfAbsent` for import retries, refer to [PutIfAbsent Explanation](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md#putifabsent-explanation) to evaluate your data and avoid poor efficiency with `PutIfAbsent`.

## Source Data

### LOAD DATA

To import data into OpenMLDB from a file, the commonly used command is LOAD DATA. For details, refer to [LOAD DATA INFILE](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md). The data sources and formats supported by LOAD DATA depend on the OpenMLDB version (single-node/cluster), execution mode, and import mode (specified by the load_mode configuration option). The cluster version defaults to load_mode=cluster, which can also be set to local; the single-node version defaults to load_mode=local and **does not support cluster**. Therefore, we have three cases to consider:

| LOAD DATA Type | Supported Execution Modes (Import Destinations) | Supports Async/Sync | Supports Data Sources | Supports Data Formats |
| :------------- | :------------------------------------------- | :------------------- | :-------------------- | :---------------------- |
| Cluster Version load_mode=cluster | Online, Offline | Async, Sync | file (with conditional limitations, refer to specific documentation)/hdfs/hive | csv/parquet (no format restrictions for hive source) |
| Cluster Version load_mode=local | Online | Sync | Client local file | csv |
| Single-node Version (only local) | Online | Sync | Client local file | csv |

When the source data for LOAD DATA is in CSV format, it's essential to pay attention to the format of timestamp columns. Timestamp formats can be "int64" (referred to as int64 type below) or "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]" (referred to as the year-month-day type below). Therefore, we have three discussions:

| LOAD DATA Type | Supports int64 | Supports Year-Month-Day |
| :------------- | :-------------- | :------------------------ |
| Cluster Version load_mode=cluster | **``✓``** | **``✓``** |
| Cluster Version load_mode=local | **``✓``** | **``X``** |
| Single-node Version (only local) | **``✓``** | **``X``** |

```{hint}
CSV file format has many inconveniences; it is recommended to use the parquet format, which requires OpenMLDB cluster version and the taskmanager component to be started.
```

## OpenMLDB SQL Development and Debugging

OpenMLDB is not fully compatible with standard SQL. Therefore, some SQL executions may not yield the expected results. If you find that SQL execution does not meet expectations, check if the SQL satisfies the [functional boundaries](./function_boundary.md).

To facilitate development, debugging, and validation using OpenMLDB SQL, we strongly recommend using the community tool [OpenMLDB SQL Emulator](https://github.com/vagetablechicken/OpenMLDBSQLEmulator) for SQL simulation development. This tool can save a significant amount of time spent on deployment, compilation, index building, and waiting for tasks. For details, refer to the README of the project: https://github.com/vagetablechicken/OpenMLDBSQLEmulator

### OpenMLDB SQL Syntax Guide

For feature engineering based on OpenMLDB SQL, `WINDOW` (including `WINDOW UNION`), `LAST JOIN`, and other clauses are commonly used to complete the calculation logic. They ensure functionality across different modes. Follow the tutorial "Feature Development Based on SQL" [(Part 1)](../tutorial/tutorial_sql_1.md) [(Part 2)](../tutorial/tutorial_sql_2.md) for learning.

If using clauses like `WHERE`, `WITH`, `HAVING`, etc., pay attention to the limiting conditions. Each clause's detailed documentation has specific instructions. For example, the [`HAVING` clause](../openmldb_sql/dql/HAVING_CLAUSE.md) is not supported in online request mode. Browse the DQL directory of OpenMLDB SQL or use the search function to quickly find detailed documentation for each clause.

For those unfamiliar with OpenMLDB SQL, it's recommended to start by writing SQL using individual clauses to ensure that each clause passes. Then gradually combine them to form a complete SQL.

It is recommended to use [OpenMLDB SQL Emulator](https://github.com/vagetablechicken/OpenMLDBSQLEmulator) for SQL exploration and validation. Once SQL validation is complete, proceed to the real cluster for deployment. This avoids wasting time on index building, data import, and waiting for tasks. The Emulator does not depend on a real OpenMLDB cluster and provides an interactive virtual environment for quickly creating tables, verifying SQL, exporting the current environment, etc. Refer to the project's README for more details.

### OpenMLDB SQL Syntax Error Messages

When encountering SQL compilation errors, it's important to examine the error message. For example, an error like `Syntax error: Expected XXX but got keyword YYY` indicates that the SQL does not conform to the syntax. Typically, it's due to incorrectly positioned keywords or unsupported syntax. Details can be found in the documentation of the error clause. Check the `Syntax` section of each clause, as it provides detailed explanations of each part. Ensure that the SQL meets the requirements.

For example, in the [`WINDOW` clause](../openmldb_sql/dql/WINDOW_CLAUSE.md#syntax), the part `WindowFrameClause (WindowAttribute)*` can be further broken down into `WindowFrameUnits WindowFrameBounds [WindowFrameMaxSize] (WindowAttribute)*`. So, `WindowFrameUnits WindowFrameBounds MAXSIZE 10 EXCLUDE CURRENT_TIME` is a syntax-compliant statement, while `WindowFrameUnits WindowFrameBounds EXCLUDE CURRENT_TIME MAXSIZE 10` is not compliant because `WindowFrameMaxSize` should be within `WindowFrameClause`.

### OpenMLDB SQL Calculation Correctness Debugging

After SQL compilation, calculations can be based on data. If the calculation result does not meet expectations, check step by step:
- Whether the calculation result of SQL, whether it's a single column or multiple columns, does not meet expectations. It's recommended to select **one column** for debugging.
- If your table has a large amount of data, it's recommended to test with a small dataset (order of a few rows or tens of rows). You can also use the [Run toydb](https://github.com/vagetablechicken/OpenMLDBSQLEmulator#run-in-toydb) feature in OpenMLDB SQL Emulator to create cases for testing.
- Whether the column represents what you intend to express and whether unexpected functions are used or there are errors in function parameters.
- If the column is the result of window aggregation, whether there is an error in WINDOW definition leading to incorrect window range. Refer to [Inferring Windows](../openmldb_sql/dql/WINDOW_CLAUSE.md#how-to-infer-a-window) for checking, and use a small dataset for verification testing.

If you still cannot resolve the issue, you can provide the YAML case of OpenMLDB SQL Emulator. If testing is done in the cluster, please [provide a reproduction script](#constructing-an-openmldb-sql-reproduction-script).

### Online Request Mode Testing

Deploying SQL, equivalent to the success of `DEPLOY <name> <SQL>`, is a significant operation. If SQL can be deployed, it will create or modify indexes and copy data to new indexes. Therefore, during the exploration of SQL, using `DEPLOY` to test whether SQL can be deployed is resource-intensive. Especially for some SQL, multiple modifications may be needed before it can be deployed, and multiple `DEPLOY` operations may generate many unnecessary indexes. During exploration, you may also modify table schemas, requiring deletion and recreation. These operations can only be done manually and are cumbersome.

If you are familiar with OpenMLDB SQL, in some scenarios, you can use "Online Preview Mode" for testing. However, "Online Preview Mode" does not guarantee "Online Request Mode," and it cannot be guaranteed that SQL can be deployed. If you are familiar with indexes, you can use `EXPLAIN <SQL>` to confirm whether SQL can be deployed. However, `EXPLAIN` checks are strict; SQL may be deemed unexecutable in "Online Request Mode" due to the lack of matching indexes on the current table (rejected because there are no indexes, and real-time performance cannot be guaranteed). 

Currently, only the Java SDK can use the [validateSQLInRequest](./sdk/java_sdk.md#sql-validation) method for checking, which is a bit cumbersome. We recommend using OpenMLDB SQL Emulator for testing. In the Emulator, you can create tables with simple syntax and then use `valreq <SQL>` to determine whether it can be deployed.

## OpenMLDB SQL Execution

All commands in OpenMLDB are SQL-based. If a SQL execution fails or there are issues with interaction (uncertain if the command was successful), first confirm whether there are syntax errors in the SQL or if the command has entered the execution phase.

For example, if you receive a Syntax error message, it indicates a syntax error in the SQL. Refer to the [OpenMLDB SQL Syntax Guide](#openmldb-sql-syntax-guide) to correct the error.
```
127.0.0.1:7527/db> create table t1(c1 int;
Error: Syntax error: Expected ")" or "," but got ";" [at 1:23]
create table t1(c1 int;
                      ^
```

If the command has entered the execution phase but failed, or there are interaction issues, consider the following:

- Is OpenMLDB a single-node or cluster deployment?
- What is the execution mode? You can use `show variable` when running commands in the CLI to get this information, but note that **the execution mode in the single-node version is not meaningful**.

Special attention should be paid to some usage logic in the cluster version.

### Considerations for Cluster Offline SQL Execution

For cluster offline commands, in asynchronous mode (default), sending a command will return a job id. You can use `show job <id>` to query the execution status of the job.

If the offline job is an asynchronous SELECT (without saving results into another table), the results won't be printed on the client side. However, synchronous SELECT will print the results to the console. You can use `show joblog <id>` to obtain the results, which include stdout (query results) and stderr (job runtime logs). If you find that the job failed or is in another unexpected state, carefully examine the job runtime logs.

The offline job logs may contain some distracting information. Users can use `openmldb_tool inspect job --id x` to parse and extract logs to help identify errors. For more information, refer to [Diagnosis Tool - Job Inspection](../maintain/diagnose.md#job-inspection).

If the task manager is in YARN mode rather than local mode, the information in `job_x_error.log` may be minimal, only printing exceptions. If the exception is not obvious, and you need logs from an earlier time, the execution logs are not in `job_x_error.log`. You will need to use the yarn app id recorded in `job_x_error.log` to query the logs of the yarn app's containers in the yarn system. Within the yarn app container, the execution logs are also saved in stderr.

```{note}
If you are unable to obtain logs using `show joblog` or want to directly access log files, you can get them directly from the TaskManager machine. The log location is configured by the `job.log.path` property in taskmanager.properties. If you changed this configuration, you need to look for logs in the configured directory. The default locations are `/work/openmldb/taskmanager/bin/logs/job_x.log` for stdout (query results) and `/work/openmldb/taskmanager/bin/logs/job_x_error.log` for stderr (job runtime logs) (note the error suffix).
```

### Considerations for Cluster Online SQL Execution

In cluster online mode, we typically recommend two use cases: using `DEPLOY` to create a deployment and then executing the deployment for real-time feature computation (SDK requests deployment or HTTP access to API Server for deployment). In CLI or other clients, you can directly perform SELECT queries in "online" mode, referred to as "online preview." Online preview has various limitations; refer to [Functional Boundaries - Cluster Online Preview Mode](./function_boundary.md#online-preview-mode) for details. Please avoid executing unsupported SQL.

### Constructing an OpenMLDB SQL Reproduction Script

If your SQL execution does not meet expectations, and you cannot resolve the issue through self-diagnosis, provide us with a reproduction script. A complete reproduction script involves only online SQL calculation or validating SQL. It is recommended to use [OpenMLDB SQL Emulator](https://github.com/vagetablechicken/OpenMLDBSQLEmulator#run-in-toydb) to construct a reproducible YAML case. If it involves data import, which must use the OpenMLDB cluster, provide a reproduction script with the following structure:

```sql
create database db;
use db;
-- create your table
create table xx ();

-- offline or online
set @@execute_mode='';

-- load data or online insert
-- load data infile '' into table xx;
-- insert into xx values (),();

-- query / deploy ...

```

If your issue requires data for reproduction, provide the data. For offline data, as offline does not support insert, provide CSV/Parquet data files. For online data, you can provide data files or directly perform inserts in the script.

Such a data script can be executed in batch using redirect symbols.
```bash
/work/openmldb/bin/openmldb --host 127.0.0.1 --port 6527 < reproduce.sql
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < reproduce.sql
```

Ensure that you can reproduce the problem using the reproduction script locally before recording an issue or sending it to us.

```{caution}
Note that offline jobs default to asynchronous. If you need to import data offline and then query, set it to synchronous mode. See [Offline Command Configuration Details](../openmldb_sql/ddl/SET_STATEMENT.md#offline-command-configuration-details) for details. Otherwise, querying before the import is complete is meaningless.
```

### Providing Configuration and Logs for Technical Support

If your SQL execution issue cannot be reproduced using a reproduction script, or if it is not an SQL execution issue but rather a cluster management issue, please provide the configuration and logs for both the client and server for further investigation.

For Docker or local clusters (where all server processes are local), you can quickly obtain configuration, logs, and other information using the diagnostic tool.

For OpenMLDB servers started using the `init.sh`/`start-all.sh` and `init.sh standalone`/`start-standalone.sh` scripts, you can use the following commands for diagnostics, corresponding to the cluster and standalone versions, respectively.

```bash
openmldb_tool --env=onebox --dist_conf=cluster_dist.yml
openmldb_tool --env=onebox --dist_conf=standalone_dist.yml
```

The `cluster_dist.yml` and `standalone_dist.yml` files can be found in the `/work/` directory of the Docker container or copied from the [GitHub directory](https://github.com/4paradigm/OpenMLDB/tree/main/demo).

For a distributed cluster, SSH passwordless authentication must be configured for the diagnostic tool to work smoothly. Refer to the documentation on [Diagnosis Tool](../maintain/diagnose.md) for more details.

If you cannot configure this in your environment, please manually obtain the configuration and logs.

## Performance Statistics

To enable deployment time statistics, use the following command:

```sql
SET GLOBAL deploy_stats = 'on';
```

Once enabled, the time taken for each Deployment execution will be recorded. Previous deployments will not be included in the statistics, and the data in the table does not include network time outside the cluster. It only records the time from the start to the end of the deployment execution on the server side.