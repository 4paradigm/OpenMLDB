# Functional Boundary

This article will introduce the functional boundary of OpenMLDB SQL.

```{note}
If you have any questions about SQL statements, please refer to OpenMLDB SQL or directly use the search function to search.
```

## System Configuration - TaskManager

You can configure the TaskManager to define various settings, including the offline storage address (`offline.data.prefix`) and the Spark mode required for offline job computation (`spark.master`), among others.

- `offline.data.prefix`: This can be configured as either a file path or an HDFS path. It is recommended to use an HDFS path for production environments, while a local file path can be configured for testing environments (specifically for onebox, such as running within a Docker container). Note that using a file path as offline storage will not support distributed deployment with multiple Task Managers (data won't be transferred between Task Managers). If you plan to deploy Task Managers on multiple hosts, please use storage media like HDFS that can be accessed simultaneously by multiple hosts. If you intend to test the collaboration of multiple Task Managers, you can deploy multiple Task Managers on a single host and use a file path as offline storage.
- `spark.master=local[*]`: The default Spark configuration is in `local[*]` mode, which automatically binds CPU cores. If offline tasks are found to be slow, it is recommended to use the Yarn mode. After changing the configuration, you need to restart the Task Manager for the changes to take effect. For more configurations, please refer to [master-urls](https://spark.apache.org/docs/3.1.2/submitting-applications.html#master-urls).

### spark.default.conf

More optional configurations can be written in the `spark.default.conf` parameter in the format of `k1=v1;k2=v2`. For example:

```Plain
spark.default.conf=spark.port.maxRetries=32;foo=bar
```

`spark.port.maxRetries`: The default is set to 16, and you can refer to [Spark Configuration](https://spark.apache.org/docs/3.1.2/configuration.html). Each offline job is associated with a Spark UI, corresponding to a port. Each port starts from the default initial port and increments by one for retry attempts. If the number of concurrently running jobs exceeds `spark.port.maxRetries`, the number of retries will also exceed `spark.port.maxRetries`, causing job startup failures. If you need to support a larger job concurrency, configure a higher value for `spark.port.maxRetries` and restart the Task Manager to apply the changes.

## DDL Boundary - DEPLOY Statement

You can deploy an online SQL solution using the `DEPLOY <deploy_name> <sql>` command. This operation automatically parses the SQL statement and helps create indexes (you can view index details using `DESC <table_name>`). For more information, please refer to the [DEPLOY STATEMENT](../openmldb_sql/deployment_manage/DEPLOY_STATEMENT.md) documentation.

The success of the deployment operation is dependent on the presence of online data in the table.

### Long Window SQL

Long Window SQL: This refers to the `DEPLOY` statement with the `OPTIONS(long_windows=...)` configuration item. For syntax details, please refer to [Long Window](../openmldb_sql/deployment_manage/DEPLOY_STATEMENT.md#long-window-optimazation). Deployment conditions for long-window SQL are relatively strict, and it's essential to ensure that the tables used in the SQL statements do not contain online data. Otherwise, even if deploying SQL that matches the previous one, the operation will still fail.

### Normal SQL

- If the relevant index already exists before deployment, the `DEPLOY` operation will not create the index. The `DEPLOY` operation will succeed regardless of whether there is online data in the table.
- If a new index needs to be created during deployment, and there is already online data in the table, the `DEPLOY` operation will fail.

There are two solutions:

1. Strictly perform `DEPLOY` before importing online data, and avoid executing `DEPLOY` after online data is present in the table.
2. The `CREATE INDEX` statement can automatically import existing online data (data from existing indexes) when creating a new index. If it is necessary to execute `DEPLOY` when the table already has online data, you can manually execute a `CREATE INDEX` to create the required index (the new index will already have data), and then execute `DEPLOY` (in this case, `DEPLOY` will not create a new index, and the manually created indexes will be used directly for computation).

```{note}
How can you determine which indexes to create?

Currently, only the Java SDK supports this feature, and all the required indexes can be obtained through `SqlClusterExecutor.genDDL`. However, you will need to manually convert them into `CREATE INDE`X statements as `genDD`L provides table creation statements. In the future, it will support ** directly obtaining index creation statements** or **automatically importing data into a new index** with `DEPLOY`.
```

## DML Boundary

### Offline Information

There are two types of paths in the offline information of a table: `offline_path` and `symbolic_paths`. `offline_path` is the actual storage path for offline data, while `symbolic_paths` are soft link paths for offline data. Both paths can be modified using the `LOAD DATA` command, and `symbolic_paths` can also be modified using the `ALTER` statement.

The key difference between `offline_path` and `symbolic_paths` is that `offline_path` is the path owned by the OpenMLDB cluster. If a hard copy is implemented, data will be written to this path. On the other hand, `symbolic_paths` are paths outside the OpenMLDB cluster, and soft copies will add a path to this information. When querying offline, data from both paths will be loaded. Both paths use the same format and read options and do not support paths with different configurations.

Therefore, if `offline_path` already exists offline, the `LOAD DATA` command can only modify `symbolic_paths`. If `symbolic_paths` already exist offline, the `LOAD DATA` command can be used to modify both `offline_path` and `symbolic_paths`.

The `errorifexists` option will raise an error if there is offline information in the table. It will raise errors if performing hard copy when there's soft links, or performing soft copy when a hard copy exisits. 

### LOAD DATA

Regardless of whether data is imported online or offline using the `LOAD DATA` command, it is considered an offline job. The format rules for source data are the same for both offline and online scenarios.

It is recommended to use HDFS files as source data. This approach allows for successful import whether TaskManager is in local mode, Yarn mode, or running on another host. However, if the source data is a local file, the ability to import it smoothly depends on the mode of TaskManager and the host where it is running:

- In local mode, TaskManager can successfully import source data only if the source data is placed on the same host as the TaskManager process.
- When TaskManager is in Yarn mode (both client and cluster), a file path cannot be used as the source data address because it is not known on which host the container is running.

### DELETE

In tables with multiple indexes in the online storage, a `DELETE` operation may not delete corresponding data in all indexes. Consequently, there may be situations where data has been deleted, but the deleted data can still be found.

For example:

```SQL
create database db;
use db;
create table t1(c1 int, c2 int,index(key=c1),index(key=c2));
desc t1;
set @@execute_mode='online';
insert into t1 values (1,1),(2,2);
delete from t1 where c2=2;
select * from t1;
select * from t1 where c2=2;
```

The results are as follows:

```Plain
 --- ------- ------ ------ ---------
     Field   Type   Null   Default
 --- ------- ------ ------ ---------
  1   c1      Int    YES
  2   c2      Int    YES
 --- ------- ------ ------ ---------
 --- -------------------- ------ ---- ------ ---------------
     name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1668504212   c1     -    0min   kAbsoluteTime
  2   INDEX_1_1668504212   c2     -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------
 ---- ----
  c1   c2
 ---- ----
  1    1
  2    2
 ---- ----

2 rows in set
 ---- ----
  c1   c2
 ---- ----

0 rows in set
```

Explanation:

Table `t1` has multiple indexes (which may be automatically created during `DEPLOY`). If you run `delete from t1 where c2=2`, it only deletes data in the second index, while the data in the first index remains unaffected. Therefore, if you subsequently run `select * from t1` and it uses the first index, there are two pieces of data that haven't been deleted. `select * from t1 where c2=2` uses the second index, and the result is empty, with data being successfully deleted.

## DQL Boundary

The supported query modes (i.e. `SELECT` statements) vary depending on the execution mode:

| Execution Mode | Query Statement                                              |
| -------------- | ------------------------------------------------------------ |
| Offline Mode   | Batch query                                                  |
| Online Mode    | Batch query (also known as online preview mode, only supports partial SQL) and request query (also known as online request mode) |

### Online Preview Mode

In OpenMLDB CLI, executing SQL in online mode puts it in online preview mode. Please note that online preview mode has limited support; you can refer to the [SELECT STATEMENT](../openmldb_sql/dql/SELECT_STATEMENT) documentation for more details.

Online preview mode is primarily for previewing query results. If you need to run complex SQL queries, it's recommended to use offline mode. To query complete online data, consider using a data export tool such as the `SELECT INTO` command. Keep in mind that if the online table contains a large volume of data, it might trigger data truncation, and executing `SELECT * FROM table` could result in some data not being returned.

Online data is usually distributed across multiple locations, and when you run `SELECT * FROM table`, it retrieves results from various Tablet Servers without performing global sorting. As a result, the order of data will be different with each execution of `SELECT * FROM table`.

### Offline Mode and Online Request Mode

In the [Full Process of Feature Engineering Development and Launch](../concepts/modes.md), offline mode and online request mode play prominent roles:

- Offline Mode Batch Query: Used for offline feature generation.
- Query in Online Request Mode: Employed for real-time feature computation.

While these two modes share the same SQL statements and produce consistent computation results, due to the use of two different execution engines (offline and online), not all SQL statements that work offline can be deployed online. SQL that can be executed in online request mode is a subset of offline executable SQL. Therefore, it's essential to test whether SQL can be deployed using `DEPLOY` after completing offline SQL development.

## Offline Command Synchronization Mode

All offline commands can be executed in synchronous mode using `set @@sync_job=true;`. In this mode, the command will only return after completion, whereas in asynchronous mode, job info is immediately returned, requires usage of `SHOW JOB <id>` to check the execution status of the job. In synchronous mode, the return values differ depending on the command.

- DML commands like `LOAD DATA` and DQL commands like `SELECT INTO` return the ResultSet of Job Info. These results are identical to those in asynchronous mode, with the only difference being the return time. 
- Normal `SELECT` queries in DQL return Job Info in asynchronous mode and query results in synchronous mode. However, support for this feature is currently incomplete, as explained in [Offline Sync Mode-select](../openmldb_sql/dql/SELECT_STATEMENT.md#offline-sync-mode-select). The results are in CSV format, but data integrity is not guaranteed, so it's not recommended to use as accurate query results.
  - In the CLI interactive mode, the results are printed directly.
  - In the SDK, ResultSet is returned, the query result as a string. Consequently, it's not recommended to use synchronous mode queries in the SDK and process their results.

Synchronous mode comes with timeout considerations, which are detailed in [Configuration](../openmldb_sql/ddl/SET_STATEMENT.md#offline-command-configuaration-details).
