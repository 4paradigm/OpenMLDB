# Client FAQ

## "fail to get tablet ..."

Prioritize the checking on whether the tablet server in the cluster is unexpectedly offline or if online tables are not readable or writable. It is recommended to use [openmldb_tool](../maintain/diagnose.md) for diagnosis, utilizing the `status` (status --diff), and `inspect online` commands for checks.
If the TODO diag tool detects abnormal conditions in offline or online tables, it will output warnings and suggest the next steps.
If manual inspection is required, follow these two steps:
- Execute `show components` to check if the server is in the list. If TaskManager is offline, it will not be on the list. If Tablet is offline, it will be in the list but with a status of offline. If there are offline servers, **restart the server and add it back to the cluster**.
- Execute `show table status like '%'` (if the lower version does not support `like`, query the system db and user db separately). Check if the "Warnings" for each table report any errors.

Common errors may include messages like `real replica number X does not match the configured replicanum X`. For detailed error information, please refer to [SHOW TABLE STATUS](../openmldb_sql/ddl/SHOW_TABLE_STATUS.md). These errors indicate that the table is currently experiencing issues, rendering it unable to provide normal read and write functions. This is typically due to Tablet issues.

## Why Do I Receive Warnings of "Reached timeout ..."?
```
rpc_client.h:xxx] request error. [E1008] Reached timeout=xxxms
```
This is because the timeout setting for the RPC request sent by the client-side is too short, and the client-side actively disconnects. Note that this is the timeout for RPC. You need to change the general `request_timeout` configuration.

1. CLI: Configure `--request_timeout_ms` at startup.
2. JAVA/Python SDK: Adjust `SdkOption.requestTimeout` in Option or URL.

```{note}
This error usually does not occur with synchronized offline commands, as the timeout of the synchronized offline command is set to the maximum acceptable time for TaskManager.
```

## Why Do I Receive Warnings of "Got EOF of Socket ..."?
```
rpc_client.h:xxx] request error. [E1014]Got EOF of Socket{id=x fd=x addr=xxx} (xx)
```
This is because the `addr` end is actively disconnected, and the `addr` address is most likely the TaskManager. This does not mean that the TaskManager is abnormal, but rather that the TaskManager end considers this connection inactive, exceeds the `keepAliveTime`, and actively disconnects the communication channel.

In versions 0.5.0 and later, you can increase the `server.channel_keep_alive_time` of the TaskManager to improve tolerance for inactive channels. The default value is 1800 seconds (0.5 hours). Especially when using synchronous offline commands, this value may need to be adjusted appropriately.

In versions before 0.5.0, this configuration cannot be changed. Please upgrade the TaskManager version.

## Why is the Offline Query Result Displaying Chinese Characters as Garbled?

When using offline queries, there may be issues with garbled query results containing Chinese characters. This is mainly related to the system's default encoding format and the encoding format parameters of Spark tasks.

If you encounter garbled characters, you can resolve this by adding the Spark advanced parameters `spark.driver.extraJavaOptions=-Dfile.encoding=utf-8` and `spark.executor.extraJavaOptions=-Dfile.encoding=utf-8`.

For client configuration methods, you can refer to the [Spark Client Configuration](../reference/client_config/client_spark_config.md), or you can add this configuration to the TaskManager configuration file.

```
spark.default.conf=spark.driver.extraJavaOptions=-Dfile.encoding=utf-8;spark.executor.extraJavaOptions=-Dfile.encoding=utf-8
```

## How to Configure TaskManager to Access a YARN Cluster with Kerberos Enabled?

If the YARN cluster has Kerberos authentication enabled, TaskManager can access the YARN cluster with Kerberos authentication by adding the following configuration. Please note to modify the `keytab` path and `principal` account according to the actual configuration.

```
spark.default.conf=spark.yarn.keytab=/tmp/test.keytab;spark.yarn.principal=test@EXAMPLE.COM
```

## How to Configure Client's Core Logs?

Client core logs mainly consist of two types: ZooKeeper logs and SDK logs (glog logs), and they are independent of each other.

ZooKeeper Logs:

1. CLI: Configure `--zk_log_level` during startup to adjust the log level, and use `--zk_log_file` to specify the log file.
2. JAVA/Python SDK: Use `zkLogLevel` to adjust the level and `zkLogFile` to specify the log file in Option or URL.

- `zk_log_level` (int, default=0, i.e., DISABLE_LOGGING): Prints logs at this level and **below**. 0 - disable all zk logs, 1 - error, 2 - warn, 3 - info, 4 - debug.

SDK Logs (glog Logs):

1. CLI: Configure `--glog_level` during startup to adjust the level, and use `--glog_dir` to specify the log file.
2. JAVA/Python SDK: Use `glogLevel` to adjust the level and `glogDir` to specify the log file in Option or URL.

- `glog_level` (int, default=1, i.e., WARNING): Prints logs at this level and **above**. INFO, WARNING, ERROR, and FATAL logs correspond to 0, 1, 2, and 3, respectively.


## Insert Error with Log `please use getInsertRow with ... first`.

When using `InsertPreparedStatement` for insertion in the JAVA client or inserting with SQL and parameters in Python, there is an underlying cache effect in the client. The process involves generating SQL cache with the first step `getInsertRow` and returning the SQL along with the parameter information to be completed. The second step actually executes the insert, and it requires using the SQL cache cached in the first step. Therefore, when multiple threads use the same client, it's possible that frequent updates to the cache table due to frequent insertions and queries might evict the SQL cache you want to execute, causing it to seem like the first step `getInsertRow` was not executed.

Currently, you can avoid this issue by increasing the `maxSqlCacheSize` configuration option. This option is only supported in the JAVA/Python SDKs.

## Offline Command Error

```
java.lang.OutOfMemoryError: Java heap space
```

```
Container killed by YARN for exceeding memory limits. 5 GB of 5 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead.
```

When encountering the aforementioned log messages, it indicates that the offline task requires more resources than the current configuration provides. This typically occurs in the following situations:

- The Spark configuration for the offline command is set to `local[*]`, the machine has a high number of cores, and the concurrency is too high, resulting in excessive resource consumption.
- The memory configuration is too small.

If using local mode and the resources on a single machine are limited, consider reducing concurrency. If you choose not to reduce concurrency, adjust the `spark.driver.memory` and `spark.executor.memory` Spark configuration options. You can write these configurations in the `conf/taskmanager.properties` file in the TaskManager's running directory, restart the TaskManager, or use the CLI client for configuration. For more information, refer to the [Spark Client Configuration](../reference/client_config/client_spark_config.md).

```
spark.default.conf=spark.driver.memory=16g;spark.executor.memory=16g
```

When the master is local, adjust the memory of the driver, not the executor. If you are unsure, you can adjust both.
