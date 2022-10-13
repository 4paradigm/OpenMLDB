# Operation and Maintenance FAQ

## Deploy and Startup FAQ

### 1. How to confirm that the cluster is running normally？
Although there is one-click to start the script, due to the numerous configurations, problems such as "the port is occupied" and "the directory does not have read and write permissions" may occur. These problems can only be identified when the server process is running, and there is no timely feedback after exiting. (If monitoring is configured, it can be checked directly by monitoring.）
Therefore, please make sure that all server processes in the cluster are running normally.

It can be queried by `ps axu | grep openmldb`. (Note that `mon` is used as the daemon process in the official run script, but the running of the `mon` process does not mean that the OpenMLDB server process is running.)

If the processes are all running and the cluster still behaves abnormally, you need to query the server log. You can give priority to 'WARN' and 'ERROR' level logs, which are most likely the root cause.

## Server FAQ

### 1. Why is there a warning of "Fail to write into Socket" in the log?
```
http_rpc_protocol.cpp:911] Fail to write into Socket{id=xx fd=xx addr=xxx} (0x7a7ca00): Unknown error 1014 [1014]
```
This is the log that the server side will print. Generally, the client side uses the connection pool or short connection mode. After the RPC times out, the connection will be closed. When the server writes back the response, it finds that the connection has been closed and reports this error. Got EOF means that EOF has been received before (the peer has closed the connection normally). The client side uses the single connection mode and the server side generally does not report this.

### 2. The initial ttl setting of table data is not suitable, how to adjust it?
This needs to be modified using nsclient, which cannot be done by ordinary clients. For nsclient startup method and command, see [ns client](../reference/cli.md#ns-client)。

Use the command `setttl` in nsclient to change the ttl of a table, similar to
```
setttl table_name ttl_type ttl [ttl] [index_name]
```
As you can see, if you configure the name of the index at the end of the command, you can only modify the ttl of a single index.
```{caution}
Changes to `setttl` will not take effect in time and will be affected by the `gc_interval` configuration of the tablet server. (The configuration of each tablet server is independent and does not affect each other.)

For example, if the `gc_interval` of a tablet server is 1h, then the ttl configuration reload will be performed at the last moment of the next gc (in the worst case, it will be reloaded after 1h). This time the gc that reloads the ttl will not eliminate the data according to the latest ttl. The latest ttl will be used for data elimination during the next gc.

Therefore, after **ttl is changed, it takes two gc intervals to take effect**. please wait patiently.

Of course, you can adjust the `gc_interval` of the tablet server, but this configuration cannot be changed dynamically, it can only take effect after restarting. Therefore, if the memory pressure is high, you can try to expand the capacity and migrate the data shards to reduce the memory pressure. Adjusting `gc_interval` lightly is not recommended.
```

### 3. If a warning log appears: Last Join right table is empty, what does it mean?
Generally speaking, this is a normal phenomenon and does not represent an anomaly in the cluster. It's just that the right table of the join in the runner is empty, while is a possible phenomenon, and is instead likely to be a data problem.

## Client FAQ

### 1. Why am I getting a warning log for Reached timeout?
```
rpc_client.h:xxx] request error. [E1008] Reached timeout=xxxms
```
This is because the timeout setting of the rpc request sent by the client itself is small, and the client itself disconnects itself. Note that this is a timeout for rpc.

It is divided into the following situations:
#### Synchronized offline job
This happens easily when using synchronized offline commands. you can use
```sql
> SET @@job_timeout = "600000";
```
To adjust the timeout time of rpc, use 'ms' units.
#### normal request
If it is a simple query or insert, still get timeout, the general `request_timeout` configuration needs to be changed.
1. CLI: set `--request_timeout` before running
2. JAVA: SDK direct connection, adjust `SdkOption.requestTimeout`; JDBC, adjust the parameter `requestTimeout` in url
3. Python: SDK direct connection(DBAPI), adjust `connect()` arg `request_timeout`; SQLAlchemy, adjust the parameter `requestTimeout` in url

### 2. Why am I getting the warning log of Got EOF of Socket?
```
rpc_client.h:xxx] request error. [E1014]Got EOF of Socket{id=x fd=x addr=xxx} (xx)
```
This is because the `addr` side actively disconnected, and the address of `addr` is most likely taskmanager. This does not mean that the taskmanager is abnormal, but that the taskmanager side thinks that the connection is inactive and has exceeded the keepAliveTime, and actively disconnects the communication channel.
In version 0.5.0 and later, the taskmanager's `server.channel_keep_alive_time` can be increased to increase the tolerance of inactive channels. The default value is 1800s (0.5h), especially when using synchronous offline commands, this value may need to be adjusted appropriately.
In versions before 0.5.0, this configuration cannot be changed, please upgrade the taskmanager version.

### 3. Why we get unrecognizable result of offline queries?

When we are using offline queries, the result which contains Chinese may be printed as unrecognizable code. It is related with default system encoding and encoding configuration of Saprk jobs. 

If we have unrecognizable code, we can set the configuration `spark.driver.extraJavaOptions=-Dfile.encoding=utf-8` and `spark.executor.extraJavaOptions=-Dfile.encoding=utf-8` for Spark jobs.

Here is the way to configure client in [Spark Client Config](../reference/client_config/client_spark_config.md) and we can add this configuration in TaskManager properties file as well.

```
spark.default.conf=spark.driver.extraJavaOptions=-Dfile.encoding=utf-8;spark.executor.extraJavaOptions=-Dfile.encoding=utf-8
```

### 4. How to config TaskManager to access Kerberos-enabled Yarn cluster?

If Yarn cluster enables Kerberos authentication, we can add the following configuration to access the Kerberos-enabled Yarn cluster. Notice that we need to update the actual keytab file path and principle account.

```
spark.default.conf=spark.yarn.keytab=/tmp/test.keytab;spark.yarn.principal=test@EXAMPLE.COM
```

### 5. How to config the cxx log in client

cxx log: zk log and sdk log(glog).

zk log：
1. CLI：set before running, `--zk_log_level`(int) to set zk log level,`--zk_log_file` to set log file(just file, not dir)
2. JAVA/Python SDK：in option or url, set `zkLogLevel` and `zkLogFile`

- `zk_log_level`(int, default=3, which is INFO): 
Log messages at or **below** this level. 0-disable all zk log, 1-error, 2-warn, 3-info, 4-debug.

sdk log(glog):
1. CLI：set before running, `--glog_level`(int) to set glog level,`--glog_dir`to set glog dir(a path, not a file)
2. JAVA/Python SDK：in option or url, set `glogLevel` and`glogDir`

- `glog_level`(int, default=0, which is INFO):
Log messages at or **above** this level. The numbers of severity levels INFO, WARNING, ERROR, and FATAL are 0, 1, 2, and 3, respectively.
