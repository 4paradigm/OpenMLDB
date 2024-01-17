# Server FAQ

If there are any changes or issues in the server, please first check the command openmldb_tool status + inspect online to see if the cluster is functioning properly.

## Deploy and Start FAQ

### 1. How to confirm that the cluster is running normally?
Although there is a one-click startup script, due to the numerous configurations, issues such as "port already in use" or "directory without read-write permissions" may occur. These problems are usually discovered only after the server process has started, and there is no immediate feedback upon exit. (If monitoring is configured, you can check directly through the monitoring system.)

Therefore, please first confirm that all server processes in the cluster are running normally.

You can use `ps axu | grep openmldb` or the SQL command `show components;` to check. (Note that if you use a daemon, the openmldb server process may be in a loop of starting and stopping, which does not necessarily mean continuous operation. You can confirm this through logs or the connection time from `show components;`.)

If the processes are working, but the cluster is still behaving abnormally, it is necessary to check the server logs. Priority should be given to examining WARN and ERROR level logs, as they often indicate the root cause.

### 2. What if the data is not automatically restored successfully?

In normal circumstances, when we restart the service, the data in the table will automatically be recovered. However, there are cases where recovery may fail, and common failure scenarios include:

- Tablet abnormal exit
- Multiple-replica tables have multiple replicas in tablets that restart simultaneously or too quickly, causing some `auto_failover` operations to restart before completion
- `auto_failover` set to `false`

After the service has successfully started, you can use `gettablestatus` to obtain the status of all tables:

```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=gettablestatus
```

If there are `Warnings` in the table, you can use `recoverdata` to automatically recover the data:
```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
```

## Server FAQ

### 1. Why is there a warning log for 'Failed to write into Socket' in the log?
```
http_rpc_protocol.cpp:911] Fail to write into Socket{id=xx fd=xx addr=xxx} (0x7a7ca00): Unknown error 1014 [1014]
```
This is the log that the server will print. Usually, if the client-side uses connection pooling or short connection mode, the connection will be closed after RPC timeout. When the server writes back the response, it will report this error if it finds that the connection has already been closed. Got EOF refers to receiving an EOF before (when the other end has closed the connection normally). When using single connection mode on the client side, the server side generally does not report this.

### 2. The initial setting of TTL for table data is not appropriate. How to adjust it?
This requires the use of nsclient to make the modification, and a regular client cannot achieve this. The startup method and commands for nsclient can be found in [ns client](https://chat.openai.com/maintain/cli.md#ns-client).

In nsclient, you can use the `setttl` command to change the ttl for a table, similar to:

```
setttl table_name ttl_type ttl [ttl] [index_name]
```
As can be seen, if the name of the index is configured at the end of the command, it is possible to only modify the ttl of a single index.
```{caution}
Changes made with `setttl` will not take effect immediately and will be influenced by the `gc_interval` configuration of the tablet server. (The configuration is independent for each tablet server and does not affect others.)

For example, if the `gc_interval` for a tablet server is set to 1 hour, then the ttl configuration reload will take place at the end of the next gc (in the worst case, it will reload after 1 hour). During this gc, the data eviction will not follow the latest ttl. It will only use the updated ttl in the subsequent gc.

Therefore, **after changing ttl, you need to wait for two cycles of the gc interval for it to take effect**. Please be patient.

Of course, you can adjust the `gc_interval` for the tablet server, but this configuration cannot be changed dynamically and takes effect only after a restart. So, if there is significant memory pressure, consider scaling or migrating data shards to reduce memory pressure. It is not recommended to adjust `gc_interval` lightly.
```

### 3. A warning log appears: Last Join right table is empty, what does this mean?
Generally speaking, this is a normal phenomenon and does not represent cluster anomalies. The right table of the join in the runner may be empty, which is likely due to data issues.

