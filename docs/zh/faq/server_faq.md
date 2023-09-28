# Server FAQ

Server中有任何上下线变化或问题，都先openmldb_tool status + inspect online检查下集群是否正常。

## 部署和启动 FAQ

### 1. 如何确认集群已经正常运行？
虽然有一键启动脚本，但由于配置繁多，可能出现“端口已被占用”，“目录无读写权限”等问题。这些问题都是server进程运行之后才能发现，退出后没有及时反馈。（如果配置了监控，可以通过监控直接检查。）
所以，请先确认集群的所有server进程都正常运行。

可以通过`ps axu | grep openmldb`或sql命令`show components;`来查询。（注意，如果你使用了守护进程，openmldb server进程可能是在启动停止的循环中，并不代表持续运行，可以通过日志或`show components;`连接时间来确认。）

如果进程都活着，集群还是表现不正常，需要查询一下server日志。可以优先看WARN和ERROR级日志，很大概率上，它们就是根本原因。

### 2. 如果数据没有自动恢复成功怎么办？

通常情况，当我们重启服务，表中数据会自动进行恢复，但有些情况可能会造成恢复失败，通常失败的情况包括：

- tablet异常退出
- 多副本表多个副本所在的tablets同时重启或者重启太快，造成某些`auto_failover`操作还没完成tablet就重启
- auto_failover设成`false`

当服务启动成功后，可以通过`gettablestatus`获得所有表的状态：
```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=gettablestatus
```

如果表中有`Warnings`，可以通过`recoverdata`来自动恢复数据：
```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
```

## Server FAQ

### 1. 为什么日志中有 Fail to write into Socket 的警告日志？
```
http_rpc_protocol.cpp:911] Fail to write into Socket{id=xx fd=xx addr=xxx} (0x7a7ca00): Unknown error 1014 [1014]
```
这是server端会打印的日志。一般是client端使用了连接池或短连接模式，在RPC超时后会关闭连接，server写回response时发现连接已经关了就报这个错。Got EOF就是指之前已经收到了EOF（对端正常关闭了连接）。client端使用单连接模式server端一般不会报这个。

### 2. 表数据的ttl初始设置不合适，如何调整？
这需要使用nsclient来修改，普通client无法做到。nsclient启动方式与命令，见[ns client](../maintain/cli.md#ns-client)。

在nsclient中使用命令`setttl`可以更改一个表的ttl，类似
```
setttl table_name ttl_type ttl [ttl] [index_name]
```
可以看到，如果在命令末尾配置index的名字，可以做到只修改单个index的ttl。
```{caution}
`setttl`的改变不会及时生效，会受到tablet server的配置`gc_interval`的影响。（每台tablet server的配置是独立的，互不影响。）

举例说明，有一个tablet server的`gc_interval`是1h，那么ttl的配置重载，会在下一次gc的最后时刻进行（最坏情况下，会在1h后重载）。重载ttl的这一次gc就不会按最新ttl来淘汰数据。再下一次gc时才会使用最新ttl进行数据淘汰。

所以，**ttl更改后，需要等待两次gc interval的时间才会生效**。请耐心等待。

当然，你可以调整tablet server的`gc_interval`，但这个配置无法动态更改，只能重启生效。所以，如果内存压力较大，可以尝试扩容，迁移数据分片，来减少内存压力。不推荐轻易调整`gc_interval`。
```

### 3. 出现警告日志：Last Join right table is empty，这是什么意思？
通常来讲，这是一个正常现象，不代表集群异常。只是runner中join右表为空，是可能的现象，大概率是数据问题。

