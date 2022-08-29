# 运维 FAQ

## 部署和启动 FAQ

### 1. 如何确认集群已经正常运行？
虽然有一键启动脚本，但由于配置繁多，可能出现“端口已被占用”，“目录无读写权限”等问题。这些问题都是server进程运行之后才能发现，退出后没有及时反馈。（如果配置了监控，可以通过监控直接检查。）
所以，请先确认集群的所有server进程都正常运行。

可以通过`ps axu | grep openmldb`来查询。（注意，官方运行脚本中使用`mon`作为守护进程，但`mon`进程运行不代表openmldb server进程正在运行。）

如果进程都活着，集群还是表现不正常，需要查询一下server日志。可以优先看WARN和ERROR级日志，很大概率上，它们就是根本原因。

## Server FAQ

### 1. 为什么日志中有 Fail to write into Socket 的警告日志？
```
http_rpc_protocol.cpp:911] Fail to write into Socket{id=xx fd=xx addr=xxx} (0x7a7ca00): Unknown error 1014 [1014]
```
这是server端会打印的日志。一般是client端使用了连接池或短连接模式，在RPC超时后会关闭连接，server写回response时发现连接已经关了就报这个错。Got EOF就是指之前已经收到了EOF（对端正常关闭了连接）。client端使用单连接模式server端一般不会报这个。

### 2. 表数据的ttl初始设置不合适，如何调整？
这需要使用nsclient来修改，普通client无法做到。nsclient启动方式与命令，见[ns client](../reference/cli.md#ns-client)。

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

## Client FAQ

### 1. 为什么收到 Reached timeout 的警告日志？
```
rpc_client.h:xxx] request error. [E1008] Reached timeout=xxxms
```
这是由于client端本身发送的rpc request的timeout设置小了，client端自己主动断开。注意，这是rpc的超时。

分为以下情况处理：
#### 同步的离线job
在使用同步的离线命令时，容易出现这个情况。你可以使用
```sql
> SET @@job_timeout = "600000";
```
来调大rpc的timeout时间，单位为ms。
#### 普通请求
如果是简单的query或insert，都会出现超时，需要更改通用的`request_timeout`配置。
1. CLI: 启动时配置`--request_timeout_ms`
2. JAVA: SDK 直连，调整`SdkOption.requestTimeout`; JDBC，调整url中的参数`requestTimeout`
3. Python: SDK 直连(DBAPI), 调整`connect()`参数`request_timeout`; SQLAlchemy, 调整url中的参数`requestTimeout`

### 2. 为什么收到 Got EOF of Socket 的警告日志？
```
rpc_client.h:xxx] request error. [E1014]Got EOF of Socket{id=x fd=x addr=xxx} (xx)
```
这是因为`addr`端主动断开了连接，`addr`的地址大概率是taskmanager。这不代表taskmanager不正常，而是taskmanager端认为这个连接没有活动，超过keepAliveTime了，而主动断开通信channel。
在0.5.0及以后的版本中，可以调大taskmanager的`server.channel_keep_alive_time`来提高对不活跃channel的容忍度。默认值为1800s(0.5h)，特别是使用同步的离线命令时，这个值可能需要适当调大。
在0.5.0以前的版本中，无法更改此配置，请升级taskmanager版本。

### 3. 离线查询结果显示中文乱码

在使用离线查询时，可能出现包含中文的查询结果乱码，主要和系统默认编码格式与Spark任务编码格式参数有关。

如果出现乱码情况，可以通过添加Spark高级参数`spark.driver.extraJavaOptions=-Dfile.encoding=utf-8`和`spark.executor.extraJavaOptions=-Dfile.encoding=utf-8`来解决。

客户端配置方法可参考[客户端Spark配置文件](../reference/client_config/client_spark_config.md)，也可以在TaskManager配置文件中添加此项配置。

```
spark.default.conf=spark.driver.extraJavaOptions=-Dfile.encoding=utf-8;spark.executor.extraJavaOptions=-Dfile.encoding=utf-8
```
