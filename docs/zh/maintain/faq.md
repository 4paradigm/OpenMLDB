# 运维 FAQ

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
这是由于client端本身发送的rpc request的timeout设置小了，client端自己主动断开，注意这是rpc的超时。需要更改通用的`request_timeout`配置。
1. CLI: 启动时配置`--request_timeout_ms`
2. JAVA/Python SDK: Option或url中调整`SdkOption.requestTimeout`
```{note}
同步的离线命令通常不会出现这个错误，因为同步离线命令的timeout设置为了TaskManager可接受的最长时间。
```
### 2. 为什么收到 Got EOF of Socket 的警告日志？
```
rpc_client.h:xxx] request error. [E1014]Got EOF of Socket{id=x fd=x addr=xxx} (xx)
```
这是因为`addr`端主动断开了连接，`addr`的地址大概率是TaskManager。这不代表TaskManager不正常，而是TaskManager端认为这个连接没有活动，超过keepAliveTime了，而主动断开通信channel。
在0.5.0及以后的版本中，可以调大TaskManager的`server.channel_keep_alive_time`来提高对不活跃channel的容忍度。默认值为1800s(0.5h)，特别是使用同步的离线命令时，这个值可能需要适当调大。
在0.5.0以前的版本中，无法更改此配置，请升级TaskManager版本。

### 3. 离线查询结果显示中文为什么乱码？

在使用离线查询时，可能出现包含中文的查询结果乱码，主要和系统默认编码格式与Spark任务编码格式参数有关。

如果出现乱码情况，可以通过添加Spark高级参数`spark.driver.extraJavaOptions=-Dfile.encoding=utf-8`和`spark.executor.extraJavaOptions=-Dfile.encoding=utf-8`来解决。

客户端配置方法可参考[客户端Spark配置文件](../reference/client_config/client_spark_config.md)，也可以在TaskManager配置文件中添加此项配置。

```
spark.default.conf=spark.driver.extraJavaOptions=-Dfile.encoding=utf-8;spark.executor.extraJavaOptions=-Dfile.encoding=utf-8
```

### 4. 如何配置TaskManager来访问开启Kerberos的Yarn集群？

如果Yarn集群开启Kerberos认证，TaskManager可以通过添加以下配置来访问开启Kerberos认证的Yarn集群。注意请根据实际配置修改keytab路径以及principal账号。

```
spark.default.conf=spark.yarn.keytab=/tmp/test.keytab;spark.yarn.principal=test@EXAMPLE.COM
```

### 5. 如何配置客户端的core日志？

客户端core日志主要有两种，zk日志和sdk日志（glog日志），两者是独立的。

zk日志：
1. CLI：启动时配置`--zk_log_level`调整level,`--zk_log_file`配置日志保存文件。
2. JAVA/Python SDK：Option或url中使用`zkLogLevel`调整level，`zkLogFile`配置日志保存文件。

- `zk_log_level`(int, 默认=0, 即DISABLE_LOGGING): 
打印这个等级及**以下**等级的日志。0-禁止所有zk log, 1-error, 2-warn, 3-info, 4-debug。

sdk日志（glog日志）：
1. CLI：启动时配置`--glog_level`调整level,`--glog_dir`配置日志保存文件。
2. JAVA/Python SDK：Option或url中使用`glogLevel`调整level，`glogDir`配置日志保存文件。

- `glog_level`(int, 默认=1, 即WARNING):
打印这个等级及**以上**等级的日志。 INFO, WARNING, ERROR, and FATAL日志分别对应 0, 1, 2, and 3。


### 6. 插入错误，日志显示`please use getInsertRow with ... first`

在JAVA client使用InsertPreparedStatement进行插入，或在Python中使用sql和parameter进行插入时，client底层实际有cache影响，第一步`getInsertRow`生成sql cache并返回sql还需要补充的parameter信息，第二步才会真正执行insert，而执行insert需要使用第一步缓存的sql cache。所以，当多线程使用同一个client时，可能因为插入和查询频繁更新cache表，将你想要执行的insert sql cache淘汰掉了，所以会出现好像第一步`getInsertRow`并未执行的样子。

目前可以通过调大`maxSqlCacheSize`这一配置项来避免错误。仅JAVA/Python SDK支持配置。

### 7. 离线命令错误`java.lang.OutOfMemoryError: Java heap space`

离线命令的spark配置默认为`local[*]`，并发较高可能出现OutOfMemoryError错误，请调整`spark.driver.memory`和`spark.executor.memory`两个spark配置项。可以写在taskmanager运行目录的`conf/taskmanager.properties`的`spark.default.conf`并重启taskmanager，或者使用CLI客户端进行配置，参考[客户端Spark配置文件](../reference/client_config/client_spark_config.md)。
```
spark.default.conf=spark.driver.memory=16g;spark.executor.memory=16g
```
