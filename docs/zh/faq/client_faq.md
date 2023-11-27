# Client FAQ

## fail to get tablet ... 的错误日志

优先检查集群中tablet server是否意外下线，或者在线表是否不可读写。推荐通过[openmldb_tool](../maintain/diagnose.md)诊断，使用`status`（status --diff）和`inspect online`两个检查命令。
TODO diag tool 测到offline或online表不正常，会输出警告和下一步应该怎么操作？
如果只能手动检查，需要两步：
- `show components`，检查server是否存在在列表中（TaskManager如果下线，将不在表中。Tablet如果下线，将在表中，但状态为offline）,以及在列表中的server的状态是否为online。如果存在offline的server，**先将server重启加入集群**。
- `show table status like '%'`（低版本如果不支持like，需要分别查询系统db和用户db），检查每个表的"Warnings"是否报错。

一般会得到`real replica number X does not match the configured replicanum X`等错误，具体错误信息请参考[SHOW TABLE STATUS](../openmldb_sql/ddl/SHOW_TABLE_STATUS.md)。这些错误都说明表目前是有问题的，无法提供正常读写功能，通常是由于Tablet

## 为什么收到 Reached timeout 的警告日志？
```
rpc_client.h:xxx] request error. [E1008] Reached timeout=xxxms
```
这是由于client端本身发送的rpc request的timeout设置小了，client端自己主动断开，注意这是rpc的超时。需要更改通用的`request_timeout`配置。
1. CLI: 启动时配置`--request_timeout_ms`
2. JAVA/Python SDK: Option或url中调整`SdkOption.requestTimeout`
```{note}
同步的离线命令通常不会出现这个错误，因为同步离线命令的timeout设置为了TaskManager可接受的最长时间。
```

## 为什么收到 Got EOF of Socket 的警告日志？
```
rpc_client.h:xxx] request error. [E1014]Got EOF of Socket{id=x fd=x addr=xxx} (xx)
```
这是因为`addr`端主动断开了连接，`addr`的地址大概率是TaskManager。这不代表TaskManager不正常，而是TaskManager端认为这个连接没有活动，超过keepAliveTime了，而主动断开通信channel。
在0.5.0及以后的版本中，可以调大TaskManager的`server.channel_keep_alive_time`来提高对不活跃channel的容忍度。默认值为1800s(0.5h)，特别是使用同步的离线命令时，这个值可能需要适当调大。
在0.5.0以前的版本中，无法更改此配置，请升级TaskManager版本。

## 离线查询结果显示中文为什么乱码？

在使用离线查询时，可能出现包含中文的查询结果乱码，主要和系统默认编码格式与Spark任务编码格式参数有关。

如果出现乱码情况，可以通过添加Spark高级参数`spark.driver.extraJavaOptions=-Dfile.encoding=utf-8`和`spark.executor.extraJavaOptions=-Dfile.encoding=utf-8`来解决。

客户端配置方法可参考[客户端Spark配置文件](../reference/client_config/client_spark_config.md)，也可以在TaskManager配置文件中添加此项配置。

```
spark.default.conf=spark.driver.extraJavaOptions=-Dfile.encoding=utf-8;spark.executor.extraJavaOptions=-Dfile.encoding=utf-8
```

## 如何配置TaskManager来访问开启Kerberos的Yarn集群？

如果Yarn集群开启Kerberos认证，TaskManager可以通过添加以下配置来访问开启Kerberos认证的Yarn集群。注意请根据实际配置修改keytab路径以及principal账号。

```
spark.default.conf=spark.yarn.keytab=/tmp/test.keytab;spark.yarn.principal=test@EXAMPLE.COM
```

## 如何配置客户端的core日志？

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


## 插入错误，日志显示`please use getInsertRow with ... first`

在JAVA client使用InsertPreparedStatement进行插入，或在Python中使用sql和parameter进行插入时，client底层实际有cache影响，第一步`getInsertRow`生成sql cache并返回sql还需要补充的parameter信息，第二步才会真正执行insert，而执行insert需要使用第一步缓存的sql cache。所以，当多线程使用同一个client时，可能因为插入和查询频繁更新cache表，将你想要执行的insert sql cache淘汰掉了，所以会出现好像第一步`getInsertRow`并未执行的样子。

目前可以通过调大`maxSqlCacheSize`这一配置项来避免错误。仅JAVA/Python SDK支持配置。

## 离线命令Spark报错

```
java.lang.OutOfMemoryError: Java heap space
```

```
Container killed by YARN for exceeding memory limits. 5 GB of 5 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead.
```

出现以上几种日志时，说明离线任务所需资源多于当前配置。一般是这几种情况：
- 离线命令的Spark配置`local[*]`，机器核数较多，并发度很高，资源占用过大
- memory配置较小

如果是local模式，单机资源比较有限，可以考虑降低并发度。如果不降低并发，请调整`spark.driver.memory`和`spark.executor.memory`两个spark配置项。可以写在TaskManager运行目录的`conf/taskmanager.properties`的`spark.default.conf`并重启TaskManager，或者使用CLI客户端进行配置，参考[客户端Spark配置文件](../reference/client_config/client_spark_config.md)。
```
spark.default.conf=spark.driver.memory=16g;spark.executor.memory=16g
```

master为local时，不是调整executor的，而是driver的memory，如果你不确定，可以两者都调节。
