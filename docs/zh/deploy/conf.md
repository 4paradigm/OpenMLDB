# 配置文件

## nameserver配置文件 conf/nameserver.flags

```
# nameserver.conf 
--endpoint=127.0.0.1:6527
--role=nameserver
# 如果是部署单机版不需要配置zk_cluster和zk_root_path，把这俩配置注释即可. 部署集群版需要配置这两项，一个集群中所有节点的这两个配置必须保持一致
#--zk_cluster=127.0.0.1:7181
#--zk_root_path=/openmldb_cluster
# 单机版需要指定tablet的地址, 集群版此配置可忽略
--tablet=127.0.0.1:9921
# 配置log目录
--openmldb_log_dir=./logs
# 配置是否开启自动恢复。如果开启的话节点挂掉会自动执行leader切换，节点进程起来之后会自动恢复数据
--auto_failover=true

# 配置线程池大小，不需要修改
#--thread_pool_size=16
# 配置失败重试次数，默认是3
#--request_max_retry=3
# 配置请求超时时间，单位是毫秒，默认是12秒
#--request_timeout_ms=12000
# 配置请求不可达时的重试间隔，一般不需要修改
#--request_sleep_time=1000
# 配置zookeeper session超时时间，单位是毫秒
--zk_session_timeout=10000
# 配置zookeeper健康检查间隔，单位是毫秒，一般不需要修改
#--zk_keep_alive_check_interval=15000
# 配置tablet心跳检测超时时间，单位是毫秒，默认是1分钟。如果tablet超过这个时间还没连接上，nameserver就认为此tablet不可用，会执行下线该节点的操作
--tablet_heartbeat_timeout=60000
# 配置tablet健康检查间隔，单位是毫秒
#--tablet_offline_check_interval=1000

# 执行高可用任务的队列数
#--name_server_task_pool_size=8
# 执行高可用任务的并发数
#--name_server_task_concurrency=2
# 执行高可用任务的最大并发数
#--name_server_task_max_concurrency=8
# 执行任务时检查任务的等待时间，单位是毫秒
#--name_server_task_wait_time=1000
# 执行任务的最大时间，如果超过后就会打日志，单位是毫秒
#--name_server_op_execute_timeout=7200000
# 获取任务的时间间隔，单位是毫秒
#--get_task_status_interval=2000
# 获取表状态的时间间隔，单位是毫秒
#--get_table_status_interval=2000
# 检查binlog同步进度的最小差值，如果主从offset小于这个值任务已同步成功
#--check_binlog_sync_progress_delta=100000
# 保存的最大任务数，如果超过这个值就会删除已完成和执行失败的op
#--max_op_num=10000

# 建表默认的副本数
#--replica_num=3
# 建表默认的分片数
#--partition_num=8
# 系统表默认的副本数
--system_table_replica_num=2
```

## tablet配置文件 conf/tablet.flags

```
# tablet.conf
# 是否使用别名
#--use_name=false
# 启动的端口号，如果指定了endpoint就不需要指定port
#--port=9527
# 启动的ip/域名和端口号
--endpoint=127.0.0.1:9921
# 启动的角色，不可修改
--role=tablet

# 如果启动集群版需要指定zk的地址和集群在zk的节点路径
#--zk_cluster=127.0.0.1:7181
#--zk_root_path=/openmldb_cluster

# 配置线程池大小，建议和cpu核数一致
--thread_pool_size=24
# zk session的超时时间，单位为毫秒
--zk_session_timeout=10000
# 检查zk状态的时间间隔，单位为毫秒
#--zk_keep_alive_check_interval=15000

# 日志文件路径
--openmldb_log_dir=./logs

# 配置tablet最大内存使用, 如果超过配置的值写入就会失败. 默认值为0即不限制
#--max_memory_mb=0

# binlog conf
# binlog没有新数据添加时的等待时间，单位是毫秒
#--binlog_coffee_time=1000
# 主从匹配offset的等待时间，单位是毫秒
#--binlog_match_logoffset_interval=1000
# 有数据写入时是否通知立马同步到follower
--binlog_notify_on_put=true
# binlog文件的最大大小，单位时M
--binlog_single_file_max_size=2048
# 主从同步的batch大小
#--binlog_sync_batch_size=32
# binlog sync到磁盘的时间间隔，单位是毫秒
--binlog_sync_to_disk_interval=5000
# 如果没有新数据同步时的wait时间，单位为毫秒
#--binlog_sync_wait_time=100
# binlog文件名长度
#--binlog_name_length=8
# 删除binlog文件的时间间隔，单位是毫秒
#--binlog_delete_interval=60000
# binlog是否开启crc校验
#--binlog_enable_crc=false

# 执行io相关操作的线程池大小
#--io_pool_size=2
# 执行删除表，发送snapshot, load snapshot等任务的线程池大小
#--task_pool_size=8
# 配置是否要把表drop后数据放在recycle目录，默认是true
#--recycle_bin_enabled=true
# 配置recycle目录里数据的保存时间，如果超过这个时间就会删除对应的目录和数据。默认为0表示永远不删除, 单位是分钟
#--recycle_ttl=0

# 内存表数据文件路径
# 配置数据目录，多个磁盘使用英文符号, 隔开
--db_root_path=./db
# 配置数据回收站目录，drop表的数据就会放在这里
--recycle_bin_root_path=./recycle
#
# HDD表数据文件路径（可选，默认为空）
# 配置数据目录，多个磁盘使用英文符号, 隔开
--hdd_root_path=./db_hdd
# 配置数据回收站目录，drop表的数据就会放在这里
--recycle_bin_hdd_root_path=./recycle_hdd
#
# SSD表数据文件路径（可选，默认为空）
# 配置数据目录，多个磁盘使用英文符号, 隔开
--ssd_root_path=./db_ssd
# 配置数据回收站目录，drop表的数据就会放在这里
--recycle_bin_ssd_root_path=./recycle_ssd

# snapshot conf
# 配置做snapshot的时间，配置为一天中的几点。如23就表示每天23点做snapshot
--make_snapshot_time=23
# 做snapshot的检查时间间隔，单位是毫秒
#--make_snapshot_check_interval=600000
# 做snapshot的offset阈值，如果和上次snapshot的offset差值小于这个值就不会生成新的snapshot
#--make_snapshot_threshold_offset=100000
# snapshot线程池大小
#--snapshot_pool_size=1
# snapshot是否开启压缩。可以设置为off，zlib, snappy
#--snapshot_compression=off

# garbage collection conf
# 执行内存表（即storage_mode=Memory）过期删除的时间间隔，单位是分钟
--gc_interval=60
# 执行磁盘表（即storage_mode=HDD/SSD）过期删除的时间间隔，单位是分钟
--disk_gc_interval=60
# 执行过期删除的线程池大小
--gc_pool_size=2

# send file conf
# 发送文件的最大重试次数
#--send_file_max_try=3
# 发送文件时的块大小
#--stream_block_size=1048576
# 发送文件时的带宽限制，默认是20M/s
--stream_bandwidth_limit=20971520
# rpc请求的最大重试次数
#--request_max_retry=3
# rpc的超时时间，单位是毫秒
#--request_timeout_ms=5000
# 如果发生异常的重试等待时间，单位是毫秒
#--request_sleep_time=1000
# 文件发送失败的重试等待时间，单位是毫秒
#--retry_send_file_wait_time_ms=3000
#
# 内存表配置
# 第一层跳表的最大高度
#--skiplist_max_height=12
# 第二层跳表的最大高度
#--key_entry_max_height=8

# 查询配置
# 最大扫描条数(全表扫描/全表聚合)，默认：0
#--max_traverse_cnt=0
# 最大扫描不同key的个数(批处理)，默认：0
#--max_traverse_key_cnt=0
# 结果最大大小（byte)，默认：2MB
#--scan_max_bytes_size=2097152

# loadtable
# load时給线程池提交一次任务的数据条数
#--load_table_batch=30
# 加载snapshot文件的线程数
#--load_table_thread_num=3
# load线程池的最大队列长度
#--load_table_queue_size=1000

# rocksdb相关配置
#--disable_wal=true
# 文件是否压缩, 支持的压缩格式为pz, lz4, zlib
#--file_compression=off
#--block_cache_mb=4096
#--block_cache_shardbits=8
#--verify_compression=false
#--max_log_file_size=100 * 1024 * 1024
#--keep_log_file_num=5
```

## apiserver配置文件 conf/apiserver.flags
```
# apiserver.conf
# 配置启动apiserver的ip/域名和端口号
--endpoint=127.0.0.1:8080
# role不可以改动
--role=apiserver
# 如果部署的openmldb是单机版，需要指定nameserver的地址
--nameserver=127.0.0.1:6527
# 如果部署的openmldb是集群版，需要指定zk地址和集群zk节点目录
#--zk_cluster=127.0.0.1:7181
#--zk_root_path=/openmldb_cluster

# 配置日志路径
--openmldb_log_dir=./logs

# 配置线程池大小
#--thread_pool_size=16
```


## TaskManager配置文件 conf/taskmanager.properties

```
# Server Config
server.host=0.0.0.0
server.port=9902
server.worker_threads=4
server.io_threads=4
server.channel_keep_alive_time=1800
prefetch.jobid.num=1
job.log.path=./logs/
external.function.dir=./udf/
track.unfinished.jobs=true
job.tracker.interval=30

# OpenMLDB Config
zookeeper.cluster=0.0.0.0:2181
zookeeper.root_path=/openmldb
zookeeper.session_timeout=5000
zookeeper.connection_timeout=5000
zookeeper.max_retries=10
zookeeper.base_sleep_time=1000
zookeeper.max_connect_waitTime=30000

# Spark Config
spark.home=
spark.master=local
spark.yarn.jars=
spark.default.conf=
spark.eventLog.dir=
spark.yarn.maxAppAttempts=1
batchjob.jar.path=
namenode.uri=
offline.data.prefix=file:///tmp/openmldb_offline_storage/
hadoop.conf.dir=
#enable.hive.support=false
```

* 如果没有配置`spark.home`，则需要在TaskManager运行的环境变量配置`SPARK_HOME`。
* `spark.master`默认配置为`local`，可以配置为`local[*]`、`yarn`、`yarn-cluster`、`yarn-client`等。如果配置为Yarn集群模式，则需要修改`offline.data.prefix`配置为HDFS路径，避免保存离线文件到Yarn容器本地，同时需要配置环境变量`HADOOP_CONF_DIR`为Hadoop配置文件所在目录。
