# Configuration File

## The Configuration File for Nameserver: conf/nameserver.flags

```
# nameserver.conf
--endpoint=127.0.0.1:6527
--role=nameserver
# If you are deploying the standalone version, you do not need to configure zk_cluster and zk_root_path, just comment these two configurations. Deploying the cluster version needs to configure these two items, and the two configurations of all nodes in a cluster must be consistent
#--zk_cluster=127.0.0.1:7181
#--zk_root_path=/openmldb_cluster
# The address of the tablet needs to be specified in the standalone version, and this configuration can be ignored in the cluster version
--tablet=127.0.0.1:9921
# Configure log directory
--openmldb_log_dir=./logs
# Configure whether to enable automatic recovery. If it is enabled, the node will automatically perform the leader switch if it hangs, and the data will be automatically restored after the node process starts.
--auto_failover=true

# Configure the thread pool size, no need to modify
#--thread_pool_size=16
# Configure the number of retry attempts, the default is 3
#--request_max_retry=3
# Configure the request timeout in milliseconds, the default is 12 seconds
#--request_timeout_ms=12000
# Configure the retry interval when the request is unreachable, generally do not need to be modified, in milliseconds
#--request_sleep_time=1000
# Configure the zookeeper session timeout in milliseconds
--zk_session_timeout=10000
# Configure the zookeeper health check interval, the unit is milliseconds, generally do not need to be modified
#--zk_keep_alive_check_interval=15000
# Configure the timeout period for tablet heartbeat detection in milliseconds, the default is 1 minute. If the tablet is still unreachable after this time, the nameserver considers that the tablet is unavailable and will perform the operation of offline the node
--tablet_heartbeat_timeout=60000
# Configure the tablet health check interval, in milliseconds
#--tablet_offline_check_interval=1000

# The number of queues to perform high-availability tasks
#--name_server_task_pool_size=8
# The number of concurrent execution of high-availability tasks
#--name_server_task_concurrency=2
# The maximum number of concurrent execution of high-availability tasks
#--name_server_task_max_concurrency=8
# Check the waiting time of the task when executing the task in milliseconds
#--name_server_task_wait_time=1000
# The maximum time to execute the task, if it exceeds, it will log. The unit is milliseconds
#--name_server_op_execute_timeout=7200000
# The time interval of receiving the status of the next task in milliseconds
#--get_task_status_interval=2000
# The time interval of receiving the status of the next table in milliseconds
#--get_table_status_interval=2000
# Check the minimum difference of binlog synchronization progress, if the master-slave offset is less than this value, the task has been successfully synchronized
#--check_binlog_sync_progress_delta=100000
# The maximum number of tasks to save, if this value is exceeded, completed and failed ops will be deleted
#--max_op_num=10000

# Create the default number of replicas for the table
#--replica_num=3
# Create the default number of shards for a table
#--partition_num=8
# The default number of replicas for system tables
--system_table_replica_num=2
```

## The Configuration File for Tablet: conf/tablet.flags

```
# tablet.conf
# Whether to use aliases
#--use_name=false
# The port number to start, if the endpoint is specified, it is not necessary to specify the port
#--port=9527
# Startup ip/domain name and port number
--endpoint=127.0.0.1:9921
# The role to start, cannot be modified
--role=tablet

# If you start the cluster version, you need to specify the address of zk and the node path of the cluster in zk
#--zk_cluster=127.0.0.1:7181
#--zk_root_path=/openmldb_cluster

# Configure the thread pool size, it is recommended to be consistent with the number of CPU cores
--thread_pool_size=24
# zk session timeout, in milliseconds
--zk_session_timeout=10000
# Interval for checking zk status, in milliseconds
#--zk_keep_alive_check_interval=15000

# log file path
--openmldb_log_dir=./logs

# Specify the max memory usage of tablet. If memory usage exceeds the value, write will fail. The default value 0 means unlimited
#--max_memory_mb=0

# binlog conf
# Binlog wait time when no new data is added, in milliseconds
#--binlog_coffee_time=1000
# Master-slave matching offset waiting time, in milliseconds
#--binlog_match_logoffset_interval=1000
# Whether to notify the follower to synchronize immediately when data is written
--binlog_notify_on_put=true
# The maximum size of the binlog file, in MB
--binlog_single_file_max_size=2048
# Master-slave synchronization batch size
#--binlog_sync_batch_size=32
# The interval between binlog sync and disk, in milliseconds
--binlog_sync_to_disk_interval=5000
# The wait time when there is no new data synchronization, in milliseconds
#--binlog_sync_wait_time=100
# binlog filename length
#--binlog_name_length=8
# The interval for deleting binlog files, in milliseconds
#--binlog_delete_interval=60000
# Whether binlog enables crc verification
#--binlog_enable_crc=false

# Thread pool size for performing io-related operations
#--io_pool_size=2
# The thread pool size for tasks such as deleting tables, sending snapshots, load snapshots, etc.
#--task_pool_size=8
# Configure whether to put the table drop data in the recycle directory, the default is true
#--recycle_bin_enabled=true
# Configure the storage time of data in the recycle directory. If this time is exceeded, the corresponding directory and data will be deleted. The default is 0 means never delete, the unit is minutes
#--recycle_ttl=0

# Configure the data directory, multiple disks are separated by commas
--db_root_path=./db
# Configure the data recycle bin directory, the data of the drop table will be placed here
--recycle_bin_root_path=./recycle
#
#Configure HDD table data file path (optional, default is empty), use English commas for multiple disks
--hdd_root_path=./db_hdd
#Configure the recycle bin directory, use English commas for multiple disks
--recycle_bin_hdd_root_path=./recycle_hdd
#
#Configure the SSD table data file path (optional, default is empty), use English commas for multiple disks
--ssd_root_path=./db_ssd
#Configure the data recycle bin directory, where the data of the drop table will be placed
--recycle_bin_ssd_root_path=./recycle_ssd

# snapshot conf
# Configure the time to do snapshots, the time of day. For example, 23 means taking a snapshot at 23 o'clock every day.
--make_snapshot_time=23
# Check interval for snapshots, in milliseconds
#--make_snapshot_check_interval=600000
# Set the offset threshold of the snapshot, if the offset difference from the last snapshot is less than this value, no new snapshot will be generated, in milliseconds
#--make_snapshot_threshold_offset=100000
# snapshot thread pool size
#--snapshot_pool_size=1
# Whether snapshot compression is enabled. Which can be set to off, zlib, snappy
#--snapshot_compression=off

# garbage collection conf
# The time interval for performing expired deletion, in minutes
--gc_interval=60
# Thread pool size to perform expired deletion
--gc_pool_size=2

# send file conf
# The Maximum number of retry attempts to send a file
#--send_file_max_try=3
# block size when sending files
#--stream_block_size=1048576
# Bandwidth limit when sending files, the default is 20M/s
--stream_bandwidth_limit=20971520
# The maximum number of retry attempts for rpc requests
#--request_max_retry=3
# rpc timeout, in milliseconds
#--request_timeout_ms=5000
# If an exception occurs, the retry wait time, in milliseconds
#--request_sleep_time=1000
# Retry wait time for file sending failure, in milliseconds
#--retry_send_file_wait_time_ms=3000
#
# table conf
# The maximum height of the first level skip list
#--skiplist_max_height=12
# The maximum height of the second level skip list
#--key_entry_max_height=8

# query conf
# max table traverse iteration（full table scan/aggregation）,default: 50000
#--max_traverse_cnt=50000
# max table traverse pk number（batch query）, default: 5000
#--max_traverse_pk_cnt=5000
# max result size in byte (default: 2MB)
#--scan_max_bytes_size=2097152

# loadtable
# The number of data bars to submit a task to the thread pool when loading
#--load_table_batch=30
# Number of threads to load snapshot files
#--load_table_thread_num=3
# The maximum queue length of the load thread pool
#--load_table_queue_size=1000

# for rocksdb
#--disable_wal=true
# Type of compression, can be off, pz, lz4, zlib
#--file_compression=off
#--block_cache_mb=4096
#--block_cache_shardbits=8
#--verify_compression=false
#--max_log_file_size=100 * 1024 * 1024
#--keep_log_file_num=5
```

## The Configuration file for APIServer: conf/apiserver.flags

```
# apiserver.conf
# Configure the ip/domain name and port number to start the apiserver
--endpoint=127.0.0.1:8080
# role cannot be changed
--role=apiserver
# If the deployed openmldb is a standalone version, you need to specify the address of the nameserver
--nameserver=127.0.0.1:6527
# If the deployed openmldb is a cluster version, you need to specify the zk address and the cluster zk node directory
#--zk_cluster=127.0.0.1:7181
#--zk_root_path=/openmldb_cluster

# configure log path
--openmldb_log_dir=./logs

# Configure thread pool size
#--thread_pool_size=16
```

## he Configuration file for TaskManager: conf/taskmanager.properties

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
```

* If configuration of `spark.home` is not set，please set the environment variable of `SPARK_HOME` in TaskManager server.
* The default value of configruation `spark.master` is `local`. We can set `local[*]`, `yarn`, `yarn-cluster` or `yarn-client` as well. If we are using Yarn mode, please set configuration `offline.data.prefix` as the HDFS path to avoid saving offline data in local filesystem of Yarn containers. Meanwhile we need to set environment variable of `HADOOP_CONF_DIR` as the directory of Hadoop configuration files.
