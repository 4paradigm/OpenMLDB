# RTIDB配置说明文档

## 通用配置
**endpoint**  指定节点的ip和port, 示例 127.0.0.0:9991

**zk_cluster**  指定连接zookeeper集群的地址, 如果zookeeper是多节点用逗号分开, 示例 172.27.2.51:6330,172.27.2.52:6331

**zk_root_path**  rtidb和nameserver在zk写入数据的根节点, 示例 /rtidb

zk_session_timeout  设置zk session的超时时间, 单位是ms

zk_keep_alive_check_interval  检查和zk连接的时间间隔, 如果断开会自动重连. 单位是ms

**gc_interval**  执行过期键删除任务的时间间隔, 单位是分钟

gc_safe_offset  ttl时间的偏移, 单位是分钟. 一般不用配置该项

gc_on_table_recover_count  loadtable时每隔多少条执行一次过期键删除, 单位是记录条数

mem_release_rate  用来设置tcmalloc SetMemoryReleaseRate

**thread_pool_size**  配置brpc内部占用线程数

**request_timeout_ms**  请求的超时时间, 单位是ms

request_sleep_time  请求失败时的等待时间

request_max_retry  请求失败时的最大重试次数



## tablet配置
**db_root_path**  配置binlog和snapshot的存放目录

**recycle_bin_root_path**  配置droptable回收站的目录

**scan_concurrency_limit**  配置scan的最大并发数

**put_concurrency_limit**  配置put的最大并发数

**get_concurrency_limit**  配置get的最大并发数

stream_wait_time_ms  streaming发送数据失败时的等待时间, 单位是ms

stream_close_wait_time_ms  streaming发送数据完毕后的等待时间, 单位是ms

stream_block_size  streaming一次发送数据块的最大值, 单位是byte

**stream_bandwidth_limit**  streaming方式发送数据时的最大速度, 单位是byte/s

**make_snapshot_time**  配置make snapshot的时间, 如配置为23表示每天23点make snapshot

make_snapshot_check_interval  检查make snapshot的时间间隔, 单位是ms

send_file_max_try  发送文件失败时的最大重试次数

task_pool_size  tablet线程池的大小

**scan_max_bytes_size**  一次scan数据的最大值, 单位是byte

scan_reserve_size  配置scan时预先分配的空间

**binlog_single_file_max_size**  配置binlog文件的大小, 单位是byte

binlog_sync_batch_size  主从同步时一次同步的最大记录条数

**binlog_notify_on_put**  配置为true时, 有写操作会立即同步到从节点

binlog_enable_crc  配置binlog是否要开启crc校验

binlog_coffee_time  没有读出最新数据的等待待时间, 单位是ms

binlog_sync_wait_time  主从同步时没有数据同步的等待时间, 单位是ms

binlog_sync_to_disk_interval sync数据到磁盘的时间间隔, 单位是ms

binlog_delete_interval  删除binlog任务的时间间隔, 单位是ms

binlog_match_logoffset_interval  同步从节点offset任务的时间间隔, 单位是ms

binlog_name_length  配置binlog名字的长度



## nameserver配置
**auto_failover**  自动failover开关. 如果设为true, 当tablet节点不可用时就会自动执行failover(重新选主等)

**auto_recover_table**  自动恢复数据开关. 如果设为true, 节点恢复上线时就会自动恢复该节点不可用前的数据

name_server_task_pool_size  配置nameserver任务线程池的大小

name_server_task_wait_time  nameserver任务执行框架中任务队列为空时的休眠时间, 单位是ms.

max_op_num  配置nameserver内存中保存op的最大数

