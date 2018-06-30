# rtidb 部署文档

## 单机版部署包介绍

* bin/start.sh 启动脚本 
* bin/stop.sh 停止脚本
* bin/mon 守护进程程序
* bin/rtidb rtidb进程程序
* logs 日志路径
* db binlog和snapshot存储路径
* conf 配置文件路径

## 单机版部署配置

### 通用相关配置

* endpoint 配置对外服务地址，格式为 ip:port或者hostname:port
* role 如果是单机版本这里需要配置为tablet
* db_root_path 配置binlog和snapshot根目录
* recycle_bin_root_path 配置存放已删除表binlog和snapshot 根目录

### 服务线程相关配置

* thread_pool_size 配置服务线程数量 建议和cpu核数一致
* scan_concurrency_limit scan操作最大并发数，默认值为8，如果请求并发超过这个数直接返回错误信息
* put_concurrency_limit put操作最大并发数，默认值为8
* get_concurrency_limit get操作最大并发数

建议至少要限制写的最大并发数，如果不限制，可能因为消耗过多限制影响读的资源

### 日志相关配置

* log_dir 配置日志存储的文件夹位置
* log_file_count 日志文件保留最大数量
* log_file_size 单个日志文件大小
* log_level 配置日志级别，目前只支持info和debug

### gc 配置

* gc_interval gc运行周期，单位为分钟，建议在一小时以上
* gc_pool_size gc线程池大小，如果tablet表比较多可以大一点，建议值为2

### binlog 相关重要配置项

* binlog_single_file_max_size binlog单个文件大小，单位是M,建议配置1024
* binlog_notify_on_put 配置是否put之后马上通过binlog变化，如果是true，主从同步延迟会很低，但是会影响写的性能
* binlog_sync_to_disk_interval binlog定时刷盘周期

### 定时做snapshot

* make_snapshot_time 配置每天做snapshot开始时间，单位为小时

### sample 配置

```
# common conf 
--endpoint=0.0.0.0:9527
--role=tablet

# concurrency conf
--thread_pool_size=16
--scan_concurrency_limit=8
--put_concurrency_limit=8
--get_concurrency_limit=8

# log conf
--log_dir=./logs
--log_file_count=24
--log_file_size=1024
--log_level=info

--db_root_path=./db
--recycle_bin_root_path=./recycle

# snapshot conf
--make_snapshot_time=23
--make_snapshot_check_interval=600000

# garbage collection conf
# 60m
--gc_interval=60
--gc_pool_size=2
# 1m
--gc_safe_offset=1

```
