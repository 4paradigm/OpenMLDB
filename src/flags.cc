//
// flags.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-07
//

#include <gflags/gflags.h>
// cluster config
DEFINE_string(endpoint, "127.0.0.1:9527", "config the ip and port that rtidb serves for");
DEFINE_int32(zk_session_timeout, 2000, "config the session timeout of tablet or nameserver");
DEFINE_string(zk_cluster,"", "config the zookeeper cluster eg ip:2181,ip2:2181,ip3:2181");
DEFINE_string(zk_root_path, "/rtidb", "config the root path of zookeeper");
DEFINE_int32(zk_keep_alive_check_interval, 5000, "config the interval of keep alive check");
DEFINE_int32(get_task_status_interval, 2000, "config the interval of get task status");
DEFINE_int32(name_server_task_pool_size, 8, "config the size of name server task pool");
DEFINE_int32(name_server_task_wait_time, 1000, "config the time of task wait");
DEFINE_bool(auto_failover, false, "enable or disable auto failover");
DEFINE_bool(auto_recover_table, false, "enable or disable auto recover table");
DEFINE_int32(max_op_num, 10000, "config the max op num");

DEFINE_int32(gc_interval, 120, "the gc interval of tablet every two hour");
DEFINE_int32(gc_pool_size, 2, "the size of tablet gc thread pool");
DEFINE_int32(gc_safe_offset, 1, "the safe offset of tablet gc in minute");
DEFINE_uint64(gc_on_table_recover_count, 10000000, "make a gc on recover count");
DEFINE_double(mem_release_rate, 5 , "specify memory release rate, which should be in 0 ~ 10");
DEFINE_int32(task_pool_size, 3, "the size of tablet task thread pool");
DEFINE_int32(io_pool_size, 2, "the size of tablet io task thread pool");

// scan configuration
DEFINE_uint32(scan_max_bytes_size, 2 * 1024 * 1024, "config the max size of scan bytes size");
DEFINE_uint32(scan_reserve_size, 1024, "config the size of vec reserve");
// binlog configuration
DEFINE_int32(binlog_single_file_max_size, 1024*4, "the max size of single binlog file");
DEFINE_int32(binlog_sync_batch_size, 32, "the batch size of sync binlog");
DEFINE_bool(binlog_notify_on_put, false, "config the sync log to follower strategy");
DEFINE_bool(binlog_enable_crc, false, "enable crc");
DEFINE_int32(binlog_coffee_time, 1000, "config the coffee time");
DEFINE_int32(binlog_sync_wait_time, 100, "config the sync log wait time");
DEFINE_int32(binlog_sync_to_disk_interval, 20000, "config the interval of sync binlog to disk time");
DEFINE_int32(binlog_delete_interval, 60000, "config the interval of delete binlog");
DEFINE_int32(binlog_match_logoffset_interval, 1000, "config the interval of match log offset ");
DEFINE_int32(binlog_name_length, 8, "binlog name length");

// local db config
DEFINE_string(db_root_path,"/tmp/", "the root path of db");

// thread pool config
DEFINE_int32(scan_concurrency_limit, 8, "the limit of scan concurrency");
DEFINE_int32(put_concurrency_limit, 8, "the limit of put concurrency");
DEFINE_int32(thread_pool_size, 16, "the size of thread pool for other api");
DEFINE_int32(get_concurrency_limit, 8, "the limit of get concurrency");
DEFINE_int32(request_max_retry, 3, "max retry time when request error");
DEFINE_int32(request_timeout_ms, 12000, "request timeout");
DEFINE_int32(request_sleep_time, 1000, "the sleep time when request error");

DEFINE_int32(send_file_max_try, 3, "the max retry time when send file failed");
DEFINE_int32(retry_send_file_wait_time_ms, 3000, "conf the wait time when retry send file");
DEFINE_int32(stream_wait_time_ms, 5, "the wait time when send too fast");
DEFINE_int32(stream_close_wait_time_ms, 1000, "the wait time before close stream");
DEFINE_int32(stream_block_size, 4 * 1024 * 1024, "config the write/read block size in streaming");
DEFINE_int32(stream_bandwidth_limit, 1 * 1204 * 1024, "the limit bandwidth. Byte/Second");

// if set 23, the task will execute 23:00 every day
DEFINE_int32(make_snapshot_time, 23, "config the time to make snapshot");
DEFINE_int32(make_snapshot_check_interval, 1000*60*10, "config the interval to check making snapshot time");

DEFINE_string(recycle_bin_root_path, "/tmp/recycle", "specify the root path of recycle bin");

DEFINE_bool(enable_show_tp, false, "enable show tp");
