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

DEFINE_int32(gc_interval, 120, "the gc interval of tablet every two hour");
DEFINE_int32(gc_pool_size, 2, "the size of tablet gc thread pool");
DEFINE_int32(gc_safe_offset, 1, "the safe offset of tablet gc in minute");
DEFINE_uint64(gc_on_table_recover_count, 10000000, "make a gc on recover count");
DEFINE_int32(statdb_ttl, 30 * 24 * 60 , "the ttl of statdb");
DEFINE_double(mem_release_rate, 5 , "specify memory release rate, which should be in 0 ~ 10");
DEFINE_bool(enable_statdb, false, "enable statdb");
DEFINE_int32(task_pool_size, 3, "the size of tablet task thread pool");

// scan configuration
DEFINE_uint32(scan_max_bytes_size, 2 * 1024 * 1024, "config the max size of scan bytes size");
// binlog configuration
DEFINE_int32(binlog_single_file_max_size, 1024*4, "the max size of single binlog file");
DEFINE_int32(binlog_sync_batch_size, 32, "the batch size of sync binlog");
DEFINE_int32(binlog_apply_batch_size, 32, "the batch size of apply binlog");
DEFINE_bool(binlog_notify_on_put, false, "config the sync log to follower strategy");
DEFINE_bool(binlog_enable_crc, false, "enable crc");
DEFINE_int32(binlog_coffee_time, 1000, "config the coffee time");
DEFINE_int32(binlog_sync_wait_time, 100, "config the sync log wait time");
DEFINE_int32(binlog_sync_to_disk_interval, 5000, "config the interval of sync binlog to disk time");
DEFINE_int32(binlog_delete_interval, 10000, "config the interval of delete binlog");
DEFINE_int32(binlog_match_logoffset_interval, 1000, "config the interval of match log offset ");
DEFINE_string(binlog_root_path, "/tmp/binlog", "the root path  of binlog");

// snapshot configuration
DEFINE_string(snapshot_root_path, "/tmp/snapshot", "config the snapshot storage path");
// local db config
DEFINE_string(db_root_path,"/tmp/", "the root path of db");
// if set 23, the task will execute 23:00 every day
DEFINE_int32(make_snapshot_time, 23, "config the time to make snapshot");
DEFINE_int32(make_snapshot_check_interval, 5, "config the interval to check making snapshot time");
