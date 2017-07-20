//
// flags.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-07
//

#include <gflags/gflags.h>

DEFINE_int32(gc_interval, 120, "the gc interval of tablet every two hour");
DEFINE_int32(gc_pool_size, 2, "the size of tablet gc thread pool");
DEFINE_int32(gc_safe_offset, 1, "the safe offset of tablet gc in minute");
DEFINE_int32(statdb_ttl, 30 * 24 * 60 , "the ttl of statdb");
DEFINE_double(mem_release_rate, 5 , "specify memory release rate, which should be in 0 ~ 10");
DEFINE_string(db_root_path,"/tmp/", "the root path of db");
DEFINE_bool(enable_statdb, false, "enable statdb");

// scan configuration
DEFINE_uint32(scan_max_bytes_size, 2 * 1024 * 1024, "config the max size of scan bytes size");
// binlog configuration
DEFINE_int32(binlog_single_file_max_size, 1024*4, "the max size of single binlog file");
DEFINE_int32(binlog_sync_batch_size, 32, "the batch size of sync binlog");
DEFINE_int32(binlog_apply_batch_size, 32, "the batch size of apply binlog");
DEFINE_bool(binlog_notify_on_put, false, "config the sync log to follower strategy");
DEFINE_int32(binlog_coffee_time, 1000, "config the coffee time");
DEFINE_int32(binlog_sync_wait_time, 100, "config the sync log wait time");
DEFINE_int32(binlog_sync_to_disk_interval, 5000, "config the interval of sync binlog to disk time");
DEFINE_int32(binlog_match_logoffset_interval, 1000, "config the interval of match log offset ");
DEFINE_string(binlog_root_path, "/tmp/binlog", "the root path  of binlog");

