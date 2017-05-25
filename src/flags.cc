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
DEFINE_string(db_root_path,"/tmp/", "the root path of db");
DEFINE_bool(enable_statdb, false, "enable statdb");

