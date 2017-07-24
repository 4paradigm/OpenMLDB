//
// snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//

#include "gflags/gflags.h"
#include "logging.h"
#include "storage/snapshot.h"
#include "leveldb/options.h"
#include "boost/lexical_cast.hpp"

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(snapshot_root_path);

namespace rtidb {
namespace storage {

Snapshot::Snapshot(uint32_t tid, uint32_t pid):tid_(tid), pid_(pid) {}

Snapshot::~Snapshot() {}

bool Snapshot::Init() {
    std::string snapshot_path = FLAGS_snapshot_root_path + "/" + boost::lexical_cast<tid_> + "_" + boost::lexical_cast<pid_>();

}

}
}



