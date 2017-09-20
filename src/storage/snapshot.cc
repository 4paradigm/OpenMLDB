//
// snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//
//
#include "storage/snapshot.h"

#include "base/file_util.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(snapshot_root_path);

namespace rtidb {
namespace storage {

Snapshot::Snapshot(uint32_t tid, uint32_t pid):tid_(tid), pid_(pid),
     offset_(0) {}

Snapshot::~Snapshot() {}

bool Snapshot::Init() {
    std::string snapshot_path = FLAGS_snapshot_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_);
    bool ok = ::rtidb::base::MkdirRecur(snapshot_path);
    if (!ok) {
        LOG(WARNING, "fail to create db meta path %s", snapshot_path.c_str());
        return false;
    }
    return true;
}

bool Snapshot::Recover(Table* table) {
    //TODO multi thread recover
    return true;
}

}
}



