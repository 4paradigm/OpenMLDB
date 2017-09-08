//
// dist_lock.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-07
//

#ifndef RTIDB_DIST_LOCK_H
#define RTIDB_DIST_LOCK_H

#include "mutex.h"
#include "zk/zk_client.h"

using ::baidu::common::MutexLock;
using ::baidu::common::Mutex;
using ::baidu::common::CondVar;
using ::rtidb::zk::ZkClient;

namespace rtidb {
namespace zk {

class DistLock {

public:
    DistLock(const std::string& root_path);
    ~DistLock();

    void Lock();
    void UnLock();

private:
    // input args
    std::string root_path_;

    // status
    Mutex mu_;
    CondVar cv_;
    ZkClient zk_client_;
    // sequence path from zookeeper
    std::string assigned_path_;

};

}
}

#endif /* !RTIDB_DIST_LOCK_H */
