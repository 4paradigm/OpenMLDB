//
// dist_lock.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-07
//

#ifndef RTIDB_DIST_LOCK_H
#define RTIDB_DIST_LOCK_H

#include "boost/function.hpp"
#include "mutex.h"
#include <vector>
#include "zk/zk_client.h"
#include "thread_pool.h"
#include "boost/atomic.hpp"

using ::baidu::common::MutexLock;
using ::baidu::common::Mutex;
using ::baidu::common::CondVar;
using ::baidu::common::ThreadPool;
using ::rtidb::zk::ZkClient;


namespace rtidb {
namespace zk {

enum LockState {
    kLocked,
    kLostLock,
    kTryLock
};

typedef boost::function<void()>  NotifyCallback;
class DistLock {

public:
    DistLock(const std::string& root_path, ZkClient* zk_client,
            NotifyCallback on_locked_cl,
            NotifyCallback on_lost_lock_cl);

    ~DistLock();

    void Lock();

    void Stop();

private:
    void InternalLock();
    void HandleChildrenChanged(const std::vector<std::string>& children);

private:
    // input args
    std::string root_path_;
    NotifyCallback on_locked_cl_;
    NotifyCallback on_lost_lock_cl_;

    // status
    Mutex mu_;
    CondVar cv_;
    ZkClient* zk_client_;
    // sequence path from zookeeper
    std::string assigned_path_;
    LockState lock_state_;
    ThreadPool pool_;
    boost::atomic<bool> running_;
};

}
}

#endif /* !RTIDB_DIST_LOCK_H */
