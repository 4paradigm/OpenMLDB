//
// dist_lock.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-07
//

#ifndef RTIDB_DIST_LOCK_H
#define RTIDB_DIST_LOCK_H

#include "boost/function.hpp"
#include <mutex>
#include <vector>
#include "zk/zk_client.h"
#include "thread_pool.h"
#include <atomic>

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
            NotifyCallback on_lost_lock_cl,
            const std::string& lock_value);

    ~DistLock();

    void Lock();

    void Stop();

    bool IsLocked();

    void CurrentLockValue(std::string& value);

private:

    void InternalLock();
    void HandleChildrenChanged(const std::vector<std::string>& children);
    void HandleChildrenChangedLocked(const std::vector<std::string>& children);
private:
    // input args
    std::string root_path_;
    NotifyCallback on_locked_cl_;
    NotifyCallback on_lost_lock_cl_;

    // status
    std::mutex mu_;
    ZkClient* zk_client_;
    // sequence path from zookeeper
    std::string assigned_path_;
    std::atomic<LockState> lock_state_;
    ThreadPool pool_;
    std::atomic<bool> running_;
    std::string lock_value_;
    std::string current_lock_node_;
    std::string current_lock_value_;
    uint64_t client_session_term_;
};

}
}

#endif /* !RTIDB_DIST_LOCK_H */
