//
// dist_lock.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-07
//

#ifndef SRC_ZK_DIST_LOCK_H_
#define SRC_ZK_DIST_LOCK_H_

#include <atomic>
#include <mutex> // NOLINT
#include <vector>
#include <string>
#include "boost/function.hpp"
#include "thread_pool.h" // NOLINT
#include "zk/zk_client.h"

using ::baidu::common::ThreadPool;
using ::rtidb::zk::ZkClient;

namespace rtidb {
namespace zk {

enum LockState { kLocked, kLostLock, kTryLock };

typedef boost::function<void()> NotifyCallback;
class DistLock {
 public:
    DistLock(const std::string& root_path, ZkClient* zk_client,
             NotifyCallback on_locked_cl, NotifyCallback on_lost_lock_cl,
             const std::string& lock_value);

    ~DistLock();

    void Lock();

    void Stop();

    bool IsLocked();

    void CurrentLockValue(std::string& value); // NOLINT

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

}  // namespace zk
}  // namespace rtidb

#endif  // SRC_ZK_DIST_LOCK_H_
