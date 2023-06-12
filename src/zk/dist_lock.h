/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_ZK_DIST_LOCK_H_
#define SRC_ZK_DIST_LOCK_H_

#include <atomic>
#include <mutex>  // NOLINT
#include <string>
#include <vector>

#include "boost/function.hpp"
#include "common/thread_pool.h"
#include "zk/zk_client.h"

namespace openmldb {
namespace zk {
using ::baidu::common::ThreadPool;

enum LockState { kLocked, kLostLock, kTryLock };

typedef boost::function<void()> NotifyCallback;
class DistLock {
 public:
    DistLock(const std::string& root_path, ZkClient* zk_client, NotifyCallback on_locked_cl,
             NotifyCallback on_lost_lock_cl, const std::string& lock_value);

    ~DistLock();

    void Lock();

    void Stop();

    bool IsLocked();

    void CurrentLockValue(std::string& value);  // NOLINT

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
}  // namespace openmldb

#endif  // SRC_ZK_DIST_LOCK_H_
