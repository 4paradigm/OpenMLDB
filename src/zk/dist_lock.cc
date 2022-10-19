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

#include "zk/dist_lock.h"

#include "base/glog_wrapper.h"
#include "boost/algorithm/string/join.hpp"
#include "boost/bind.hpp"
extern "C" {
#include "zookeeper/zookeeper.h"
}

namespace openmldb {
namespace zk {

DistLock::DistLock(const std::string& root_path, ZkClient* zk_client, NotifyCallback on_locked_cl,
                   NotifyCallback on_lost_lock_cl, const std::string& lock_value)
    : root_path_(root_path),
      on_locked_cl_(on_locked_cl),
      on_lost_lock_cl_(on_lost_lock_cl),
      mu_(),
      zk_client_(zk_client),
      assigned_path_(),
      lock_state_(kLostLock),
      pool_(1),
      running_(true),
      lock_value_(lock_value),
      current_lock_node_(),
      client_session_term_(0) {}

DistLock::~DistLock() {}

void DistLock::Lock() { pool_.AddTask(boost::bind(&DistLock::InternalLock, this)); }

void DistLock::Stop() {
    running_.store(false, std::memory_order_relaxed);
    pool_.Stop(true);
    lock_state_.store(kLostLock, std::memory_order_relaxed);
}

void DistLock::InternalLock() {
    while (running_.load(std::memory_order_relaxed)) {
        sleep(1);
        uint64_t cur_session_term = zk_client_->GetSessionTerm();
        if (lock_state_.load(std::memory_order_relaxed) == kLostLock || cur_session_term != client_session_term_) {
            std::lock_guard<std::mutex> lock(mu_);
            zk_client_->CancelWatchChildren(root_path_);
            // ZOO_SEQUENCE: node path is assigned_path_, may != supplied path
            bool ok = zk_client_->CreateNode(root_path_ + "/lock_request", lock_value_, ZOO_EPHEMERAL | ZOO_SEQUENCE,
                                             assigned_path_);
            if (!ok) {
                PDLOG(WARNING, "create node falied. lock value %s", lock_value_.c_str());
                continue;
            }
            PDLOG(INFO, "create node ok with assigned path %s", assigned_path_.c_str());
            std::vector<std::string> children;
            ok = zk_client_->GetChildren(root_path_, children);
            if (!ok) {
                continue;
            }
            lock_state_.store(kTryLock, std::memory_order_relaxed);
            client_session_term_ = cur_session_term;
            HandleChildrenChanged(children);
            zk_client_->WatchChildren(root_path_, boost::bind(&DistLock::HandleChildrenChangedLocked, this, _1));
        }
    }
}

void DistLock::HandleChildrenChanged(const std::vector<std::string>& children) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    current_lock_node_ = "";
    if (!children.empty()) {
        current_lock_node_ = root_path_ + "/" + children[0];
    }
    PDLOG(INFO, "first child %s", current_lock_node_.c_str());
    if (current_lock_node_.compare(assigned_path_) == 0) {
        // first get lock
        if (lock_state_.load(std::memory_order_relaxed) == kTryLock) {
            PDLOG(INFO, "get lock with assigned_path %s", assigned_path_.c_str());
            on_locked_cl_();
            lock_state_.store(kLocked, std::memory_order_relaxed);
            current_lock_value_ = lock_value_;
        }
    } else {
        bool ok = zk_client_->GetNodeValue(current_lock_node_, current_lock_value_);
        if (!ok) {
            PDLOG(WARNING, "fail to get lock value");
        }
        // lost lock
        if (lock_state_.load(std::memory_order_relaxed) == kLocked) {
            on_lost_lock_cl_();
            lock_state_.store(kLostLock, std::memory_order_relaxed);
            PDLOG(INFO, "lock lost. wait a channce to get a lock");
        } else if (lock_state_.load(std::memory_order_relaxed) == kTryLock && current_lock_value_ == lock_value_) {
            PDLOG(INFO, "get lock with assigned_path %s", assigned_path_.c_str());
            on_locked_cl_();
            lock_state_.store(kLocked, std::memory_order_relaxed);
            current_lock_value_ = lock_value_;
        } else {
            PDLOG(INFO, "wait a channce to get a lock");
        }
    }
    PDLOG(INFO, "my path %s , first child %s , lock value %s", assigned_path_.c_str(), current_lock_node_.c_str(),
          current_lock_value_.c_str());
    PDLOG(INFO, "all child: %s", boost::algorithm::join(children, ", ").c_str());
}

void DistLock::HandleChildrenChangedLocked(const std::vector<std::string>& children) {
    std::lock_guard<std::mutex> lock(mu_);
    HandleChildrenChanged(children);
}

bool DistLock::IsLocked() { return lock_state_.load(std::memory_order_relaxed) == kLocked; }

void DistLock::CurrentLockValue(std::string& value) { value.assign(current_lock_value_); }

}  // namespace zk
}  // namespace openmldb
