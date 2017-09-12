//
// dist_lock.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-08
//

#include "zk/dist_lock.h"
#include "boost/bind.hpp"
#include "logging.h"
extern "C" {
#include "zookeeper/zookeeper.h"
}

using ::baidu::common::INFO;
using ::baidu::common::WARNING;

namespace rtidb {
namespace zk {

DistLock::DistLock(const std::string& root_path, ZkClient* zk_client,
        NotifyCallback on_locked_cl,
        NotifyCallback on_lost_lock_cl,
        const std::string& lock_value):root_path_(root_path),
    on_locked_cl_(on_locked_cl), on_lost_lock_cl_(on_lost_lock_cl),
    mu_(), cv_(&mu_), zk_client_(zk_client), assigned_path_(), lock_state_(kLostLock), pool_(1),
    running_(true), lock_value_(lock_value), current_lock_node_(){}

DistLock::~DistLock() {}

void DistLock::Lock() {
    pool_.AddTask(boost::bind(&DistLock::InternalLock, this));
}

void DistLock::Stop() {
    running_.store(false, boost::memory_order_relaxed);
    pool_.Stop(true);
}

void DistLock::InternalLock() {
    while (running_.load(boost::memory_order_relaxed)) {
        sleep(1);
        MutexLock lock(&mu_);
        if (lock_state_.load(boost::memory_order_relaxed) == kLostLock) {
            zk_client_->CancelWatchChildren(root_path_);
            bool ok = zk_client_->CreateNode(root_path_ + "/lock_request", lock_value_, ZOO_EPHEMERAL | ZOO_SEQUENCE, assigned_path_);
            if (!ok) {
                continue;
            }
            LOG(INFO, "create node ok with assigned path %s", assigned_path_.c_str());
            std::vector<std::string> children;
            ok = zk_client_->GetChildren(root_path_, children);
            if (!ok) {
                continue;
            }
            lock_state_.store(kTryLock, boost::memory_order_relaxed);
            HandleChildrenChangedLocked(children);
            zk_client_->WatchChildren(root_path_, boost::bind(&DistLock::HandleChildrenChanged, this, _1));
        }
    }
}

void DistLock::HandleChildrenChangedLocked(const std::vector<std::string>& children) {
    if (!running_.load(boost::memory_order_relaxed)) {
        return ;
    }
    mu_.AssertHeld();
    current_lock_node_ = "";
    if (children.size() > 0) {
        current_lock_node_ = root_path_ + "/" + children[0];
    }
    LOG(INFO, "first child %s", current_lock_node_.c_str());
    if (current_lock_node_.compare(assigned_path_) == 0) {
        // first get lock
        if (lock_state_.load(boost::memory_order_relaxed) == kTryLock) {
            LOG(INFO, "get lock with assigned_path %s", assigned_path_.c_str());
            on_locked_cl_();
            lock_state_.store(kLocked, boost::memory_order_relaxed);
            current_lock_value_ = lock_value_;
        }
    }else {
        bool ok = zk_client_->GetNodeValue(current_lock_node_, current_lock_value_);
        if (!ok) {
            LOG(WARNING, "fail to get lock value");
        }
        // lost lock
        if (lock_state_.load(boost::memory_order_relaxed) == kLocked) {
                       on_lost_lock_cl_();
            lock_state_.store(kLostLock, boost::memory_order_relaxed);
        }
        LOG(INFO, "wait a channce to get a lock");
    }
    LOG(INFO, "my path %s , first child %s , lock value %s", 
               assigned_path_.c_str(), current_lock_node_.c_str(), current_lock_value_.c_str());
}

void DistLock::HandleChildrenChanged(const std::vector<std::string>& children) {
    MutexLock lock(&mu_);
    HandleChildrenChangedLocked(children);
}

bool DistLock::IsLocked() {
    return lock_state_.load(boost::memory_order_relaxed) == kLocked;
}

void DistLock::CurrentLockValue(std::string& value) {
    mu_.AssertHeld();
    value.assign(current_lock_value_);
}


}
}


