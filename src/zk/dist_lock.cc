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
        NotifyCallback on_lost_lock_cl):root_path_(root_path),
    on_locked_cl_(on_locked_cl), on_lost_lock_cl_(on_lost_lock_cl),
    mu_(), cv_(&mu_), zk_client_(zk_client), assigned_path_(), lock_state_(kLostLock), pool_(1),
    running_(true){}

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
        MutexLock lock(&mu_);
        if (lock_state_ == kLostLock) {
            zk_client_->CancelWatchChildren(root_path_);
            bool ok = zk_client_->CreateNode(root_path_ + "/lock_request", "", ZOO_EPHEMERAL | ZOO_SEQUENCE, assigned_path_);
            if (!ok) {
                sleep(1);
                continue;
            }
            LOG(INFO, "create node ok with assigned path %s", assigned_path_.c_str());
            std::vector<std::string> children;
            ok = zk_client_->GetChildren(root_path_, children);
            if (!ok) {
                sleep(1);
                continue;
            }
            lock_state_ = kTryLock;
            HandleChildrenChangedLocked(children);
            zk_client_->WatchChildren(root_path_, boost::bind(&DistLock::HandleChildrenChanged, this, _1));
        }else {
            sleep(1);
        }
    }
}

void DistLock::HandleChildrenChangedLocked(const std::vector<std::string>& children) {
    if (!running_.load(boost::memory_order_relaxed)) {
        return ;
    }
    mu_.AssertHeld();
    std::string firstChild;
    if (children.size() > 0) {
        firstChild = root_path_ + "/" + children[0];
    }
    LOG(INFO, "first child %s", firstChild.c_str());
    if (firstChild.compare(assigned_path_) == 0) {
        // first get lock
        if (lock_state_ == kTryLock) {
            LOG(INFO, "get lock with assigned_path %s", assigned_path_.c_str());
            on_locked_cl_();
            lock_state_ = kLocked;
        }
    }else {
        // lost lock
        if (lock_state_ == kLocked) {
            LOG(INFO, "lost lock with my path %s , first child %s", assigned_path_.c_str(), firstChild.c_str());
            on_lost_lock_cl_();
            lock_state_ = kLostLock;
        }
        LOG(INFO, "wait a channce to get a lock");
    }

}

void DistLock::HandleChildrenChanged(const std::vector<std::string>& children) {
    MutexLock lock(&mu_);
    HandleChildrenChangedLocked(children);
}


}
}


