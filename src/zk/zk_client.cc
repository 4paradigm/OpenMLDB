//
// zk_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-04
//

#include "zk/zk_client.h"

#include "logging.h"
#include "boost/bind.hpp"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace zk {

void LogEventWrapper(zhandle_t* zh, int type, int state, 
        const char* path, void* watcher_ctx) {
    if(zoo_get_context(zh)) {
        ZkClient* client = (ZkClient*)zoo_get_context(zh);
        client->LogEvent(type, state, path);
    }

}

void NodeWatcher(zhandle_t* zh, int type, int state,
                 const char* path, void* watcher_ctx) {
    if (zoo_get_context(zh)) {
        ZkClient* client = (ZkClient*)zoo_get_context(zh);
        client->HandleNodesChanged();
    }

}


ZkClient::ZkClient(const std::string& hosts, int32_t session_timeout,
        const std::string& endpoint, const std::string& zk_root_path):hosts_(hosts),
    session_timeout_(session_timeout), endpoint_(endpoint), zk_root_path_(zk_root_path),
    nodes_root_path_(zk_root_path_ + "/nodes"), nodes_watch_callbacks_(), mu_(), zk_(NULL) {}

ZkClient::~ZkClient() {
    zookeeper_close(zk_);
}

bool ZkClient::Init() {
    zk_ = zookeeper_init(hosts_.c_str(),
                         LogEventWrapper, 
                         session_timeout_, 0, (void *)this, 0);
    if (zk_ == NULL) {
        LOG(WARNING, "fail to init zk handler with hosts %s, session_timeout %d", hosts_.c_str(), session_timeout_);
        return false;
    }
    return true;
}

void ZkClient::HandleNodesChanged() {}


bool ZkClient::Register() {
    MutexLock lock(&mu_);
    std::string node = nodes_root_path_ + "/" + endpoint_;
    int ret = zoo_create(zk_, node.c_str(), endpoint_.c_str(),
                         endpoint_.size(), &ZOO_OPEN_ACL_UNSAFE, 
                         ZOO_EPHEMERAL | ZOO_SEQUENCE, NULL, 0);
    if (ret == ZOK) {
        LOG(INFO, "register self with endpoint %s ok", endpoint_.c_str());
        return true;
    }
    LOG(WARNING, "fail to register self with endpoint %s, err from zk %d", endpoint_.c_str(), ret);
    return false;
}

void ZkClient::WatchNodes(NodesChangedCallback callback) {
    MutexLock lock(&mu_);
    nodes_watch_callbacks_.push_back(callback);
}

bool ZkClient::GetNodes(std::vector<std::string>& endpoints) {
    return true;
}

void ZkClient::LogEvent(int type, int state, const char* path) {
    LOG(INFO, "zookeeper event with type %d, state %d, path %s", type, state, path);
}

}
}



