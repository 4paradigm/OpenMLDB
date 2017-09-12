//
// zk_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-04
//

#include "zk/zk_client.h"

#include "logging.h"
#include "boost/bind.hpp"
#include <boost/algorithm/string.hpp>

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

void ChildrenWatcher(zhandle_t* zh, int type, int state, const char* path, void* watcher_ctx) {
    if (zoo_get_context(zh)) {
        ZkClient* client = (ZkClient*)zoo_get_context(zh);
        std::string path_str(path);
        client->HandleChildrenChanged(path_str, type, state);
    }
}

void NodeWatcher(zhandle_t* zh, int type, int state,
                 const char* path, void* watcher_ctx) {
    LOG(INFO, "node watcher with event type %d, state %d", type, state);
    if (zoo_get_context(zh)) {
        ZkClient* client = (ZkClient*)zoo_get_context(zh);
        client->HandleNodesChanged(type, state);
        // zookeeper is just one time watching, so need to watch nodes again
        client->WatchNodes();
    }
}

ZkClient::ZkClient(const std::string& hosts, int32_t session_timeout,
        const std::string& endpoint, const std::string& zk_root_path):hosts_(hosts),
    session_timeout_(session_timeout), endpoint_(endpoint), zk_root_path_(zk_root_path),
    nodes_root_path_(zk_root_path_ + "/nodes"), nodes_watch_callbacks_(), mu_(), cv_(&mu_),
    zk_(NULL),
    nodes_watching_(false), data_(), connected_(false), children_callbacks_() {
        data_.count = 0;
        data_.data = NULL;
    }

ZkClient::~ZkClient() {
    zookeeper_close(zk_);
}

bool ZkClient::Init() {
    MutexLock lock(&mu_);
    zk_ = zookeeper_init(hosts_.c_str(),
                         LogEventWrapper, 
                         session_timeout_, 0, (void *)this, 0);
    // one second
    cv_.TimeWait(1000 * 5);
    if (zk_ == NULL || !connected_) {
        LOG(WARNING, "fail to init zk handler with hosts %s, session_timeout %d", hosts_.c_str(), session_timeout_);
        return false;
    }
    return true;
}

void ZkClient::HandleNodesChanged(int type, int state) {
    if (type == ZOO_CHILD_EVENT) {
        std::vector<std::string> endpoints;
        bool ok = GetNodes(endpoints);
        if (!ok) {
            return;
        }
        MutexLock lock(&mu_);
        LOG(INFO, "handle node changed event with type %d, and state %d, endpoints size %d, callback size %d", 
                type, state, endpoints.size(), nodes_watch_callbacks_.size());
        std::vector<NodesChangedCallback>::iterator it = nodes_watch_callbacks_.begin();
        for (; it != nodes_watch_callbacks_.end(); ++it) {
            (*it)(endpoints);
        }
    }
}


bool ZkClient::Register() {
    std::string node = nodes_root_path_ + "/" + endpoint_;
    bool ok = Mkdir(nodes_root_path_);
    if (!ok) {
        return false;
    }
    MutexLock lock(&mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    int ret = zoo_create(zk_, node.c_str(), endpoint_.c_str(),
                         endpoint_.size(), &ZOO_OPEN_ACL_UNSAFE, 
                         ZOO_EPHEMERAL, NULL, 0);
    if (ret == ZOK) {
        LOG(INFO, "register self with endpoint %s ok", endpoint_.c_str());
        return true;
    }
    LOG(WARNING, "fail to register self with endpoint %s, err from zk %d", endpoint_.c_str(), ret);
    return false;
}

bool ZkClient::CreateNode(const std::string& node,
                          const std::string& value) {
    std::string assigned_path_name;
    return CreateNode(node, value, 0, assigned_path_name);
}

bool ZkClient::CreateNode(const std::string& node, 
                          const std::string& value,
                          int flags,
                          std::string& assigned_path_name) {
    if (node.empty()) {
        return false;
    }
    size_t pos = node.find_last_of('/');
    if (pos != std::string::npos && pos == node.length() - 1) {
        LOG(WARNING, "node path[%s] is illegal", node.c_str());
        return false;
    }
    if (pos != std::string::npos && pos != node.find_first_of('/')) {
        if(!Mkdir(node.substr(0, pos))) {
            return false;
        }
    }
    MutexLock lock(&mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    uint32_t size = node.size() + 11;
    char path_buffer[size];
    int ret = zoo_create(zk_, node.c_str(), value.c_str(),
                         value.size(), &ZOO_OPEN_ACL_UNSAFE, 
                         flags,
                         path_buffer,
                         size);
    if (ret == ZOK) {
        assigned_path_name.assign(path_buffer, size - 1);
        LOG(INFO, "create node %s ok and real node name %s", node.c_str(), assigned_path_name.c_str());
        return true;
    }
    LOG(WARNING, "fail to create node %s with errno %d", node.c_str(), ret);
    return false;
}

void ZkClient::HandleChildrenChanged(const std::string& path, int type, int state) {
    std::map<std::string, NodesChangedCallback>::iterator it = children_callbacks_.find(path);
    if (it == children_callbacks_.end()) {
        LOG(INFO, "watch for path %s exist", path.c_str());
        return;
    }
    if (type == ZOO_CHILD_EVENT) {
        std::vector<std::string> children;
        bool ok = GetChildren(path, children);
        if (!ok) {
            LOG(WARNING, "fail to get nodes for path %s", path.c_str());
            WatchChildren(path, it->second);
            return;
        }
        MutexLock lock(&mu_);
        LOG(INFO, "handle node changed event with type %d, and state %d for path %s", 
                type, state, path.c_str());
        it->second(children);
    }
    WatchChildren(path, it->second);
}

void ZkClient::CancelWatchChildren(const std::string& node) {
    MutexLock lock(&mu_);
    children_callbacks_.erase(node);
}

bool ZkClient::WatchChildren(const std::string& node, NodesChangedCallback callback) {
    MutexLock lock(&mu_);
    std::map<std::string, NodesChangedCallback>::iterator it = children_callbacks_.find(node);
    if (it == children_callbacks_.end()) {
        children_callbacks_.insert(std::make_pair(node, callback));
    }
    if (zk_ == NULL || !connected_) {
        return false;
    }
    deallocate_String_vector(&data_);
    int ret = zoo_wget_children(zk_, node.c_str(), ChildrenWatcher, NULL, &data_);
    if (ret != ZOK) {
        LOG(WARNING, "fail to watch path %s errno %d", node.c_str(), ret);
        return false;
    }
    return true;
}

bool ZkClient::SetNodeWatcher(const std::string& node, watcher_fn watcher, void* watcherCtx) {
    Stat stat;
    int ret = zoo_wexists(zk_, node.c_str(), watcher, watcherCtx, &stat);
    if (ret == ZOK || ret == ZNONODE) {
        return true;
    }
    return false;
}

bool ZkClient::GetNodeValue(const std::string& node, std::string& value) {
    int buffer_len = ZK_MAX_BUFFER_SIZE;
    Stat stat;
    MutexLock lock(&mu_);
    if (zoo_get(zk_, node.c_str(), 0, buffer_, &buffer_len, &stat) == ZOK) {
        value.assign(buffer_, buffer_len);
        return true;
    }
    return false;
}

bool ZkClient::SetNodeValue(const std::string& node, const std::string& value) {
    if (node.empty()) {
        return false;
    }
    MutexLock lock(&mu_);
    if (zoo_set(zk_, node.c_str(), value.c_str(), value.length(), -1) == ZOK) {
        return true;
    }
    return false;
}

bool ZkClient::WatchNodes() {
    MutexLock lock(&mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    deallocate_String_vector(&data_);
    int ret = zoo_wget_children(zk_, nodes_root_path_.c_str(), NodeWatcher, NULL, &data_);
    if (ret != ZOK) {
        LOG(WARNING, "fail to watch path %s errno %d", nodes_root_path_.c_str(), ret);
        return false;
    }
    return true;
}

void ZkClient::WatchNodes(NodesChangedCallback callback) {
    MutexLock lock(&mu_);
    nodes_watch_callbacks_.push_back(callback);
}

bool ZkClient::GetChildren(const std::string& path, std::vector<std::string>& children) {
    MutexLock lock(&mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    struct String_vector data;
    data.count = 0;
    data.data = NULL;
    int ret = zoo_get_children(zk_, path.c_str(), 0, &data);
    if (ret != ZOK) {
        LOG(WARNING, "fail to get children from path %s with errno %d", path.c_str(),
                ret);
        return false;
    }
    for (int32_t i = 0; i < data.count; i++) {
        children.push_back(std::string(data.data[i]));
    }
    return true;

}
bool ZkClient::GetNodes(std::vector<std::string>& endpoints) {
    MutexLock lock(&mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    struct String_vector data;
    data.count = 0;
    data.data = NULL;
    int ret = zoo_get_children(zk_, nodes_root_path_.c_str(), 0, &data);
    if (ret != ZOK) {
        LOG(WARNING, "fail to get children from path %s with errno %d", nodes_root_path_.c_str(),
                ret);
        return false;
    }
    for (int32_t i = 0; i < data.count; i++) {
        endpoints.push_back(std::string(data.data[i]));
    }
    return true;
}

bool ZkClient::Reconnect() {
    MutexLock lock(&mu_);
    if (zk_ != NULL) {
        zookeeper_close(zk_);
    }
    zk_ = zookeeper_init(hosts_.c_str(),
                         LogEventWrapper, 
                         session_timeout_, 0, (void *)this, 0);

    cv_.TimeWait(1000 * 5);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    return true;
}

void ZkClient::LogEvent(int type, int state, const char* path) {
    LOG(INFO, "zookeeper event with type %d, state %d, path %s", type, state, path);
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            Connected(); 
        }else if (state == ZOO_EXPIRED_SESSION_STATE) {
            connected_ = false;
        }
    }
}

void ZkClient::Connected() {
    MutexLock lock(&mu_);
    connected_ = true;
    cv_.Signal();
}

bool ZkClient::Mkdir(const std::string& path) {
    MutexLock lock(&mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    std::vector<std::string> parts;
    boost::split(parts, path, boost::is_any_of("/"));
    std::string full_path = "/";
    std::vector<std::string>::iterator it = parts.begin();
    int32_t index = 0;
    for (; it != parts.end(); ++it) {
        if (it->empty()) {
            continue;
        }
        if (index > 0) {
            full_path += "/";
        }
        full_path += *it;
        index ++;
        int ret = zoo_create(zk_, full_path.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE,
                       0, NULL, 0);
        if (ret == ZNODEEXISTS || ret == ZOK) {
            continue;
        }
        LOG(WARNING, "fail to create zk node with path %s , errno %d",
                full_path.c_str(), ret);
        return false;
    }
    return true;
}

}
}


