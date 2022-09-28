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

#include "zk/zk_client.h"

#include <algorithm>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "base/glog_wrapper.h"
#include "base/strings.h"
#include "boost/algorithm/string.hpp"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"

namespace openmldb {
namespace zk {

void LogEventWrapper(zhandle_t* zh, int type, int state, const char* path, void* watcher_ctx) {
    if (zoo_get_context(zh)) {
        ZkClient* client = const_cast<ZkClient*>(reinterpret_cast<const ZkClient*>(zoo_get_context(zh)));
        client->LogEvent(type, state, path);
    }
}

void ChildrenWatcher(zhandle_t* zh, int type, int state, const char* path, void* watcher_ctx) {
    if (zoo_get_context(zh)) {
        ZkClient* client = const_cast<ZkClient*>(reinterpret_cast<const ZkClient*>(zoo_get_context(zh)));
        std::string path_str(path);
        client->HandleChildrenChanged(path_str, type, state);
    }
}

void NodeWatcher(zhandle_t* zh, int type, int state, const char* path, void* watcher_ctx) {
    PDLOG(INFO, "node watcher with event type %d, state %d", type, state);
    if (zoo_get_context(zh)) {
        ZkClient* client = const_cast<ZkClient*>(reinterpret_cast<const ZkClient*>(zoo_get_context(zh)));
        client->HandleNodesChanged(type, state);
        // zookeeper is just one time watching, so need to watch nodes again
        client->WatchNodes();
    }
}

void ItemWatcher(zhandle_t* zh, int type, int state, const char* path, void* watcher_ctx) {
    PDLOG(INFO, "node watcher with event type %d, state %d", type, state);
    if (zoo_get_context(zh)) {
        ZkClient* client = const_cast<ZkClient*>(reinterpret_cast<const ZkClient*>(zoo_get_context(zh)));
        std::string path_str(path);
        client->HandleItemChanged(path_str, type, state);
    }
}

ZkClient::ZkClient(const std::string& hosts, const std::string& real_endpoint, int32_t session_timeout,
                   const std::string& endpoint, const std::string& zk_root_path)
    : hosts_(hosts),
      session_timeout_(session_timeout),
      endpoint_(endpoint),
      zk_root_path_(zk_root_path),
      real_endpoint_(real_endpoint),
      nodes_root_path_(zk_root_path_ + "/nodes"),
      nodes_watch_callbacks_(),
      names_root_path_(zk_root_path_ + "/map/names"),
      mu_(),
      cv_(),
      zk_(NULL),
      nodes_watching_(false),
      data_(),
      connected_(false),
      registed_(false),
      children_callbacks_(),
      item_callbacks_(),
      session_term_(0) {
    data_.count = 0;
    data_.data = NULL;
}

ZkClient::ZkClient(const std::string& hosts, int32_t session_timeout, const std::string& endpoint,
                   const std::string& zk_root_path, const std::string& zone_path)
    : hosts_(hosts),
      session_timeout_(session_timeout),
      endpoint_(endpoint),
      zk_root_path_(zk_root_path),
      nodes_root_path_(zone_path),
      nodes_watch_callbacks_(),
      mu_(),
      cv_(),
      zk_(NULL),
      nodes_watching_(false),
      data_(),
      connected_(false),
      registed_(false),
      children_callbacks_(),
      item_callbacks_(),
      session_term_(0) {
    data_.count = 0;
    data_.data = NULL;
}

ZkClient::~ZkClient() {
    if (zk_) {
        zookeeper_close(zk_);
    }
    if (zk_log_stream_file_) {
        fclose(zk_log_stream_file_);
    }
}

bool ZkClient::Init(int log_level, const std::string& log_file) {
    std::unique_lock<std::mutex> lock(mu_);
    zoo_set_debug_level(ZooLogLevel(log_level));
    if (!log_file.empty()) {
        zk_log_stream_file_ = fopen(log_file.c_str(), "a");
        zoo_set_log_stream(zk_log_stream_file_);
    }

    zk_ = zookeeper_init(hosts_.c_str(), LogEventWrapper, session_timeout_, 0, (void*)this, 0);  // NOLINT
    // one second
    cv_.wait_for(lock, std::chrono::milliseconds(session_timeout_));
    if (zk_ == NULL || !connected_) {
        PDLOG(WARNING, "fail to init zk handler with hosts %s, session_timeout %d", hosts_.c_str(), session_timeout_);
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
        std::vector<NodesChangedCallback> watch_callbacks_vec;
        {
            std::lock_guard<std::mutex> lock(mu_);
            watch_callbacks_vec = nodes_watch_callbacks_;
        }
        PDLOG(INFO,
              "handle node changed event with type %d, and state %d, endpoints "
              "size %d, callback size %d",
              type, state, endpoints.size(), watch_callbacks_vec.size());
        std::vector<NodesChangedCallback>::iterator it = watch_callbacks_vec.begin();
        for (; it != watch_callbacks_vec.end(); ++it) {
            (*it)(endpoints);
        }
    }
}

bool ZkClient::Register(bool startup_flag) {
    std::string node = nodes_root_path_ + "/" + endpoint_;
    bool ok = Mkdir(nodes_root_path_);
    if (!ok) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    std::string value = endpoint_.c_str();
    if (startup_flag) {
        value = "startup_" + endpoint_;
    }
    int ret = zoo_create(zk_, node.c_str(), value.c_str(), value.size(), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
    if (ret == ZOK) {
        PDLOG(INFO, "register self with endpoint %s ok", endpoint_.c_str());
        registed_.store(true, std::memory_order_relaxed);
        return true;
    }
    PDLOG(WARNING, "fail to register self with endpoint %s, err from zk %d", endpoint_.c_str(), ret);
    return false;
}

bool ZkClient::RegisterName() {
    bool ok = Mkdir(names_root_path_);
    if (!ok) {
        return false;
    }
    if (zk_ == NULL || !connected_) {
        return false;
    }
    std::string sname = endpoint_;
    // check server name duplicate
    std::vector<std::string> sname_vec;
    std::string leader_path = zk_root_path_ + "/leader";
    std::vector<std::string> children;
    if (GetChildren(leader_path, children)) {
        for (auto path : children) {
            std::string endpoint;
            std::string real_path = leader_path + "/" + path;
            if (GetNodeValue(real_path, endpoint)) {
                sname_vec.push_back(endpoint);
            }
        }
    }
    std::vector<std::string> endpoints;
    if (GetNodes(endpoints)) {
        std::vector<std::string>::const_iterator it = endpoints.begin();
        for (; it != endpoints.end(); ++it) {
            sname_vec.push_back(*it);
        }
    }
    if (std::find(sname_vec.begin(), sname_vec.end(), sname) != sname_vec.end()) {
        std::string ep;
        if (GetNodeValue(names_root_path_ + "/" + sname, ep) && ep == real_endpoint_) {
            LOG(INFO) << "node:" << sname << "value:" << ep << " exist";
            return true;
        }
        LOG(WARNING) << "server name:" << sname << " duplicate";
        return false;
    }

    std::string name = names_root_path_ + "/" + sname;
    std::string value = real_endpoint_.c_str();
    if (IsExistNode(name) == 0) {
        if (SetNodeValue(name, value)) {
            PDLOG(INFO, "set node with name %s value %s ok", sname.c_str(), value.c_str());
            return true;
        }
        PDLOG(WARNING, "set node with name %s value %s failed", sname.c_str(), value.c_str());
    } else {
        int ret = zoo_create(zk_, name.c_str(), value.c_str(), value.size(), &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
        if (ret == ZOK) {
            PDLOG(INFO, "register with name %s value %s ok", sname.c_str(), value.c_str());
            return true;
        }
        PDLOG(WARNING, "fail to register with name %s value %s, err from zk %d", sname.c_str(), value.c_str(), ret);
    }
    return false;
}

bool ZkClient::CloseZK() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        connected_ = false;
        registed_.store(false, std::memory_order_relaxed);
    }
    if (zk_) {
        zookeeper_close(zk_);
        zk_ = NULL;
    }
    return true;
}

bool ZkClient::CreateNode(const std::string& node, const std::string& value) {
    std::string assigned_path_name;
    return CreateNode(node, value, 0, assigned_path_name);
}

bool ZkClient::CreateNode(const std::string& node, const std::string& value, int flags,
                          std::string& assigned_path_name) {
    if (node.empty()) {
        return false;
    }
    size_t pos = node.find_last_of('/');
    if (pos != std::string::npos && pos == node.length() - 1) {
        PDLOG(WARNING, "node path[%s] is illegal", node.c_str());
        return false;
    }
    if (pos != std::string::npos && pos != node.find_first_of('/')) {
        if (!Mkdir(node.substr(0, pos))) {
            return false;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    uint32_t size = node.size() + 11;
    char path_buffer[size];  // NOLINT
    int ret =
        zoo_create(zk_, node.c_str(), value.c_str(), value.size(), &ZOO_OPEN_ACL_UNSAFE, flags, path_buffer, size);
    if (ret == ZOK) {
        assigned_path_name.assign(path_buffer, size - 1);
        PDLOG(INFO, "create node %s ok and real node name %s", node.c_str(), assigned_path_name.c_str());
        return true;
    }
    PDLOG(WARNING, "fail to create node %s with errno %d", node.c_str(), ret);
    return false;
}

void ZkClient::HandleChildrenChanged(const std::string& path, int type, int state) {
    NodesChangedCallback callback;
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::map<std::string, NodesChangedCallback>::iterator it = children_callbacks_.find(path);
        if (it == children_callbacks_.end()) {
            PDLOG(INFO, "watch for path %s not exist", path.c_str());
            return;
        }
        callback = it->second;
    }
    if (type == ZOO_CHILD_EVENT) {
        std::vector<std::string> children;
        bool ok = GetChildren(path, children);
        if (!ok) {
            PDLOG(WARNING, "fail to get nodes for path %s", path.c_str());
            WatchChildren(path, callback);
            return;
        }
        PDLOG(INFO, "handle node changed event with type %d, and state %d for path %s", type, state, path.c_str());
        callback(children);
    }
    WatchChildren(path, callback);
}

void ZkClient::CancelWatchChildren(const std::string& node) {
    std::lock_guard<std::mutex> lock(mu_);
    children_callbacks_.erase(node);
}

bool ZkClient::WatchChildren(const std::string& node, NodesChangedCallback callback) {
    std::lock_guard<std::mutex> lock(mu_);
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
        PDLOG(WARNING, "fail to watch path %s errno %d", node.c_str(), ret);
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

bool ZkClient::GetNodeValueUnLocked(const std::string& node, std::string& value) {
    int buffer_len = ZK_MAX_BUFFER_SIZE;
    Stat stat;
    if (zoo_get(zk_, node.c_str(), 0, buffer_, &buffer_len, &stat) == ZOK) {
        value.assign(buffer_, buffer_len);
        return true;
    }
    return false;
}

bool ZkClient::GetNodeValueAndStat(const char* node, std::string* value, Stat* stat) {
    std::lock_guard<std::mutex> lock(mu_);
    DCHECK(value != nullptr && stat != nullptr);
    int buffer_len = ZK_MAX_BUFFER_SIZE;
    if (zoo_get(zk_, node, 0, buffer_, &buffer_len, stat) == ZOK) {
        value->assign(buffer_, buffer_len);
        return true;
    }
    return false;
}

bool ZkClient::DeleteNode(const std::string& node) {
    std::lock_guard<std::mutex> lock(mu_);
    if (zoo_delete(zk_, node.c_str(), -1) == ZOK) {
        return true;
    }
    return false;
}

bool ZkClient::GetNodeValue(const std::string& node, std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetNodeValueUnLocked(node, value);
}

bool ZkClient::SetNodeValue(const std::string& node, const std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);
    if (node.empty()) {
        return false;
    }
    if (zoo_set(zk_, node.c_str(), value.c_str(), value.length(), -1) == ZOK) {
        return true;
    }
    return false;
}

bool ZkClient::Increment(const std::string& node) {
    int try_num = 3;
    while (try_num-- > 0) {
        std::string value;
        int buffer_len = ZK_MAX_BUFFER_SIZE;
        Stat stat;
        std::lock_guard<std::mutex> lock(mu_);
        if (zoo_get(zk_, node.c_str(), 0, buffer_, &buffer_len, &stat) == ZOK) {
            value.assign(buffer_, buffer_len);
        } else {
            continue;
        }
        uint64_t number = 0;
        try {
            number = boost::lexical_cast<uint64_t>(value);
        } catch (const std::exception& e) {
            return false;
        }
        std::string new_value = std::to_string(number + 1);
        if (zoo_set(zk_, node.c_str(), new_value.c_str(), new_value.length(), stat.version) == ZOK) {
            return true;
        }
        PDLOG(INFO, "retry increment %s", node);
    }
    return false;
}

int ZkClient::IsExistNodeUnLocked(const std::string& node) {
    if (node.empty()) {
        return -1;
    }
    Stat stat;
    int ret = zoo_exists(zk_, node.c_str(), 0, &stat);
    if (ret == ZOK) {
        return 0;
    } else if (ret == ZNONODE) {
        return 1;
    }
    return -1;
}

int ZkClient::IsExistNode(const std::string& node) {
    std::lock_guard<std::mutex> lock(mu_);
    return IsExistNodeUnLocked(node);
}

bool ZkClient::WatchNodes() {
    std::lock_guard<std::mutex> lock(mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    deallocate_String_vector(&data_);
    int ret = zoo_wget_children(zk_, nodes_root_path_.c_str(), NodeWatcher, NULL, &data_);
    if (ret != ZOK) {
        PDLOG(WARNING, "fail to watch path %s errno %d", nodes_root_path_.c_str(), ret);
        return false;
    }
    return true;
}

void ZkClient::HandleItemChanged(const std::string& path, int type, int state) {
    ItemChangedCallback callback;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = item_callbacks_.find(path);
        if (it == item_callbacks_.end()) {
            PDLOG(INFO, "watch for path %s does not exist", path.c_str());
            return;
        }
        callback = it->second;
    }
    WatchItem(path, callback);
    if (type == ZOO_CHANGED_EVENT) {
        callback();
    }
}

void ZkClient::CancelWatchItem(const std::string& path) {
    std::lock_guard<std::mutex> lock(mu_);
    item_callbacks_.erase(path);
}

bool ZkClient::WatchItem(const std::string& path, ItemChangedCallback callback) {
    std::lock_guard<std::mutex> lock(mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    auto it = item_callbacks_.find(path);
    if (it == item_callbacks_.end()) {
        item_callbacks_.insert(std::make_pair(path, callback));
    }
    deallocate_String_vector(&data_);
    int buffer_len = ZK_MAX_BUFFER_SIZE;
    int ret = zoo_wget(zk_, path.data(), ItemWatcher, NULL, buffer_, &buffer_len, NULL);
    if (ret != ZOK) {
        PDLOG(WARNING, "fail to watch item %s errno %d", nodes_root_path_.c_str(), ret);
        return false;
    }
    return true;
}

void ZkClient::WatchNodes(NodesChangedCallback callback) {
    std::lock_guard<std::mutex> lock(mu_);
    nodes_watch_callbacks_.push_back(callback);
}

bool ZkClient::GetChildrenUnLocked(const std::string& path, std::vector<std::string>& children) {
    if (zk_ == NULL || !connected_) {
        return false;
    }
    struct String_vector data;
    data.count = 0;
    data.data = NULL;
    absl::Cleanup data_deallocator = [&data] { deallocate_String_vector(&data); };
    int ret = zoo_get_children(zk_, path.c_str(), 0, &data);
    if (ret != ZOK) {
        PDLOG(WARNING, "fail to get children from path %s with errno %d", path.c_str(), ret);
        return false;
    }
    for (int32_t i = 0; i < data.count; i++) {
        children.push_back(std::string(data.data[i]));
    }
    std::sort(children.begin(), children.end());
    return true;
}

bool ZkClient::GetChildren(const std::string& path, std::vector<std::string>& children) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetChildrenUnLocked(path, children);
}

bool ZkClient::GetNodes(std::vector<std::string>& endpoints) {
    std::lock_guard<std::mutex> lock(mu_);
    if (zk_ == NULL || !connected_) {
        return false;
    }
    struct String_vector data;
    data.count = 0;
    data.data = NULL;
    absl::Cleanup data_deallocator = [&data] { deallocate_String_vector(&data); };
    int ret = zoo_get_children(zk_, nodes_root_path_.c_str(), 0, &data);
    if (ret != ZOK) {
        PDLOG(WARNING, "fail to get children from path %s with errno %d", nodes_root_path_.c_str(), ret);
        return false;
    }
    for (int32_t i = 0; i < data.count; i++) {
        endpoints.push_back(std::string(data.data[i]));
    }
    return true;
}

bool ZkClient::Reconnect() {
    std::unique_lock<std::mutex> lock(mu_);
    if (zk_ != NULL) {
        zookeeper_close(zk_);
    }
    registed_.store(false, std::memory_order_relaxed);
    zk_ = zookeeper_init(hosts_.c_str(), LogEventWrapper, session_timeout_, 0, (void*)this, 0);  // NOLINT

    cv_.wait_for(lock, std::chrono::milliseconds(session_timeout_));
    if (zk_ == NULL || !connected_) {
        PDLOG(WARNING, "fail to init zk handler with hosts %s, session_timeout %d", hosts_.c_str(), session_timeout_);
        return false;
    }
    return true;
}

void ZkClient::LogEvent(int type, int state, const char* path) {
    PDLOG(INFO, "zookeeper event with type %d, state %d, path %s", type, state, path);
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            Connected();
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            connected_ = false;
        }
    }
}

void ZkClient::Connected() {
    std::lock_guard<std::mutex> lock(mu_);
    connected_ = true;
    session_term_.fetch_add(1, std::memory_order_relaxed);
    cv_.notify_one();
    PDLOG(INFO, "connect success");
}

bool ZkClient::MkdirNoLock(const std::string& path) {
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
        index++;
        int ret = zoo_create(zk_, full_path.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
        if (ret == ZNODEEXISTS || ret == ZOK) {
            continue;
        }
        PDLOG(WARNING, "fail to create zk node with path %s , errno %d", full_path.c_str(), ret);
        return false;
    }
    return true;
}

bool ZkClient::Mkdir(const std::string& path) {
    std::lock_guard<std::mutex> lock(mu_);
    return MkdirNoLock(path);
}

}  // namespace zk
}  // namespace openmldb
