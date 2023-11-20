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

#ifndef SRC_ZK_ZK_CLIENT_H_
#define SRC_ZK_ZK_CLIENT_H_

#include <atomic>
#include <condition_variable>  // NOLINT
#include <cstdio>
#include <map>
#include <mutex>  // NOLINT
#include <string>
#include <vector>

#include "boost/function.hpp"

extern "C" {
#include "zookeeper/zookeeper.h"
}

namespace openmldb {
namespace zk {

typedef boost::function<void(const std::vector<std::string>& endpoint)> NodesChangedCallback;

typedef boost::function<void(void)> ItemChangedCallback;

const uint32_t ZK_MAX_BUFFER_SIZE = 1024 * 1024;

class ZkClient {
 public:
    // hosts, the zookeeper server lists eg host1:2181,host2:2181
    // session_timeout, the session timeout
    // endpoint, the client endpoint
    ZkClient(const std::string& hosts, const std::string& real_endpoint, int32_t session_timeout,
             const std::string& endpoint, const std::string& zk_root_path,
             const std::string& auth_schema, const std::string& cert);

    ZkClient(const std::string& hosts, int32_t session_timeout, const std::string& endpoint,
             const std::string& zk_root_path, const std::string& zone_path,
             const std::string& auth_schema, const std::string& cert);
    ~ZkClient();

    // init zookeeper connections
    // log level: DISABLE_LOGGING=0, ZOO_LOG_LEVEL_ERROR=1, ZOO_LOG_LEVEL_WARN=2, ZOO_LOG_LEVEL_INFO=3,
    // ZOO_LOG_LEVEL_DEBUG=4
    // log file: empty means no file, it'll print to stderr If you want to disable zk log, set
    // level 0, or set the file "/dev/null".
    bool Init(int log_level = 3, const std::string& log_file = {});

    // the client will create a ephemeral node in zk_root_path
    // eg {zk_root_path}/nodes/000000 -> endpoint
    bool Register(bool startup_flag = false);

    bool RegisterName();

    // close zk connection
    bool CloseZK();

    // always watch the all nodes in {zk_root_path}/nodes/
    void WatchNodes(NodesChangedCallback callback);

    // handle node change events
    void HandleNodesChanged(int type, int state);

    // get all alive nodes
    bool GetNodes(std::vector<std::string>& endpoints);  // NOLINT

    bool GetChildrenUnLocked(const std::string& path,
                             std::vector<std::string>& children);  // NOLINT
    bool GetChildren(const std::string& path,
                     std::vector<std::string>& children);  // NOLINT

    // log all event from zookeeper
    void LogEvent(int type, int state, const char* path);

    bool MkdirNoLock(const std::string& path);
    bool Mkdir(const std::string& path);

    bool GetNodeValue(const std::string& node, std::string& value);  // NOLINT
    bool GetNodeValueUnLocked(const std::string& node,
                              std::string& value);  // NOLINT

    bool GetNodeValueAndStat(const char* node, std::string* value, Stat* stat);

    bool SetNodeValue(const std::string& node, const std::string& value);

    bool SetNodeWatcher(const std::string& node, watcher_fn watcher, void* watcherCtx);

    bool Increment(const std::string& node);

    bool WatchChildren(const std::string& node, NodesChangedCallback callback);

    void CancelWatchChildren(const std::string& node);

    void HandleChildrenChanged(const std::string& path, int type, int state);

    bool DeleteNode(const std::string& node);

    // create a persistence node
    bool CreateNode(const std::string& node, const std::string& value);

    bool CreateNode(const std::string& node, const std::string& value, int flags,
                    std::string& assigned_path_name);  // NOLINT

    bool WatchNodes();

    void HandleItemChanged(const std::string& path, int type, int state);

    bool WatchItem(const std::string& path, ItemChangedCallback callback);
    void CancelWatchItem(const std::string& path);

    int IsExistNodeUnLocked(const std::string& node);
    int IsExistNode(const std::string& node);

    inline bool IsConnected() {
        std::lock_guard<std::mutex> lock(mu_);
        return connected_;
    }

    inline bool IsRegisted() { return registed_.load(std::memory_order_relaxed); }

    inline uint64_t GetSessionTerm() { return session_term_.load(std::memory_order_relaxed); }

    // when reconnect, need Register and Watchnodes again
    bool Reconnect();

 private:
    void Connected();

 private:
    // input args
    std::string hosts_;
    int32_t session_timeout_;
    std::string endpoint_;
    std::string zk_root_path_;
    std::string auth_schema_;
    std::string cert_;
    struct ACL_vector acl_vector_;
    std::string real_endpoint_;

    FILE* zk_log_stream_file_ = NULL;

    // internal args
    std::string nodes_root_path_;
    std::vector<NodesChangedCallback> nodes_watch_callbacks_;
    std::string names_root_path_;

    //
    std::mutex mu_;
    std::condition_variable cv_;
    zhandle_t* zk_;
    bool nodes_watching_;

    struct String_vector data_;
    bool connected_;
    std::atomic<bool> registed_;
    std::map<std::string, NodesChangedCallback> children_callbacks_;
    std::map<std::string, ItemChangedCallback> item_callbacks_;
    char buffer_[ZK_MAX_BUFFER_SIZE];
    std::atomic<uint64_t> session_term_;
};

}  // namespace zk
}  // namespace openmldb

#endif  // SRC_ZK_ZK_CLIENT_H_
