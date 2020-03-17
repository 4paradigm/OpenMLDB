//
// zk_client.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-04
//


#ifndef RTIDB_ZK_CLIENT_H
#define RTIDB_ZK_CLIENT_H

#include "boost/function.hpp"
#include <mutex>
#include <condition_variable>
#include <map>
#include <vector>
#include <atomic>

extern "C" {
#include "zookeeper/zookeeper.h"
}

namespace rtidb {
namespace zk {

typedef boost::function<void (const std::vector<std::string>& endpoint)> NodesChangedCallback;

const uint32_t ZK_MAX_BUFFER_SIZE = 1024 * 1024;

class ZkClient {

public:

    // hosts , the zookeeper server lists eg host1:2181,host2:2181
    // session_timeout, the session timeout
    // endpoint, the client endpoint
    ZkClient(const std::string& hosts, 
             int32_t session_timeout,
             const std::string& endpoint,
             const std::string& zk_root_path);

    ZkClient(const std::string &hosts,
             int32_t session_timeout,
             const std::string &endpoint,
             const std::string &zk_root_path,
             const std::string &zone_path);
    ~ZkClient();

    // init zookeeper connections
    bool Init();

    // the client will create a ephemeral node in zk_root_path
    // eg {zk_root_path}/nodes/000000 -> endpoint
    bool Register(bool startup_flag = false);

    // close zk connection
    bool CloseZK();

    // always watch the all nodes in {zk_root_path}/nodes/
    void WatchNodes(NodesChangedCallback callback);

    // handle node change events
    void HandleNodesChanged(int type, int state);

    // get all alive nodes
    bool GetNodes(std::vector<std::string>& endpoints);

    bool GetChildren(const std::string& path, std::vector<std::string>& children);

    // log all event from zookeeper
    void LogEvent(int type, int state, const char* path);

    bool Mkdir(const std::string& path);

    bool GetNodeValue(const std::string& node, std::string& value);
    bool GetNodeValueLocked(const std::string& node, std::string& value);

    bool SetNodeValue(const std::string& node, const std::string& value);

    bool SetNodeWatcher(const std::string& node, 
                        watcher_fn watcher, 
                        void* watcherCtx);

    bool WatchChildren(const std::string& node, 
                       NodesChangedCallback callback);

    void CancelWatchChildren(const std::string& node);

    void HandleChildrenChanged(const std::string& path, 
                               int type, int state);

    bool DeleteNode(const std::string& node);                           

    // create a persistence node
    bool CreateNode(const std::string& node,
                    const std::string& value);

    bool CreateNode(const std::string& node, 
                    const std::string& value, 
                    int flags,
                    std::string& assigned_path_name);

    bool WatchNodes();

    int IsExistNode(const std::string& node);

    inline bool IsConnected() {
        std::lock_guard<std::mutex> lock(mu_);
        return connected_;
    }

    inline bool IsRegisted() {
        return registed_.load(std::memory_order_relaxed);
    }

	inline uint64_t GetSessionTerm() {
        return session_term_.load(std::memory_order_relaxed);
    }

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

    // internal args
    std::string nodes_root_path_;
    std::vector<NodesChangedCallback> nodes_watch_callbacks_;

    //
    std::mutex mu_;
    std::condition_variable cv_;
    zhandle_t* zk_;
    bool nodes_watching_;

    struct String_vector data_;
    bool connected_;
    std::atomic<bool> registed_;
    std::map<std::string, NodesChangedCallback> children_callbacks_;
    char buffer_[ZK_MAX_BUFFER_SIZE];
    std::atomic<uint64_t> session_term_;
};

}
}

#endif /* !RTIDB_ZK_CLIENT_H */
