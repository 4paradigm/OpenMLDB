//
// name_server_impl.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05

#ifndef RTIDB_NAME_SERVER_H
#define RTIDB_NAME_SERVER_H

#include "client/tablet_client.h"
#include "mutex.h"
#include "proto/name_server.pb.h"
#include "zk/dist_lock.h"
#include "zk/zk_client.h"
#include <atomic>
#include <map>
#include <sofa/pbrpc/pbrpc.h>

namespace rtidb {
namespace nameserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::rtidb::zk::ZkClient;
using ::rtidb::zk::DistLock;
using ::rtidb::api::TabletState;
using ::rtidb::client::TabletClient;

// tablet info
struct TabletInfo {
    // tablet state
    TabletState state_;
    // tablet rpc handle
    std::shared_ptr<TabletClient> client_; 
    // the date create
    uint64_t ctime_;
};

// the container of tablet
typedef std::map<std::string, std::shared_ptr<TabletInfo>> Tablets;

class NameServerImpl : public NameServer {

public:

    NameServerImpl();

    ~NameServerImpl();

    bool Init();

    NameServerImpl(const NameServerImpl&) = delete;

    NameServerImpl& operator= (const NameServerImpl&) = delete; 

    bool WebService(const sofa::pbrpc::HTTPRequest& request,
                sofa::pbrpc::HTTPResponse& response);

    void CreateTable(RpcController* controller,
        const CreateTableRequest* request,
        GeneralResponse* response, 
        Closure* done);

    void ShowTablet(RpcController* controller,
            const ShowTabletRequest* request,
            ShowTabletResponse* response,
            Closure* done);

    int CreateTable(const ::rtidb::nameserver::TableMeta& table_meta, uint32_t tid,
                    bool is_leader, std::map<uint32_t, std::vector<std::string> >& endpoint_vec);

    void CheckZkClient();

private:

    // Recover all memory status, the steps
    // 1.recover table meta from zookeeper
    // 2.recover table status from all tablets
    bool Recover();

    // Get the lock
    void OnLocked();
    // Lost the lock
    void OnLostLock();

    // Update tablets from zookeeper
    void UpdateTablets(const std::vector<std::string>& endpoints);
    void UpdateTabletsLocked(const std::vector<std::string>& endpoints);

private:
    ::baidu::common::Mutex mu_;
    Tablets tablets_;
    std::map<std::string, ::rtidb::nameserver::TableMeta> table_info_;
    ZkClient* zk_client_;
    DistLock* dist_lock_;
    ::baidu::common::ThreadPool thread_pool_;
    std::string zk_table_path_;
    std::string zk_data_path_;
    std::string zk_table_index_node_;
    std::atomic<bool> running_;
};

}
}
#endif
