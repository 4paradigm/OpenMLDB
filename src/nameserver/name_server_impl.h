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
#include "proto/tablet.pb.h"
#include "zk/dist_lock.h"
#include "zk/zk_client.h"
#include <atomic>
#include <map>
#include <list>
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

typedef boost::function<void ()> TaskFun;

struct Task {
    Task(uint64_t op_id, ::rtidb::api::OPType op_type, ::rtidb::api::TaskType task_type, 
            ::rtidb::api::TaskStatus task_status, const std::string& endpoint) : 
            op_id_(op_id), op_type_(op_type), task_type_(task_type), task_status_(task_status), endpoint_(endpoint){}
    ~Task() {}
    uint64_t op_id_;
    ::rtidb::api::OPType op_type_;
    ::rtidb::api::TaskType task_type_;
    ::rtidb::api::TaskStatus task_status_;
    std::string endpoint_;
    TaskFun fun;
};

struct OPData {
    ::rtidb::api::OPInfo op_info_;
    std::list<std::shared_ptr<Task>> task_list;
};

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

    void MakeSnapshotNS(RpcController* controller,
            const MakeSnapshotNSRequest* request,
            GeneralResponse* response,
            Closure* done);

    int ConstructCreateTableTask(std::shared_ptr<::rtidb::nameserver::TableMeta> table_meta,
            bool is_leader,
            std::list<std::shared_ptr<Task>>& task_list,
            std::vector<std::string>& endpoint_vec);

    void CheckZkClient();

    int UpdateTaskStatus();

    int DeleteTask();

    void ProcessTask();

    int UpdateZKTaskStatus(const std::vector<uint64_t>& run_task_vec);

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
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableMeta>> table_info_;
    ZkClient* zk_client_;
    DistLock* dist_lock_;
    ::baidu::common::ThreadPool thread_pool_;
    ::baidu::common::ThreadPool task_thread_pool_;
    std::string zk_table_path_;
    std::string zk_data_path_;
    std::string zk_table_index_node_;
    uint32_t table_index_;
    std::string zk_op_index_node_;
    uint64_t op_index_;
    std::string zk_op_path_;
    std::atomic<bool> running_;
    std::map<uint64_t, std::shared_ptr<OPData>> task_map_;
    std::map<std::string, uint64_t> table_task_map_;
    CondVar cv_;
};

}
}
#endif
