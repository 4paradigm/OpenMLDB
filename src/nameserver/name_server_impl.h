//
// name_server_impl.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05

#ifndef RTIDB_NAME_SERVER_H
#define RTIDB_NAME_SERVER_H

#include "client/tablet_client.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "zk/dist_lock.h"
#include "zk/zk_client.h"
#include <atomic>
#include <map>
#include <list>
#include <brpc/server.h>
#include <mutex>
#include <condition_variable>

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
    Task(const std::string& endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info) : 
            endpoint_(endpoint), task_info_(task_info) {}
    ~Task() {}
    std::string endpoint_;
    std::shared_ptr<::rtidb::api::TaskInfo> task_info_;
    TaskFun fun_;
};

struct OPData {
    OPData() : start_time_(0), end_time_(0) {}
    ::rtidb::api::OPInfo op_info_;
    ::rtidb::api::TaskStatus task_status_;
    std::list<std::shared_ptr<Task>> task_list_;
    uint64_t start_time_;
    uint64_t end_time_;
};

class NameServerImpl : public NameServer {

public:

    NameServerImpl();

    ~NameServerImpl();

    bool Init();

    NameServerImpl(const NameServerImpl&) = delete;

    NameServerImpl& operator= (const NameServerImpl&) = delete; 

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

    void AddReplicaNS(RpcController* controller,
            const AddReplicaNSRequest* request,
            GeneralResponse* response,
            Closure* done);

    void ShowOPStatus(RpcController* controller,
            const ShowOPStatusRequest* request,
            ShowOPStatusResponse* response,
            Closure* done);

    int CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader,
            std::map<uint32_t, std::vector<std::string>>& endpoint_map);

    void CheckZkClient();

    int UpdateTaskStatus();

    int DeleteTask();

    void ProcessTask();

    int UpdateZKTaskStatus();

private:

    // Recover all memory status, the steps
    // 1.recover table meta from zookeeper
    // 2.recover table status from all tablets
    bool Recover();

    bool RecoverTableInfo();

    bool RecoverOPTask();

    bool RecoverMakeSnapshot(std::shared_ptr<OPData> op_data);

    void SkipDoneTask(uint32_t task_index, std::list<std::shared_ptr<Task>>& task_list);

    // Get the lock
    void OnLocked();
    // Lost the lock
    void OnLostLock();

    // Update tablets from zookeeper
    void UpdateTablets(const std::vector<std::string>& endpoints);

    void UpdateTabletsLocked(const std::vector<std::string>& endpoints);

    std::shared_ptr<Task> CreateMakeSnapshotTask(const std::string& endpoint, 
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid);

    std::shared_ptr<Task> CreatePauseSnapshotTask(const std::string& endpoint, 
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid);

    std::shared_ptr<Task> CreateRecoverSnapshotTask(const std::string& endpoint, 
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid);

	std::shared_ptr<Task> CreateSendSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
                    const std::string& des_endpoint);

    std::shared_ptr<Task> CreateLoadTableTask(const std::string& endpoint, 
                    uint64_t op_index, ::rtidb::api::OPType op_type, const std::string& name,
                    uint32_t tid, uint32_t pid, uint64_t ttl, uint32_t seg_cnt);

    std::shared_ptr<Task> CreateAddReplicaTask(const std::string& endpoint, 
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
					const std::string& des_endpoint);

private:
    std::mutex mu_;
    Tablets tablets_;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>> table_info_;
    ZkClient* zk_client_;
    DistLock* dist_lock_;
    ::baidu::common::ThreadPool thread_pool_;
    ::baidu::common::ThreadPool task_thread_pool_;
    std::string zk_table_index_node_;
    std::string zk_table_data_path_;
    uint32_t table_index_;
    std::string zk_op_index_node_;
    std::string zk_op_data_path_;
    uint64_t op_index_;
    std::atomic<bool> running_;
    std::map<uint64_t, std::shared_ptr<OPData>> task_map_;
    std::condition_variable cv_;
};

}
}
#endif
