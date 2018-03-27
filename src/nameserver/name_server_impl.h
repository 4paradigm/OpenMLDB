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
    ::rtidb::api::OPInfo op_info_;
    std::list<std::shared_ptr<Task>> task_list_;
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

    void DropTable(RpcController* controller,
        const DropTableRequest* request,
        GeneralResponse* response, 
        Closure* done);

    void ShowTablet(RpcController* controller,
            const ShowTabletRequest* request,
            ShowTabletResponse* response,
            Closure* done);

    void ShowTable(RpcController* controller,
            const ShowTableRequest* request,
            ShowTableResponse* response,
            Closure* done);

    void MakeSnapshotNS(RpcController* controller,
            const MakeSnapshotNSRequest* request,
            GeneralResponse* response,
            Closure* done);

    void AddReplicaNS(RpcController* controller,
            const AddReplicaNSRequest* request,
            GeneralResponse* response,
            Closure* done);

    void DelReplicaNS(RpcController* controller,
            const DelReplicaNSRequest* request,
            GeneralResponse* response,
            Closure* done);

    void ShowOPStatus(RpcController* controller,
            const ShowOPStatusRequest* request,
            ShowOPStatusResponse* response,
            Closure* done);

    void ConfSet(RpcController* controller,
            const ConfSetRequest* request,
            GeneralResponse* response,
            Closure* done);

    void ConfGet(RpcController* controller,
            const ConfGetRequest* request,
            ConfGetResponse* response,
            Closure* done);

    void ChangeLeader(RpcController* controller,
            const ChangeLeaderRequest* request,
            GeneralResponse* response,
            Closure* done);

    void OfflineEndpoint(RpcController* controller,
            const OfflineEndpointRequest* request,
            GeneralResponse* response,
            Closure* done);

    void Migrate(RpcController* controller,
            const MigrateRequest* request,
            GeneralResponse* response,
            Closure* done);

    void RecoverEndpoint(RpcController* controller,
            const RecoverEndpointRequest* request,
            GeneralResponse* response,
            Closure* done);

    void RecoverTable(RpcController* controller,
            const RecoverTableRequest* request,
            GeneralResponse* response,
            Closure* done);

    void ConnectZK(RpcController* controller,
            const ConnectZKRequest* request,
            GeneralResponse* response,
            Closure* done);

    void DisConnectZK(RpcController* controller,
            const DisConnectZKRequest* request,
            GeneralResponse* response,
            Closure* done);

    int CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader, const std::vector<::rtidb::base::ColumnDesc>& columns,
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

    int CreateMakeSnapshotOPTask(std::shared_ptr<OPData> op_data);

    int CreateAddReplicaOPTask(std::shared_ptr<OPData> op_data);

    int CreateChangeLeaderOPTask(std::shared_ptr<OPData> op_data);

    int CreateMigrateTask(std::shared_ptr<OPData> op_data);

    int CreateRecoverTableOPTask(std::shared_ptr<OPData> op_data);

    int CreateOfflineReplicaTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaNoSendTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaWithDropTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaSimplifyTask(std::shared_ptr<OPData> op_data);

    int CreateUpdateTableAliveOPTask(std::shared_ptr<OPData> op_data);

    int CreateReLoadTableTask(std::shared_ptr<OPData> op_data);

    int CreateUpdatePartitionStatusOPTask(std::shared_ptr<OPData> op_data);

    bool SkipDoneTask(std::shared_ptr<OPData> op_data);

    // Get the lock
    void OnLocked();
    // Lost the lock
    void OnLostLock();

    // Update tablets from zookeeper
    void UpdateTablets(const std::vector<std::string>& endpoints);

    void OnTabletOffline(const std::string& endpoint);

    void OnTabletOnline(const std::string& endpoint);

    void UpdateTabletsLocked(const std::vector<std::string>& endpoints);

    void DelTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    int ConvertColumnDesc(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
                    std::vector<::rtidb::base::ColumnDesc>& columns);                

    void UpdatePartitionStatus(const std::string& name, const std::string& endpoint, uint32_t pid,
                    bool is_leader, bool is_alive, std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    void UpdateTableAlive(const std::string& name, const std::string& endpoint, 
                    bool is_alive, std::shared_ptr<::rtidb::api::TaskInfo> task_info);

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

    std::shared_ptr<Task> CreateAddTableInfoTask(const std::string& name,  uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type);

    void AddTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    std::shared_ptr<Task> CreateDelReplicaTask(const std::string& endpoint, 
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
					const std::string& follower_endpoint);

    std::shared_ptr<Task> CreateDelTableInfoTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type);

    std::shared_ptr<Task> CreateUpdateTableInfoTask(const std::string& src_endpoint, 
                    const std::string& name, uint32_t pid, const std::string& des_endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type);

    void UpdateTableInfo(const std::string& src_endpoint, const std::string& name, uint32_t pid,
                    const std::string& des_endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    std::shared_ptr<Task> CreateUpdatePartitionStatusTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, bool is_leader, bool is_alive, 
                    uint64_t op_index, ::rtidb::api::OPType op_type);

    std::shared_ptr<Task> CreateUpdateTableAliveTask(const std::string& name, 
                    const std::string& endpoint, bool is_alive, 
                    uint64_t op_index, ::rtidb::api::OPType op_type);

    std::shared_ptr<Task> CreateSelectLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t tid, uint32_t pid,
                    std::vector<std::string>& follower_endpoint);

    std::shared_ptr<Task> CreateChangeLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid);

    std::shared_ptr<Task> CreateUpdateLeaderInfoTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid);

	std::shared_ptr<Task> CreateDropTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid);

	std::shared_ptr<Task> CreateRecoverTableTask(uint64_t op_index, ::rtidb::api::OPType op_type, 
                    const std::string& name, uint32_t pid, const std::string& endpoint);

    int CreateOPData(::rtidb::api::OPType op_type, const std::string& value, std::shared_ptr<OPData>& op_data);
    int AddOPData(const std::shared_ptr<OPData>& op_data);
    int CreateDelReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint,
                     ::rtidb::api::OPType op_type);
    int CreateChangeLeaderOP(const std::string& name, uint32_t pid);
    int CreateRecoverTableOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    void SelectLeader(const std::string& name, uint32_t tid, uint32_t pid, 
                    std::vector<std::string>& follower_endpoint, 
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);
    void ChangeLeader(std::shared_ptr<::rtidb::api::TaskInfo> task_info);                
    void UpdateLeaderInfo(std::shared_ptr<::rtidb::api::TaskInfo> task_info);                
    int CreateMigrateOP(const std::string& src_endpoint, const std::string& name, uint32_t pid,
                    const std::string& des_endpoint);
    void RecoverEndpointTable(const std::string& name, uint32_t pid, const std::string& endpoint,
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);
    int GetLeader(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info, uint32_t pid, std::string& leader_endpoint);
    int MatchTermOffset(const std::string& name, uint32_t pid, bool has_table, uint64_t term, uint64_t offset);
    int CreateReAddReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    int CreateReAddReplicaSimplifyOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    int CreateReAddReplicaWithDropOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    int CreateReAddReplicaNoSendOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    int CreateUpdateTableAliveOP(const std::string& name, const std::string& endpoint, bool is_alive);
    int CreateReLoadTableOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    int CreateUpdatePartitionStatusOP(const std::string& name, uint32_t pid, const std::string& endpoint,
                    bool is_leader, bool is_alive);

    void NotifyTableChanged();

private:
    std::mutex mu_;
    Tablets tablets_;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>> table_info_;
    ZkClient* zk_client_;
    DistLock* dist_lock_;
    ::baidu::common::ThreadPool thread_pool_;
    ::baidu::common::ThreadPool task_thread_pool_;
    std::string zk_table_index_node_;
    std::string zk_term_node_;
    std::string zk_table_data_path_;
    std::string zk_auto_failover_node_;
    std::string zk_auto_recover_table_node_;
    std::string zk_table_changed_notify_node_;
    uint32_t table_index_;
    uint64_t term_;
    std::string zk_op_index_node_;
    std::string zk_op_data_path_;
    uint64_t op_index_;
    uint64_t doing_op_num_;
    std::atomic<bool> running_;
    std::map<uint64_t, std::shared_ptr<OPData>> task_map_;
    std::condition_variable cv_;
    std::atomic<bool> auto_failover_;
    std::atomic<bool> auto_recover_table_;
};

}
}
#endif
