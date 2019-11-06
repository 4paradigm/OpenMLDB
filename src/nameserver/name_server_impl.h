//
// name_server_impl.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05

#ifndef RTIDB_NAME_SERVER_H
#define RTIDB_NAME_SERVER_H

#include "client/tablet_client.h"
#include "client/ns_client.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "zk/dist_lock.h"
#include "zk/zk_client.h"
#include <atomic>
#include <map>
#include <unordered_map>
#include <list>
#include <brpc/server.h>
#include <mutex>
#include <condition_variable>
#include "base/random.h"
#include "base/schema_codec.h"

DECLARE_uint32(name_server_task_concurrency);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace nameserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::rtidb::zk::ZkClient;
using ::rtidb::zk::DistLock;
using ::rtidb::api::TabletState;
using ::rtidb::client::TabletClient;

const uint64_t INVALID_PARENT_ID = UINT64_MAX;

// tablet info
struct TabletInfo {
    // tablet state
    TabletState state_;
    // tablet rpc handle
    std::shared_ptr<TabletClient> client_;
    // the date create
    uint64_t ctime_;
};

class ClusterInfo {
public:
    ClusterInfo(const ::rtidb::nameserver::ClusterAddress& cd, ::baidu::common::ThreadPool* tp): mu_(), thread_pool_(tp) {
        cluster_add_.CopyFrom(cd);
        ctime_ = ::baidu::common::timer::get_micros()/1000;
    }
    ~ClusterInfo() {
        std::lock_guard<std::mutex> lock(mu_);
        thread_pool_->CancelTask(task_id_);
    }
    void CheckZkClient() {
        if (!zk_client_->IsConnected()) {
            PDLOG(WARNING, "reconnect zk");
            if (zk_client_->Reconnect()) {
                PDLOG(INFO, "reconnect zk ok");
            }
        }
        if (session_term_ != zk_client_->GetSessionTerm()) {
            if (zk_client_->WatchNodes()) {
                session_term_ = zk_client_->GetSessionTerm();
                PDLOG(INFO, "watch node ok");
            } else {
                PDLOG(WARNING, "watch node failed");
            }
        }
        std::lock_guard<std::mutex> lock(mu_);
        task_id_ = thread_pool_->DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&ClusterInfo::CheckZkClient, this));
    }
    void UpdateNSClient(const std::vector<std::string>& children) {
        std::string endpoint;
        if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/leader/" + children[0], endpoint)) {
            PDLOG(WARNING, "get replica cluster leader ns failed");
            return;
        }
        std::shared_ptr<::rtidb::client::NsClient> tmp_ptr = std::make_shared<::rtidb::client::NsClient>(endpoint);
        if (tmp_ptr->Init() < 0) {
            PDLOG(WARNING, "replica cluster ns client init failed");
            return;
        }
        std::lock_guard<std::mutex> lock(mu_);
        client_ = tmp_ptr;
        ctime_ = ::baidu::common::timer::get_micros() / 1000;
    }
    bool Init(std::string& msg) {

        zk_client_ = std::make_shared<ZkClient>(cluster_add_.zk_endpoints(), 6000, "", \
        cluster_add_.zk_path(), cluster_add_.zk_path() + "/leader");
        if (!zk_client_->Init()) {
            msg = "zk client init failed";
            PDLOG(WARNING, "zk client init failed, zk endpoints: %s, zk path: %s",
                  cluster_add_.zk_endpoints().c_str(), cluster_add_.zk_path().c_str());
            return false;
        }
        session_term_ = zk_client_->GetSessionTerm();
        std::vector<std::string> children;
        if (!zk_client_->GetChildren(cluster_add_.zk_path() + "/leader", children) || children.empty()) {
            msg = "replica cluster get chhilren failed";
            PDLOG(WARNING, "replica cluster get children failed");
            return false;
        }
        std::string endpoint;
        if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/leader/" + children[0], endpoint)) {
            msg = "get replica cluster leader ns failed";
            PDLOG(WARNING, "get replica cluster leader ns failed");
            return false;
        }
        client_ = std::make_shared<::rtidb::client::NsClient>(endpoint);
        if (client_->Init() < 0) {
            msg = "replica cluster ns client init failed";
            PDLOG(WARNING, "replica cluster ns client init failed");
            return false;
        }
        /*
        std::vector<::rtidb::nameserver::TableInfo> tables;
        std::string rpc_msg;
        if (!client.ShowTable("", tables, rpc_msg)) {
            code = 300;
            break;
        }
        if (tables.size() > 0) {
            code = 300; rpc_msg = "remote cluster already has table, cann't add replica cluster";
            break;
        }
         */
        zk_client_->WatchNodes(boost::bind(&ClusterInfo::UpdateNSClient, this, _1));
        zk_client_->WatchNodes();
        task_id_ = thread_pool_->DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&ClusterInfo::CheckZkClient, this));
        return true;
    }
    bool MakeReplicaCluster(const std::string& alias, const std::string& zone_name, const uint64_t& term, std::string& msg) {
        if (!client_->MakeReplicaCluster(alias, zone_name, term, msg)) {
            PDLOG(WARNING, "send MakeReplicaCluster request failed");
            return false;
        }
        return true;
    }

  bool KickReplicaCluster(const std::string& alias, const std::string& zone_name, const uint64_t& term, std::string& msg) {
      if (!client_->KickReplicaCluster(alias, zone_name, term, msg)) {
          PDLOG(WARNING, "send MakeReplicaCluster request failed");
          return false;
      }
      return true;
  }

  const ::rtidb::nameserver::ClusterAddress& ReturnAdd() {
        return cluster_add_;
    }
    const uint64_t& ReturnCt() {
        return ctime_;
    }


private:
  std::shared_ptr<::rtidb::client::NsClient> client_;
  std::shared_ptr<ZkClient> zk_client_;
  ::baidu::common::ThreadPool* thread_pool_;
  ::rtidb::nameserver::ClusterAddress cluster_add_;
  uint64_t session_term_;
  int64_t task_id_;
  std::mutex mu_;
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

class NameServerImplTest;

class NameServerImpl : public NameServer {
    // used for ut
    friend class NameServerImplTest;

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

    void AddTableField(RpcController* controller,
            const AddTableFieldRequest* request,
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

    void UpdateTTL(RpcController* controller,
            const ::rtidb::nameserver::UpdateTTLRequest* request,
            ::rtidb::nameserver::UpdateTTLResponse* response,
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

    void SetTablePartition(RpcController* controller,
            const SetTablePartitionRequest* request,
            GeneralResponse* response,
            Closure* done);

    void GetTablePartition(RpcController* controller,
            const GetTablePartitionRequest* request,
            GetTablePartitionResponse* response,
            Closure* done);

    void UpdateTableAliveStatus(RpcController* controller,
            const UpdateTableAliveRequest* request,
            GeneralResponse* response,
            Closure* done);

    void CancelOP(RpcController* controller,
            const CancelOPRequest* request,
            GeneralResponse* response,
            Closure* done);

    void AddReplicaCluster(RpcController* controller,
            const AddReplicaClusterRequest* request,
            GeneralResponse* response,
            Closure* done);

    void MakeReplicaCluster(RpcController *controller,
            const MakeReplicaClusterRequest *request,
            MakeReplicaClusterResponse *response,
            Closure *done);
    void ShowReplicaCluster(RpcController *controller,
            const GeneralRequest *request,
            ShowReplicaClusterResponse *response,
            Closure *done);
    void RemoveReplicaCLuster(RpcController *controller,
            const RemoveReplicaOfRequest *request,
            GeneralResponse *response,
            Closure *done);
    void KickReplicaCluster(RpcController *controller,
            const KickReplicaClusterRequest *request,
            GeneralResponse *response,
            Closure *done);

  int CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader, const std::vector<::rtidb::base::ColumnDesc>& columns,
            std::map<uint32_t, std::vector<std::string>>& endpoint_map, uint64_t term);

    void CheckZkClient();

    int UpdateTaskStatus(bool is_recover_op);

    int DeleteTask();

    void ProcessTask();

    int UpdateZKTaskStatus();

private:

    // Recover all memory status, the steps
    // 1.recover table meta from zookeeper
    // 2.recover table status from all tablets
    bool Recover();

    bool RecoverTableInfo();

    void RecoverClusterInfo();

    bool RecoverOPTask();

    int SetPartitionInfo(TableInfo& table_info);

    int CheckTableMeta(const TableInfo& table_info);

    int FillColumnKey(TableInfo& table_info);

    int CreateMakeSnapshotOPTask(std::shared_ptr<OPData> op_data);

    int CreateAddReplicaOPTask(std::shared_ptr<OPData> op_data);

    int CreateChangeLeaderOPTask(std::shared_ptr<OPData> op_data);

    int CreateMigrateTask(std::shared_ptr<OPData> op_data);

    int CreateRecoverTableOPTask(std::shared_ptr<OPData> op_data);

    int CreateDelReplicaOPTask(std::shared_ptr<OPData> op_data);

    int CreateOfflineReplicaTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaNoSendTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaWithDropTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaSimplifyTask(std::shared_ptr<OPData> op_data);

    int CreateReLoadTableTask(std::shared_ptr<OPData> op_data);

    int CreateUpdatePartitionStatusOPTask(std::shared_ptr<OPData> op_data);

    bool SkipDoneTask(std::shared_ptr<OPData> op_data);

    // Get the lock
    void OnLocked();
    // Lost the lock
    void OnLostLock();

    // Update tablets from zookeeper
    void UpdateTablets(const std::vector<std::string>& endpoints);

    void OnTabletOffline(const std::string& endpoint, bool startup_flag);

    void RecoverOfflineTablet();

    void OnTabletOnline(const std::string& endpoint);

    void OfflineEndpointInternal(const std::string& endpoint, uint32_t concurrency);

    void RecoverEndpointInternal(const std::string& endpoint, bool need_restore, uint32_t concurrency);

    void UpdateTabletsLocked(const std::vector<std::string>& endpoints);

    void DelTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    void UpdatePartitionStatus(const std::string& name, const std::string& endpoint, uint32_t pid,
                    bool is_leader, bool is_alive, std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    int UpdateEndpointTableAlive(const std::string& endpoint, bool is_alive);

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
                    uint32_t tid, uint32_t pid, uint64_t ttl, uint32_t seg_cnt, bool is_leader,
                    ::rtidb::common::StorageMode storage_mode);

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

    std::shared_ptr<Task> CreateSelectLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t tid, uint32_t pid,
                    std::vector<std::string>& follower_endpoint);

    std::shared_ptr<Task> CreateChangeLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid);

    std::shared_ptr<Task> CreateUpdateLeaderInfoTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid);

    std::shared_ptr<Task> CreateCheckBinlogSyncProgressTask(uint64_t op_index, 
                    ::rtidb::api::OPType op_type, const std::string& name, uint32_t pid,
                    const std::string& follower, uint64_t offset_delta);

	std::shared_ptr<Task> CreateDropTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid);

	std::shared_ptr<Task> CreateRecoverTableTask(uint64_t op_index, ::rtidb::api::OPType op_type, 
                    const std::string& name, uint32_t pid, const std::string& endpoint, 
                    uint64_t offset_delta, uint32_t concurrency);

    std::shared_ptr<TableInfo> GetTableInfo(const std::string& name);

    int CreateOPData(::rtidb::api::OPType op_type, const std::string& value, std::shared_ptr<OPData>& op_data,
                    const std::string& name, uint32_t pid, uint64_t parent_id = INVALID_PARENT_ID);
    int AddOPData(const std::shared_ptr<OPData>& op_data, uint32_t concurrency = FLAGS_name_server_task_concurrency);
    int CreateDelReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint);
    int CreateChangeLeaderOP(const std::string& name, uint32_t pid, 
                    const std::string& candidate_leader, bool need_restore, 
                    uint32_t concurrency = FLAGS_name_server_task_concurrency);
    int CreateRecoverTableOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, bool is_leader, 
                    uint64_t offset_delta, uint32_t concurrency);
    void SelectLeader(const std::string& name, uint32_t tid, uint32_t pid, 
                    std::vector<std::string>& follower_endpoint, 
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);
    void ChangeLeader(std::shared_ptr<::rtidb::api::TaskInfo> task_info);                
    void UpdateLeaderInfo(std::shared_ptr<::rtidb::api::TaskInfo> task_info);                
    int CreateMigrateOP(const std::string& src_endpoint, const std::string& name, uint32_t pid,
                    const std::string& des_endpoint);
    void RecoverEndpointTable(const std::string& name, uint32_t pid, std::string& endpoint, 
                    uint64_t offset_delta, uint32_t concurrency,
                    std::shared_ptr<::rtidb::api::TaskInfo> task_info);
    int GetLeader(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info, uint32_t pid, std::string& leader_endpoint);
    int MatchTermOffset(const std::string& name, uint32_t pid, bool has_table, uint64_t term, uint64_t offset);
    int CreateReAddReplicaOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency);
    int CreateReAddReplicaSimplifyOP(const std::string& name, uint32_t pid,
                    const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency);
    int CreateReAddReplicaWithDropOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency);
    int CreateReAddReplicaNoSendOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency);
    int CreateReLoadTableOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, uint64_t parent_id, uint32_t concurrency);
    int CreateUpdatePartitionStatusOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, bool is_leader, bool is_alive, uint64_t parent_id, uint32_t concurrency);
    int CreateOfflineReplicaOP(const std::string& name, uint32_t pid, 
                    const std::string& endpoint, uint32_t concurrency = FLAGS_name_server_task_concurrency);

    void NotifyTableChanged();
    void DeleteDoneOP();
    void UpdateTableStatus();
    int DropTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info);
    void CheckBinlogSyncProgress(const std::string& name, uint32_t pid,
                                 const std::string& follower, uint64_t offset_delta, 
                                 std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    void WrapTaskFun(const boost::function<bool ()>& fun, std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    // get tablet info
    std::shared_ptr<TabletInfo> GetTabletInfo(const std::string& endpoint);
    std::shared_ptr<OPData> FindRunningOP(uint64_t op_id);

    // update ttl for partition
    bool UpdateTTLOnTablet(const std::string& endpoint,
                           int32_t tid, int32_t pid, 
                           const ::rtidb::api::TTLType& type, 
                           uint64_t ttl, const std::string& ts_name);

private:
    std::mutex mu_;
    Tablets tablets_;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>> table_info_;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::ClusterInfo>> nsc_;
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
    std::string zk_offline_endpoint_lock_node_;
    std::string zk_zone_data_path_, zk_zone_name_, replica_alias_;
    uint32_t table_index_;
    uint64_t term_, zone_term_;
    std::string zk_op_index_node_;
    std::string zk_op_data_path_;
    uint64_t op_index_;
    std::atomic<bool> running_;
    std::atomic<bool> follower_;
    std::list<std::shared_ptr<OPData>> done_op_list_;
    std::vector<std::list<std::shared_ptr<OPData>>> task_vec_;
    std::condition_variable cv_;
    std::atomic<bool> auto_failover_;
    std::map<std::string, uint64_t> offline_endpoint_map_;
    ::rtidb::base::Random rand_;
    uint64_t session_term_;
    std::atomic<uint64_t> task_rpc_version_;
};

}
}
#endif
