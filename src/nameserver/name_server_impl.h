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

#ifndef SRC_NAMESERVER_NAME_SERVER_IMPL_H_
#define SRC_NAMESERVER_NAME_SERVER_IMPL_H_

#include <atomic>
#include <condition_variable>  // NOLINT
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/hash.h"
#include "base/random.h"
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "codec/schema_codec.h"
#include "nameserver/cluster_info.h"
#include "nameserver/system_table.h"
#include "nameserver/task.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "sdk/sql_cluster_router.h"
#include "zk/dist_lock.h"
#include "zk/zk_client.h"

DECLARE_uint32(name_server_task_concurrency);
DECLARE_uint32(name_server_task_concurrency_for_replica_cluster);
DECLARE_int32(zk_keep_alive_check_interval);

namespace openmldb {
namespace nameserver {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::openmldb::client::NsClient;
using ::openmldb::client::TabletClient;
using ::openmldb::zk::DistLock;
using ::openmldb::zk::ZkClient;

using Schema = ::google::protobuf::RepeatedPtrField<openmldb::common::ColumnDesc>;

const uint64_t INVALID_PARENT_ID = UINT64_MAX;
const uint32_t INVALID_PID = UINT32_MAX;

struct EndpointInfo {
    ::openmldb::type::EndpointState state_ = ::openmldb::type::EndpointState::kOffline;
    uint64_t ctime_ = 0;
    bool Health() const { return state_ == ::openmldb::type::EndpointState::kHealthy; }
};

// tablet info
struct TabletInfo {
    // tablet state
    ::openmldb::type::EndpointState state_;
    // tablet rpc handle
    std::shared_ptr<TabletClient> client_;
    uint64_t ctime_;

    bool Health() const { return state_ == ::openmldb::type::EndpointState::kHealthy; }
};


// the container of tablet
typedef std::map<std::string, std::shared_ptr<TabletInfo>> Tablets;
typedef std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>> TableInfos;

struct ZkPath {
    std::string zk_cluster_;
    std::string root_path_;
    std::string db_path_;
    std::string table_index_node_;
    std::string term_node_;
    std::string table_data_path_;
    std::string db_table_data_path_;
    std::string db_sp_data_path_;
    std::string auto_failover_node_;
    std::string table_changed_notify_node_;
    std::string offline_endpoint_lock_node_;
    std::string zone_data_path_;
    std::string op_index_node_;
    std::string op_data_path_;
    std::string op_sync_path_;
    std::string globalvar_changed_notify_node_;
    std::string external_function_path_;
};

class NameServerImpl : public NameServer {
    // used for ut
    friend class NameServerImplTest;
    friend class NameServerImplRemoteTest;

 public:
    NameServerImpl();

    ~NameServerImpl() override;

    bool Init(const std::string& real_endpoint);
    bool Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint,
              const std::string& real_endpoint);

    NameServerImpl(const NameServerImpl&) = delete;

    NameServerImpl& operator=(const NameServerImpl&) = delete;

    inline bool IsClusterMode() const { return startup_mode_ == ::openmldb::type::StartupMode::kCluster; }

    void DeleteOPTask(RpcController* controller, const ::openmldb::api::DeleteTaskRequest* request,
                      ::openmldb::api::GeneralResponse* response, Closure* done);

    void GetTaskStatus(RpcController* controller, const ::openmldb::api::TaskStatusRequest* request,
                       ::openmldb::api::TaskStatusResponse* response, Closure* done);

    void LoadTable(RpcController* controller, const LoadTableRequest* request, GeneralResponse* response,
                   Closure* done);

    void CreateTableInternel(GeneralResponse& response,  // NOLINT
                             std::shared_ptr<::openmldb::nameserver::TableInfo> table_info, uint64_t cur_term,
                             uint32_t tid, std::shared_ptr<::openmldb::api::TaskInfo> task_ptr);


    void RefreshTablet(uint32_t tid);

    void CreateTableInfoSimply(RpcController* controller, const CreateTableInfoRequest* request,
                               CreateTableInfoResponse* response, Closure* done);

    void CreateTableInfo(RpcController* controller, const CreateTableInfoRequest* request,
                         CreateTableInfoResponse* response, Closure* done);

    void CreateTable(RpcController* controller, const CreateTableRequest* request, GeneralResponse* response,
                     Closure* done);

    void CreateProcedure(RpcController* controller, const api::CreateProcedureRequest* request,
                         GeneralResponse* response, Closure* done);

    void DropTableInternel(const DropTableRequest& request, GeneralResponse& response,  // NOLINT
                           std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                           std::shared_ptr<::openmldb::api::TaskInfo> task_ptr);

    void DropTable(RpcController* controller, const DropTableRequest* request, GeneralResponse* response,
                   Closure* done);

    void AddTableField(RpcController* controller, const AddTableFieldRequest* request, GeneralResponse* response,
                       Closure* done);

    void ShowTablet(RpcController* controller, const ShowTabletRequest* request, ShowTabletResponse* response,
                    Closure* done);
    void ShowSdkEndpoint(RpcController* controller, const ShowSdkEndpointRequest* request,
                         ShowSdkEndpointResponse* response, Closure* done);

    void ShowTable(RpcController* controller, const ShowTableRequest* request, ShowTableResponse* response,
                   Closure* done);

    void CreateFunction(RpcController* controller, const CreateFunctionRequest* request,
                        CreateFunctionResponse* response, Closure* done);

    void DropFunction(RpcController* controller, const DropFunctionRequest* request,
                        DropFunctionResponse* response, Closure* done);

    void ShowFunction(RpcController* controller, const ShowFunctionRequest* request,
                        ShowFunctionResponse* response, Closure* done);

    void ShowProcedure(RpcController* controller, const api::ShowProcedureRequest* request,
                       api::ShowProcedureResponse* response, Closure* done);

    void MakeSnapshotNS(RpcController* controller, const MakeSnapshotNSRequest* request, GeneralResponse* response,
                        Closure* done);

    int AddReplicaSimplyRemoteOP(const std::string& alias, const std::string& name, const std::string& db,
                                 const std::string& endpoint, uint32_t tid, uint32_t pid);

    int AddReplicaRemoteOP(const std::string& alias, const std::string& name, const std::string& db,
                           const ::openmldb::nameserver::TablePartition& table_partition, uint32_t remote_tid,
                           uint32_t pid);

    void AddReplicaNS(RpcController* controller, const AddReplicaNSRequest* request, GeneralResponse* response,
                      Closure* done);

    void AddReplicaNSFromRemote(RpcController* controller, const AddReplicaNSRequest* request,
                                GeneralResponse* response, Closure* done);

    int DelReplicaRemoteOP(const std::string& endpoint, const std::string& name, const std::string& db, uint32_t pid);

    void DelReplicaNS(RpcController* controller, const DelReplicaNSRequest* request, GeneralResponse* response,
                      Closure* done);

    void ShowOPStatus(RpcController* controller, const ShowOPStatusRequest* request, ShowOPStatusResponse* response,
                      Closure* done);

    void ShowCatalog(RpcController* controller, const ShowCatalogRequest* request, ShowCatalogResponse* response,
                     Closure* done);

    void ConfSet(RpcController* controller, const ConfSetRequest* request, GeneralResponse* response, Closure* done);

    void ConfGet(RpcController* controller, const ConfGetRequest* request, ConfGetResponse* response, Closure* done);

    void ChangeLeader(RpcController* controller, const ChangeLeaderRequest* request, GeneralResponse* response,
                      Closure* done);

    void OfflineEndpoint(RpcController* controller, const OfflineEndpointRequest* request, GeneralResponse* response,
                         Closure* done);

    void OfflineEndpointDBInternal(
        const std::string& endpoint, uint32_t concurrency,
        const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_info);

    void UpdateTTL(RpcController* controller, const ::openmldb::nameserver::UpdateTTLRequest* request,
                   ::openmldb::nameserver::UpdateTTLResponse* response, Closure* done);

    void Migrate(RpcController* controller, const MigrateRequest* request, GeneralResponse* response, Closure* done);

    void RecoverEndpoint(RpcController* controller, const RecoverEndpointRequest* request, GeneralResponse* response,
                         Closure* done);

    void RecoverEndpointDBInternal(
        const std::string& endpoint, bool need_restore, uint32_t concurrency,
        const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_info);

    void RecoverTable(RpcController* controller, const RecoverTableRequest* request, GeneralResponse* response,
                      Closure* done);

    void ConnectZK(RpcController* controller, const ConnectZKRequest* request, GeneralResponse* response,
                   Closure* done);

    void DisConnectZK(RpcController* controller, const DisConnectZKRequest* request, GeneralResponse* response,
                      Closure* done);

    void SetTablePartition(RpcController* controller, const SetTablePartitionRequest* request,
                           GeneralResponse* response, Closure* done);

    void GetTablePartition(RpcController* controller, const GetTablePartitionRequest* request,
                           GetTablePartitionResponse* response, Closure* done);

    void UpdateTableAliveStatus(RpcController* controller, const UpdateTableAliveRequest* request,
                                GeneralResponse* response, Closure* done);

    void CancelOP(RpcController* controller, const CancelOPRequest* request, GeneralResponse* response, Closure* done);

    void AddReplicaCluster(RpcController* controller, const ClusterAddress* request, GeneralResponse* response,
                           Closure* done);

    void AddReplicaClusterByNs(RpcController* controller, const ReplicaClusterByNsRequest* request,
                               AddReplicaClusterByNsResponse* response, Closure* done);

    void ShowReplicaCluster(RpcController* controller, const GeneralRequest* request,
                            ShowReplicaClusterResponse* response, Closure* done);

    void RemoveReplicaCluster(RpcController* controller, const RemoveReplicaOfRequest* request,
                              GeneralResponse* response, Closure* done);

    void RemoveReplicaClusterByNs(RpcController* controller, const ReplicaClusterByNsRequest* request,
                                  GeneralResponse* response, Closure* done);

    void SwitchMode(RpcController* controller, const SwitchModeRequest* request, GeneralResponse* response,
                    Closure* done);

    void SyncTable(RpcController* controller, const SyncTableRequest* request, GeneralResponse* response,
                   Closure* done);

    void DeleteIndex(RpcController* controller, const DeleteIndexRequest* request, GeneralResponse* response,
                     Closure* done);

    void AddIndex(RpcController* controller, const AddIndexRequest* request, GeneralResponse* response, Closure* done);

    void UseDatabase(RpcController* controller, const UseDatabaseRequest* request, GeneralResponse* response,
                     Closure* done);

    void CreateDatabase(RpcController* controller, const CreateDatabaseRequest* request, GeneralResponse* response,
                        Closure* done);

    void ShowDatabase(RpcController* controller, const GeneralRequest* request, ShowDatabaseResponse* response,
                      Closure* done);

    void DropDatabase(RpcController* controller, const DropDatabaseRequest* request, GeneralResponse* response,
                      Closure* done);

    void SetSdkEndpoint(RpcController* controller, const SetSdkEndpointRequest* request, GeneralResponse* response,
                        Closure* done);

    void UpdateOfflineTableInfo(::google::protobuf::RpcController* controller,
                                const ::openmldb::nameserver::TableInfo* request,
                       ::openmldb::nameserver::GeneralResponse* response, ::google::protobuf::Closure* done);

    int SyncExistTable(const std::string& alias, const std::string& name, const std::string& db,
                       const std::vector<::openmldb::nameserver::TableInfo> tables_remote,
                       const ::openmldb::nameserver::TableInfo& table_info_local, uint32_t pid, int& code,  // NOLINT
                       std::string& msg);                                                                   // NOLINT

    int CreateTableOnTablet(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info, bool is_leader,
                            std::map<uint32_t, std::vector<std::string>>& endpoint_map, uint64_t term);  // NOLINT

    void CheckZkClient();

    int UpdateTaskStatusRemote(bool is_recover_op);

    int UpdateTask(const std::list<std::shared_ptr<OPData>>& op_list, const std::string& endpoint,
                   const std::string& msg, bool is_recover_op,
                   ::openmldb::api::TaskStatusResponse& response);  // NOLINT

    int UpdateTaskStatus(bool is_recover_op);

    int DeleteTaskRemote(const std::vector<uint64_t>& done_task_vec,
                         bool& has_failed);  // NOLINT

    void UpdateTaskMapStatus(uint64_t remote_op_id, uint64_t op_id, const ::openmldb::api::TaskStatus& status);

    int DeleteTask();

    void DeleteTask(const std::vector<uint64_t>& done_task_vec);

    void ProcessTask();

    int UpdateZKTaskStatus();

    void CheckClusterInfo();

    bool CreateTableRemote(const ::openmldb::api::TaskInfo& task_info,
                           const ::openmldb::nameserver::TableInfo& table_info,
                           const std::shared_ptr<::openmldb::nameserver::ClusterInfo> cluster_info);

    bool DropTableRemote(const ::openmldb::api::TaskInfo& task_info, const std::string& name, const std::string& db,
                         const std::shared_ptr<::openmldb::nameserver::ClusterInfo> cluster_info);

    bool RegisterName();

    bool CreateProcedureOnTablet(const api::CreateProcedureRequest& sp_request, std::string& err_msg);  // NOLINT

    void DropProcedure(RpcController* controller, const api::DropProcedureRequest* request, GeneralResponse* response,
                       Closure* done);

 private:
    base::Status InitGlobalVarTable();

    // create the database if not exists, exit on fail
    void CreateDatabaseOrExit(const std::string& db_name);

    // create table if not exists, exit on fail
    void CreateSystemTableOrExit(SystemTableType type);

    base::Status CreateSystemTable(SystemTableType table_type);

    // Recover all memory status, the steps
    // 1.recover table meta from zookeeper
    // 2.recover table status from all tablets
    bool Recover();

    bool RecoverDb();

    bool RecoverTableInfo();

    void RecoverClusterInfo();

    bool RecoverOPTask();

    bool UpdateSdkEpMap();

    bool RecoverProcedureInfo();

    int SetPartitionInfo(TableInfo& table_info);  // NOLINT

    void AddDataType(std::shared_ptr<TableInfo> table_info);

    int CreateMakeSnapshotOPTask(std::shared_ptr<OPData> op_data);

    int CreateAddReplicaSimplyRemoteOPTask(std::shared_ptr<OPData> op_data);

    int CreateAddReplicaOPTask(std::shared_ptr<OPData> op_data);

    int CreateAddReplicaRemoteOPTask(std::shared_ptr<OPData> op_data);

    int CreateChangeLeaderOPTask(std::shared_ptr<OPData> op_data);

    int CreateMigrateTask(std::shared_ptr<OPData> op_data);

    int CreateRecoverTableOPTask(std::shared_ptr<OPData> op_data);

    int CreateDelReplicaRemoteOPTask(std::shared_ptr<OPData> op_data);

    int CreateDelReplicaOPTask(std::shared_ptr<OPData> op_data);

    int CreateOfflineReplicaTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaNoSendTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaWithDropTask(std::shared_ptr<OPData> op_data);

    int CreateReAddReplicaSimplifyTask(std::shared_ptr<OPData> op_data);

    int CreateReLoadTableTask(std::shared_ptr<OPData> op_data);

    int CreateUpdatePartitionStatusOPTask(std::shared_ptr<OPData> op_data);

    bool SkipDoneTask(std::shared_ptr<OPData> op_data);

    int CreateTableRemoteTask(std::shared_ptr<OPData> op_data);

    int DropTableRemoteTask(std::shared_ptr<OPData> op_data);

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

    void DelTableInfo(const std::string& name, const std::string& db, const std::string& endpoint, uint32_t pid,
                      std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    void DelTableInfo(const std::string& name, const std::string& db, const std::string& endpoint, uint32_t pid,
                      std::shared_ptr<::openmldb::api::TaskInfo> task_info, uint32_t flag);

    void UpdatePartitionStatus(const std::string& name, const std::string& db, const std::string& endpoint,
                               uint32_t pid, bool is_leader, bool is_alive,
                               std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    int UpdateEndpointTableAliveHandle(const std::string& endpoint, TableInfos& table_infos, bool is_alive);  // NOLINT

    int UpdateEndpointTableAlive(const std::string& endpoint, bool is_alive);

    std::shared_ptr<Task> CreateTask(const std::shared_ptr<TaskMeta>& task_meta);

    std::shared_ptr<Task> CreateLoadTableRemoteTask(const std::string& alias, const std::string& name,
                                                    const std::string& db, const std::string& endpoint, uint32_t pid,
                                                    uint64_t op_index, ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> CreateAddReplicaRemoteTask(const std::string& endpoint, uint64_t op_index,
                                                     ::openmldb::api::OPType op_type, uint32_t tid, uint32_t remote_tid,
                                                     uint32_t pid, const std::string& des_endpoint,
                                                     uint64_t task_id = INVALID_PARENT_ID);

    std::shared_ptr<Task> CreateAddReplicaNSRemoteTask(const std::string& alias, const std::string& name,
                                                       const std::vector<std::string>& endpoint_vec, uint32_t pid,
                                                       uint64_t op_index, ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> CreateAddTableInfoTask(const std::string& alias, const std::string& endpoint,
                                                 const std::string& name, const std::string& db, uint32_t remote_tid,
                                                 uint32_t pid, uint64_t op_index, ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> CreateAddTableInfoTask(const std::string& name, const std::string& db, uint32_t pid,
                                                 const std::string& endpoint, uint64_t op_index,
                                                 ::openmldb::api::OPType op_type);

    void AddTableInfo(const std::string& alias, const std::string& endpoint, const std::string& name,
                      const std::string& db, uint32_t pid, uint32_t remote_tid,
                      std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    void AddTableInfo(const std::string& name, const std::string& db, const std::string& endpoint, uint32_t pid,
                      std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    std::shared_ptr<Task> CreateDelTableInfoTask(const std::string& name, const std::string& db, uint32_t pid,
                                                 const std::string& endpoint, uint64_t op_index,
                                                 ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> CreateDelTableInfoTask(const std::string& name, const std::string& db, uint32_t pid,
                                                 const std::string& endpoint, uint64_t op_index,
                                                 ::openmldb::api::OPType op_type, uint32_t flag);

    std::shared_ptr<Task> CreateUpdateTableInfoTask(const std::string& src_endpoint, const std::string& name,
                                                    const std::string& db, uint32_t pid,
                                                    const std::string& des_endpoint, uint64_t op_index,
                                                    ::openmldb::api::OPType op_type);

    void UpdateTableInfo(const std::string& src_endpoint, const std::string& name, const std::string& db, uint32_t pid,
                         const std::string& des_endpoint, std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    std::shared_ptr<Task> CreateUpdatePartitionStatusTask(const std::string& name, const std::string& db, uint32_t pid,
                                                          const std::string& endpoint, bool is_leader, bool is_alive,
                                                          uint64_t op_index, ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> CreateSelectLeaderTask(uint64_t op_index, ::openmldb::api::OPType op_type,
                                                 const std::string& name, const std::string& db, uint32_t tid,
                                                 uint32_t pid,
                                                 std::vector<std::string>& follower_endpoint);  // NOLINT

    std::shared_ptr<Task> CreateChangeLeaderTask(uint64_t op_index, ::openmldb::api::OPType op_type,
                                                 const std::string& name, uint32_t pid);

    std::shared_ptr<Task> CreateUpdateLeaderInfoTask(uint64_t op_index, ::openmldb::api::OPType op_type,
                                                     const std::string& name, uint32_t pid);

    std::shared_ptr<Task> CreateCheckBinlogSyncProgressTask(uint64_t op_index, ::openmldb::api::OPType op_type,
                                                            const std::string& name, const std::string& db,
                                                            uint32_t pid, const std::string& follower,
                                                            uint64_t offset_delta);

    std::shared_ptr<Task> CreateRecoverTableTask(uint64_t op_index, ::openmldb::api::OPType op_type,
                                                 const std::string& name, const std::string& db, uint32_t pid,
                                                 const std::string& endpoint, uint64_t offset_delta,
                                                 uint32_t concurrency);

    std::shared_ptr<Task> CreateTableRemoteTask(const ::openmldb::nameserver::TableInfo& table_info,
                                                const std::string& alias, uint64_t op_index,
                                                ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> DropTableRemoteTask(const std::string& name, const std::string& db, const std::string& alias,
                                              uint64_t op_index, ::openmldb::api::OPType op_type);

    std::shared_ptr<Task> CreateDumpIndexDataTask(uint64_t op_index, ::openmldb::api::OPType op_type, uint32_t tid,
                                                  uint32_t pid, const std::string& endpoint, uint32_t partition_num,
                                                  const ::openmldb::common::ColumnKey& column_key, uint32_t idx);

    std::shared_ptr<Task> CreateSendIndexDataTask(uint64_t op_index, ::openmldb::api::OPType op_type, uint32_t tid,
                                                  uint32_t pid, const std::string& endpoint,
                                                  const std::map<uint32_t, std::string>& pid_endpoint_map);

    std::shared_ptr<Task> CreateLoadIndexDataTask(uint64_t op_index, ::openmldb::api::OPType op_type, uint32_t tid,
                                                  uint32_t pid, const std::string& endpoint, uint32_t partition_num);

    std::shared_ptr<Task> CreateExtractIndexDataTask(uint64_t op_index, ::openmldb::api::OPType op_type, uint32_t tid,
                                                     uint32_t pid, const std::vector<std::string>& endpoints,
                                                     uint32_t partition_num,
                                                     const ::openmldb::common::ColumnKey& column_key, uint32_t idx);

    std::shared_ptr<Task> CreateAddIndexToTabletTask(uint64_t op_index, ::openmldb::api::OPType op_type, uint32_t tid,
                                                     uint32_t pid, const std::vector<std::string>& endpoints,
                                                     const ::openmldb::common::ColumnKey& column_key);

    bool GetTableInfo(const std::string& table_name, const std::string& db_name,
                      std::shared_ptr<TableInfo>* table_info);

    bool GetTableInfoUnlock(const std::string& table_name, const std::string& db_name,
                            std::shared_ptr<TableInfo>* table_info);

    int AddOPTask(const ::openmldb::api::TaskInfo& task_info, ::openmldb::api::TaskType task_type,
                  std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr,  // NOLINT
                  std::vector<uint64_t> rep_cluster_op_id_vec);

    std::shared_ptr<::openmldb::api::TaskInfo> FindTask(uint64_t op_id, ::openmldb::api::TaskType task_type);

    bool SaveTableInfo(std::shared_ptr<TableInfo> table_info);

    int CreateOPData(::openmldb::api::OPType op_type, const std::string& value,
                     std::shared_ptr<OPData>& op_data,  // NOLINT
                     const std::string& name, const std::string& db, uint32_t pid,
                     uint64_t parent_id = INVALID_PARENT_ID, uint64_t remote_op_id = INVALID_PARENT_ID);
    int AddOPData(const std::shared_ptr<OPData>& op_data, uint32_t concurrency = FLAGS_name_server_task_concurrency);
    int CreateDelReplicaOP(const std::string& name, const std::string& db, uint32_t pid, const std::string& endpoint);
    int CreateChangeLeaderOP(const std::string& name, const std::string& db, uint32_t pid,
                             const std::string& candidate_leader, bool need_restore,
                             uint32_t concurrency = FLAGS_name_server_task_concurrency);

    std::shared_ptr<openmldb::nameserver::ClusterInfo> GetHealthCluster(const std::string& alias);

    int CreateRecoverTableOP(const std::string& name, const std::string& db, uint32_t pid, const std::string& endpoint,
                             bool is_leader, uint64_t offset_delta, uint32_t concurrency);
    void SelectLeader(const std::string& name, const std::string& db, uint32_t tid, uint32_t pid,
                      std::vector<std::string>& follower_endpoint,  // NOLINT
                      std::shared_ptr<::openmldb::api::TaskInfo> task_info);
    void ChangeLeader(std::shared_ptr<::openmldb::api::TaskInfo> task_info);
    void UpdateLeaderInfo(std::shared_ptr<::openmldb::api::TaskInfo> task_info);
    int CreateMigrateOP(const std::string& src_endpoint, const std::string& name, const std::string& db, uint32_t pid,
                        const std::string& des_endpoint);
    void RecoverEndpointTable(const std::string& name, const std::string& db, uint32_t pid,
                              std::string& endpoint,  // NOLINT
                              uint64_t offset_delta, uint32_t concurrency,
                              std::shared_ptr<::openmldb::api::TaskInfo> task_info);
    int GetLeader(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info, uint32_t pid,
                  std::string& leader_endpoint);  // NOLINT
    int MatchTermOffset(const std::string& name, const std::string& db, uint32_t pid, bool has_table, uint64_t term,
                        uint64_t offset);
    int CreateReAddReplicaOP(const std::string& name, const std::string& db, uint32_t pid, const std::string& endpoint,
                             uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency);
    int CreateReAddReplicaSimplifyOP(const std::string& name, const std::string& db, uint32_t pid,
                                     const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                     uint32_t concurrency);
    int CreateReAddReplicaWithDropOP(const std::string& name, const std::string& db, uint32_t pid,
                                     const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                     uint32_t concurrency);
    int CreateReAddReplicaNoSendOP(const std::string& name, const std::string& db, uint32_t pid,
                                   const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                   uint32_t concurrency);
    int CreateReLoadTableOP(const std::string& name, const std::string& db, uint32_t pid, const std::string& endpoint,
                            uint64_t parent_id, uint32_t concurrency);
    int CreateReLoadTableOP(const std::string& name, const std::string& db, uint32_t pid, const std::string& endpoint,
                            uint64_t parent_id, uint32_t concurrency, uint64_t remote_op_id,
                            uint64_t& rep_cluter_op_id);  // NOLINT
    int CreateUpdatePartitionStatusOP(const std::string& name, const std::string& db, uint32_t pid,
                                      const std::string& endpoint, bool is_leader, bool is_alive, uint64_t parent_id,
                                      uint32_t concurrency);
    int CreateOfflineReplicaOP(const std::string& name, const std::string& db, uint32_t pid,
                               const std::string& endpoint, uint32_t concurrency = FLAGS_name_server_task_concurrency);
    int CreateTableRemoteOP(const ::openmldb::nameserver::TableInfo& table_info,
                            const ::openmldb::nameserver::TableInfo& remote_table_info, const std::string& alias,
                            uint64_t parent_id = INVALID_PARENT_ID,
                            uint32_t concurrency = FLAGS_name_server_task_concurrency_for_replica_cluster);

    int CreateAddIndexOP(const std::string& name, const std::string& db, uint32_t pid,
                         const std::vector<openmldb::common::ColumnDesc>& new_cols,
                         const ::openmldb::common::ColumnKey& column_key, uint32_t idx);

    int CreateAddIndexOPTask(std::shared_ptr<OPData> op_data);

    int DropTableRemoteOP(const std::string& name, const std::string& db, const std::string& alias,
                          uint64_t parent_id = INVALID_PARENT_ID,
                          uint32_t concurrency = FLAGS_name_server_task_concurrency_for_replica_cluster);
    // kTable for normal table and kGlobalVar for global var table
    void NotifyTableChanged(::openmldb::type::NotifyType type);
    void DeleteDoneOP();
    void UpdateTableStatus();
    int DropTableOnTablet(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info);

    void CheckBinlogSyncProgress(const std::string& name, const std::string& db, uint32_t pid,
                                 const std::string& follower, uint64_t offset_delta,
                                 std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    bool AddIndexToTableInfo(const std::string& name, const std::string& db,
                             const ::openmldb::common::ColumnKey& column_key, uint32_t index_pos);

    void WrapTaskFun(const boost::function<bool()>& fun, std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    void RunSyncTaskFun(uint32_t tid, const boost::function<bool()>& fun,
                        std::shared_ptr<::openmldb::api::TaskInfo> task_info);

    void RunSubTask(std::shared_ptr<Task> task);

    // get tablet info
    std::shared_ptr<TabletInfo> GetTabletInfo(const std::string& endpoint);

    std::shared_ptr<TabletInfo> GetTabletInfoWithoutLock(const std::string& endpoint);

    std::shared_ptr<TabletInfo> GetHealthTabletInfoNoLock(const std::string& endpoint);

    std::shared_ptr<OPData> FindRunningOP(uint64_t op_id);

    // update ttl for partition
    bool UpdateTTLOnTablet(const std::string& endpoint, int32_t tid, int32_t pid, const std::string& index_name,
                           const ::openmldb::common::TTLSt& ttl);

    void CheckSyncExistTable(const std::string& alias,
                             const std::vector<::openmldb::nameserver::TableInfo>& tables_remote,
                             const std::shared_ptr<::openmldb::client::NsClient> ns_client);

    void CheckSyncTable(const std::string& alias, const std::vector<::openmldb::nameserver::TableInfo> tables,
                        const std::shared_ptr<::openmldb::client::NsClient> ns_client);

    bool CompareTableInfo(const std::vector<::openmldb::nameserver::TableInfo>& tables, bool period_check);

    void CheckTableInfo(std::shared_ptr<ClusterInfo>& ci,  // NOLINT
                        const std::vector<::openmldb::nameserver::TableInfo>& tables);

    bool CompareSnapshotOffset(
        const std::vector<TableInfo>& tables, std::string& msg,                                       // NOLINT
        int& code,                                                                                    // NOLINT
        std::map<std::string, std::map<uint32_t, std::map<uint32_t, uint64_t>>>& table_part_offset);  // NOLINT

    void DistributeTabletMode();

    void SchedMakeSnapshot();

    void MakeTablePartitionSnapshot(uint32_t pid, uint64_t end_offset,
                                    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info);

    void DropTableFun(const DropTableRequest* request, GeneralResponse* response,
                      std::shared_ptr<::openmldb::nameserver::TableInfo> table_info);

    bool SetTableInfo(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info);

    void UpdateTableStatusFun(
        const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_info_map,
        const std::unordered_map<std::string, ::openmldb::api::TableStatus>& pos_response);

    void UpdateRealEpMapToTablet(bool check_running);

    void UpdateRemoteRealEpMap();

    bool UpdateZkTableNode(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info);  // NOLINT

    bool UpdateZkTableNodeWithoutNotify(const TableInfo* table_info);

    void ShowDbTable(const std::map<std::string, std::shared_ptr<TableInfo>>& table_infos,
                     const ShowTableRequest* request, ShowTableResponse* response);

    void TableInfoToVec(const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_infos,
                        const std::vector<uint32_t>& table_tid_vec,
                        std::vector<::openmldb::nameserver::TableInfo>* local_table_info_vec);

    bool AddFieldToTablet(const std::vector<openmldb::common::ColumnDesc>& cols, std::shared_ptr<TableInfo> table_info,
                          openmldb::common::VersionPair* new_pair);

    base::Status AddMultiIndexs(const std::string& db, const std::string& name,
            std::shared_ptr<TableInfo> table_info,
            const ::google::protobuf::RepeatedPtrField<openmldb::common::ColumnKey>& column_keys);

    void DropProcedureOnTablet(const std::string& db_name, const std::string& sp_name);

    std::shared_ptr<TabletInfo> GetTablet(const std::string& endpoint);

    std::vector<std::shared_ptr<TabletInfo>> GetAllHealthTablet();

    bool AllocateTableId(uint32_t* id);

    base::Status CreateDatabase(const std::string& db_name, bool if_not_exists = false);

    uint64_t GetTerm() const;

    // write deploy statistics into table
    void SyncDeployStats();

    void ScheduleSyncDeployStats();

    bool GetSdkConnection();

    void FreeSdkConnection();

    bool RecoverExternalFunction();

    ::openmldb::base::Status CheckZoneInfo(const ::openmldb::nameserver::ZoneInfo& zone_info);

 private:
    std::mutex mu_;
    Tablets tablets_;
    ::openmldb::nameserver::TableInfos table_info_;
    std::map<std::string, ::openmldb::nameserver::TableInfos> db_table_info_;
    std::map<std::string, std::shared_ptr<::openmldb::nameserver::ClusterInfo>> nsc_;
    ZoneInfo zone_info_;
    ZkClient* zk_client_;
    ZkPath zk_path_;
    DistLock* dist_lock_;
    ::baidu::common::ThreadPool thread_pool_;
    ::baidu::common::ThreadPool task_thread_pool_;
    uint32_t table_index_ = 0;
    uint64_t term_ = 0;
    uint64_t op_index_;
    std::atomic<bool> running_;  // whether the current ns is the master
    std::list<std::shared_ptr<OPData>> done_op_list_;
    std::vector<std::list<std::shared_ptr<OPData>>> task_vec_;
    std::condition_variable cv_;
    std::atomic<bool> auto_failover_;
    std::atomic<uint32_t> mode_;
    std::map<std::string, uint64_t> offline_endpoint_map_;
    ::openmldb::base::Random rand_;
    uint64_t session_term_;
    std::atomic<uint64_t> task_rpc_version_;
    std::map<uint64_t, std::list<std::shared_ptr<::openmldb::api::TaskInfo>>> task_map_;
    std::set<std::string> databases_;
    std::string endpoint_;
    std::map<std::string, std::string> real_ep_map_;
    std::map<std::string, std::string> remote_real_ep_map_;
    std::map<std::string, std::string> sdk_endpoint_map_;
    std::map<std::string, std::shared_ptr<::openmldb::common::ExternalFun>> external_fun_;
    // database
    //    -> procedure
    //       -> (db_name, table_name)
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>>
        db_sp_table_map_;
    // database
    //      -> table
    //          -> (db_name, procedure_name)
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>>
        db_table_sp_map_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<api::ProcedureInfo>>>
        db_sp_info_map_;
    ::openmldb::type::StartupMode startup_mode_;

    // sr_ could be a real instance or nothing, remember always use atomic_* function to access it
    std::shared_ptr<::openmldb::sdk::SQLClusterRouter> sr_ = nullptr;
};

}  // namespace nameserver
}  // namespace openmldb
#endif  // SRC_NAMESERVER_NAME_SERVER_IMPL_H_
