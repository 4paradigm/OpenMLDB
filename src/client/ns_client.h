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

#ifndef SRC_CLIENT_NS_CLIENT_H_
#define SRC_CLIENT_NS_CLIENT_H_

#include <stdint.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/status.h"
#include "client/client.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"

namespace openmldb {
namespace client {

const uint32_t INVALID_PID = UINT32_MAX;

struct TabletInfo {
    std::string endpoint;
    std::string state;
    uint64_t age;
    std::string real_endpoint;
};

class NsClient : public Client {
 public:
    explicit NsClient(const std::string& endpoint, const std::string& real_endpoint);
    ~NsClient() override = default;

    int Init() override;

    const std::string& GetDb();

    void ClearDb();

    bool Use(const std::string& db, std::string& msg);  // NOLINT

    bool CreateDatabase(const std::string& db, std::string& msg, bool if_not_exists = false);  // NOLINT

    base::Status CreateDatabaseRemote(const std::string& db, const ::openmldb::nameserver::ZoneInfo& zone_info);

    base::Status DropDatabaseRemote(const std::string& db, const ::openmldb::nameserver::ZoneInfo& zone_info);

    bool ShowDatabase(std::vector<std::string>* dbs,
                      std::string& msg);  // NOLINT

    bool DropDatabase(const std::string& db, std::string& msg, bool if_exists = false);  // NOLINT

    bool ShowTablet(std::vector<TabletInfo>& tablets,  // NOLINT
                    std::string& msg);                 // NOLINT

    bool ShowSdkEndpoint(std::vector<TabletInfo>& tablets,  // NOLINT
                         std::string& msg);                 // NOLINT

    bool ShowTable(const std::string& name,
                   std::vector<::openmldb::nameserver::TableInfo>& tables,  // NOLINT
                   std::string& msg);                                       // NOLINT

    base::Status ShowDBTable(const std::string& db_name, std::vector<::openmldb::nameserver::TableInfo>* tables);

    bool ShowTable(const std::string& name, const std::string& db, bool show_all,
                   std::vector<::openmldb::nameserver::TableInfo>& tables,  // NOLINT
                   std::string& msg);                                       // NOLINT

    bool ShowCatalogVersion(std::map<std::string, uint64_t>* version_map, std::string* msg);

    bool ShowAllTable(std::vector<::openmldb::nameserver::TableInfo>& tables,  // NOLINT
                      std::string& msg);                                       // NOLINT

    bool MakeSnapshot(const std::string& name, uint32_t pid, uint64_t end_offset, std::string& msg);  // NOLINT

    bool MakeSnapshot(const std::string& name, const std::string& db, uint32_t pid, uint64_t end_offset,
                      std::string& msg);  // NOLINT

    base::Status ShowOPStatus(const std::string& name, uint32_t pid, nameserver::ShowOPStatusResponse* response);

    base::Status ShowOPStatus(uint64_t op_id, ::openmldb::nameserver::ShowOPStatusResponse* response);

    base::Status CancelOP(uint64_t op_id);

    bool AddTableField(const std::string& table_name, const ::openmldb::common::ColumnDesc& column_desc,
                       std::string& msg);  // NOLINT

    bool CreateTable(const ::openmldb::nameserver::TableInfo& table_info, const bool create_if_not_exist,
                     std::string& msg);  // NOLINT

    bool DropTable(const std::string& name, std::string& msg);  // NOLINT

    bool DropTable(const std::string& db, const std::string& name,
                   std::string& msg);  // NOLINT

    bool SyncTable(const std::string& name, const std::string& cluster_alias, uint32_t pid,
                   std::string& msg);  // NOLINT

    bool SetSdkEndpoint(const std::string& server_name, const std::string& sdk_endpoint, std::string* msg);

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    bool GetTaskStatus(::openmldb::api::TaskStatusResponse& response);  // NOLINT

    bool LoadTable(const std::string& name, const std::string& endpoint, uint32_t pid,
                   const ::openmldb::nameserver::ZoneInfo& zone_info, const ::openmldb::api::TaskInfo& task_info);

    bool LoadTable(const std::string& name, const std::string& db, const std::string& endpoint, uint32_t pid,
                   const ::openmldb::nameserver::ZoneInfo& zone_info, const ::openmldb::api::TaskInfo& task_info);

    bool CreateRemoteTableInfo(const ::openmldb::nameserver::ZoneInfo& zone_info,
                               ::openmldb::nameserver::TableInfo& table_info,  // NOLINT
                               std::string& msg);                              // NOLINT

    bool CreateRemoteTableInfoSimply(const ::openmldb::nameserver::ZoneInfo& zone_info,
                                     ::openmldb::nameserver::TableInfo& table_info,  // NOLINT
                                     std::string& msg);                              // NOLINT

    bool DropTableRemote(const ::openmldb::api::TaskInfo& task_info, const std::string& name, const std::string& db,
                         const ::openmldb::nameserver::ZoneInfo& zone_info,
                         std::string& msg);  // NOLINT

    bool CreateTableRemote(const ::openmldb::api::TaskInfo& task_info,
                           const ::openmldb::nameserver::TableInfo& table_info,
                           const ::openmldb::nameserver::ZoneInfo& zone_info,
                           std::string& msg);  // NOLINT

    base::Status AddReplica(const std::string& name, const std::set<uint32_t>& pid_set, const std::string& endpoint);

    bool AddReplicaNS(const std::string& name, const std::vector<std::string>& endpoint_vec, uint32_t pid,
                      const ::openmldb::nameserver::ZoneInfo& zone_info, const ::openmldb::api::TaskInfo& task_info);

    base::Status DelReplica(const std::string& name, const std::set<uint32_t>& pid_set, const std::string& endpoint);

    bool ConfSet(const std::string& key, const std::string& value,
                 std::string& msg);  // NOLINT

    bool ConfGet(const std::string& key, std::map<std::string, std::string>& conf_map,  // NOLINT
                 std::string& msg);                                                     // NOLINT

    base::Status ChangeLeader(const std::string& name, uint32_t pid,
                              std::string& candidate_leader);  // NOLINT

    bool OfflineEndpoint(const std::string& endpoint, uint32_t concurrency,
                         std::string& msg);  // NOLINT

    base::Status Migrate(const std::string& src_endpoint, const std::string& name, const std::set<uint32_t>& pid_set,
                         const std::string& des_endpoint);

    bool RecoverEndpoint(const std::string& endpoint, bool need_restore, uint32_t concurrency,
                         std::string& msg);  // NOLINT

    base::Status RecoverTable(const std::string& name, uint32_t pid, const std::string& endpoint);

    bool ConnectZK(std::string& msg);  // NOLINT

    bool DisConnectZK(std::string& msg);  // NOLINT

    bool SetTablePartition(const std::string& name, const ::openmldb::nameserver::TablePartition& table_partition,
                           std::string& msg);  // NOLINT

    bool GetTablePartition(const std::string& name, uint32_t pid,
                           ::openmldb::nameserver::TablePartition& table_partition,  // NOLINT
                           std::string& msg);                                        // NOLINT

    base::Status UpdateTableAliveStatus(const std::string& endpoint, const std::string& name, uint32_t pid,
                                        bool is_alive);

    bool UpdateTTL(const std::string& name, const ::openmldb::type::TTLType& type, uint64_t abs_ttl, uint64_t lat_ttl,
                   const std::string& ts_name, std::string& msg);  // NOLINT

    bool UpdateTTL(const std::string& db, const std::string& name, const ::openmldb::type::TTLType& type,
            uint64_t abs_ttl, uint64_t lat_ttl, const std::string& ts_name, std::string& msg);  // NOLINT

    bool AddReplicaClusterByNs(const std::string& alias, const std::string& name, uint64_t term,
                               std::string& msg);  // NOLINT

    bool AddReplicaCluster(const std::string& zk_ep, const std::string& zk_path, const std::string& alias,
                           std::string& msg);  // NOLINT

    bool ShowReplicaCluster(std::vector<::openmldb::nameserver::ClusterAddAge>& clusterinfo,  // NOLINT
                            std::string& msg);                                                // NOLINT

    bool RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name, uint64_t term,
                                  int& code,          // NOLINT
                                  std::string& msg);  // NOLINT

    bool RemoveReplicaCluster(const std::string& alias,
                              std::string& msg);  // NOLINT

    bool SwitchMode(const ::openmldb::nameserver::ServerMode& mode, std::string& msg);  // NOLINT

    bool AddIndex(const std::string& table_name, const ::openmldb::common::ColumnKey& column_key,
                  std::vector<openmldb::common::ColumnDesc>* cols,
                  std::string& msg);  // NOLINT

    bool AddIndex(const std::string& db_name, const std::string& table_name,
                  const ::openmldb::common::ColumnKey& column_key, std::vector<openmldb::common::ColumnDesc>* cols,
                  std::string& msg);  // NOLINT

    base::Status AddMultiIndex(const std::string& db, const std::string& table_name,
                               const std::vector<::openmldb::common::ColumnKey>& column_keys, bool skip_load_data);

    bool DeleteIndex(const std::string& table_name, const std::string& idx_name,
                     std::string& msg);  // NOLINT

    bool DeleteIndex(const std::string& db, const std::string& table_name, const std::string& idx_name,
                     std::string& msg);  // NOLINT

    bool DropProcedure(const std::string& db_name, const std::string& sp_name,
                       std::string& msg);  // NOLINT

    base::Status CreateProcedure(const ::openmldb::api::ProcedureInfo& sp_info, uint64_t request_timeout);

    base::Status CreateFunction(const ::openmldb::common::ExternalFun& fun);

    base::Status DropFunction(const std::string& name, bool if_exists);

    base::Status ShowFunction(const std::string& name, std::vector<::openmldb::common::ExternalFun>* fun_vec);

    bool ShowProcedure(const std::string& db_name, const std::string& sp_name, std::vector<api::ProcedureInfo>* infos,
                       std::string* msg);

    base::Status UpdateOfflineTableInfo(const nameserver::TableInfo& table_info);

    base::Status DeploySQL(
        const ::openmldb::api::ProcedureInfo& sp_info,
        const std::map<std::string, std::map<std::string, std::vector<::openmldb::common::ColumnKey>>>& new_index_map,
        uint64_t* op_id);

 private:
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> client_;
    std::string db_;
};

}  // namespace client
}  // namespace openmldb

#endif  // SRC_CLIENT_NS_CLIENT_H_
