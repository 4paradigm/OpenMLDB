//
// ns_client.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//

#ifndef SRC_CLIENT_NS_CLIENT_H_
#define SRC_CLIENT_NS_CLIENT_H_

#include <stdint.h>
#include <map>
#include <vector>
#include <string>
#include <set>
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"

namespace rtidb {
namespace client {

const uint32_t INVALID_PID = UINT32_MAX;

struct TabletInfo {
    std::string endpoint;
    std::string state;
    uint64_t age;
};

class NsClient {
 public:
    explicit NsClient(const std::string& endpoint);

    int Init();

    std::string GetEndpoint();

    bool ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg); // NOLINT

    bool ShowTable(const std::string& name,
                   std::vector<::rtidb::nameserver::TableInfo>& tables, // NOLINT
                   std::string& msg); // NOLINT

    bool MakeSnapshot(const std::string& name, uint32_t pid,
                      uint64_t end_offset, std::string& msg); // NOLINT

    bool ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response, // NOLINT
                      const std::string& name, uint32_t pid, std::string& msg); // NOLINT

    bool CancelOP(uint64_t op_id, std::string& msg); // NOLINT

    bool AddTableField(const std::string& table_name,
                       const ::rtidb::common::ColumnDesc& column_desc,
                       std::string& msg); // NOLINT

    bool CreateTable(const ::rtidb::nameserver::TableInfo& table_info,
                     std::string& msg); // NOLINT

    bool DropTable(const std::string& name, std::string& msg); // NOLINT

    bool SyncTable(const std::string& name, const std::string& cluster_alias,
                   uint32_t pid, std::string& msg); // NOLINT

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    bool GetTaskStatus(::rtidb::api::TaskStatusResponse& response); // NOLINT

    bool LoadTable(const std::string& name, const std::string& endpoint,
                   uint32_t pid, const ::rtidb::nameserver::ZoneInfo& zone_info,
                   const ::rtidb::api::TaskInfo& task_info);

    bool CreateRemoteTableInfo(const ::rtidb::nameserver::ZoneInfo& zone_info,
                               ::rtidb::nameserver::TableInfo& table_info, // NOLINT
                               std::string& msg); // NOLINT

    bool CreateRemoteTableInfoSimply(
        const ::rtidb::nameserver::ZoneInfo& zone_info,
        ::rtidb::nameserver::TableInfo& table_info, std::string& msg); // NOLINT

    bool DropTableRemote(const ::rtidb::api::TaskInfo& task_info,
                         const std::string& name,
                         const ::rtidb::nameserver::ZoneInfo& zone_info,
                         std::string& msg); // NOLINT

    bool CreateTableRemote(const ::rtidb::api::TaskInfo& task_info,
                           const ::rtidb::nameserver::TableInfo& table_info,
                           const ::rtidb::nameserver::ZoneInfo& zone_info,
                           std::string& msg); // NOLINT

    bool AddReplica(const std::string& name, const std::set<uint32_t>& pid_set,
                    const std::string& endpoint, std::string& msg); // NOLINT

    bool AddReplicaNS(const std::string& name,
                      const std::vector<std::string>& endpoint_vec,
                      uint32_t pid,
                      const ::rtidb::nameserver::ZoneInfo& zone_info,
                      const ::rtidb::api::TaskInfo& task_info);

    bool DelReplica(const std::string& name, const std::set<uint32_t>& pid_set,
                    const std::string& endpoint, std::string& msg); // NOLINT

    bool ConfSet(const std::string& key, const std::string& value,
                 std::string& msg); // NOLINT

    bool ConfGet(const std::string& key,
                 std::map<std::string, std::string>& conf_map, // NOLINT
                 std::string& msg); // NOLINT

    bool ChangeLeader(const std::string& name, uint32_t pid,
                      std::string& candidate_leader, std::string& msg); // NOLINT

    bool OfflineEndpoint(const std::string& endpoint, uint32_t concurrency,
                         std::string& msg); // NOLINT

    bool Migrate(const std::string& src_endpoint, const std::string& name,
                 const std::set<uint32_t>& pid_set,
                 const std::string& des_endpoint, std::string& msg); // NOLINT

    bool RecoverEndpoint(const std::string& endpoint, bool need_restore,
                         uint32_t concurrency, std::string& msg); // NOLINT

    bool RecoverTable(const std::string& name, uint32_t pid,
                      const std::string& endpoint, std::string& msg); // NOLINT

    bool ConnectZK(std::string& msg); // NOLINT

    bool DisConnectZK(std::string& msg); // NOLINT

    bool SetTablePartition(
        const std::string& name,
        const ::rtidb::nameserver::TablePartition& table_partition,
        std::string& msg); // NOLINT

    bool GetTablePartition(const std::string& name, uint32_t pid,
                           ::rtidb::nameserver::TablePartition& table_partition, // NOLINT
                           std::string& msg); // NOLINT

    bool UpdateTableAliveStatus(const std::string& endpoint, std::string& name, // NOLINT
                                uint32_t pid, bool is_alive, std::string& msg); // NOLINT

    bool UpdateTTL(const std::string& name, const ::rtidb::api::TTLType& type,
                   uint64_t abs_ttl, uint64_t lat_ttl,
                   const std::string& ts_name, std::string& msg); // NOLINT

    bool AddReplicaClusterByNs(const std::string& alias,
                               const std::string& name, const uint64_t term,
                               std::string& msg); // NOLINT

    bool AddReplicaCluster(const std::string& zk_ep, const std::string& zk_path,
                           const std::string& alias, std::string& msg); // NOLINT

    bool ShowReplicaCluster(
        std::vector<::rtidb::nameserver::ClusterAddAge>& clusterinfo, // NOLINT
        std::string& msg); // NOLINT

    bool RemoveReplicaClusterByNs(const std::string& alias,
                                  const std::string& zone_name,
                                  const uint64_t term, int& code, // NOLINT
                                  std::string& msg); // NOLINT

    bool RemoveReplicaCluster(const std::string& alias, std::string& msg); // NOLINT

    bool SwitchMode(const ::rtidb::nameserver::ServerMode mode,
                    std::string& msg); // NOLINT

    bool AddIndex(const std::string& table_name,
                  const ::rtidb::common::ColumnKey& column_key,
                  std::string& msg); // NOLINT

    bool DeleteIndex(const std::string& table_name, const std::string& idx_name,
                     std::string& msg); // NOLINT

 private:
    std::string endpoint_;
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub> client_;
};

}  // namespace client
}  // namespace rtidb

#endif  // SRC_CLIENT_NS_CLIENT_H_
