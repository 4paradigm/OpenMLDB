//
// ns_client.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//


#ifndef RTIDB_NS_CLIENT_H
#define RTIDB_NS_CLIENT_H

#include <stdint.h>
#include <vector>
#include <map>
#include "rpc/rpc_client.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"

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
    NsClient(const std::string& endpoint);

    int Init();

    std::string GetEndpoint();

    bool ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg);
    
    bool ShowTable(const std::string& name, std::vector<::rtidb::nameserver::TableInfo>& tables, std::string& msg);

    bool MakeSnapshot(const std::string& name, uint32_t pid, uint64_t end_offset, std::string& msg);

    bool ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response, 
                const std::string& name, uint32_t pid, std::string& msg);

    bool CancelOP(uint64_t op_id, std::string& msg);

    bool AddTableField(const std::string& table_name,
            const ::rtidb::common::ColumnDesc& column_desc,
            std::string& msg);

    bool CreateTable(const ::rtidb::nameserver::TableInfo& table_info, std::string& msg);

    bool DropTable(const std::string& name, std::string& msg);

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    bool GetTaskStatus(::rtidb::api::TaskStatusResponse& response);

    bool LoadTable(const std::string& name,
            const std::string& endpoint,
            uint32_t pid,
            const ::rtidb::nameserver::ZoneInfo& zone_info, 
            const ::rtidb::api::TaskInfo& task_info);

    bool CreateRemoteTableInfo(const ::rtidb::nameserver::ZoneInfo& zone_info, 
            ::rtidb::nameserver::TableInfo& table_info,
            std::string& msg);

    bool CreateRemoteTableInfoSimply(const ::rtidb::nameserver::ZoneInfo& zone_info, 
            ::rtidb::nameserver::TableInfo& table_info,
            std::string& msg);

    bool DropTableRemote(const ::rtidb::api::TaskInfo& task_info, const std::string& name, const ::rtidb::nameserver::ZoneInfo& zone_info, std::string& msg);
    
    bool CreateTableRemote(const ::rtidb::api::TaskInfo& task_info, const ::rtidb::nameserver::TableInfo& table_info, const ::rtidb::nameserver::ZoneInfo& zone_info, std::string& msg);

    bool AddReplica(const std::string& name, const std::set<uint32_t>& pid_set, const std::string& endpoint, std::string& msg);

    bool AddReplicaNS(const std::string& name, 
            const std::vector<std::string>& endpoint_vec,
            uint32_t pid,
            const ::rtidb::nameserver::ZoneInfo& zone_info, 
            const ::rtidb::api::TaskInfo& task_info); 

    bool DelReplica(const std::string& name, const std::set<uint32_t>& pid_set, const std::string& endpoint, std::string& msg);

    bool ConfSet(const std::string& key, const std::string& value, std::string& msg);

    bool ConfGet(const std::string& key, std::map<std::string, std::string>& conf_map, std::string& msg);

    bool ChangeLeader(const std::string& name, uint32_t pid, std::string& candidate_leader, std::string& msg);

    bool OfflineEndpoint(const std::string& endpoint, uint32_t concurrency, std::string& msg);

    bool Migrate(const std::string& src_endpoint, const std::string& name, const std::set<uint32_t>& pid_set, 
                 const std::string& des_endpoint, std::string& msg);

    bool RecoverEndpoint(const std::string& endpoint, bool need_restore, uint32_t concurrency, std::string& msg);

    bool RecoverTable(const std::string& name, uint32_t pid, const std::string& endpoint, std::string& msg);

    bool ConnectZK(std::string& msg);

    bool DisConnectZK(std::string& msg);

    bool SetTablePartition(const std::string& name,
                           const ::rtidb::nameserver::TablePartition& table_partition, 
                           std::string& msg);

    bool GetTablePartition(const std::string& name, uint32_t pid,
                           ::rtidb::nameserver::TablePartition& table_partition, 
                           std::string& msg);

    bool UpdateTableAliveStatus(const std::string& endpoint, std::string& name, 
                            uint32_t pid, bool is_alive, std::string& msg);

    bool UpdateTTL(const std::string& name, 
                   const ::rtidb::api::TTLType& type,
                   uint64_t abs_ttl, uint64_t lat_ttl,
                   const std::string& ts_name,
                   std::string& msg);

    bool AddReplicaClusterByNs(const std::string& alias, const std::string& name, const uint64_t term, std::string& msg);

    bool AddReplicaCluster(const std::string& zk_ep, const std::string& zk_path, const std::string& alias, std::string& msg);

    bool ShowReplicaCluster(std::vector<::rtidb::nameserver::ClusterAddAge>& clusterinfo, std::string& msg);

    bool RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
                          int& code, std::string& msg);

    bool RemoveReplicaCluster(const std::string& alias, std::string& msg);

    bool SwitchMode(const ::rtidb::nameserver::ServerMode mode, std::string& msg);

private:
    std::string endpoint_;
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub> client_;
};

}
}

#endif /* !RTIDB_NS_CLIENT_H */
