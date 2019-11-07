//
// ns_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//

#include "client/ns_client.h"
#include "base/strings.h"

DECLARE_int32(request_timeout_ms);
namespace rtidb {
namespace client {

NsClient::NsClient(const std::string& endpoint):endpoint_(endpoint),client_(endpoint) {}

int NsClient::Init() {
    return client_.Init();
}

bool NsClient::ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg) {
    ::rtidb::nameserver::ShowTabletRequest request;
    ::rtidb::nameserver::ShowTabletResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTablet,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::rtidb::nameserver::TabletStatus status = response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.state = status.state();
            info.age = status.age();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowTable(const std::string& name, 
            std::vector<::rtidb::nameserver::TableInfo>& tables, std::string& msg) {
    ::rtidb::nameserver::ShowTableRequest request;
    if (!name.empty()) {
        request.set_name(name);
    }
    ::rtidb::nameserver::ShowTableResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTable,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.table_info_size(); i++) {
            ::rtidb::nameserver::TableInfo table_info;
            table_info.CopyFrom(response.table_info(i));
            tables.push_back(table_info);
        }
        return true;
    }
    return false;
}

bool NsClient::MakeSnapshot(const std::string& name, uint32_t pid, std::string& msg) {
    ::rtidb::nameserver::MakeSnapshotNSRequest request;
    request.set_name(name);
    request.set_pid(pid);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::MakeSnapshotNS,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response, 
            const std::string& name, uint32_t pid, std::string& msg) {
    ::rtidb::nameserver::ShowOPStatusRequest request;
    if (!name.empty()) {
        request.set_name(name);
    }
    if (pid != INVALID_PID) {
        request.set_pid(pid);
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowOPStatus,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CancelOP(uint64_t op_id, std::string& msg) {
    ::rtidb::nameserver::CancelOPRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_op_id(op_id);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CancelOP,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddTableField(const std::string& table_name, 
        const ::rtidb::common::ColumnDesc& column_desc, 
        std::string& msg) {
    ::rtidb::nameserver::AddTableFieldRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(table_name);
    ::rtidb::common::ColumnDesc* column_desc_ptr = request.mutable_column_desc();
    column_desc_ptr->CopyFrom(column_desc);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddTableField,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateTable(const ::rtidb::nameserver::TableInfo& table_info, std::string& msg) {
    ::rtidb::nameserver::CreateTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::TableInfo* table_info_r = request.mutable_table_info();
    table_info_r->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CreateTable,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DropTable(const std::string& name, std::string& msg) {
    ::rtidb::nameserver::DropTableRequest request;
    request.set_name(name);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DropTable,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplica(const std::string& name, const std::set<uint32_t>& pid_set, 
            const std::string& endpoint, std::string& msg) {
    if (pid_set.empty()) {
        return false;
    }
    ::rtidb::nameserver::AddReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(*(pid_set.begin()));
    request.set_endpoint(endpoint);
    if (pid_set.size() > 1) {
        for (auto pid : pid_set) {
            request.add_pid_group(pid);
        }
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddReplicaNS,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DelReplica(const std::string& name, const std::set<uint32_t>& pid_set, 
            const std::string& endpoint, std::string& msg) {
    if (pid_set.empty()) {
        return false;
    }
    ::rtidb::nameserver::DelReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(*(pid_set.begin()));
    request.set_endpoint(endpoint);
    if (pid_set.size() > 1) {
        for (auto pid : pid_set) {
            request.add_pid_group(pid);
        }
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DelReplicaNS,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfSet(const std::string& key, const std::string& value, std::string& msg) {
    ::rtidb::nameserver::ConfSetRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::Pair* conf = request.mutable_conf();
    conf->set_key(key);
    conf->set_value(value);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConfSet,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfGet(const std::string& key, std::map<std::string, std::string>& conf_map, 
            std::string& msg) {
    conf_map.clear();        
    ::rtidb::nameserver::ConfGetRequest request;
    ::rtidb::nameserver::ConfGetResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConfGet,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int idx = 0; idx < response.conf_size(); idx++) {
            if (key.empty()) {
                conf_map.insert(std::make_pair(response.conf(idx).key(), response.conf(idx).value()));
            } else if (key == response.conf(idx).key()) {
                conf_map.insert(std::make_pair(key, response.conf(idx).value()));
                break;
            }
        }
        if (!key.empty() && conf_map.empty()) {
            msg = "cannot found key " + key;
            return false;
        }
        return true;
    }
    return false;
}

bool NsClient::ChangeLeader(const std::string& name, uint32_t pid, std::string& candidate_leader, std::string& msg) {
    ::rtidb::nameserver::ChangeLeaderRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    if (!candidate_leader.empty()) {
        request.set_candidate_leader(candidate_leader);
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ChangeLeader,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::OfflineEndpoint(const std::string& endpoint, uint32_t concurrency, std::string& msg) {
    ::rtidb::nameserver::OfflineEndpointRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    if (concurrency > 0) {
        request.set_concurrency(concurrency);
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::OfflineEndpoint,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::Migrate(const std::string& src_endpoint, const std::string& name, 
            const std::set<uint32_t>& pid_set, const std::string& des_endpoint, std::string& msg) {
    ::rtidb::nameserver::MigrateRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_src_endpoint(src_endpoint);
    request.set_name(name);
    request.set_des_endpoint(des_endpoint);
    for (auto pid : pid_set) {
        request.add_pid(pid);       
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::Migrate,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}    

bool NsClient::RecoverEndpoint(const std::string& endpoint, bool need_restore, 
            uint32_t concurrency, std::string& msg) {
    ::rtidb::nameserver::RecoverEndpointRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    if (concurrency > 0) {
        request.set_concurrency(concurrency);
    }
    request.set_need_restore(need_restore);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::RecoverEndpoint,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}    

bool NsClient::RecoverTable(const std::string& name, uint32_t pid, 
            const std::string& endpoint, std::string& msg) {
    ::rtidb::nameserver::RecoverTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::RecoverTable,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}    

bool NsClient::ConnectZK(std::string& msg) {
    ::rtidb::nameserver::ConnectZKRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConnectZK,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DisConnectZK(std::string& msg) {
    ::rtidb::nameserver::DisConnectZKRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DisConnectZK,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}            

bool NsClient::SetTablePartition(const std::string& name,
            const ::rtidb::nameserver::TablePartition& table_partition, std::string& msg) {
    ::rtidb::nameserver::SetTablePartitionRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    ::rtidb::nameserver::TablePartition* cur_table_partition = request.mutable_table_partition();
    cur_table_partition->CopyFrom(table_partition);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::SetTablePartition,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}            

bool NsClient::GetTablePartition(const std::string& name, uint32_t pid,
            ::rtidb::nameserver::TablePartition& table_partition, std::string& msg) {
    ::rtidb::nameserver::GetTablePartitionRequest request;
    ::rtidb::nameserver::GetTablePartitionResponse response;
    request.set_name(name);
    request.set_pid(pid);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::GetTablePartition,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        table_partition.CopyFrom(response.table_partition());
        return true;
    }
    return false;
}            

bool NsClient::UpdateTableAliveStatus(const std::string& endpoint, std::string& name, 
                    uint32_t pid, bool is_alive, std::string& msg) {
    ::rtidb::nameserver::UpdateTableAliveRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    request.set_name(name);
    request.set_is_alive(is_alive);
    if (pid < UINT32_MAX) {
        request.set_pid(pid);
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::UpdateTableAliveStatus,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::UpdateTTL(const std::string& name, 
                         const std::string& ttl_type,
                         uint64_t ttl, const std::string& ts_name,
                         std::string& msg) {
    ::rtidb::nameserver::UpdateTTLRequest request;
    ::rtidb::nameserver::UpdateTTLResponse response;
    request.set_name(name);
    if (ttl_type == "absolute") {
        request.set_ttl_type("kAbsoluteTime");
    } else {
        request.set_ttl_type("kLatestTime");
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    request.set_value(ttl);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::UpdateTTL,
                                  &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaClusterByNs(const std::string& alias, const std::string& name, const uint64_t& term, std::string& msg) {
    ::rtidb::nameserver::ReplicaClusterRequestByNs request;
    ::rtidb::nameserver::AddReplicaClusterRequestByNsResponse response;
    request.set_replica_alias(alias);
    request.set_zone_name(name);
    request.set_zone_term(term);
    request.set_follower(true);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddReplicaClusterByNs,
                                    &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && ((response.code() == 0) || (response.code() == 1))) {
        return true;
    }
    return false;
}

bool NsClient::ReplicaOf(const std::string& zk_ep, const std::string& zk_path, const std::string& alias, std::string& msg) {
    ::rtidb::nameserver::AddReplicaClusterRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_alias(alias);
    auto cluster_add = request.mutable_cluster_add();
    cluster_add->set_zk_path(zk_path);
    cluster_add->set_zk_endpoints(zk_ep);

    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddReplicaCluster,
                                    &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();

    if (ok && (response.code() == 0)) {
        return true;
    }
    return false;
}

bool NsClient::ShowReplicaOf(std::vector<::rtidb::nameserver::ClusterAddAge>& clusterinfo, std::string& msg) {
    ::rtidb::nameserver::GeneralRequest request;
    ::rtidb::nameserver::ShowReplicaClusterResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowReplicaCluster,
        &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && (response.code() == 0)) {
        for(int32_t i = 0; i < response.replicas_size(); i++) {
            auto status = response.replicas(i);
            clusterinfo.push_back(status);
        }
        return true;
    }

    return false;
}

bool NsClient::RemoveReplicaCluster(const std::string& alias, std::string& msg) {
    ::rtidb::nameserver::RemoveReplicaOfRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_alias(alias);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::RemoveReplicaCluster,
        &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && (response.code() == 0)) {
        return true;
    }
    return false;
}

bool NsClient::RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name,
    const uint64_t term, std::string& msg) {
    ::rtidb::nameserver::ReplicaClusterRequestByNs request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_replica_alias(alias);
    request.set_zone_term(term);
    request.set_zone_name(zone_name);
    request.set_follower(false);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::RemoveReplicaClusterByNs,
        &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && (response.code() == 0)) {
        return true;
    }
    return false;
}

}
}



