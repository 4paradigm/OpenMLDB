//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "nameserver/name_server_impl.h"

#include <gflags/gflags.h>
#include "timer.h"
#include <strings.h>
#include <chrono>
#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <base/strings.h>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(get_task_status_interval);
DECLARE_int32(name_server_task_pool_size);
DECLARE_int32(name_server_task_wait_time);
DECLARE_int32(max_op_num);
DECLARE_uint32(partition_num);
DECLARE_uint32(replica_num);
DECLARE_bool(auto_failover);
DECLARE_uint32(tablet_heartbeat_timeout);
DECLARE_uint32(tablet_offline_check_interval);
DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(get_table_status_interval);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_uint32(check_binlog_sync_progress_delta);
DECLARE_uint32(name_server_op_execute_timeout);
DECLARE_uint32(get_replica_status_interval);
DECLARE_int32(make_snapshot_time);
DECLARE_int32(make_snapshot_check_interval);

namespace rtidb {
namespace nameserver {

const std::string OFFLINE_LEADER_ENDPOINT = "OFFLINE_LEADER_ENDPOINT";
const uint8_t MAX_ADD_TABLE_FIELD_COUNT = 63;

ClusterInfo::ClusterInfo(const ::rtidb::nameserver::ClusterAddress& cd) : client_(),
    last_status(), zk_client_(), mu_(), session_term_(), task_id_() {
    cluster_add_.CopyFrom(cd);
    state_ = kClusterOffline;
    ctime_ = ::baidu::common::timer::get_micros() / 1000;
}

void ClusterInfo::CheckZkClient() {
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
}

void ClusterInfo::UpdateNSClient(const std::vector<std::string>& children) {
    if (children.empty()) {
        PDLOG(INFO, "children is empty on UpdateNsClient");
        return;
    }
    std::vector<std::string> tmp_children(children.begin(), children.end());
    std::sort(tmp_children.begin(), tmp_children.end());
    std::string endpoint;
    if (tmp_children[0] == client_->GetEndpoint()) {
        return;
    }
    if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/leader/" + tmp_children[0], endpoint)) {
        PDLOG(WARNING, "get replica cluster leader ns failed");
        return;
    }
    std::shared_ptr<::rtidb::client::NsClient> tmp_ptr = std::make_shared<::rtidb::client::NsClient>(endpoint);
    if (tmp_ptr->Init() < 0) {
        PDLOG(WARNING, "replica cluster ns client init failed");
        return;
    }
    std::atomic_store_explicit(&client_, tmp_ptr, std::memory_order_relaxed);
    ctime_ = ::baidu::common::timer::get_micros() / 1000;
    state_.store(kClusterHealthy, std::memory_order_relaxed);
}

int ClusterInfo::Init(std::string& msg) {
    zk_client_ = std::make_shared<ZkClient>(cluster_add_.zk_endpoints(), FLAGS_zk_session_timeout, "", \
    cluster_add_.zk_path(), cluster_add_.zk_path() + "/leader");
    bool ok = zk_client_->Init();
    for (int i = 1; i < 3; i++) {
        if (ok) {
            break;
        }
        PDLOG(WARNING, "count %d fail to init zookeeper with cluster %s %s", i, cluster_add_.zk_endpoints().c_str(), cluster_add_.zk_path().c_str());
        ok = zk_client_->Init();
    }
    if (!ok) {
        msg = "connect relica cluster zk failed";
        return 401;
    }
    session_term_ = zk_client_->GetSessionTerm();
    std::vector<std::string> children;
    if (!zk_client_->GetChildren(cluster_add_.zk_path() + "/leader", children) || children.empty()) {
        msg = "get zk failed";
        PDLOG(WARNING, "get zk failed, get children");
        return 451;
    }
    std::string endpoint;
    if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/leader/" + children[0], endpoint)) {
        msg = "get zk failed";
        PDLOG(WARNING, "get zk failed, get replica cluster leader ns failed");
        return 451;
    }
    client_ = std::make_shared<::rtidb::client::NsClient>(endpoint);
    if (client_->Init() < 0) {
        msg = "connect ns failed";
        PDLOG(WARNING, "connect ns failed, replica cluster ns");
        return 403;
    }
    zk_client_->WatchNodes(boost::bind(&ClusterInfo::UpdateNSClient, this, _1));
    zk_client_->WatchNodes();
    return 0;
}

bool ClusterInfo::DropTableRemote(const ::rtidb::api::TaskInfo& task_info, 
        const std::string& name, 
        const ::rtidb::nameserver::ZoneInfo& zone_info) {
    std::string msg;
    if (!std::atomic_load_explicit(&client_, std::memory_order_relaxed)->DropTableRemote(task_info, name, zone_info, msg)) {
        PDLOG(WARNING, "drop table for replica cluster failed!, msg is: %s", msg.c_str());
        return false;
    }
    return true;
}

bool ClusterInfo::CreateTableRemote(const ::rtidb::api::TaskInfo& task_info,
        const ::rtidb::nameserver::TableInfo& table_info, 
        const ::rtidb::nameserver::ZoneInfo& zone_info) {
    std::string msg;
    if (!std::atomic_load_explicit(&client_, std::memory_order_relaxed)->CreateTableRemote(task_info, table_info, zone_info, msg)) {
        PDLOG(WARNING, "create table for replica cluster failed!, msg is: %s", msg.c_str());
        return false;
    }
    return true;
}

void NameServerImpl::CheckSyncExistTable(const std::string& alias, 
        const std::vector<::rtidb::nameserver::TableInfo>& tables_remote, 
        const std::shared_ptr<::rtidb::client::NsClient> ns_client) {
    for (const TableInfo& table_info_remote : tables_remote) {
        std::string name = table_info_remote.name();
        ::rtidb::nameserver::TableInfo table_info_local;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto iter = table_info_.find(name);
            if (iter == table_info_.end()) {
                PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
                continue;
            }
            table_info_local = *(iter->second);
        }
        bool is_continue = false;
        for (int idx = 0; idx < table_info_local.table_partition_size(); idx++) {
            ::rtidb::nameserver::TablePartition table_partition_local = table_info_local.table_partition(idx);
            for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                if (table_partition_local.partition_meta(midx).is_leader() &&
                        (!table_partition_local.partition_meta(midx).is_alive())) {
                    PDLOG(WARNING, "table [%s] pid [%u] has a no alive leader partition", 
                            name.c_str(), table_partition_local.pid());
                    is_continue = true;
                }
            }
        }
        //remote table
        for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
            ::rtidb::nameserver::TablePartition table_partition = table_info_remote.table_partition(idx);
            for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                if (table_partition.partition_meta(midx).is_leader()) { 
                    if (!table_partition.partition_meta(midx).is_alive()) {
                        PDLOG(WARNING, "remote table [%s] has a no alive leader partition pid[%u]", 
                                name.c_str(), table_partition.pid());
                        is_continue = true;
                    }
                }
            }
        }
        if (is_continue) {
            PDLOG(WARNING, "table [%s] does not sync to replica cluster [%s]", 
                    name.c_str(), alias.c_str());
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
                ::rtidb::nameserver::TablePartition table_partition = table_info_remote.table_partition(idx);
                uint32_t cur_pid = table_partition.pid();
                for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                    if (table_partition.partition_meta(midx).is_leader() && 
                            table_partition.partition_meta(midx).is_alive()) {
                        if (AddReplicaSimplyRemoteOP(name, table_partition.partition_meta(midx).endpoint(), 
                                    table_info_remote.tid(), cur_pid) < 0) {
                            PDLOG(WARNING, "create AddReplicasSimplyRemoteOP failed. table[%s] pid[%u] alias[%s]", 
                                    name.c_str(), cur_pid, alias.c_str());
                        }
                    }
                }
            }
        }
    }
}

void NameServerImpl::CheckSyncTable(const std::string& alias, 
        const std::vector<::rtidb::nameserver::TableInfo> tables, 
        const std::shared_ptr<::rtidb::client::NsClient> ns_client) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info_.empty()) {
            PDLOG(INFO, "leader cluster has no table");
            return;
        }
    }
    std::vector<std::string> table_name_vec;
    for (auto& rkv : tables) {
        table_name_vec.push_back(rkv.name());
    }
    std::vector<::rtidb::nameserver::TableInfo> local_table_info_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : table_info_) {
            if (std::find(table_name_vec.begin(), table_name_vec.end(), kv.first) == table_name_vec.end()) {
                bool has_no_alive_leader_partition = false;
                for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
                    ::rtidb::nameserver::TablePartition table_partition_local = kv.second->table_partition(idx);
                    for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                        if (table_partition_local.partition_meta(midx).is_leader() &&
                                (!table_partition_local.partition_meta(midx).is_alive())) {
                            has_no_alive_leader_partition = true;
                            PDLOG(WARNING, "table [%s] pid [%u] has a no alive leader partition",
                                    kv.second->name().c_str(), table_partition_local.pid());
                            break;
                        }
                    }
                    if (has_no_alive_leader_partition) {
                        break;
                    }
                }
                if (!has_no_alive_leader_partition) {
                    local_table_info_vec.push_back(*(kv.second));
                }
            }
        }
    }
    for (const auto& table_tmp : local_table_info_vec) { 
        ::rtidb::nameserver::TableInfo table_info(table_tmp);
        //get remote table_info: tid and leader partition info
        std::string msg;
        if (!ns_client->CreateRemoteTableInfo(zone_info_, table_info, msg)) {
            PDLOG(WARNING, "create remote table_info erro, wrong msg is [%s]", msg.c_str()); 
            return;
        }
        std::lock_guard<std::mutex> lock(mu_);
        for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
            ::rtidb::nameserver::TablePartition table_partition = table_info.table_partition(idx);
            AddReplicaRemoteOP(alias, table_info.name(), table_partition,
                    table_info.tid(), table_partition.pid());
        }
    }
}

void NameServerImpl::CheckTableInfo(std::shared_ptr<ClusterInfo>& ci, const std::vector<::rtidb::nameserver::TableInfo>& tables) {
    std::map<std::string, std::shared_ptr<TableInfo>>::iterator table_info_iter;
    for (const auto& table : tables) {
        table_info_iter = table_info_.find(table.name());
        if (table_info_iter == table_info_.end()) {
            PDLOG(WARNING, "talbe [%s] not found in table_info", table.name().c_str());
            continue;
        }
        int ready_num = -2; // default is not found
        for (auto& alias_pair : table_info_iter->second->alias_pair()) {
            if (alias_pair.alias() == ci->cluster_add_.alias()) {
                ready_num = alias_pair.ready_num();
                break;
            }
        }
        if (ready_num != 0) {
            PDLOG(WARNING, "table [%s] current ready_num is %d, not equal 0", table.name().c_str(), ready_num);
            continue;
        }
        auto status_iter = ci->last_status.find(table.name());
        if (status_iter == ci->last_status.end()) {
            std::vector<TablePartition> tbs;
            for (auto& part : table.table_partition()) {
                ::rtidb::nameserver::TablePartition tb;
                tb.set_pid(part.pid());
                tb.set_record_byte_size(part.record_byte_size());
                tb.set_record_cnt(part.record_cnt());
                PartitionMeta* m = tb.add_partition_meta();
                m->set_endpoint("");
                for (auto& meta : part.partition_meta()) {
                    if (meta.is_alive() && meta.is_leader()) {
                        m->set_endpoint(meta.endpoint());
                        break;
                    }
                }
                tbs.push_back(tb);
            }
            ci->last_status.insert(std::make_pair(table.name(), tbs));
        } else {
            // cache endpoint
            std::map<uint32_t, std::string> pid_endpoint;
            std::set<uint32_t> parts;
            for (const auto& part : table_info_iter->second->table_partition()) {
                for (auto& meta : part.partition_meta()) {
                    if (meta.is_leader() && meta.is_alive()) {
                        parts.insert(part.pid());
                    }
                }
                for (auto& meta : part.remote_partition_meta()) {
                    if (meta.alias() == ci->cluster_add_.alias()) {
                        pid_endpoint.insert(std::make_pair(part.pid(), meta.endpoint()));
                        break;
                    }
                }
            }
            // cache endpoint && part reference
            std::map<uint32_t, std::vector<TablePartition>::iterator> part_refer;
            for (auto iter = status_iter->second.begin(); iter != status_iter->second.end(); iter++) {
                part_refer.insert(std::make_pair(iter->pid(), iter));
            }
            for (const auto& part : table.table_partition()) {
                if (parts.find(part.pid()) == parts.end()) {
                    PDLOG(WARNING, "table [%s] pid [%u] partition leader is offline", table.name().c_str(), part.pid());
                    continue; // leader partition is offline, can't add talbe replica
                }
                for (auto& meta : part.partition_meta()) {
                    if (meta.is_leader() && meta.is_alive()) {
                        auto iter = part_refer.find(part.pid());
                        if (iter == part_refer.end()) {
                            PDLOG(WARNING, "table [%s] pid [%u] not found", table.name().c_str(), part.pid());
                            break;
                        }
                        auto endpoint_iter = pid_endpoint.find(part.pid());
                        if (endpoint_iter != pid_endpoint.end()) {
                            if (meta.endpoint() == endpoint_iter->second) {
                                break;
                            } else {
                                PDLOG(INFO, "table [%s] pid[%u] will remove endpoint %s", table.name().c_str(), part.pid(), endpoint_iter->second.c_str());
                                DelReplicaRemoteOP(endpoint_iter->second, table.name(), part.pid());
                            }
                        }
                        iter->second->clear_partition_meta();
                        iter->second->add_partition_meta()->CopyFrom(meta);

                        PDLOG(INFO, "table [%s] pid[%u] will add remote endpoint %s", table.name().c_str(), part.pid(), meta.endpoint().c_str());
                        AddReplicaSimplyRemoteOP(table.name(), meta.endpoint(), table.tid(), part.pid());
                        break;
                    }
                }
            }
        } 
    }
}

bool NameServerImpl::CompareSnapshotOffset(const std::vector<TableInfo>& tables, std::string& msg, int& code, std::map<std::string, std::map<uint32_t, std::map<uint32_t, uint64_t>>>& table_part_offset) {
    for (const auto& table : tables) {
        // iter == table_info_.end() is impossible, because CompareTableInfo has checked it
        std::map<uint32_t, uint64_t> pid_offset;
        auto iter = table_info_.find(table.name());
        int32_t tid = iter->second->tid();
        for (const auto& part : iter->second->table_partition()) {
            for (const auto& meta : part.partition_meta()) {
                if (meta.is_alive() && meta.is_leader()) {
                    auto tablet_it = table_part_offset.find(meta.endpoint());
                    if (tablet_it == table_part_offset.end()) {
                        PDLOG(WARNING, "%s not found in table info", meta.endpoint().c_str());
                        msg = "tablet endpoint not found";
                        code = 411;
                        return false;
                    }
                    auto tid_it = tablet_it->second.find(tid);
                    if (tid_it == tablet_it->second.end()) {
                        PDLOG(WARNING, "tid [%u] not found on tablet %s", tid, meta.endpoint().c_str());
                        msg = "tid not found";
                        code = 412;
                        return false;
                    }
                    auto pid_it = tid_it->second.find(part.pid());
                    if (pid_it == tid_it->second.end()) {
                        PDLOG(WARNING, "tid [%u] pid [%u] not found on tablet %s", tid, part.pid(), meta.endpoint().c_str());
                        msg = "pid not found";
                        code = 413;
                        return false;
                    }
                    pid_offset.insert(std::make_pair(part.pid(), pid_it->second));
                }
            }
        }
        // remote table
        for (auto& part : table.table_partition()) {
            auto offset_iter = pid_offset.find(part.pid());
            if (offset_iter == pid_offset.end()) {
                PDLOG(WARNING, "table [%s] pid [%u] is not found", table.name().c_str(), part.pid());
                msg = "partition offline";
                code = 407;
                return false;
            }

            for (auto& meta : part.partition_meta()) {
                if (meta.is_leader() && meta.is_alive()) {
                    if (meta.offset() < offset_iter->second) {
                        PDLOG(WARNING, "table [%s] pid [%u] offset less than local table snapshot", table.name().c_str(), part.pid());
                        msg = "rep cluster offset too small";
                        code = 406;
                        return false;
                    }
                    break;
                }
            }
        }
    }
    return true;
}

bool NameServerImpl::CompareTableInfo(const std::vector<::rtidb::nameserver::TableInfo>& tables) {
    for (auto& table : tables) {
        auto iter = table_info_.find(table.name());
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "table [%s] not found in table_info_", table.name().c_str());
            return false;
        }
        if (table.ttl() != iter->second->ttl()) {
            PDLOG(WARNING, "table [%s] ttl not equal, remote [%d] local [%d]", table.name().c_str(), table.ttl(), iter->second->ttl());
            return false;
        }
        if (table.ttl_type() != iter->second->ttl_type()) {
            PDLOG(WARNING, "table [%s] ttl type not equal, remote [%s] local [%s]", table.name().c_str(), table.ttl_type().c_str(), iter->second->ttl_type().c_str());
            return false;
        }
        if (table.table_partition_size() != iter->second->table_partition_size()) {
            PDLOG(WARNING, "table [%s] partition num not equal, remote [%d] local [%d]", table.name().c_str(), table.table_partition_size(), iter->second->table_partition_size());
            return false;
        }
        if (table.compress_type() != iter->second->compress_type()) {
            PDLOG(WARNING, "table [%s] compress type not equal", table.name().c_str());
            return false;
        }
        if (table.column_desc_size() != iter->second->column_desc_size()) {
            PDLOG(WARNING, "table [%s] column desc size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < iter->second->column_desc_size(); i++) {
                std::string name = iter->second->column_desc(i).name();
                std::string value;
                iter->second->column_desc(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& column : table.column_desc()) {
                auto iter = tmp_map.find(column.name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING, "table [%s] not found column desc [%s] in local cluster", table.name().c_str(), column.name().c_str());
                    return false;
                }
                if (column.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(), column.name().c_str());
                    return false;
                }

            }
        }
        if (table.column_desc_v1_size() != iter->second->column_desc_v1_size()) {
            PDLOG(WARNING, "table [%s] column desc v1 size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < iter->second->column_desc_v1_size(); i++) {
                std::string name = iter->second->column_desc_v1(i).name();
                std::string value;
                iter->second->column_desc_v1(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& column_v1 : table.column_desc_v1()) {
                auto iter = tmp_map.find(column_v1.name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING, "table [%s] not found column desc [%s] in local cluster", table.name().c_str(), column_v1.name().c_str());
                    return false;
                }
                if (column_v1.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(), column_v1.name().c_str());
                    return false;
                }

            }
        }
        if (table.column_key_size() != iter->second->column_key_size()) {
            PDLOG(WARNING, "table [%s] column key size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < iter->second->column_key_size(); i++) {
                std::string name = iter->second->column_key(i).index_name();
                std::string value;
                iter->second->column_key(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& key : table.column_key()) {
                auto iter = tmp_map.find(key.index_name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING, "table [%s] not found column desc [%s] in local cluster", table.name().c_str(), key.index_name().c_str());
                    return false;
                }
                if (key.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(), key.index_name().c_str());
                    return false;
                }

            }
        }
        if (table.added_column_desc_size() != iter->second->added_column_desc_size()) {
            PDLOG(WARNING, "table [%s] added column desc size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < iter->second->added_column_desc_size(); i++) {
                std::string name = iter->second->added_column_desc(i).name();
                std::string value;
                iter->second->added_column_desc(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& added_column : table.added_column_desc()) {
                auto iter = tmp_map.find(added_column.name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING, "table [%s] not found column desc [%s] in local cluster", table.name().c_str(), added_column.name().c_str());
                    return false;
                }
                if (added_column.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(), added_column.name().c_str());
                    return false;
                }

            }
        }
    }
    return true;
}

bool ClusterInfo::AddReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
        std::string& msg) {
    if (!std::atomic_load_explicit(&client_, std::memory_order_relaxed)->AddReplicaClusterByNs(alias, zone_name, term, msg)) {
        PDLOG(WARNING, "send MakeReplicaCluster request failed");
        return false;
    }
    return true;
}

bool ClusterInfo::RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name,
    const uint64_t term, int& code, std::string& msg) {
    return std::atomic_load_explicit(&client_, std::memory_order_relaxed)->RemoveReplicaClusterByNs(alias, zone_name, term, code, msg);
  }

NameServerImpl::NameServerImpl():mu_(), tablets_(),
                                 table_info_(), zk_client_(NULL), dist_lock_(NULL), thread_pool_(1),
                                 task_thread_pool_(FLAGS_name_server_task_pool_size), cv_(),
    rand_(0xdeadbeef), session_term_(0) {
    std::string zk_table_path = FLAGS_zk_root_path + "/table";
    zk_table_index_node_ = zk_table_path + "/table_index";
    zk_table_data_path_ = zk_table_path + "/table_data";
    zk_term_node_ = zk_table_path + "/term";
    std::string zk_op_path = FLAGS_zk_root_path + "/op";
    zk_op_index_node_ = zk_op_path + "/op_index";
    zk_op_data_path_ = zk_op_path + "/op_data";
    zk_offline_endpoint_lock_node_ = FLAGS_zk_root_path + "/offline_endpoint_lock";
    std::string zk_config_path = FLAGS_zk_root_path + "/config";
    zk_zone_data_path_ = FLAGS_zk_root_path + "/cluster";
    zk_auto_failover_node_ = zk_config_path + "/auto_failover";
    zk_table_changed_notify_node_ = zk_table_path + "/notify";
    running_.store(false, std::memory_order_release);
    mode_.store(kNORMAL, std::memory_order_release);
    auto_failover_.store(FLAGS_auto_failover, std::memory_order_release);
    task_rpc_version_.store(0, std::memory_order_relaxed);
    zone_info_.set_mode(kNORMAL);
    zone_info_.set_zone_name(FLAGS_endpoint + FLAGS_zk_root_path);
    zone_info_.set_replica_alias("");
    zone_info_.set_zone_term(1);
}

NameServerImpl::~NameServerImpl() {
    running_.store(false, std::memory_order_release);
    thread_pool_.Stop(true);
    task_thread_pool_.Stop(true);
    delete zk_client_;
}

// become name server leader
bool NameServerImpl::Recover() {
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        PDLOG(WARNING, "get endpoints node failed!");
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);

        std::string value;
        if (zk_client_->GetNodeValue(zk_zone_data_path_ + "/follower", value)) {
            zone_info_.ParseFromString(value);
            mode_.store(zone_info_.mode(), std::memory_order_release);
            PDLOG(WARNING, "recover zone info : %s", value.c_str());
        }
        UpdateTablets(endpoints);
        value.clear();
        if (!zk_client_->GetNodeValue(zk_table_index_node_, value)) {
            if (!zk_client_->CreateNode(zk_table_index_node_, "1")) {
                PDLOG(WARNING, "create table index node failed!");
                return false;
            }
            table_index_ = 1;
            PDLOG(INFO, "init table_index[%u]", table_index_);
        } else {
            table_index_ = std::stoull(value);
            PDLOG(INFO, "recover table_index[%u]", table_index_);
        }
        value.clear();
        if (!zk_client_->GetNodeValue(zk_term_node_, value)) {
            if (!zk_client_->CreateNode(zk_term_node_, "1")) {
                PDLOG(WARNING, "create term node failed!");
                return false;
            }
            term_ = 1;
            PDLOG(INFO, "init term[%lu]", term_);
        } else {
            term_ = std::stoull(value);
            PDLOG(INFO, "recover term[%u]", term_);
        }
        value.clear();
        if (!zk_client_->GetNodeValue(zk_op_index_node_, value)) {
            if (!zk_client_->CreateNode(zk_op_index_node_, "1")) {
                PDLOG(WARNING, "create op index node failed!");
                return false;
            }
            op_index_ = 1;
            PDLOG(INFO, "init op_index[%u]", op_index_);
        } else {
            op_index_ = std::stoull(value);
            PDLOG(INFO, "recover op_index[%u]", op_index_);
        }
        value.clear();
        if (!zk_client_->GetNodeValue(zk_table_changed_notify_node_, value)) {
            if (!zk_client_->CreateNode(zk_table_changed_notify_node_, "1")) {
                PDLOG(WARNING, "create zk table changed notify node failed");
                return false;
            }
        }
        value.clear();
        if (!zk_client_->GetNodeValue(zk_auto_failover_node_, value)) {
            auto_failover_.load(std::memory_order_acquire) ? value = "true" : value = "false";
            if (!zk_client_->CreateNode(zk_auto_failover_node_, value)) {
                PDLOG(WARNING, "create auto failover node failed!");
                return false;
            }
            PDLOG(INFO, "set zk_auto_failover_node[%s]", value.c_str());
        } else {
            value == "true" ? auto_failover_.store(true, std::memory_order_release) :
                auto_failover_.store(false, std::memory_order_release);
            PDLOG(INFO, "get zk_auto_failover_node[%s]", value.c_str());
        }
        if (!RecoverTableInfo()) {
            PDLOG(WARNING, "recover table info failed!");
            return false;
        }
    }
    UpdateTableStatus();
    {
        std::lock_guard<std::mutex> lock(mu_);
        RecoverClusterInfo();
        if (!RecoverOPTask()) {
            PDLOG(WARNING, "recover task failed!");
            return false;
        }
        RecoverOfflineTablet();
    }
    UpdateTaskStatus(true);
    return true;
}

void NameServerImpl::RecoverOfflineTablet() {
    offline_endpoint_map_.clear();
    for (const auto& tablet : tablets_) {
        if (tablet.second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            offline_endpoint_map_.insert(std::make_pair(tablet.first, tablet.second->ctime_));
            thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval, boost::bind(&NameServerImpl::OnTabletOffline, this, tablet.first, false));
            PDLOG(INFO, "recover offlinetablet. endpoint %s", tablet.first.c_str());
        }
    }
}

void NameServerImpl::RecoverClusterInfo() {
    nsc_.clear();
    std::vector<std::string> cluster_vec;
    if (!zk_client_->GetChildren(zk_zone_data_path_ + "/replica", cluster_vec)) {
        if (zk_client_->IsExistNode(zk_zone_data_path_ + "/replica") > 0) {
            PDLOG(WARNING, "cluster info node is not exist");
            return;
        }
        PDLOG(WARNING, "get cluster info failed!");
        return;
    }
    PDLOG(INFO, "need to recover cluster info[%d]", cluster_vec.size());

    std::string value, rpc_msg;
    for (const auto& alias: cluster_vec) {

        value.clear();
        if (!zk_client_->GetNodeValue(zk_zone_data_path_ + "/replica/" + alias, value)) {
            PDLOG(WARNING, "get cluster info failed! name[%s]", alias.c_str());
            continue;
        }

        ::rtidb::nameserver::ClusterAddress cluster_add;
        cluster_add.ParseFromString(value);
        std::shared_ptr<::rtidb::nameserver::ClusterInfo> cluster_info = std::make_shared<::rtidb::nameserver::ClusterInfo>(cluster_add);
        PDLOG(INFO, "zk add %s|%s", cluster_add.zk_endpoints().c_str(), cluster_add.zk_path().c_str());
        cluster_info->state_ = kClusterHealthy;

        if (cluster_info->Init(rpc_msg) != 0) {
            PDLOG(WARNING, "%s init failed, error: %s", alias.c_str(), rpc_msg.c_str());
            // todo :: add cluster status, need show in showreplica
            cluster_info->state_ = kClusterOffline;
        }
        nsc_.insert(std::make_pair(alias, cluster_info));
    }
}

bool NameServerImpl::RecoverTableInfo() {
    table_info_.clear();
    std::vector<std::string> table_vec;
    if (!zk_client_->GetChildren(zk_table_data_path_, table_vec)) {
        if (zk_client_->IsExistNode(zk_table_data_path_) > 0) {
            PDLOG(WARNING, "table data node is not exist");
            return true;
        }
        PDLOG(WARNING, "get table name failed!");
        return false;
    }
    PDLOG(INFO, "need to recover table num[%d]", table_vec.size());
    for (const auto& table_name : table_vec) {
        std::string table_name_node = zk_table_data_path_ + "/" + table_name;
        std::string value;
        if (!zk_client_->GetNodeValue(table_name_node, value)) {
            PDLOG(WARNING, "get table info failed! name[%s] table node[%s]", table_name.c_str(), table_name_node.c_str());
            continue;
        }
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info =
                    std::make_shared<::rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            PDLOG(WARNING, "parse table info failed! name[%s] value[%s] value size[%d]", table_name.c_str(), value.c_str(), value.length());
            continue;
        }
        table_info_.insert(std::make_pair(table_name, table_info));
        PDLOG(INFO, "recover table[%s] success", table_name.c_str());
    }
    return true;
}

bool NameServerImpl::RecoverOPTask() {
    for (auto& op_list : task_vec_) {
        op_list.clear();
    }
    std::vector<std::string> op_vec;
    if (!zk_client_->GetChildren(zk_op_data_path_, op_vec)) {
        if (zk_client_->IsExistNode(zk_op_data_path_) > 0) {
            PDLOG(WARNING, "op data node is not exist");
            return true;
        }
        PDLOG(WARNING, "get op failed!");
        return false;
    }
    PDLOG(INFO, "need to recover op num[%d]", op_vec.size());
    for (const auto& op_id : op_vec) {
        std::string op_node = zk_op_data_path_ + "/" + op_id;
        std::string value;
        if (!zk_client_->GetNodeValue(op_node, value)) {
            PDLOG(WARNING, "get table info failed! table node[%s]", op_node.c_str());
            continue;
        }
        std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
        if (!op_data->op_info_.ParseFromString(value)) {
            PDLOG(WARNING, "parse op info failed! value[%s]", value.c_str());
            continue;
        }
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kDone) {
            PDLOG(DEBUG, "op status is kDone. op_id[%lu]", op_data->op_info_.op_id());
            continue;
        }
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kCanceled) {
            PDLOG(DEBUG, "op status is kCanceled. op_id[%lu]", op_data->op_info_.op_id());
            continue;
        }
        switch (op_data->op_info_.op_type()) {
            case ::rtidb::api::OPType::kMakeSnapshotOP:
                if (CreateMakeSnapshotOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kAddReplicaOP:
                if (CreateAddReplicaOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kChangeLeaderOP:
                if (CreateChangeLeaderOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kMigrateOP:
                if (CreateMigrateTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kRecoverTableOP:
                if (CreateRecoverTableOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kOfflineReplicaOP:
                if (CreateOfflineReplicaTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kDelReplicaOP:
                if (CreateDelReplicaOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaOP:
                if (CreateReAddReplicaTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaNoSendOP:
                if (CreateReAddReplicaNoSendTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaWithDropOP:
                if (CreateReAddReplicaWithDropTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaSimplifyOP:
                if (CreateReAddReplicaSimplifyTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReLoadTableOP:
                if (CreateReLoadTableTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kUpdatePartitionStatusOP:
                if (CreateUpdatePartitionStatusOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kCreateTableRemoteOP:
                if (CreateTableRemoteTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                            ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                            op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kDropTableRemoteOP:
                if (DropTableRemoteTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                            ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                            op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kDelReplicaRemoteOP:
                if (CreateDelReplicaRemoteOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kAddReplicaSimplyRemoteOP:
                if (CreateAddReplicaSimplyRemoteOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kAddReplicaRemoteOP:
                if (CreateAddReplicaRemoteOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            default:
                PDLOG(WARNING, "unsupport recover op[%s]! op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                continue;
        }
        if (!SkipDoneTask(op_data)) {
            PDLOG(WARNING, "SkipDoneTask task failed. op_id[%lu] task_index[%u]",
                            op_data->op_info_.op_id(), op_data->op_info_.task_index());
            continue;
        }
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kFailed ||
                op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kCanceled) {
            done_op_list_.push_back(op_data);
        } else {
            uint32_t idx = 0;
            if (op_data->op_info_.for_replica_cluster() == 1) {
                idx = op_data->op_info_.vec_idx();
                PDLOG(INFO, "current task is for replica cluster, op_index [%lu] op_type[%s]", 
                        op_data->op_info_.op_id(), ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
            } else {
                idx = op_data->op_info_.pid() % task_vec_.size();
                if (op_data->op_info_.has_vec_idx() && op_data->op_info_.vec_idx() < task_vec_.size()) {
                    idx = op_data->op_info_.vec_idx();
                }
            }
            task_vec_[idx].push_back(op_data);
        }
        PDLOG(INFO, "recover op[%s] success. op_id[%lu]",
                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(), op_data->op_info_.op_id());
    }
    for (auto& op_list : task_vec_) {
        op_list.sort([](const std::shared_ptr<OPData>& a, const std::shared_ptr<OPData>& b) {
            if (a->op_info_.parent_id() < b->op_info_.parent_id()) {
                return true;
            } else if (a->op_info_.parent_id() > b->op_info_.parent_id()) {
                return false;
            } else {
                return a->op_info_.op_id() < b->op_info_.op_id();
            }
        });
    }
    return true;
}
int NameServerImpl::CreateMakeSnapshotOPTask(std::shared_ptr<OPData> op_data) {
    MakeSnapshotNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto iter = table_info_.find(request.name());
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "get table info failed! name[%s]", request.name().c_str());
        return -1;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    uint32_t pid = request.pid();
    std::string endpoint;
    if (GetLeader(table_info, pid, endpoint) < 0 || endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return -1;
    }
    uint64_t end_offset = 0;
    if (request.has_offset() && request.offset() > 0) {
        end_offset = request.offset();
    }
    std::shared_ptr<Task> task = CreateMakeSnapshotTask(endpoint, op_data->op_info_.op_id(),
                ::rtidb::api::OPType::kMakeSnapshotOP, tid, pid, end_offset);
    if (!task) {
        PDLOG(WARNING, "create makesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create makesnapshot op task ok. tid[%u] pid[%u]", tid, pid);
    return 0;
}

bool NameServerImpl::SkipDoneTask(std::shared_ptr<OPData> op_data) {
    uint64_t op_id = op_data->op_info_.op_id();
    std::string op_type = ::rtidb::api::OPType_Name(op_data->op_info_.op_type());
    if (op_data->op_info_.task_status() == ::rtidb::api::kInited) {
        PDLOG(INFO, "op_id[%lu] op_type[%s] status is kInited, need not skip",
                    op_id, op_type.c_str());
        return true;
    }
    uint32_t task_index = op_data->op_info_.task_index();
    if (op_data->task_list_.empty()) {
        PDLOG(WARNING, "skip task failed, task_list is empty. op_id[%lu] op_type[%s]",
                        op_id, op_type.c_str());
        return false;
    }
    if (task_index > op_data->task_list_.size() - 1) {
        PDLOG(WARNING, "skip task failed. op_id[%lu] op_type[%s] task_index[%u]",
                        op_id, op_type.c_str(), task_index);
        return false;
    }
    for (uint32_t idx = 0; idx < task_index; idx++) {
        op_data->task_list_.pop_front();
    }
    if (!op_data->task_list_.empty()) {
        std::shared_ptr<Task> task = op_data->task_list_.front();
        PDLOG(INFO, "cur task[%s]. op_id[%lu] op_type[%s]",
                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                    op_id, op_type.c_str());
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kFailed) {
            task->task_info_->set_status(::rtidb::api::TaskStatus::kFailed);
            return true;
        }
        switch (task->task_info_->task_type()) {
            case ::rtidb::api::TaskType::kSelectLeader:
            case ::rtidb::api::TaskType::kUpdateLeaderInfo:
            case ::rtidb::api::TaskType::kUpdatePartitionStatus:
            case ::rtidb::api::TaskType::kUpdateTableInfo:
            case ::rtidb::api::TaskType::kRecoverTable:
            case ::rtidb::api::TaskType::kAddTableInfo:
            case ::rtidb::api::TaskType::kCheckBinlogSyncProgress:
                // execute the task again
                task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
                break;
            default:
                task->task_info_->set_status(::rtidb::api::TaskStatus::kDoing);
        }
    }
    return true;
}

void NameServerImpl::UpdateTabletsLocked(const std::vector<std::string>& endpoints) {
    std::lock_guard<std::mutex> lock(mu_);
    UpdateTablets(endpoints);
}


void NameServerImpl::UpdateTablets(const std::vector<std::string>& endpoints) {
    // check exist and newly add tablets
    std::set<std::string> alive;
    std::vector<std::string>::const_iterator it = endpoints.begin();
    for (; it != endpoints.end(); ++it) {
        alive.insert(*it);
        Tablets::iterator tit = tablets_.find(*it);
        // register a new tablet
        if (tit == tablets_.end()) {
            std::shared_ptr<TabletInfo> tablet = std::make_shared<TabletInfo>();
            tablet->state_ = ::rtidb::api::TabletState::kTabletHealthy;
            tablet->client_ = std::make_shared<::rtidb::client::TabletClient>(*it, true);
            if (tablet->client_->Init() != 0) {
                PDLOG(WARNING, "tablet client init error. endpoint[%s]", it->c_str());
                continue;
            }
            tablet->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            tablets_.insert(std::make_pair(*it, tablet));
            PDLOG(INFO, "add tablet client. endpoint[%s]", it->c_str());
        } else {
            if (tit->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                tit->second->state_ = ::rtidb::api::TabletState::kTabletHealthy;
                tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
                PDLOG(INFO, "tablet is online. endpoint[%s]", tit->first.c_str());
                thread_pool_.AddTask(boost::bind(&NameServerImpl::OnTabletOnline, this, tit->first));
            }
        }
        PDLOG(INFO, "healthy tablet with endpoint[%s]", it->c_str());
    }
    // handle offline tablet
    for (Tablets::iterator tit = tablets_.begin(); tit != tablets_.end(); ++tit) {
        if (alive.find(tit->first) == alive.end()
                && tit->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
            // tablet offline
            PDLOG(INFO, "offline tablet with endpoint[%s]", tit->first.c_str());
            tit->second->state_ = ::rtidb::api::TabletState::kTabletOffline;
            tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            if (offline_endpoint_map_.find(tit->first) == offline_endpoint_map_.end()) {
                offline_endpoint_map_.insert(std::make_pair(tit->first, tit->second->ctime_));
                if (running_.load(std::memory_order_acquire)) {
                    thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval, boost::bind(&NameServerImpl::OnTabletOffline, this, tit->first, false));
                }
            } else {
                offline_endpoint_map_[tit->first] = tit->second->ctime_;
            }
        }
    }
    thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
}

void NameServerImpl::OnTabletOffline(const std::string& endpoint, bool startup_flag) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto tit = tablets_.find(endpoint);
        if (tit == tablets_.end()) {
            PDLOG(WARNING, "cannot find endpoint %s in tablet map", endpoint.c_str());
            return;
        }
        auto iter = offline_endpoint_map_.find(endpoint);
        if (iter == offline_endpoint_map_.end()) {
            PDLOG(WARNING, "cannot find endpoint %s in offline endpoint map", endpoint.c_str());
            return;
        }
        if (!startup_flag && tit->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
            PDLOG(INFO, "endpoint %s is healthy, need not offline endpoint", endpoint.c_str());
            return;
        }
        if (table_info_.empty()) {
            PDLOG(INFO, "endpoint %s has no table, need not offline endpoint", endpoint.c_str());
            return;
        }
        uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
        if (!startup_flag && cur_time < iter->second + FLAGS_tablet_heartbeat_timeout) {
            thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval, boost::bind(&NameServerImpl::OnTabletOffline, this, endpoint, false));
            return;
        }
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        PDLOG(INFO, "Run OfflineEndpoint. endpoint is %s", endpoint.c_str());
        UpdateEndpointTableAlive(endpoint, false);
        OfflineEndpointInternal(endpoint, FLAGS_name_server_task_concurrency);
    }
}

void NameServerImpl::OnTabletOnline(const std::string& endpoint) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (!auto_failover_.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(mu_);
        offline_endpoint_map_.erase(endpoint);
        return;
    }
    std::string value;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = offline_endpoint_map_.find(endpoint);
        if (iter == offline_endpoint_map_.end()) {
            PDLOG(WARNING, "cannot find endpoint %s in offline endpoint map. need not recover", endpoint.c_str());
            return;
        }
        if (!zk_client_->GetNodeValue(FLAGS_zk_root_path + "/nodes/" + endpoint, value)) {
            PDLOG(WARNING, "get tablet node value failed");
            offline_endpoint_map_.erase(iter);
            return;
        }
        if (table_info_.empty()) {
            PDLOG(INFO, "endpoint %s has no table, need not recover endpoint", endpoint.c_str());
            offline_endpoint_map_.erase(iter);
            return;
        }
        if (!boost::starts_with(value, "startup_")) {
            uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
            if (cur_time < iter->second + FLAGS_tablet_heartbeat_timeout) {
                PDLOG(INFO, "need not recover. endpoint[%s] cur_time[%lu] offline_time[%lu]",
                            endpoint.c_str(), cur_time, iter->second);
                offline_endpoint_map_.erase(iter);
                return;
            }
        }
    }
    if (boost::starts_with(value, "startup_")) {
        PDLOG(INFO, "endpoint %s is startup, exe tablet offline", endpoint.c_str());
        OnTabletOffline(endpoint, true);
    }
    PDLOG(INFO, "Run RecoverEndpoint. endpoint is %s", endpoint.c_str());
    RecoverEndpointInternal(endpoint, false, FLAGS_name_server_task_concurrency);
    {
        std::lock_guard<std::mutex> lock(mu_);
        offline_endpoint_map_.erase(endpoint);
    }
}

void NameServerImpl::RecoverEndpointInternal(const std::string& endpoint, bool need_restore, uint32_t concurrency) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid =  kv.second->table_partition(idx).pid();
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (kv.second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    if (kv.second->table_partition(idx).partition_meta(meta_idx).is_alive() &&
                            kv.second->table_partition(idx).partition_meta_size() > 1) {
                        PDLOG(INFO, "table[%s] pid[%u] endpoint[%s] is alive, need not recover",
                                    kv.first.c_str(), pid, endpoint.c_str());
                        break;
                    }
                    PDLOG(INFO, "recover table[%s] pid[%u] endpoint[%s]", kv.first.c_str(), pid, endpoint.c_str());
                    bool is_leader = false;
                    if (kv.second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        is_leader = true;
                    }
                    uint64_t offset_delta = need_restore ? 0 : FLAGS_check_binlog_sync_progress_delta;
                    CreateRecoverTableOP(kv.first, pid, endpoint, is_leader, offset_delta, concurrency);
                    if (need_restore && is_leader) {
                        PDLOG(INFO, "restore table[%s] pid[%u] endpoint[%s]", kv.first.c_str(), pid, endpoint.c_str());
                        CreateChangeLeaderOP(kv.first, pid, endpoint, need_restore, concurrency);
                        CreateRecoverTableOP(kv.first, pid, OFFLINE_LEADER_ENDPOINT, true,
                                             FLAGS_check_binlog_sync_progress_delta, concurrency);
                    }
                    break;
                }
            }
        }
    }
}

void NameServerImpl::ShowTablet(RpcController* controller,
        const ShowTabletRequest* request,
        ShowTabletResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    Tablets::iterator it = tablets_.begin();
    for (; it !=  tablets_.end(); ++it) {
        TabletStatus* status = response->add_tablets();
        status->set_endpoint(it->first);
        status->set_state(::rtidb::api::TabletState_Name(it->second->state_));
        status->set_age(::baidu::common::timer::get_micros() / 1000 - it->second->ctime_);
    }
    response->set_code(0);
    response->set_msg("ok");
}

bool NameServerImpl::Init() {
    if (FLAGS_zk_cluster.empty()) {
        PDLOG(WARNING, "zk cluster disabled");
        return false;
    }
    zk_client_ = new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
            FLAGS_endpoint, FLAGS_zk_root_path);
    if (!zk_client_->Init()) {
        PDLOG(WARNING, "fail to init zookeeper with cluster[%s]", FLAGS_zk_cluster.c_str());
        return false;
    }
    task_vec_.resize(FLAGS_name_server_task_max_concurrency + FLAGS_name_server_task_concurrency_for_replica_cluster);
    std::string value;
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        zk_client_->CreateNode(FLAGS_zk_root_path + "/nodes", "");
    } else {
        UpdateTablets(endpoints);
    }
    zk_client_->WatchNodes(boost::bind(&NameServerImpl::UpdateTabletsLocked, this, _1));
    zk_client_->WatchNodes();
    session_term_ = zk_client_->GetSessionTerm();

    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
    dist_lock_ = new DistLock(FLAGS_zk_root_path + "/leader", zk_client_,
            boost::bind(&NameServerImpl::OnLocked, this), boost::bind(&NameServerImpl::OnLostLock, this),
            FLAGS_endpoint);
    dist_lock_->Lock();
    return true;
}

void NameServerImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        OnLostLock();
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
            PDLOG(WARNING, "watch node falied");
        }
    }
    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
}

int NameServerImpl::UpdateTaskStatus(bool is_recover_op) {
    std::map<std::string, std::shared_ptr<TabletClient>> client_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                PDLOG(DEBUG, "tablet[%s] is not Healthy", iter->first.c_str());
                uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
                if (cur_time < iter->second->ctime_ + FLAGS_tablet_heartbeat_timeout) {
                    continue;
                }
                // clear the task in offline tablet
                for (const auto& op_list : task_vec_) {
                    if (op_list.empty()) {
                        continue;
                    }
                    std::shared_ptr<OPData> op_data = op_list.front();
                    if (op_data->task_list_.empty()) {
                        continue;
                    }
                    // update task status
                    std::shared_ptr<Task> task = op_data->task_list_.front();
                    if (task->task_info_->status() != ::rtidb::api::kDoing) {
                        continue;
                    }
                    if (task->task_info_->has_endpoint() && task->task_info_->endpoint() == iter->first) {
                        PDLOG(WARNING, "tablet is offline. update task status from[kDoing] to[kFailed]. "
                                       "op_id[%lu], task_type[%s] endpoint[%s]",
                                        op_data->op_info_.op_id(),
                                        ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                                        iter->first.c_str());
                        task->task_info_->set_status(::rtidb::api::kFailed);
                    }
                }
            } else {
                client_map.insert(std::make_pair(iter->first, iter->second->client_));
            }
        }
    }
    uint64_t last_task_rpc_version = task_rpc_version_.load(std::memory_order_acquire);
    for (auto iter = client_map.begin(); iter != client_map.end(); ++iter) {
        ::rtidb::api::TaskStatusResponse response;
        // get task status from tablet
        if (iter->second->GetTaskStatus(response)) {
            std::lock_guard<std::mutex> lock(mu_);
            if (last_task_rpc_version != task_rpc_version_.load(std::memory_order_acquire)) {
                PDLOG(DEBUG, "task_rpc_version mismatch");
                break;
            }
            std::string endpoint = iter->first;
            for (const auto& op_list : task_vec_) {
                std::string endpoint_role = "tablet";
                if (UpdateTask(op_list, endpoint, endpoint_role, is_recover_op, response) < 0) {
                    continue;
                }
            }
        }
    }
    UpdateTaskStatusRemote(is_recover_op);
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_task_status_interval, boost::bind(&NameServerImpl::UpdateTaskStatus, this, false));
    }
    return 0;
}

int NameServerImpl::UpdateTaskStatusRemote(bool is_recover_op) {
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        return 0;
    }
    std::map<std::string, std::shared_ptr<::rtidb::client::NsClient>> client_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (nsc_.empty()) {
            return 0;
        }
        for (auto iter = nsc_.begin(); iter != nsc_.end(); ++iter) {
            if (iter->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                PDLOG(INFO, "cluster[%s] is not Healthy", iter->first.c_str()); 
                continue;
            }
            client_map.insert(std::make_pair(iter->first, std::atomic_load_explicit(&iter->second->client_, std::memory_order_relaxed)));
        }
    }
    uint64_t last_task_rpc_version = task_rpc_version_.load(std::memory_order_acquire);
    for (auto iter = client_map.begin(); iter != client_map.end(); ++iter) {
        ::rtidb::api::TaskStatusResponse response;
        // get task status from replica cluster
        if (iter->second->GetTaskStatus(response)) {
            std::lock_guard<std::mutex> lock(mu_);
            if (last_task_rpc_version != task_rpc_version_.load(std::memory_order_acquire)) {
                PDLOG(DEBUG, "task_rpc_version mismatch");
                break;
            }
            std::string endpoint = iter->first;
            uint32_t index = 0;
            for (const auto& op_list : task_vec_) {
                index++;
                if (index <= FLAGS_name_server_task_max_concurrency) {
                    continue;
                }
                std::string endpoint_role = "replica cluster";
                if (UpdateTask(op_list, endpoint, endpoint_role, is_recover_op, response) < 0) {
                    continue;
                }
            }
        } else {
            if (response.has_msg()) {
                PDLOG(WARNING, "get task status faild : [%s]", response.msg().c_str());
            }
        }
    }
    return 0;
}

int NameServerImpl::UpdateTask(const std::list<std::shared_ptr<OPData>>& op_list, 
        const std::string& endpoint, 
        const std::string& msg,
        bool is_recover_op, 
        ::rtidb::api::TaskStatusResponse& response) {
    if (op_list.empty()) {
        return -1;
    }
    std::shared_ptr<OPData> op_data = op_list.front();
    if (op_data->task_list_.empty()) {
        return -1;
    }
    // update task status
    std::shared_ptr<Task> task = op_data->task_list_.front();
    if (task->task_info_->status() != ::rtidb::api::kDoing) {
        return -1;
    }
    bool has_op_task = false;
    for (int idx = 0; idx < response.task_size(); idx++) {
        if (op_data->op_info_.op_id() == response.task(idx).op_id() &&
                task->task_info_->task_type() == response.task(idx).task_type()) {
            has_op_task = true;
            if (response.task(idx).status() != ::rtidb::api::kInited &&
                    task->task_info_->status() != response.task(idx).status()) {
                PDLOG(INFO, "update task status from[%s] to[%s]. op_id[%lu], task_type[%s]", 
                        ::rtidb::api::TaskStatus_Name(task->task_info_->status()).c_str(), 
                        ::rtidb::api::TaskStatus_Name(response.task(idx).status()).c_str(), 
                        response.task(idx).op_id(), 
                        ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
                task->task_info_->set_status(response.task(idx).status());
                
            }
            break;
        }
    }
    if (!has_op_task && (is_recover_op || task->task_info_->is_rpc_send())) {
        if (task->task_info_->has_endpoint() && task->task_info_->endpoint() == endpoint) {
            PDLOG(WARNING, "not found op in [%s]. update task status from[kDoing] to[kFailed]. " 
                    "op_id[%lu], task_type[%s] endpoint[%s]", 
                    msg.c_str(),
                    op_data->op_info_.op_id(),
                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                    endpoint.c_str());
            task->task_info_->set_status(::rtidb::api::kFailed);
        }
    }
    return 1;
}

int NameServerImpl::UpdateZKTaskStatus() {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        std::shared_ptr<OPData> op_data = op_list.front();
        if (op_data->task_list_.empty()) {
            continue;
        }
        std::shared_ptr<Task> task = op_data->task_list_.front();
        if (task->task_info_->status() == ::rtidb::api::kDone) {
            uint32_t cur_task_index = op_data->op_info_.task_index();
            op_data->op_info_.set_task_index(cur_task_index + 1);
            std::string value;
            op_data->op_info_.SerializeToString(&value);
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
            if (zk_client_->SetNodeValue(node, value)) {
                PDLOG(DEBUG, "set zk status value success. node[%s] value[%s]",
                            node.c_str(), value.c_str());
                op_data->task_list_.pop_front();
                continue;
            }
            // revert task index
            op_data->op_info_.set_task_index(cur_task_index);
            PDLOG(WARNING, "set zk status value failed! node[%s] op_id[%lu] op_type[%s] task_index[%u]",
                          node.c_str(), op_data->op_info_.op_id(),
                          ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                          op_data->op_info_.task_index());
        }
    }
    return 0;
}

void NameServerImpl::UpdateTaskMapStatus(uint64_t remote_op_id, 
        uint64_t op_id,
        const ::rtidb::api::TaskStatus& status) {
    auto iter = task_map_.find(remote_op_id);
    if (iter == task_map_.end()) {
        PDLOG(DEBUG, "op [%lu] is not in task_map_", remote_op_id);
        return;
    } 
    for (auto& task_info : iter->second) {
        for (int idx = 0; idx < task_info->rep_cluster_op_id_size(); idx++) {
            uint64_t rep_cluster_op_id = task_info->rep_cluster_op_id(idx);
            if (rep_cluster_op_id == op_id) {
                if (status == ::rtidb::api::kFailed ||
                        status == ::rtidb::api::kCanceled) {
                    task_info->set_status(status);
                    if (status == ::rtidb::api::kFailed) {
                        PDLOG(DEBUG, "update task status from[kDoing] to[kFailed]. op_id[%lu], task_type[%s]", 
                                task_info->op_id(), 
                                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
                    } else {
                        PDLOG(DEBUG, "update task status from[kDoing] to[kCanceled]. op_id[%lu], task_type[%s]", 
                                task_info->op_id(), 
                                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
                    }
                }
                if (idx == task_info->rep_cluster_op_id_size() - 1){
                    if (status == ::rtidb::api::kDone &&
                            task_info->status() != ::rtidb::api::kFailed &&
                            task_info->status() != ::rtidb::api::kCanceled) {
                        task_info->set_status(status);
                        PDLOG(DEBUG, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                                task_info->op_id(), 
                                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());

                    }
                }
            }
        }
    }
}

int NameServerImpl::DeleteTask() {
    std::vector<uint64_t> done_task_vec;
    std::vector<uint64_t> done_task_vec_remote;
    std::vector<std::shared_ptr<TabletClient>> client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& op_list : task_vec_) {
            if (op_list.empty()) {
                continue;
            }
            std::shared_ptr<OPData> op_data = op_list.front();
            if (op_data->task_list_.empty()) {
                done_task_vec.push_back(op_data->op_info_.op_id());
                // for multi cluster -- leader cluster judge
                if (op_data->op_info_.for_replica_cluster() == 1) {
                    done_task_vec_remote.push_back(op_data->op_info_.op_id());            
                }
                // for multi cluster -- replica cluster judge
                if (op_data->op_info_.has_remote_op_id()) {
                    UpdateTaskMapStatus(op_data->op_info_.remote_op_id(), 
                            op_data->op_info_.op_id(), ::rtidb::api::TaskStatus::kDone); 
                }
            } else {
                std::shared_ptr<Task> task = op_data->task_list_.front();
                if (task->task_info_->status() == ::rtidb::api::kFailed ||
                        op_data->op_info_.task_status() == ::rtidb::api::kCanceled) {
                    done_task_vec.push_back(op_data->op_info_.op_id());
                    // for multi cluster -- leader cluster judge
                    if (op_data->op_info_.for_replica_cluster() == 1) {
                        done_task_vec_remote.push_back(op_data->op_info_.op_id());            
                    }
                    // for multi cluster -- replica cluster judge
                    PDLOG(WARNING, "task failed or canceled. op_id[%lu], task_type[%s]", 
                            task->task_info_->op_id(), 
                            ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
                    if (op_data->op_info_.has_remote_op_id()) {
                        UpdateTaskMapStatus(op_data->op_info_.remote_op_id(), 
                                op_data->op_info_.op_id(), task->task_info_->status()); 
                    }
                }
            }
        }
        if (done_task_vec.empty()) {
            return 0;
        }
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                PDLOG(DEBUG, "tablet[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            client_vec.push_back(iter->second->client_);
        }
    }
    bool has_failed = false;
    for (auto iter = client_vec.begin(); iter != client_vec.end(); ++iter) {
        if (!(*iter)->DeleteOPTask(done_task_vec)) {
            PDLOG(WARNING, "tablet[%s] delete op failed", (*iter)->GetEndpoint().c_str());
            has_failed = true;
            continue;
        }
        PDLOG(DEBUG, "tablet[%s] delete op success", (*iter)->GetEndpoint().c_str());
    }
    DeleteTaskRemote(done_task_vec_remote, has_failed);
    if (!has_failed) {
        DeleteTask(done_task_vec);        
    }
    return 0;
}

int NameServerImpl::DeleteTaskRemote(const std::vector<uint64_t>& done_task_vec, bool& has_failed) {
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        return 0;
    }
    std::vector<std::shared_ptr<::rtidb::client::NsClient>> client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (nsc_.empty()) {
            return 0;
        }
        for (auto iter = nsc_.begin(); iter != nsc_.end(); ++iter) {
            if (iter->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                PDLOG(INFO, "cluster[%s] is not Healthy", iter->first.c_str()); 
                continue;
            }
            client_vec.push_back(std::atomic_load_explicit(&iter->second->client_, std::memory_order_relaxed));
        }
    }
    for (auto iter = client_vec.begin(); iter != client_vec.end(); ++iter) {
        if (!(*iter)->DeleteOPTask(done_task_vec)) {
            PDLOG(WARNING, "replica cluster[%s] delete op failed", (*iter)->GetEndpoint().c_str()); 
            has_failed = true;
            continue;
        }
        PDLOG(DEBUG, "replica cluster[%s] delete op success", (*iter)->GetEndpoint().c_str()); 
    }
    return 0;
}

void NameServerImpl::DeleteTask(const std::vector<uint64_t>& done_task_vec) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto op_id : done_task_vec) {
        std::shared_ptr<OPData> op_data;
        uint32_t index = 0;
        for (uint32_t idx = 0; idx < task_vec_.size(); idx++) {
            if (task_vec_[idx].empty()) {
                continue;
            }
            if (task_vec_[idx].front()->op_info_.op_id() == op_id) {
                op_data = task_vec_[idx].front();
                index = idx;
                break;
            }
        }    
        if (!op_data) {
            PDLOG(WARNING, "has not found op[%lu] in running op", op_id); 
            continue;
        }
        std::string node = zk_op_data_path_ + "/" + std::to_string(op_id);
        if (!op_data->task_list_.empty() && 
                op_data->task_list_.front()->task_info_->status() == ::rtidb::api::kFailed) {
            op_data->op_info_.set_task_status(::rtidb::api::kFailed);
            op_data->op_info_.set_end_time(::baidu::common::timer::now_time());
            PDLOG(WARNING, "set op[%s] status failed. op_id[%lu]",
                    ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                    op_id);
            std::string value;
            op_data->op_info_.SerializeToString(&value);
            if (!zk_client_->SetNodeValue(node, value)) {
                PDLOG(WARNING, "set zk status value failed. node[%s] value[%s]",
                        node.c_str(), value.c_str());
            }
            done_op_list_.push_back(op_data);
            task_vec_[index].pop_front();
            PDLOG(INFO, "delete op[%lu] in running op", op_id); 
        } else {
            if (zk_client_->DeleteNode(node)) {
                PDLOG(INFO, "delete zk op node[%s] success.", node.c_str()); 
                op_data->op_info_.set_end_time(::baidu::common::timer::now_time());
                if (op_data->op_info_.task_status() == ::rtidb::api::kDoing) {
                    op_data->op_info_.set_task_status(::rtidb::api::kDone);
                    op_data->task_list_.clear();
                }
                done_op_list_.push_back(op_data);
                task_vec_[index].pop_front();
                PDLOG(INFO, "delete op[%lu] in running op", op_id);
            } else {
                PDLOG(WARNING, "delete zk op_node failed. opid[%lu] node[%s]", op_id, node.c_str()); 
            }
        }
    }
}

void NameServerImpl::ProcessTask() {
    while (running_.load(std::memory_order_acquire)) {
        {
            bool has_task = false;
            std::unique_lock<std::mutex> lock(mu_);
            for (const auto& op_list : task_vec_) {
                if (!op_list.empty()) {
                    has_task = true;
                    break;
                }
            }
            if (!has_task) {
                cv_.wait_for(lock, std::chrono::milliseconds(FLAGS_name_server_task_wait_time));
                if (!running_.load(std::memory_order_acquire)) {
                    PDLOG(WARNING, "cur nameserver is not leader");
                    return;
                }
            }

            for (const auto& op_list : task_vec_) {
                if (op_list.empty()) {
                    continue;
                }
                std::shared_ptr<OPData> op_data = op_list.front();
                if (op_data->task_list_.empty() ||
                        op_data->op_info_.task_status() == ::rtidb::api::kFailed ||
                        op_data->op_info_.task_status() == ::rtidb::api::kCanceled) {
                    continue;
                }
                if (op_data->op_info_.task_status() == ::rtidb::api::kInited) {
                    op_data->op_info_.set_start_time(::baidu::common::timer::now_time());
                    op_data->op_info_.set_task_status(::rtidb::api::kDoing);
                    std::string value;
                    op_data->op_info_.SerializeToString(&value);
                    std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
                    if (!zk_client_->SetNodeValue(node, value)) {
                        PDLOG(WARNING, "set zk op status value failed. node[%s] value[%s]",
                                        node.c_str(), value.c_str());
                        op_data->op_info_.set_task_status(::rtidb::api::kInited);
                        continue;
                    }
                }
                std::shared_ptr<Task> task = op_data->task_list_.front();
                if (task->task_info_->status() == ::rtidb::api::kFailed) {
                    PDLOG(WARNING, "task[%s] run failed, terminate op[%s]. op_id[%lu]",
                                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                                    ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                                    task->task_info_->op_id());
                } else if (task->task_info_->status() == ::rtidb::api::kInited) {
                    PDLOG(DEBUG, "run task. opid[%lu] op_type[%s] task_type[%s]", task->task_info_->op_id(),
                                ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                                ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
                    task_thread_pool_.AddTask(task->fun_);
                    task->task_info_->set_status(::rtidb::api::kDoing);;
                } else if (task->task_info_->status() == ::rtidb::api::kDoing) {
                    if (::baidu::common::timer::now_time() - op_data->op_info_.start_time() >
                            FLAGS_name_server_op_execute_timeout / 1000) {
                        PDLOG(INFO, "The execution time of op is too long. "
                                    "opid[%lu] op_type[%s] cur task_type[%s] start_time[%lu] cur_time[%lu]",
                                    task->task_info_->op_id(),
                                    ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                                    op_data->op_info_.start_time(),
                                    ::baidu::common::timer::now_time());
                        cv_.wait_for(lock, std::chrono::milliseconds(FLAGS_name_server_task_wait_time));
                    }
                }
            }
        }
        UpdateZKTaskStatus();
        DeleteTask();
    }
}

void NameServerImpl::ConnectZK(RpcController* controller,
        const ConnectZKRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (zk_client_->Reconnect()) {
        if (session_term_ != zk_client_->GetSessionTerm()) {
            if (zk_client_->WatchNodes()) {
                session_term_ = zk_client_->GetSessionTerm();
                PDLOG(INFO, "watch node ok");
            }
        }
        response->set_code(0);
        response->set_msg("ok");
        PDLOG(INFO, "connect zk ok");
        return;
    }
    response->set_code(-1);
    response->set_msg("reconnect failed");
}

void NameServerImpl::DisConnectZK(RpcController* controller,
        const DisConnectZKRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    zk_client_->CloseZK();
    OnLostLock();
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "disconnect zk ok");
}

void NameServerImpl::GetTablePartition(RpcController* controller,
        const GetTablePartitionRequest* request,
        GetTablePartitionResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string name = request->name();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        ::rtidb::nameserver::TablePartition *table_partition = response->mutable_table_partition();
        table_partition->CopyFrom(iter->second->table_partition(idx));
        break;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::SetTablePartition(RpcController* controller,
        const SetTablePartitionRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::string name = request->name();
    uint32_t pid = request->table_partition().pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> cur_table_info(iter->second->New());
    cur_table_info->CopyFrom(*(iter->second));
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (cur_table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        ::rtidb::nameserver::TablePartition* table_partition =
                cur_table_info->mutable_table_partition(idx);
        table_partition->Clear();
        table_partition->CopyFrom(request->table_partition());
        std::string table_value;
        cur_table_info->SerializeToString(&table_value);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]",
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            response->set_code(304);
            response->set_msg("set zk failed");
            return;
        }
        NotifyTableChanged();
        iter->second = cur_table_info;
        break;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::MakeSnapshotNS(RpcController* controller,
        const MakeSnapshotNSRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", request->name().c_str());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (request->offset() > 0) {
        if (iter->second->storage_mode() != common::kMemory) {
            PDLOG(WARNING, "table[%s] is not memory table, can't do snapshot with end offset", request->name().c_str());
        } else {
            thread_pool_.AddTask(boost::bind(&NameServerImpl::MakeTablePartitionSnapshot, this, request->pid(), request->offset(), iter->second));
        }
        response->set_code(0);
        return;
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    request->SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kMakeSnapshotOP, value, op_data,
                    request->name(), request->pid()) < 0) {
        response->set_code(304);
        response->set_msg("set zk failed");
        PDLOG(WARNING, "create makesnapshot op data error. name[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        return;
    }
    if (CreateMakeSnapshotOPTask(op_data) < 0) {
        response->set_code(305);
        response->set_msg("create op failed");
        PDLOG(WARNING, "create makesnapshot op task failed. name[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        return;
    }
    if (AddOPData(op_data) < 0) {
        response->set_code(306);
        response->set_msg("add op data failed");
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "add makesnapshot op ok. op_id[%lu] name[%s] pid[%u]",
                 op_data->op_info_.op_id(), request->name().c_str(), request->pid());
}

int NameServerImpl::CheckTableMeta(const TableInfo& table_info) {
    if (table_info.column_desc_v1_size() > 0) {
        std::map<std::string, std::string> column_map;
        for (const auto& column_desc : table_info.column_desc_v1()) {
           if (column_desc.add_ts_idx() && ((column_desc.type() == "float") || (column_desc.type() == "double"))) {
               PDLOG(WARNING, "float or double type column can not be index, column is: %s", column_desc.name().c_str());
               return -1;
           }
           column_map.insert(std::make_pair(column_desc.name(), column_desc.type()));
        }
        if (table_info.column_key_size() > 0) {
            for (const auto &column_key : table_info.column_key()) {
                bool has_iter = false;
                for (const auto &column_name : column_key.col_name()) {
                    has_iter = true;
                    auto iter = column_map.find(column_name);
                    if ((iter != column_map.end() && ((iter->second == "float") || (iter->second == "double")))) {
                        PDLOG(WARNING, "float or double type column can not be index, column is: %s",
                              column_key.index_name().c_str());
                        return -1;
                    }
                }
                if (!has_iter) {
                    auto iter = column_map.find(column_key.index_name());
                    if (iter == column_map.end()) {
                        PDLOG(WARNING, "index must member of columns when column key col name is empty");
                        return -1;
                    }
                    if ((iter->second == "float") || (iter->second == "double")) {
                        PDLOG(WARNING, "float or double column can not be index");
                        return -1;
                    }
                }
            }
        }
    } else if (table_info.column_desc_size() > 0) {
        for (const auto& column_desc : table_info.column_desc()) {
            if (column_desc.add_ts_idx() && ((column_desc.type() == "float") || (column_desc.type() == "double"))) {
                PDLOG(WARNING, "float or double type column can not be index, column is: %s", column_desc.name().c_str());
                return -1;
            }
        }
    }

    return 0;
}

int NameServerImpl::FillColumnKey(TableInfo& table_info) {
    if (table_info.column_desc_v1_size() == 0) {
        return 0;
    } else if (table_info.column_key_size() > 0) {
        for (int idx = 0; idx < table_info.column_key_size(); idx++) {
            if (table_info.column_key(idx).col_name_size() == 0) {
                ::rtidb::common::ColumnKey* column_key = table_info.mutable_column_key(idx);
                column_key->add_col_name(table_info.column_key(idx).index_name());
            }
        }
        return 0;
    }
    std::vector<std::string> ts_vec;
    std::vector<std::string> index_vec;
    for (const auto& column_desc : table_info.column_desc_v1()) {
        if (column_desc.is_ts_col()) {
            ts_vec.push_back(column_desc.name());
        }
        if (column_desc.add_ts_idx()) {
            index_vec.push_back(column_desc.name());
        }
    }
    if (ts_vec.size() > 1) {
        return -1;
    }
    for (const auto& index : index_vec) {
        ::rtidb::common::ColumnKey* column_key = table_info.add_column_key();
        column_key->set_index_name(index);
        if (!ts_vec.empty()) {
            column_key->add_ts_name(ts_vec[0]);
        }
    }
    return 0;
}

int NameServerImpl::SetPartitionInfo(TableInfo& table_info) {
    uint32_t partition_num = FLAGS_partition_num;
    if (table_info.has_partition_num() && table_info.partition_num() > 0) {
        partition_num = table_info.partition_num();
    } else {
        table_info.set_partition_num(partition_num);
    }
    std::vector<std::string> endpoint_vec;
    std::map<std::string, uint64_t> endpoint_pid_bucked;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                endpoint_pid_bucked.insert(std::make_pair(kv.first, 0));
            }
        }
    }
    endpoint_vec.reserve(endpoint_pid_bucked.size());
    uint32_t replica_num = std::min(FLAGS_replica_num, (uint32_t)endpoint_pid_bucked.size());
    if (table_info.has_replica_num() && table_info.replica_num() > 0) {
        replica_num = table_info.replica_num();
    } else {
        table_info.set_replica_num(replica_num);
    }
    if (endpoint_pid_bucked.size() < replica_num) {
        PDLOG(WARNING, "healthy endpoint num[%u] is less than replica_num[%u]",
                        endpoint_pid_bucked.size(), replica_num);
        return -1;
    }
    std::map<std::string, uint64_t> endpoint_leader = endpoint_pid_bucked;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& iter: table_info_) {
            auto table_info = iter.second;
            for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
                for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                    std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                    if (endpoint_pid_bucked.find(endpoint) == endpoint_pid_bucked.end() ||
                        !table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                        continue;
                    }
                    endpoint_pid_bucked[endpoint]++;
                    if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        endpoint_leader[endpoint]++;
                    }
                }
            }
        }
    }
    int index = 0;
    int pos = 0;
    uint64_t min = UINT64_MAX;
    for (const auto& iter: endpoint_pid_bucked) {
        endpoint_vec.push_back(iter.first);
        if (iter.second < min) {
            min = iter.second;
            pos = index;
        }
        index++;
    }
    for (uint32_t pid = 0; pid < partition_num; pid++) {
        TablePartition* table_partition = table_info.add_table_partition();
        table_partition->set_pid(pid);
        uint32_t min_leader_num = UINT32_MAX;
        PartitionMeta* leader_partition_meta = NULL;
        for (uint32_t idx = 0; idx < replica_num; idx++) {
            PartitionMeta* partition_meta = table_partition->add_partition_meta();
            std::string endpoint = endpoint_vec[pos % endpoint_vec.size()];
            partition_meta->set_endpoint(endpoint);
            partition_meta->set_is_leader(false);
            if (endpoint_leader[endpoint] < min_leader_num) {
                min_leader_num = endpoint_leader[endpoint];
                leader_partition_meta = partition_meta;
            }
            pos++;
        }
        if (leader_partition_meta != NULL) {
            leader_partition_meta->set_is_leader(true);
            endpoint_leader[leader_partition_meta->endpoint()]++;
        }
    }
    PDLOG(INFO, "set table partition ok. name[%s] partition_num[%u] replica_num[%u]",
                 table_info.name().c_str(), partition_num, replica_num);
    return 0;
}

int NameServerImpl::CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader, const std::vector<::rtidb::base::ColumnDesc>& columns,
            std::map<uint32_t, std::vector<std::string>>& endpoint_map, uint64_t term) {
    ::rtidb::api::TTLType ttl_type = ::rtidb::api::TTLType::kAbsoluteTime;
    if (!table_info->has_ttl_desc()) {
        if (table_info->ttl_type() == "kLatestTime") {
            ttl_type = ::rtidb::api::TTLType::kLatestTime;
        } else if (table_info->ttl_type() == "kAbsOrLat") {
            ttl_type = ::rtidb::api::TTLType::kAbsOrLat;
        } else if (table_info->ttl_type() == "kAbsAndLat") {
            ttl_type = ::rtidb::api::TTLType::kAbsAndLat;
        } else if (table_info->ttl_type() != "kAbsoluteTime") {
            return -1;
        }
    } else {
        ttl_type = table_info->ttl_desc().ttl_type();
    }
    ::rtidb::api::CompressType compress_type = ::rtidb::api::CompressType::kNoCompress;
    if (table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        compress_type = ::rtidb::api::CompressType::kSnappy;
    }
    ::rtidb::common::StorageMode storage_mode = ::rtidb::common::StorageMode::kMemory;
    if (table_info->storage_mode() == ::rtidb::common::StorageMode::kSSD) {
        storage_mode = ::rtidb::common::StorageMode::kSSD;
    } else if (table_info->storage_mode() == ::rtidb::common::StorageMode::kHDD) {
        storage_mode = ::rtidb::common::StorageMode::kHDD;
    }
    ::rtidb::api::TableMeta table_meta;
    for (uint32_t i = 0; i < columns.size(); i++) {
        if (columns[i].add_ts_idx) {
            table_meta.add_dimensions(columns[i].name);
        }
    }
    std::string schema;
    ::rtidb::base::SchemaCodec codec;
    bool codec_ok = codec.Encode(columns, schema);
    if (!codec_ok) {
        return false;
    }

    table_meta.set_name(table_info->name());
    table_meta.set_tid(table_info->tid());
    table_meta.set_ttl(table_info->ttl());
    table_meta.set_seg_cnt(table_info->seg_cnt());
    table_meta.set_schema(schema);
    table_meta.set_ttl_type(ttl_type);
    table_meta.set_compress_type(compress_type);
    if (table_info->has_ttl_desc()) {
        table_meta.mutable_ttl_desc()->CopyFrom(table_info->ttl_desc());
    }
    table_meta.set_storage_mode(storage_mode);
    if (table_info->has_key_entry_max_height()) {
        table_meta.set_key_entry_max_height(table_info->key_entry_max_height());
    }
    for (int idx = 0; idx < table_info->column_desc_v1_size(); idx++) {
        ::rtidb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
        column_desc->CopyFrom(table_info->column_desc_v1(idx));
    }
    for (int idx = 0; idx < table_info->column_key_size(); idx++) {
        ::rtidb::common::ColumnKey* column_key = table_meta.add_column_key();
        column_key->CopyFrom(table_info->column_key(idx));
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        table_meta.set_pid(pid);
        table_meta.clear_replicas();
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader() != is_leader) {
                continue;
            }
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            std::shared_ptr<TabletInfo> tablet_ptr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto iter = tablets_.find(endpoint);
                // check tablet if exist
                if (iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    return -1;
                }
                tablet_ptr = iter->second;
                // check tablet healthy
                if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    return -1;
                }
            }
            if (is_leader) {
                ::rtidb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
                ::rtidb::nameserver::TermPair* term_pair = table_partition->add_term_offset();
                term_pair->set_term(term);
                term_pair->set_offset(0);
                table_meta.set_mode(::rtidb::api::TableMode::kTableLeader);
                table_meta.set_term(term);
                for (const auto& endpoint : endpoint_map[pid]) {
                    table_meta.add_replicas(endpoint);
                }
            } else {
                if (endpoint_map.find(pid) == endpoint_map.end()) {
                    endpoint_map.insert(std::make_pair(pid, std::vector<std::string>()));
                }
                endpoint_map[pid].push_back(endpoint);
                table_meta.set_mode(::rtidb::api::TableMode::kTableFollower);
            }
            if (!tablet_ptr->client_->CreateTable(table_meta)) {
                PDLOG(WARNING, "create table failed. tid[%u] pid[%u] endpoint[%s]", 
                        table_info->tid(), pid, endpoint.c_str());
                return -1;

            }
            PDLOG(INFO, "create table success. tid[%u] pid[%u] endpoint[%s] idx[%d]", 
                        table_info->tid(), pid, endpoint.c_str(), idx);
        }
    }
    return 0;
}

int NameServerImpl::DropTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info) {
    uint32_t tid = table_info->tid();
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            std::shared_ptr<TabletInfo> tablet_ptr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto iter = tablets_.find(endpoint);
                // check tablet if exist
                if (iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    continue;
                }
                tablet_ptr = iter->second;
                // check tablet healthy
                if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    continue;
                }
            }
            if (!tablet_ptr->client_->DropTable(tid, pid)) {
                PDLOG(WARNING, "drop table failed. tid[%u] pid[%u] endpoint[%s]",
                                tid, pid, endpoint.c_str());
            }
            PDLOG(INFO, "drop table success. tid[%u] pid[%u] endpoint[%s]",
                        tid, pid, endpoint.c_str());
        }
    }
    return 0;
}

void NameServerImpl::ConfSet(RpcController* controller,
        const ConfSetRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string key = request->conf().key();
    std::string value = request->conf().value();
    if (key.empty() || value.empty()) {
        response->set_code(307);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "key[%s] value[%s]", key.c_str(), value.c_str());
        return;
    }
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value != "true" && value != "false") {
        response->set_code(307);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "invalid value[%s]", request->conf().value().c_str());
        return;
    }
    if (key == "auto_failover") {
        if (!zk_client_->SetNodeValue(zk_auto_failover_node_, value)) {
            PDLOG(WARNING, "set auto_failover_node failed!");
            response->set_code(304);
            response->set_msg("set zk failed");
            return;
        }
        if (value == "true") {
            auto_failover_.store(true, std::memory_order_release);
        } else {
            auto_failover_.store(false, std::memory_order_release);
        }
    } else {
        response->set_code(307);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "unsupport set key[%s]", key.c_str());
        return;
    }
    PDLOG(INFO, "config set ok. key[%s] value[%s]", key.c_str(), value.c_str());
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::ConfGet(RpcController* controller,
        const ConfGetRequest* request,
        ConfGetResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    ::rtidb::nameserver::Pair* conf = response->add_conf();
    conf->set_key("auto_failover");
    auto_failover_.load(std::memory_order_acquire) ? conf->set_value("true") : conf->set_value("false");

    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::ChangeLeader(RpcController* controller,
        const ChangeLeaderRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::string name = request->name();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (pid > (uint32_t)iter->second->table_partition_size() - 1) {
        PDLOG(WARNING, "pid[%u] is not exist, table[%s]", pid, name.c_str());
        response->set_code(308);
        response->set_msg("pid is not exist");
        return;
    }
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        if (iter->second->table_partition(idx).partition_meta_size() == 1) {
            PDLOG(WARNING, "table[%s] pid[%u] has no followers, cannot change leader",
                        name.c_str(), iter->second->table_partition(idx).pid());
            response->set_code(134);
            response->set_msg("no follower");
            return;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                if (!iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    follower_endpoint.push_back(iter->second->table_partition(idx).partition_meta(meta_idx).endpoint());
                } else if (!request->has_candidate_leader()) {
                    PDLOG(WARNING, "leader is alive, cannot change leader. table[%s] pid[%u]",
                                    name.c_str(), pid);
                    response->set_code(309);
                    response->set_msg("leader is alive");
                    return;
                }
            }
        }
        break;
    }
    if (follower_endpoint.empty()) {
        response->set_code(310);
        response->set_msg("no alive follower");
        PDLOG(WARNING, "no alive follower. table[%s] pid[%u]", name.c_str(), pid);
        return;
    }
    std::string candidate_leader;
    if (request->has_candidate_leader() && request->candidate_leader() != "auto") {
        candidate_leader = request->candidate_leader();
    }
    if (CreateChangeLeaderOP(name, pid, candidate_leader, false) < 0) {
        response->set_code(305);
        response->set_msg("create op failed");
        PDLOG(WARNING, "change leader failed. name[%s] pid[%u]", name.c_str(), pid);
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::OfflineEndpoint(RpcController* controller,
        const OfflineEndpointRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    uint32_t concurrency = FLAGS_name_server_task_concurrency;
    if (request->has_concurrency()) {
        if (request->concurrency() > FLAGS_name_server_task_max_concurrency) {
            response->set_code(307);
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "concurrency is greater than the max value %u", FLAGS_name_server_task_max_concurrency);
            return;
        } else {
            concurrency = request->concurrency();
        }
    }
    std::string endpoint = request->endpoint();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(endpoint);
        if (iter == tablets_.end()) {
            response->set_code(302);
            response->set_msg("endpoint is not exist");
            PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
            return;
        }
    }
    OfflineEndpointInternal(endpoint, concurrency);
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::OfflineEndpointInternal(const std::string& endpoint, uint32_t concurrency) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid = kv.second->table_partition(idx).pid();
            if (kv.second->table_partition(idx).partition_meta_size() == 1 &&
                    kv.second->table_partition(idx).partition_meta(0).endpoint() == endpoint) {
                PDLOG(INFO, "table[%s] pid[%u] has no followers", kv.first.c_str(), pid);
                CreateUpdatePartitionStatusOP(kv.first, pid, endpoint, true, false, INVALID_PARENT_ID, concurrency);
                continue;
            }
            std::string alive_leader;
            int endpoint_index = -1;
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                const ::rtidb::nameserver::PartitionMeta& partition_meta =
                    kv.second->table_partition(idx).partition_meta(meta_idx);
                if (partition_meta.is_leader() && partition_meta.is_alive()) {
                    alive_leader = partition_meta.endpoint();
                }
                if (partition_meta.endpoint() == endpoint) {
                    endpoint_index = meta_idx;
                }
            }
            if (endpoint_index < 0) {
                continue;
            }
            const ::rtidb::nameserver::PartitionMeta& partition_meta =
                    kv.second->table_partition(idx).partition_meta(endpoint_index);
            if (partition_meta.is_leader() || alive_leader.empty()) {
                // leader partition lost
                if (alive_leader.empty() || alive_leader == endpoint) {
                    PDLOG(INFO, "table[%s] pid[%u] change leader", kv.first.c_str(), pid);
                    CreateChangeLeaderOP(kv.first, pid, "", false, concurrency);
                } else {
                    PDLOG(INFO, "table[%s] pid[%u] need not change leader", kv.first.c_str(), pid);
                }
            } else {
                CreateOfflineReplicaOP(kv.first, pid, endpoint, concurrency);
            }
        }
    }
}

void NameServerImpl::RecoverEndpoint(RpcController* controller,
        const RecoverEndpointRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    uint32_t concurrency = FLAGS_name_server_task_concurrency;
    if (request->has_concurrency()) {
        if (request->concurrency() > FLAGS_name_server_task_max_concurrency) {
            response->set_code(307);
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "concurrency is greater than the max value %u", FLAGS_name_server_task_max_concurrency);
            return;
        } else {
            concurrency = request->concurrency();
        }
    }
    std::string endpoint = request->endpoint();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(endpoint);
        if (iter == tablets_.end()) {
            response->set_code(302);
            response->set_msg("endpoint is not exist");
            PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
            return;
        } else if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            response->set_code(303);
            response->set_msg("tablet is not healthy");
            PDLOG(WARNING, "tablet[%s] is not healthy", endpoint.c_str());
            return;
        }
    }
    bool need_restore = false;
    if (request->has_need_restore() && request->need_restore()) {
        need_restore = true;
    }
    RecoverEndpointInternal(endpoint, need_restore, concurrency);
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::RecoverTable(RpcController* controller,
        const RecoverTableRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::string name = request->name();
    std::string endpoint = request->endpoint();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        response->set_code(302);
        response->set_msg("endpoint is not exist");
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        return;
    } else if (it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(303);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", endpoint.c_str());
        return;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    bool has_found = false;
    bool is_leader = false;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    PDLOG(WARNING, "status is alive, need not recover. name[%s] pid[%u] endpoint[%s]",
                                    name.c_str(), pid, endpoint.c_str());
                    response->set_code(311);
                    response->set_msg("table is alive, need not recover");
                    return;
                }
                if (iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    is_leader = true;
                }
                has_found = true;
            }
        }
        break;
    }
    if (!has_found) {
        PDLOG(WARNING, "not found table[%s] pid[%u] in endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        response->set_code(308);
        response->set_msg("pid is not exist");
        return;
    }
    CreateRecoverTableOP(name, pid, endpoint, is_leader,
            FLAGS_check_binlog_sync_progress_delta, FLAGS_name_server_task_concurrency);
    PDLOG(INFO, "recover table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::CancelOP(RpcController* controller,
        const CancelOPRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        auto iter = op_list.begin();
        for ( ; iter != op_list.end(); iter++) {
            if ((*iter)->op_info_.op_id() == request->op_id()) {
                break;
            }
        }
        if (iter != op_list.end()) {
            (*iter)->op_info_.set_task_status(::rtidb::api::kCanceled);
            for (auto& task : (*iter)->task_list_) {
                task->task_info_->set_status(::rtidb::api::kCanceled);
            }
            response->set_code(0);
            response->set_msg("ok");
            PDLOG(INFO, "op[%lu] is canceled! op_type[%s]",
                        request->op_id(), ::rtidb::api::OPType_Name((*iter)->op_info_.op_type()).c_str());
            return;
        }
    }
    response->set_code(312);
    response->set_msg("op status is not kDoing or kInited");
    PDLOG(WARNING, "op[%lu] status is not kDoing or kInited", request->op_id());
    return;
}

void NameServerImpl::ShowOPStatus(RpcController* controller,
        const ShowOPStatusRequest* request,
        ShowOPStatusResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::map<uint64_t, std::shared_ptr<OPData>> op_map;
    std::lock_guard<std::mutex> lock(mu_);
    DeleteDoneOP();
    for (const auto& op_data : done_op_list_) {
        if (request->has_name() && op_data->op_info_.name() != request->name()) {
            continue;
        }
        if (request->has_pid() && op_data->op_info_.pid() != request->pid()) {
            continue;
        }
        op_map.insert(std::make_pair(op_data->op_info_.op_id(), op_data));
    }
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        for (const auto& op_data : op_list) {
            if (request->has_name() && op_data->op_info_.name() != request->name()) {
                continue;
            }
            if (request->has_pid() && op_data->op_info_.pid() != request->pid()) {
                continue;
            }
            op_map.insert(std::make_pair(op_data->op_info_.op_id(), op_data));
        }
    }
    for (const auto& kv : op_map) {
        OPStatus* op_status = response->add_op_status();
        op_status->set_op_id(kv.second->op_info_.op_id());
        op_status->set_op_type(::rtidb::api::OPType_Name(kv.second->op_info_.op_type()));
        op_status->set_name(kv.second->op_info_.name());
        op_status->set_pid(kv.second->op_info_.pid());
        op_status->set_status(::rtidb::api::TaskStatus_Name(kv.second->op_info_.task_status()));
        op_status->set_for_replica_cluster(kv.second->op_info_.for_replica_cluster());
        if (kv.second->task_list_.empty() || kv.second->op_info_.task_status() == ::rtidb::api::kInited) {
            op_status->set_task_type("-");
        } else {
            std::shared_ptr<Task> task = kv.second->task_list_.front();
            op_status->set_task_type(::rtidb::api::TaskType_Name(task->task_info_->task_type()));
        }
        op_status->set_start_time(kv.second->op_info_.start_time());
        op_status->set_end_time(kv.second->op_info_.end_time());
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::ShowTable(RpcController* controller,
        const ShowTableRequest* request,
        ShowTableResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        if (request->has_name() && request->name() != kv.first) {
            continue;
        }
        ::rtidb::nameserver::TableInfo* table_info = response->add_table_info();
        table_info->CopyFrom(*(kv.second));
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::DropTable(RpcController* controller,
        const DropTableRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request->has_zone_info()) {
            response->set_code(501);
            response->set_msg("nameserver is follower");
            PDLOG(WARNING, "cur nameserver is follower");
            return;
        } else if (request->zone_info().zone_name() != zone_info_.zone_name() ||
                request->zone_info().zone_term() != zone_info_.zone_term()) {
            response->set_code(502);
            response->set_msg("zone_info mismathch");
            PDLOG(WARNING, "zone_info mismathch, expect zone name[%s], zone term [%lu], but zone name [%s], zone term [%u]", 
                    zone_info_.zone_name().c_str(), zone_info_.zone_term(),
                    request->zone_info().zone_name().c_str(), request->zone_info().zone_term());
            return;
        }
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(request->name());
        if (iter == table_info_.end()) {
            response->set_code(100);
            response->set_msg("table is not exist!");
            PDLOG(WARNING, "table[%s] is not exist!", request->name().c_str());
            return;
        }
        table_info = iter->second;
    }

    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_zone_info() && request->has_task_info() && request->task_info().IsInitialized()) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<uint64_t> rep_cluster_op_id_vec;
            if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kDropTableRemote, task_ptr, rep_cluster_op_id_vec) < 0) {
                response->set_code(504);
                response->set_msg("add task in replica cluster ns failed");
                return;
            }
            PDLOG(INFO, "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] task_status [%s]" , 
                    task_ptr->op_id(), ::rtidb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                    ::rtidb::api::TaskStatus_Name(task_ptr->status()).c_str());
        }
        task_thread_pool_.AddTask(boost::bind(&NameServerImpl::DropTableInternel, this, request->name(), *response, table_info, task_ptr));
        response->set_code(0);
        response->set_msg("ok");
    } else {
        DropTableInternel(request->name(), *response, table_info, task_ptr);
        response->set_code(response->code());
        response->set_msg(response->msg());
    }
}

void NameServerImpl::DropTableInternel(const std::string name, 
        GeneralResponse& response,
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
        std::shared_ptr<::rtidb::api::TaskInfo> task_ptr) {
    std::lock_guard<std::mutex> lock(mu_);
    int code = 0;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            do {
                std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (!table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    PDLOG(WARNING, "table[%s] is not alive. pid[%u] endpoint[%s]", 
                                    name.c_str(), table_info->table_partition(idx).pid(), endpoint.c_str());
                    continue;
                }
                auto tablets_iter = tablets_.find(endpoint);
                // check tablet if exist
                if (tablets_iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    break;
                }
                // check tablet healthy
                if (tablets_iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    continue;
                }
                if (!tablets_iter->second->client_->DropTable(table_info->tid(),
                            table_info->table_partition(idx).pid())){
                    PDLOG(WARNING, "drop table failed. tid[%u] pid[%u] endpoint[%s]", 
                            table_info->tid(), table_info->table_partition(idx).pid(),
                            endpoint.c_str());
                    code = 313; // if drop table failed, return error
                    break;
                }
                PDLOG(INFO, "drop table. tid[%u] pid[%u] endpoint[%s]", 
                                table_info->tid(), table_info->table_partition(idx).pid(),
                                endpoint.c_str());
            } while (0);
        }
    }
    if (!zk_client_->DeleteNode(zk_table_data_path_ + "/" + name)) {
        PDLOG(WARNING, "delete table node[%s/%s] failed!", 
                zk_table_data_path_.c_str(), name.c_str());
        code = 304;
    } else {
        PDLOG(INFO, "delete table node[%s/%s]", zk_table_data_path_.c_str(), name.c_str());
        table_info_.erase(name);
    }
    if (!nsc_.empty()) {
        for (auto kv : nsc_) {
            if (kv.second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                PDLOG(INFO, "cluster[%s] is not Healthy", kv.first.c_str()); 
                continue;
            }
            if(DropTableRemoteOP(name, kv.first, INVALID_PARENT_ID, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
                PDLOG(WARNING, "drop table for replica cluster failed, table_name: %s, alias: %s", name.c_str(), kv.first.c_str());
                code = 505;
                break;
            }
        }
    }
    response.set_code(code);
    code == 0 ?  response.set_msg("ok") : response.set_msg("drop table error");
    if (task_ptr) {
        if (code != 0) {
            task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
        } else {
            task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
        }
    }
    NotifyTableChanged();
}

void NameServerImpl::AddTableField(RpcController* controller,
        const AddTableFieldRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::map<std::string, std::shared_ptr<TabletClient>> tablet_client_map;
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    std::string schema;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(request->name());
        if (iter == table_info_.end()) {
            response->set_code(100);
            response->set_msg("table doesn`t exist!");
            PDLOG(WARNING, "table[%s] is doesn`t exist!", request->name().c_str());
            return;
        }
        table_info = iter->second;
        if (table_info->added_column_desc_size() == MAX_ADD_TABLE_FIELD_COUNT) {
            response->set_code(324);
            response->set_msg("the count of adding field is more than 63");
            PDLOG(WARNING, "the count of adding field is more than 63 in table %s!", request->name().c_str());
            return;
        }
        //judge if field exists in table_info
        std::string col_name = request->column_desc().name();
        if (table_info->column_desc_v1_size() > 0) {
            for (const auto& column : table_info->column_desc_v1()) {
                if (column.name() == col_name) {
                    response->set_code(323);
                    response->set_msg("field name repeated in table_info!");
                    PDLOG(WARNING, "field name[%s] repeated in table_info!", col_name.c_str());
                    return;
                }
            }
        } else {
            for (const auto& column : table_info->column_desc()) {
                if (column.name() == col_name) {
                    response->set_code(323);
                    response->set_msg("field name repeated in table_info!");
                    PDLOG(WARNING, "field name[%s] repeated in table_info!", col_name.c_str());
                    return;
                }
            }
        }
        for (const auto& column : table_info->added_column_desc()) {
            if (column.name() == col_name) {
                response->set_code(323);
                response->set_msg("field name repeated in table_info!");
                PDLOG(WARNING, "field name[%s] repeated in table_info!", col_name.c_str());
                return;
            }
        }
        // 1.update tablet tableMeta
        std::vector<std::string> endpoint_vec;
        for (auto it = table_info->table_partition().begin(); it != table_info->table_partition().end(); it++) {
            for (auto tit = it->partition_meta().begin(); tit != it->partition_meta().end(); tit++) {
                endpoint_vec.push_back(tit->endpoint());
            }
        }
        Tablets::iterator it = tablets_.begin();
        for (; it != tablets_.end(); ++it) {
            std::string endpoint = it->first;
            if (std::find(endpoint_vec.begin(), endpoint_vec.end(), endpoint) == endpoint_vec.end()) {
                continue;
            }
            std::shared_ptr<TabletInfo> tablet_ptr = it->second;
            // check tablet healthy
            if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                response->set_code(303);
                response->set_msg("tablet is not healthy!");
                PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                return;
            }
            tablet_client_map.insert(std::make_pair(endpoint, tablet_ptr->client_));
        }
    }
    //update tableMeta.schema
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (table_info->added_column_desc_size() > 0) {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(*table_info, columns, table_info->added_column_desc_size()) < 0) {
            PDLOG(WARNING, "convert table %s column desc failed", request->name().c_str());
            return;
        }
    } else {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(*table_info, columns) < 0) {
            PDLOG(WARNING, "convert table %s column desc failed", request->name().c_str());
            return;
        }
    }
    ::rtidb::base::ColumnDesc column;
    column.name = request->column_desc().name();
    column.type = rtidb::base::SchemaCodec::ConvertType(request->column_desc().type());
    column.add_ts_idx = false;
    column.is_ts_col = false;
    columns.push_back(column);
    ::rtidb::base::SchemaCodec codec;
    if (!codec.Encode(columns, schema)) {
        PDLOG(WARNING, "Fail to encode schema from columns in table %s!", request->name().c_str()) ;
        return;
    }

    uint32_t tid = table_info->tid();
    std::string msg;
    for (auto it = tablet_client_map.begin(); it != tablet_client_map.end(); it++ ){
        if (!it->second->UpdateTableMetaForAddField(tid, request->column_desc(), schema, msg)) {
            response->set_code(325);
            response->set_msg("fail to update tableMeta for adding field: "+ msg);
            PDLOG(WARNING, "update table_meta on endpoint[%s] for add table field failed!",
                    it->first.c_str());
            return;
        }
        PDLOG(INFO, "update table_meta on endpoint[%s] for add table field succeeded!",
                it->first.c_str());
    }
    //update zk node
    ::rtidb::nameserver::TableInfo table_info_zk;
    table_info_zk.CopyFrom(*table_info);
    ::rtidb::common::ColumnDesc* added_column_desc_zk = table_info_zk.add_added_column_desc();
    added_column_desc_zk->CopyFrom(request->column_desc());
    std::string table_value;
    table_info_zk.SerializeToString(&table_value);
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + table_info_zk.name(), table_value)) {
            response->set_code(304);
            response->set_msg("set zk failed!");
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]",
                    zk_table_data_path_.c_str(), table_info_zk.name().c_str(), table_value.c_str());
            return;
        }
        PDLOG(INFO, "update table node[%s/%s]. value is [%s]",
                zk_table_data_path_.c_str(), table_info_zk.name().c_str(), table_value.c_str());
        // 2.update ns table_info_
        ::rtidb::common::ColumnDesc* added_column_desc = table_info->add_added_column_desc();
        added_column_desc->CopyFrom(request->column_desc());
        NotifyTableChanged();
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::DeleteOPTask(RpcController* controller,
        const ::rtidb::api::DeleteTaskRequest* request,
        ::rtidb::api::GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (int idx = 0; idx < request->op_id_size(); idx++) {
        auto iter = task_map_.find(request->op_id(idx));
        if (iter == task_map_.end()) {
            continue;
        }
        if (!iter->second.empty()) {
            PDLOG(INFO, "delete op task. op_id[%lu] op_type[%s] task_num[%u]",
                    request->op_id(idx),
                    ::rtidb::api::OPType_Name(iter->second.front()->op_type()).c_str(),
                    iter->second.size());
            iter->second.clear();
        }
        task_map_.erase(iter);
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::GetTaskStatus(RpcController* controller,
        const ::rtidb::api::TaskStatusRequest* request,
        ::rtidb::api::TaskStatusResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : task_map_) {
        for (const auto& task_info : kv.second) {
            ::rtidb::api::TaskInfo* task = response->add_task();
            task->CopyFrom(*task_info);
        }
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::LoadTable(RpcController* controller, 
        const LoadTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request->has_zone_info()) {
            response->set_code(501);
            response->set_msg("request has no zono info");
            PDLOG(WARNING, "request has no zono info");
            return;
        } else if (request->zone_info().zone_name() != zone_info_.zone_name() ||
                request->zone_info().zone_term() != zone_info_.zone_term()) {
            response->set_code(502);
            response->set_msg("zone_info mismathch");
            PDLOG(WARNING, "zone_info mismathch, expect zone name[%s], zone term [%lu], but zone name [%s], zone term [%u]", 
                    zone_info_.zone_name().c_str(), zone_info_.zone_term(),
                    request->zone_info().zone_name().c_str(), request->zone_info().zone_term());
            return;
        }
    }
    std::string name = request->name();
    std::string endpoint = request->endpoint();
    uint32_t pid = request->pid();
        
    if (request->has_zone_info() && request->has_task_info() && request->task_info().IsInitialized()) {
        std::lock_guard<std::mutex> lock(mu_);
        uint64_t rep_cluster_op_id = INVALID_PARENT_ID; 
        if(CreateReLoadTableOP(name, pid, endpoint, INVALID_PARENT_ID, FLAGS_name_server_task_concurrency, 
                    request->task_info().op_id(), rep_cluster_op_id) < 0) {
            PDLOG(WARNING, "create load table op failed, table_name: %s, endpoint: %s", name.c_str(), endpoint.c_str());
            response->set_code(305);
            response->set_msg("create op failed");
            return; 
        }
        std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
        std::vector<uint64_t> rep_cluster_op_id_vec = {rep_cluster_op_id};
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kLoadTable, task_ptr, rep_cluster_op_id_vec) < 0) {
            response->set_code(504);
            response->set_msg("add task in replica cluster ns failed");
            return;
        }
        PDLOG(INFO, "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] task_status [%s]" , 
                task_ptr->op_id(), ::rtidb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                ::rtidb::api::TaskStatus_Name(task_ptr->status()).c_str());
        response->set_code(0);
        response->set_msg("ok");
    } else {
        PDLOG(WARNING, "request has no zone_info or task_info!"); 
        response->set_code(504);
        response->set_msg("add task in replica cluster ns failed");
    }
} 

//for multi cluster createtable
void NameServerImpl::CreateTableInfoSimply(RpcController* controller, 
        const CreateTableInfoRequest* request, 
        CreateTableInfoResponse* response, 
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request->has_zone_info()) {
            response->set_code(501);
            response->set_msg("request has no zono info");
            PDLOG(WARNING, "request has no zono info");
            return;
        } else if (request->zone_info().zone_name() != zone_info_.zone_name() ||
                request->zone_info().zone_term() != zone_info_.zone_term()) {
            response->set_code(502);
            response->set_msg("zone_info mismathch");
            PDLOG(WARNING, "zone_info mismathch, expect zone name[%s], zone term [%lu], but zone name [%s], zone term [%u]", 
                    zone_info_.zone_name().c_str(), zone_info_.zone_term(),
                    request->zone_info().zone_name().c_str(), request->zone_info().zone_term());
            return;
        }
    } else {
        response->set_code(506);
        response->set_msg("nameserver is not replica cluster");
        PDLOG(WARNING, "nameserver is not replica cluster");
        return;
    }

    ::rtidb::nameserver::TableInfo* table_info = response->mutable_table_info();
    table_info->CopyFrom(request->table_info());
    uint32_t tablets_size = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                tablets_size++;
            }
        }
    }
    if (table_info->table_partition_size() > 0) {
        int max_replica_num = 0; 
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            int count = 0;
            for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (!table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    continue;
                }
                count++;
            }
            if (max_replica_num < count) {
                max_replica_num = count; 
            }
        }
        table_info->set_replica_num(std::min(tablets_size, (uint32_t)max_replica_num));
        table_info->set_partition_num(table_info->table_partition_size());
        table_info->clear_table_partition();
    } else {
        table_info->set_replica_num(std::min(tablets_size, table_info->replica_num()));
    }
    if (table_info->table_partition_size() > 0) {
        std::set<uint32_t> pid_set;
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            pid_set.insert(table_info->table_partition(idx).pid());
        }
        auto iter = pid_set.rbegin();
        if (*iter != (uint32_t)table_info->table_partition_size() - 1) {
            response->set_code(307);
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "pid is not start with zero and consecutive");
            return;
        }
    } else {
        if (SetPartitionInfo(*table_info) < 0) {
            response->set_code(314);
            response->set_msg("set partition info failed");
            PDLOG(WARNING, "set partition info failed");
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_table_index_node_, std::to_string(table_index_ + 1))) {
            response->set_code(304);
            response->set_msg("set zk failed");
            PDLOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
            return;
        }
        table_index_++;
        table_info->set_tid(table_index_);
    }
    response->set_code(0);
    response->set_msg("ok");
}

//for multi cluster addreplica
void NameServerImpl::CreateTableInfo(RpcController* controller, 
        const CreateTableInfoRequest* request, 
        CreateTableInfoResponse* response, 
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request->has_zone_info()) {
            response->set_code(501);
            response->set_msg("request has no zono info");
            PDLOG(WARNING, "request has no zono info");
            return;
        } else if (request->zone_info().zone_name() != zone_info_.zone_name() ||
                request->zone_info().zone_term() != zone_info_.zone_term()) {
            response->set_code(502);
            response->set_msg("zone_info mismathch");
            PDLOG(WARNING, "zone_info mismathch, expect zone name[%s], zone term [%lu], but zone name [%s], zone term [%u]", 
                    zone_info_.zone_name().c_str(), zone_info_.zone_term(),
                    request->zone_info().zone_name().c_str(), request->zone_info().zone_term());
            return;
        }
    } else {
        response->set_code(506);
        response->set_msg("nameserver is not replica cluster");
        PDLOG(WARNING, "nameserver is not  replica cluster");
        return;
    }

    ::rtidb::nameserver::TableInfo* table_info = response->mutable_table_info();
    table_info->CopyFrom(request->table_info());
    uint32_t tablets_size = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                tablets_size++;
            }
        }
    }
    if (table_info->table_partition_size() > 0) {
        int max_replica_num = 0; 
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            int count = 0;
            for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (!table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    continue;
                }
                count++;
            }
            if (max_replica_num < count) {
                max_replica_num = count; 
            }
        }
        table_info->set_replica_num(std::min(tablets_size, (uint32_t)max_replica_num));
        table_info->set_partition_num(table_info->table_partition_size());
        table_info->clear_table_partition();
    } else {
        table_info->set_replica_num(std::min(tablets_size, table_info->replica_num()));
    }
    if (table_info->table_partition_size() > 0) {
        std::set<uint32_t> pid_set;
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            pid_set.insert(table_info->table_partition(idx).pid());
        }
        auto iter = pid_set.rbegin();
        if (*iter != (uint32_t)table_info->table_partition_size() - 1) {
            response->set_code(307);
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "pid is not start with zero and consecutive");
            return;
        }
    } else {
        if (SetPartitionInfo(*table_info) < 0) {
            response->set_code(314);
            response->set_msg("set partition info failed");
            PDLOG(WARNING, "set partition info failed");
            return;
        }
    }

    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_table_index_node_, std::to_string(table_index_ + 1))) {
            response->set_code(304);
            response->set_msg("set zk failed");
            PDLOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
            return;
        }
        table_index_++;
        table_info->set_tid(table_index_);
        cur_term = term_;
    }
    // response table_info
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        ::rtidb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            table_partition->clear_term_offset();
            ::rtidb::nameserver::TermPair* term_pair = table_partition->add_term_offset();
            term_pair->set_term(cur_term);
            term_pair->set_offset(0);
            break;
        }
    }
    //zk table_info
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info_zk(table_info->New());
    table_info_zk->CopyFrom(*table_info);
    for (int idx = 0; idx < table_info_zk->table_partition_size(); idx++) {
        ::rtidb::nameserver::PartitionMeta leader_partition_meta;
        ::rtidb::nameserver::TablePartition* table_partition = table_info_zk->mutable_table_partition(idx);
        for (int meta_idx = 0; meta_idx < table_info_zk->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_partition->partition_meta(meta_idx).is_leader() &&
                    table_partition->partition_meta(meta_idx).is_alive()) {
                ::rtidb::nameserver::PartitionMeta* partition_meta = table_partition->mutable_partition_meta(meta_idx);
                partition_meta->set_is_alive(false);
                leader_partition_meta = *partition_meta;
                //clear follower partition_meta
                table_partition->clear_partition_meta();
                ::rtidb::nameserver::PartitionMeta* partition_meta_ptr = table_partition->add_partition_meta();
                partition_meta_ptr->CopyFrom(leader_partition_meta);
                break;
            } 
        }
    }
    std::string table_value;
    table_info_zk->SerializeToString(&table_value);
    if (!zk_client_->CreateNode(zk_table_data_path_ + "/" + table_info_zk->name(), table_value)) {
        PDLOG(WARNING, "create table node[%s/%s] failed! value[%s] value_size[%u]", 
                zk_table_data_path_.c_str(), table_info_zk->name().c_str(), table_value.c_str(), table_value.length());
        response->set_code(304);
        response->set_msg("set zk failed");
        return;
    }
    PDLOG(INFO, "create table node[%s/%s] success! value[%s] value_size[%u]", 
            zk_table_data_path_.c_str(), table_info_zk->name().c_str(), table_value.c_str(), table_value.length());
    {
        std::lock_guard<std::mutex> lock(mu_);
        table_info_.insert(std::make_pair(table_info_zk->name(), table_info_zk));
        NotifyTableChanged();
    }

    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request->has_zone_info()) {
            response->set_code(501);
            response->set_msg("nameserver is follower");
            PDLOG(WARNING, "cur nameserver is follower");
            return;
        } else if (request->zone_info().zone_name() != zone_info_.zone_name() ||
                request->zone_info().zone_term() != zone_info_.zone_term()) {
            response->set_code(502);
            response->set_msg("zone_info mismathch");
            PDLOG(WARNING, "zone_info mismathch, expect zone name[%s], zone term [%lu], but zone name [%s], zone term [%u]", 
                    zone_info_.zone_name().c_str(), zone_info_.zone_term(),
                    request->zone_info().zone_name().c_str(), request->zone_info().zone_term());
            return;
        }
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info(request->table_info().New());
    table_info->CopyFrom(request->table_info());
    if (CheckTableMeta(*table_info) < 0) {
        response->set_code(307);
        response->set_msg("check TableMeta failed, index column type can not float or double");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info_.find(table_info->name()) != table_info_.end()) {
            response->set_code(101);
            response->set_msg("table already exists");
            PDLOG(WARNING, "table[%s] already exists", table_info->name().c_str());
            return;
        }
    }
    if (table_info->has_ttl_desc()) {
        if ((table_info->ttl_desc().abs_ttl() > FLAGS_absolute_ttl_max) || (table_info->ttl_desc().lat_ttl() > FLAGS_latest_ttl_max)) {
            response->set_code(307);
            uint32_t max_ttl = table_info->ttl_desc().ttl_type() == ::rtidb::api::TTLType::kAbsoluteTime ? FLAGS_absolute_ttl_max : FLAGS_latest_ttl_max;
            uint64_t ttl = table_info->ttl_desc().abs_ttl() > FLAGS_absolute_ttl_max ? table_info->ttl_desc().abs_ttl() : table_info->ttl_desc().lat_ttl();
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "ttl is greater than conf value. ttl[%lu] ttl_type[%s] max ttl[%u]",
                            ttl, ::rtidb::api::TTLType_Name(table_info->ttl_desc().ttl_type()).c_str(), max_ttl);
            return;
        }
    } else if (table_info->has_ttl()) {
        if ((table_info->ttl_type() == "kAbsoluteTime" && table_info->ttl() > FLAGS_absolute_ttl_max)
                || (table_info->ttl_type() == "kLatestTime" && table_info->ttl() > FLAGS_latest_ttl_max)) {
            response->set_code(307);
            uint32_t max_ttl = table_info->ttl_type() == "kAbsoluteTime" ? FLAGS_absolute_ttl_max : FLAGS_latest_ttl_max;
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "ttl is greater than conf value. ttl[%lu] ttl_type[%s] max ttl[%u]",
                            table_info->ttl(), table_info->ttl_type().c_str(), max_ttl);
            return;
        }
    }
    if (!request->has_zone_info()) { 
        if (FillColumnKey(*table_info) < 0) {
            response->set_code(307);
            response->set_msg("fill column key failed");
            PDLOG(WARNING, "fill column key failed");
            return;
        }
        if (table_info->table_partition_size() > 0) {
            std::set<uint32_t> pid_set;
            for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
                pid_set.insert(table_info->table_partition(idx).pid());
            }
            auto iter = pid_set.rbegin();
            if (*iter != (uint32_t)table_info->table_partition_size() - 1) {
                response->set_code(307);
                response->set_msg("invalid parameter");
                PDLOG(WARNING, "pid is not start with zero and consecutive");
                return;
            }
        } else {
            if (SetPartitionInfo(*table_info) < 0) {
                response->set_code(314);
                response->set_msg("set partition info failed");
                PDLOG(WARNING, "set partition info failed");
                return;
            }
        }
    }
    uint32_t tid = 0;
    if (request->has_zone_info()) {
        tid = table_info->tid();
    }
    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request->has_zone_info()) {
            if (!zk_client_->SetNodeValue(zk_table_index_node_, std::to_string(table_index_ + 1))) {
                response->set_code(304);
                response->set_msg("set zk failed");
                PDLOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
                return;
            }
            table_index_++;
            table_info->set_tid(table_index_);
            tid = table_index_;
        }
        cur_term = term_;
    }
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (::rtidb::base::SchemaCodec::ConvertColumnDesc(*table_info, columns) < 0) {
        response->set_code(315);
        response->set_msg("convert column desc failed");
        PDLOG(WARNING, "convert table column desc failed. name[%s] tid[%u]", 
                table_info->name().c_str(), tid);
        return;
    }

    if (request->has_zone_info() && request->has_task_info() && request->task_info().IsInitialized()) {
        std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<uint64_t> rep_cluster_op_id_vec;
            if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kCreateTableRemote, task_ptr, rep_cluster_op_id_vec) < 0) {
                response->set_code(504);
                response->set_msg("add task in replica cluster ns failed");
                return;
            }
            PDLOG(INFO, "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] task_status [%s]" , 
                    task_ptr->op_id(), ::rtidb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                    ::rtidb::api::TaskStatus_Name(task_ptr->status()).c_str());
        }
        task_thread_pool_.AddTask(boost::bind(&NameServerImpl::CreateTableInternel, this, *response, table_info, columns, cur_term, tid, task_ptr));
        response->set_code(0);
        response->set_msg("ok");
    } else {
        std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
        CreateTableInternel(*response, table_info, columns, cur_term, tid, task_ptr);
        response->set_code(response->code());
        response->set_msg(response->msg());
    }
}

void NameServerImpl::CreateTableInternel(GeneralResponse& response,
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
        const std::vector<::rtidb::base::ColumnDesc>& columns,
        uint64_t cur_term,
        uint32_t tid,
        std::shared_ptr<::rtidb::api::TaskInfo> task_ptr) {
    std::map<uint32_t, std::vector<std::string>> endpoint_map;
    do {
        if (CreateTableOnTablet(table_info, false, columns, endpoint_map, cur_term) < 0 ||
                CreateTableOnTablet(table_info, true, columns, endpoint_map, cur_term) < 0) {
            response.set_code(316);
            response.set_msg("create table failed on tablet");
            PDLOG(WARNING, "create table failed. name[%s] tid[%u]", 
                            table_info->name().c_str(), tid);
            break;
        }
        std::string table_value;
        table_info->SerializeToString(&table_value);
        if (!zk_client_->CreateNode(zk_table_data_path_ + "/" + table_info->name(), table_value)) {
            PDLOG(WARNING, "create table node[%s/%s] failed! value[%s] value_size[%u]",
                            zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str(), table_value.length());
            response.set_code(304);
            response.set_msg("set zk failed");
            break;
        }
        PDLOG(INFO, "create table node[%s/%s] success! value[%s] value_size[%u]",
                      zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str(), table_value.length());
        {
            std::lock_guard<std::mutex> lock(mu_);
            table_info_.insert(std::make_pair(table_info->name(), table_info));
            NotifyTableChanged();
            if (task_ptr) {
                task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(INFO, "set task type success, op_id [%lu] task_tpye [%s] task_status [%s]" , 
                        task_ptr->op_id(), ::rtidb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                        ::rtidb::api::TaskStatus_Name(task_ptr->status()).c_str());
            }
        }
        if (mode_.load(std::memory_order_acquire) == kLEADER) {
            decltype(nsc_) tmp_nsc;
            {
                std::lock_guard<std::mutex> lock(mu_);
                tmp_nsc = nsc_;
            }
            for (const auto& kv : tmp_nsc) {
                if (kv.second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                    PDLOG(INFO, "cluster[%s] is not Healthy", kv.first.c_str()); 
                    continue;
                }
                ::rtidb::nameserver::TableInfo remote_table_info(*table_info);
                std::string msg;
                if (!std::atomic_load_explicit(&kv.second->client_, std::memory_order_relaxed)->CreateRemoteTableInfoSimply(zone_info_, remote_table_info, msg)) {
                    PDLOG(WARNING, "create remote table_info erro, wrong msg is [%s]", msg.c_str()); 
                    return;
                }
                std::lock_guard<std::mutex> lock(mu_);
                if(CreateTableRemoteOP(*table_info, remote_table_info, kv.first, 
                            INVALID_PARENT_ID, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
                    PDLOG(WARNING, "create table for replica cluster failed, table_name: %s, alias: %s", table_info->name().c_str(), kv.first.c_str());
                    response.set_code(503);
                    response.set_msg( "create table for replica cluster failed");
                    break;
                } 
            }
            if (response.code() != 0) {
                break;    
            }
        }
        response.set_code(0);
        response.set_msg("ok");
        return;
    } while (0);
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::DropTableOnTablet, this, table_info));
}

//called by function CheckTableInfo and SyncTable
int NameServerImpl::AddReplicaSimplyRemoteOP(const std::string& name, 
        const std::string& endpoint, 
        uint32_t remote_tid, 
        uint32_t pid) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return -1;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    AddReplicaData data;
    data.set_name(name);
    data.set_pid(pid);
    data.set_endpoint(endpoint);
    data.set_remote_tid(remote_tid);
    std::string value;
    data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kAddReplicaSimplyRemoteOP, value, op_data, 
                name, pid) < 0) {
        PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]",
                name.c_str(), pid);
        return -1;
    }
    if (CreateAddReplicaSimplyRemoteOPTask(op_data) < 0) {
        PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]",
                name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
        PDLOG(WARNING, "add AddReplicaOP data failed. table[%s] pid[%u]",
                name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add AddReplicasSimplyRemoteOP ok. op_id[%lu] table[%s] pid[%u]", 
            op_data->op_info_.op_id(), name.c_str(), pid);
    return 0;
}

int NameServerImpl::CreateAddReplicaSimplyRemoteOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaData add_replica_data;
    if (!add_replica_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse add_replica_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto pos = table_info_.find(add_replica_data.name());
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", add_replica_data.name().c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint32_t pid = add_replica_data.pid();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", add_replica_data.name().c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateAddReplicaRemoteTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kAddReplicaSimplyRemoteOP, tid, add_replica_data.remote_tid(), pid, add_replica_data.endpoint());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]",
                tid, add_replica_data.remote_tid(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddTableInfoTask("", add_replica_data.endpoint(), add_replica_data.name(), add_replica_data.remote_tid(), pid, 
            op_index, ::rtidb::api::OPType::kAddReplicaSimplyRemoteOP);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create AddReplicaSimplyRemoteOP task ok. tid[%u] pid[%u] endpoint[%s]", 
                    tid, pid, add_replica_data.endpoint().c_str());
    return 0;
}

int NameServerImpl::AddReplicaRemoteOP(const std::string& alias,
        const std::string& name, 
        const ::rtidb::nameserver::TablePartition& table_partition,
        uint32_t remote_tid, 
        uint32_t pid) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    AddReplicaData data;
    data.set_alias(alias);
    data.set_name(name);
    data.set_pid(pid);
    data.set_remote_tid(remote_tid);
    ::rtidb::nameserver::TablePartition* table_partition_ptr = data.mutable_table_partition();
    table_partition_ptr->CopyFrom(table_partition);

    std::string value;
    data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kAddReplicaRemoteOP, value, op_data, 
                name, pid) < 0) {
        PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]",
                name.c_str(), pid);
        return -1;
    }
    if (CreateAddReplicaRemoteOPTask(op_data) < 0) {
        PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] ",
                name.c_str(), pid);
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
        PDLOG(WARNING, "add AddReplicaOP data failed. table[%s] pid[%u]",
                name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add AddReplicaRemoteOP ok. op_id[%lu] table[%s] pid[%u]", 
            op_data->op_info_.op_id(), name.c_str(), pid);
    return 0;
}

int NameServerImpl::CreateAddReplicaRemoteOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaData add_replica_data;
    if (!add_replica_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse add_replica_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto pos = table_info_.find(add_replica_data.name());
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", add_replica_data.name().c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint32_t pid = add_replica_data.pid();
    uint32_t remote_tid = add_replica_data.remote_tid();
    std::string name = add_replica_data.name();
    std::string alias = add_replica_data.alias();
    ::rtidb::nameserver::TablePartition table_partition = add_replica_data.table_partition();
    std::string endpoint;
    for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
        if (table_partition.partition_meta(meta_idx).is_leader()) {
            endpoint = table_partition.partition_meta(meta_idx).endpoint();
            break;
        }
    }

    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task  = CreatePauseSnapshotTask(leader_endpoint, op_index, 
            ::rtidb::api::OPType::kAddReplicaRemoteOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
            ::rtidb::api::OPType::kAddReplicaRemoteOP, tid, remote_tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]",
                tid, remote_tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateLoadTableRemoteTask(alias, name, endpoint, pid,  
            op_index, ::rtidb::api::OPType::kAddReplicaRemoteOP);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u]", tid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateAddReplicaRemoteTask(leader_endpoint, op_index, 
            ::rtidb::api::OPType::kAddReplicaRemoteOP, tid, remote_tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]",
                tid, remote_tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateRecoverSnapshotTask(leader_endpoint, op_index, 
            ::rtidb::api::OPType::kAddReplicaRemoteOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task); 
    
    // AddReplicaNSRemote
    std::vector<std::string> endpoint_vec;
    for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
        if (!table_partition.partition_meta(meta_idx).is_leader()) {
            endpoint_vec.push_back(table_partition.partition_meta(meta_idx).endpoint());
        }
    }
    if (!endpoint_vec.empty()) {
        task = CreateAddReplicaNSRemoteTask(alias, name, endpoint_vec, pid, op_index, 
                ::rtidb::api::OPType::kAddReplicaRemoteOP);
        if (!task) {
            PDLOG(WARNING, "create addreplicaNS remote task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]",
                    tid, remote_tid, pid);
            return -1;
        }
        op_data->task_list_.push_back(task);
    }

    task = CreateAddTableInfoTask(alias, endpoint, name, remote_tid, pid, 
            op_index, ::rtidb::api::OPType::kAddReplicaRemoteOP);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    PDLOG(INFO, "create AddReplicaRemoteOP task ok. tid[%u] pid[%u] endpoint[%s]", 
            tid, pid, endpoint.c_str());
    return 0;
}

void NameServerImpl::AddReplicaNS(RpcController* controller,
       const AddReplicaNSRequest* request,
              GeneralResponse* response,
              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::set<uint32_t> pid_group;
    if (request->pid_group_size() > 0) {
        for (int idx = 0; idx < request->pid_group_size(); idx++) {
            pid_group.insert(request->pid_group(idx));
        }
    } else {
        pid_group.insert(request->pid());
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(303);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", request->endpoint().c_str());
        return;
    }
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(100);
        response->set_msg("table is not exist");
        PDLOG(WARNING, "table[%s] is not exist", request->name().c_str());
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    if (*(pid_group.rbegin()) > (uint32_t)table_info->table_partition_size() - 1) {
        response->set_code(307);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "max pid is greater than partition size. table[%s]", request->name().c_str());
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (pid_group.find(table_info->table_partition(idx).pid()) == pid_group.end()) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == request->endpoint()) {
                response->set_code(317);
                char msg[100];
                sprintf(msg, "pid %u is exist in %s", 
                        table_info->table_partition(idx).pid(), request->endpoint().c_str());
                response->set_msg(msg);
                PDLOG(WARNING, "table %s %s", request->name().c_str(), msg);
                return;
            }
        }
    }
    for (auto pid : pid_group) {
        std::shared_ptr<OPData> op_data;
        AddReplicaNSRequest cur_request;
        cur_request.CopyFrom(*request);
        cur_request.set_pid(pid);
        std::string value;
        cur_request.SerializeToString(&value);
        if (CreateOPData(::rtidb::api::OPType::kAddReplicaOP, value, op_data, 
                    request->name(), pid) < 0) {
            PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]",
                    request->name().c_str(), pid);
            response->set_code(304);
            response->set_msg("set zk failed");
            return;
        }
        if (CreateAddReplicaOPTask(op_data) < 0) {
            PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]",
                    request->name().c_str(), pid, request->endpoint().c_str());
            response->set_code(305);
            response->set_msg("create op failed");
            return;
        }
        if (AddOPData(op_data, 1) < 0) {
            response->set_code(306);
            response->set_msg("add op data failed");
            PDLOG(WARNING, "add op data failed. table[%s] pid[%u]",
                    request->name().c_str(), pid);
            return;
        }
        PDLOG(INFO, "add addreplica op ok. op_id[%lu] table[%s] pid[%u]", 
                op_data->op_info_.op_id(), request->name().c_str(), pid);
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::AddReplicaNSFromRemote(RpcController* controller,
       const AddReplicaNSRequest* request,
       GeneralResponse* response,
       Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(501);
            response->set_msg("request has no zono info");
            PDLOG(WARNING, "request has no zono info");
            return;
        } else if (request->zone_info().zone_name() != zone_info_.zone_name() ||
                request->zone_info().zone_term() != zone_info_.zone_term()) {
            response->set_code(502);
            response->set_msg("zone_info mismathch");
            PDLOG(WARNING, "zone_info mismathch, expect zone name[%s], zone term [%lu], but zone name [%s], zone term [%u]", 
                    zone_info_.zone_name().c_str(), zone_info_.zone_term(),
                    request->zone_info().zone_name().c_str(), request->zone_info().zone_term());
            return;
        }
    }
    uint32_t pid = request->pid();
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(303);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", request->endpoint().c_str());
        return;
    }
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(100);
        response->set_msg("table is not exist");
        PDLOG(WARNING, "table[%s] is not exist", request->name().c_str());
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    if (pid > (uint32_t)table_info->table_partition_size() - 1) {
        response->set_code(307);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "max pid is greater than partition size. table[%s]", request->name().c_str());
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (pid == table_info->table_partition(idx).pid()) {
            for (int group_idx = 0; group_idx < request->endpoint_group_size(); group_idx++) {
                for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                    if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == request->endpoint_group(group_idx)) {
                        response->set_code(317);
                        char msg[100];
                        sprintf(msg, "pid %u is exist in %s", 
                                table_info->table_partition(idx).pid(), request->endpoint_group(group_idx).c_str());
                        response->set_msg(msg);
                        PDLOG(WARNING, "table %s %s", request->name().c_str(), msg);
                        return;
                    }
                }
            }
            break;
        }
    }  
    std::vector<uint64_t> rep_cluster_op_id_vec;
    for (int idx = 0; idx < request->endpoint_group_size(); idx++) {
        std::string endpoint = request->endpoint_group(idx);
        std::shared_ptr<OPData> op_data;
        AddReplicaNSRequest cur_request;
        cur_request.CopyFrom(*request);
        cur_request.set_pid(pid);
        cur_request.set_endpoint(endpoint);
        std::string value;
        cur_request.SerializeToString(&value);
        if (CreateOPData(::rtidb::api::OPType::kAddReplicaOP, value, op_data, 
                    request->name(), pid, INVALID_PARENT_ID, request->task_info().op_id()) < 0) {
            PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]",
                    request->name().c_str(), pid);
            response->set_code(304);
            response->set_msg("set zk failed");
            return;
        }
        if (CreateAddReplicaOPTask(op_data) < 0) {
            PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]",
                    request->name().c_str(), pid, endpoint.c_str());
            response->set_code(305);
            response->set_msg("create op failed");
            return;
        }
        if (AddOPData(op_data, 1) < 0) {
            response->set_code(306);
            response->set_msg("add op data failed");
            PDLOG(WARNING, "add op data failed. table[%s] pid[%u]",
                    request->name().c_str(), pid);
            return;
        }
        rep_cluster_op_id_vec.push_back(op_data->op_info_.op_id());//for multi cluster 
        PDLOG(INFO, "add addreplica op ok. op_id[%lu] table[%s] pid[%u]", 
                op_data->op_info_.op_id(), request->name().c_str(), pid);
    }
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kAddReplicaNSRemote, task_ptr, rep_cluster_op_id_vec) < 0) {
        response->set_code(504);
        response->set_msg("add task in replica cluster ns failed");
        return;
    }
    PDLOG(INFO, "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] task_status [%s]" , 
            task_ptr->op_id(), ::rtidb::api::TaskType_Name(task_ptr->task_type()).c_str(),
            ::rtidb::api::TaskStatus_Name(task_ptr->status()).c_str());
    response->set_code(0);
    response->set_msg("ok");
}

int NameServerImpl::CreateAddReplicaOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto it = tablets_.find(request.endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", request.endpoint().c_str());
        return -1;
    }
    auto pos = table_info_.find(request.name());
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", request.name().c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint32_t pid = request.pid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(request.endpoint(), op_index,
                ::rtidb::api::OPType::kAddReplicaOP, request.name(),
                tid, pid, ttl, seg_cnt, false, pos->second->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddTableInfoTask(request.name(), pid, request.endpoint(),
                op_index, ::rtidb::api::OPType::kAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateCheckBinlogSyncProgressTask(op_index, ::rtidb::api::OPType::kAddReplicaOP,
                request.name(), pid, request.endpoint(), FLAGS_check_binlog_sync_progress_delta);
    if (!task) {
        PDLOG(WARNING, "create checkbinlogsyncprogress task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(request.name(), pid, request.endpoint(), false, true,
                op_index, ::rtidb::api::OPType::kAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        request.name().c_str(), pid, request.endpoint().c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create AddReplicaOP task ok. tid[%u] pid[%u] endpoint[%s]",
                    tid, pid, request.endpoint().c_str());
    return 0;
}

void NameServerImpl::Migrate(RpcController* controller,
       const MigrateRequest* request,
       GeneralResponse* response,
       Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto pos = tablets_.find(request->src_endpoint());
    if (pos == tablets_.end() || pos->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(318);
        response->set_msg("src_endpoint is not exist or not healthy");
        PDLOG(WARNING, "src_endpoint[%s] is not exist or not healthy", request->src_endpoint().c_str());
        return;
    }
    pos = tablets_.find(request->des_endpoint());
    if (pos == tablets_.end() || pos->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(319);
        response->set_msg("des_endpoint is not exist or not healthy");
        PDLOG(WARNING, "des_endpoint[%s] is not exist or not healthy", request->des_endpoint().c_str());
        return;
    }
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(100);
        response->set_msg("table is not exist");
        PDLOG(WARNING, "table[%s] is not exist", request->name().c_str());
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    char error_msg[1024];
    bool has_error = false;
    for (int i = 0; i < request->pid_size(); i++) {
        uint32_t pid = request->pid(i);
        std::string leader_endpoint;
        bool has_found_src_endpoint = false;
        bool has_found_des_endpoint = false;
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            if (table_info->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                    if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        leader_endpoint = endpoint;
                    }
                    if (request->src_endpoint() == endpoint) {
                        has_found_src_endpoint = true;
                    } else if (request->des_endpoint() == endpoint) {
                        has_found_des_endpoint = true;
                    }
                }
            }
            break;
        }
        if (leader_endpoint.empty()) {
            sprintf(error_msg, "leader endpoint is empty. name[%s] pid[%u]",
                            request->name().c_str(), pid);
            has_error = true;
            break;
        }
        if (leader_endpoint == request->src_endpoint()) {
            sprintf(error_msg, "cannot migrate leader. name[%s] pid[%u]",
                            request->name().c_str(), pid);
            has_error = true;
            break;
        }
        auto it = tablets_.find(leader_endpoint);
        if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            sprintf(error_msg, "leader[%s] is offline. name[%s] pid[%u]",
                                leader_endpoint.c_str(), request->name().c_str(), pid);
            has_error = true;
            break;
        }
        if (!has_found_src_endpoint) {
            sprintf(error_msg, "src_endpoint[%s] has not partition[%u]. name[%s]",
                            request->src_endpoint().c_str(), pid, request->name().c_str());
            has_error = true;
            break;
        }
        if (has_found_des_endpoint) {
            sprintf(error_msg, "partition[%u] is already in des_endpoint[%s]. name[%s]",
                                pid, request->des_endpoint().c_str(), request->name().c_str());
            has_error = true;
            break;
        }
    }
    if (has_error) {
        response->set_code(320);
        response->set_msg(error_msg);
        PDLOG(WARNING, "%s", error_msg);
        return;
    }
    for (int i = 0; i < request->pid_size(); i++) {
        uint32_t pid = request->pid(i);
        CreateMigrateOP(request->src_endpoint(), request->name(), pid, request->des_endpoint());
    }
    response->set_code(0);
    response->set_msg("ok");
}

int NameServerImpl::CreateMigrateOP(const std::string& src_endpoint, const std::string& name,
            uint32_t pid, const std::string& des_endpoint) {
    std::shared_ptr<OPData> op_data;
    MigrateInfo migrate_info;
    migrate_info.set_src_endpoint(src_endpoint);
    migrate_info.set_des_endpoint(des_endpoint);
    std::string value;
    migrate_info.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kMigrateOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create migrate op data failed. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    if (CreateMigrateTask(op_data) < 0) {
        PDLOG(WARNING, "create migrate op task failed. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add migrate op data failed. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add migrate op ok. op_id[%lu] src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                 op_data->op_info_.op_id(), src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateMigrateTask(std::shared_ptr<OPData> op_data) {
    MigrateInfo migrate_info;
    if (!migrate_info.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse migrate_info failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string src_endpoint = migrate_info.src_endpoint();
    std::string des_endpoint = migrate_info.des_endpoint();
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "get table info failed! name[%s]", name.c_str());
        return -1;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    auto it = tablets_.find(leader_endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "leader[%s] is not online", leader_endpoint.c_str());
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kMigrateOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u] endpoint[%s]",
                        tid, pid, leader_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, tid, pid, des_endpoint.c_str());
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]",
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kMigrateOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]",
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(des_endpoint, op_index, ::rtidb::api::OPType::kMigrateOP,
                 name, tid, pid, table_info->ttl(), table_info->seg_cnt(), false, table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u] endpoint[%s]",
                        tid, pid, des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kMigrateOP, tid, pid, des_endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]",
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddTableInfoTask(name, pid, des_endpoint,
                op_index, ::rtidb::api::OPType::kMigrateOP);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]",
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateCheckBinlogSyncProgressTask(op_index,
                ::rtidb::api::OPType::kMigrateOP, name, pid, des_endpoint, FLAGS_check_binlog_sync_progress_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDelReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kMigrateOP, tid, pid, src_endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. tid[%u] pid[%u] leader[%s] follower[%s]",
                        tid, pid, leader_endpoint.c_str(), src_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdateTableInfoTask(src_endpoint, name, pid, des_endpoint,
                op_index, ::rtidb::api::OPType::kMigrateOP);
    if (!task) {
        PDLOG(WARNING, "create update table info task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]",
                        tid, pid, src_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(src_endpoint, op_index,
                ::rtidb::api::OPType::kMigrateOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u] endpoint[%s]",
                        tid, pid, src_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create migrate op task ok. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                 src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
    return 0;
}

void NameServerImpl::DelReplicaNS(RpcController* controller,
       const DelReplicaNSRequest* request,
       GeneralResponse* response,
       Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::set<uint32_t> pid_group;
    if (request->pid_group_size() > 0) {
        for (int idx = 0; idx < request->pid_group_size(); idx++) {
            pid_group.insert(request->pid_group(idx));
        }
    } else {
        pid_group.insert(request->pid());
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(100);
        response->set_msg("table is not exist");
        PDLOG(WARNING, "table[%s] is not exist", request->name().c_str());
        return;
    }
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(303);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", request->endpoint().c_str());
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    if (*(pid_group.rbegin()) > (uint32_t) table_info->table_partition_size() - 1) {
        response->set_code(307);
        response->set_msg("max pid is greater than partition size");
        PDLOG(WARNING, "max pid is greater than partition size. table[%s]", request->name().c_str());
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (pid_group.find(table_info->table_partition(idx).pid()) == pid_group.end()) {
            continue;
        }
        bool pid_in_endpoint = false;
        bool is_leader = false;
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == request->endpoint()) {
                pid_in_endpoint = true;
                if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    is_leader = true;
                }
                break;
            }
        }
        if (!pid_in_endpoint) {
            char msg[100];
            response->set_code(308);
            sprintf(msg, "pid %u is not in %s",
                         table_info->table_partition(idx).pid(), request->endpoint().c_str());
            response->set_msg(msg);
            PDLOG(WARNING, "table %s %s", request->name().c_str(), msg);
            return;
        } else if (is_leader) {
            char msg[100];
            response->set_code(102);
            sprintf(msg, "can not del leader. pid %u endpoint %s" ,
                         table_info->table_partition(idx).pid(), request->endpoint().c_str());
            response->set_msg(msg);
            PDLOG(WARNING, "table %s %s", request->name().c_str(), msg);
            return;
        }

    }
    for (auto pid : pid_group) {
        if (CreateDelReplicaOP(request->name(), pid, request->endpoint()) < 0) {
            response->set_code(-1);
            response->set_msg("create op failed");
            return;
        }
    }
    response->set_code(0);
    response->set_msg("ok");
}

int NameServerImpl::DelReplicaRemoteOP(const std::string& endpoint,
        const std::string name,
        uint32_t pid) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kDelReplicaRemoteOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create op data error. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateDelReplicaRemoteOPTask(op_data) < 0) {
        PDLOG(WARNING, "create delreplica op task failed. name[%s] pid[%u] endpoint[%s]", 
                name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add delreplica op. op_id[%lu] table[%s] pid[%u] endpoint[%s]", 
            op_index_, name.c_str(), pid, endpoint.c_str());
    return 0;

}

int NameServerImpl::AddOPTask(const ::rtidb::api::TaskInfo& task_info, ::rtidb::api::TaskType task_type,
        std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr, std::vector<uint64_t> rep_cluster_op_id_vec) {
    if (FindTask(task_info.op_id(), task_info.task_type())) {
        PDLOG(WARNING, "task is running. op_id[%lu] op_type[%s] task_type[%s]",
                task_info.op_id(),
                ::rtidb::api::OPType_Name(task_info.op_type()).c_str(),
                ::rtidb::api::TaskType_Name(task_info.task_type()).c_str());
        return -1;
    }
    task_ptr.reset(task_info.New());
    task_ptr->CopyFrom(task_info);
    task_ptr->set_status(::rtidb::api::TaskStatus::kDoing);
    for (auto op_id : rep_cluster_op_id_vec) {
        task_ptr->add_rep_cluster_op_id(op_id);
    }
    auto iter = task_map_.find(task_info.op_id());
    if (iter == task_map_.end()) {
        task_map_.insert(std::make_pair(task_info.op_id(),
                    std::list<std::shared_ptr<::rtidb::api::TaskInfo>>()));
    }
    task_map_[task_info.op_id()].push_back(task_ptr);
    if (task_info.task_type() != task_type) {
        PDLOG(WARNING, "task type is not match. type is[%s]",
                ::rtidb::api::TaskType_Name(task_info.task_type()).c_str());
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
        return -1;
    }
    return 0;
}

std::shared_ptr<::rtidb::api::TaskInfo> NameServerImpl::FindTask(uint64_t op_id, 
        ::rtidb::api::TaskType task_type) {
    auto iter = task_map_.find(op_id);
    if (iter == task_map_.end()) {
        return std::shared_ptr<::rtidb::api::TaskInfo>();
    }
    for (auto& task : iter->second) {
        if (task->op_id() == op_id && task->task_type() == task_type) {
            return task;
        }
    }
    return std::shared_ptr<::rtidb::api::TaskInfo>();
}

int NameServerImpl::CreateOPData(::rtidb::api::OPType op_type, const std::string& value, 
        std::shared_ptr<OPData>& op_data, const std::string& name, uint32_t pid, 
        uint64_t parent_id, uint64_t remote_op_id) {
    if (!zk_client_->SetNodeValue(zk_op_index_node_, std::to_string(op_index_ + 1))) {
        PDLOG(WARNING, "set op index node failed! op_index[%lu]", op_index_);
        return -1;
    }
    op_index_++;
    op_data = std::make_shared<OPData>();
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(op_type);
    op_data->op_info_.set_task_index(0);
    op_data->op_info_.set_data(value);
    op_data->op_info_.set_task_status(::rtidb::api::kInited);
    op_data->op_info_.set_name(name);
    op_data->op_info_.set_pid(pid);
    op_data->op_info_.set_parent_id(parent_id);
    if (remote_op_id != INVALID_PARENT_ID) {
        op_data->op_info_.set_remote_op_id(remote_op_id);
    }
    return 0;
}

int NameServerImpl::AddOPData(const std::shared_ptr<OPData>& op_data, 
        uint32_t concurrency) {
    uint32_t idx = 0;
    if (op_data->op_info_.for_replica_cluster() == 1) {
        idx = FLAGS_name_server_task_max_concurrency + (rand_.Next() % concurrency); 
        PDLOG(INFO, "current task is for replica cluster, op_index [%lu] op_type[%s]", 
                op_data->op_info_.op_id(), ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
    } else {
        idx = op_data->op_info_.pid() % task_vec_.size();
        if (concurrency < task_vec_.size() && concurrency > 0) {
            idx = op_data->op_info_.pid() % concurrency;
        }
    }
    op_data->op_info_.set_vec_idx(idx);
    std::string value;
    op_data->op_info_.SerializeToString(&value);
    std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
    if (!zk_client_->CreateNode(node, value)) {
        PDLOG(WARNING, "create op node[%s] failed. op_index[%lu] op_type[%s]",
                        node.c_str(), op_data->op_info_.op_id(),
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
        return -1;
    }
    uint64_t parent_id = op_data->op_info_.parent_id();
    if (parent_id != INVALID_PARENT_ID) {
        std::list<std::shared_ptr<OPData>>::iterator iter = task_vec_[idx].begin();
        for ( ; iter != task_vec_[idx].end(); iter++) {
            if ((*iter)->op_info_.op_id() == parent_id) {
                break;
            }
        }
        if (iter != task_vec_[idx].end()) {
            iter++;
            task_vec_[idx].insert(iter, op_data);
        } else {
            PDLOG(WARNING, "not found parent_id[%lu] with index[%u]. add op[%lu] failed, op_type[%s]",
                            parent_id, idx, op_data->op_info_.op_id(),
                            ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
            return -1;
        }
    } else {
        task_vec_[idx].push_back(op_data);
    }
    DeleteDoneOP();
    cv_.notify_one();
    return 0;
}

void NameServerImpl::DeleteDoneOP() {
    if (done_op_list_.empty()) {
        return;
    }
    while (done_op_list_.size() > (uint32_t)FLAGS_max_op_num) {
        std::shared_ptr<OPData> op_data = done_op_list_.front();
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kFailed) {
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
            if (zk_client_->DeleteNode(node)) {
                PDLOG(INFO, "delete zk op node[%s] success.", node.c_str());
                op_data->task_list_.clear();
            } else {
                PDLOG(WARNING, "delete zk op_node failed. op_id[%lu] node[%s]",
                                op_data->op_info_.op_id(), node.c_str());
                break;
            }
        }
        PDLOG(INFO, "done_op_list size[%u] is greater than the max_op_num[%u], delete op[%lu]",
                    done_op_list_.size(), (uint32_t)FLAGS_max_op_num, op_data->op_info_.op_id());
        done_op_list_.pop_front();
    }
}

void NameServerImpl::SchedMakeSnapshot() {
    if (!running_.load(std::memory_order_acquire)) {
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
        return;
    }
    int now_hour = ::rtidb::base::GetNowHour();
    if (now_hour != FLAGS_make_snapshot_time) {
        task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
        return;
    }
    std::map<std::string, std::shared_ptr<TabletInfo>> tablet_ptr_map;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>> table_infos;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::NsClient>> ns_client;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info_.size() < 1) {
            task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
            return;
        }
        for (const auto& kv : tablets_) {
            if (kv.second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                continue;
            }
            tablet_ptr_map.insert(std::make_pair(kv.first, kv.second));
        }
        for (auto iter = nsc_.begin(); iter != nsc_.end(); ++iter) {
            if (iter->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                PDLOG(INFO, "cluster[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            ns_client.insert(std::make_pair(iter->first, std::atomic_load_explicit(&iter->second->client_, std::memory_order_relaxed)));
        }
        for (auto iter = table_info_.begin(); iter != table_info_.end(); ++iter) {
            if (iter->second->storage_mode() != common::kMemory) {
                continue;
            }
            table_infos.insert(std::make_pair(iter->first, iter->second));
        }
    }
    std::map<std::string, std::map<uint32_t, uint64_t>> table_part_offset;
    {
        std::vector<TableInfo> tables;
        std::vector<std::string> delete_map;
        std::string msg;
        for (const auto& ns : ns_client) {
            if(!ns.second->ShowTable("", tables, msg)) {
                delete_map.push_back(ns.first);
                continue;
            }
            for (const auto& table : tables) {
                if (table.storage_mode() != common::kMemory) {
                    continue;
                }
                auto table_iter = table_part_offset.find(table.name());
                if (table_iter == table_part_offset.end()) {
                    std::map<uint32_t, uint64_t> part_offset;
                    auto result = table_part_offset.insert(std::make_pair(table.name(), part_offset));
                    table_iter = result.first;
                }
                for (const auto& part : table.table_partition()) {
                    for (const auto& part_meta : part.partition_meta()) {
                        if (!part_meta.is_alive()) {
                            continue;
                        }
                        auto part_iter = table_iter->second.find(part.pid());
                        if (part_iter != table_iter->second.end()) {
                            if (part_meta.offset() < part_iter->second) {
                                part_iter->second = part_meta.offset();
                            }
                        } else {
                            table_iter->second.insert(std::make_pair(part.pid(), part_meta.offset()));
                        }
                    }
                }
            }
            tables.clear();
        }
        for (const auto& alias : delete_map) {
            ns_client.erase(alias);
        }
        for (const auto& table : table_infos) {
            auto table_iter = table_part_offset.find(table.second->name());
            if (table_iter == table_part_offset.end()) {
                std::map<uint32_t, uint64_t> part_offset;
                auto result = table_part_offset.insert(std::make_pair(table.second->name(), part_offset));
                table_iter = result.first;
            }
            for (const auto& part : table.second->table_partition()) {
                for (const auto& part_meta : part.partition_meta()) {
                    if (!part_meta.is_alive()) {
                        continue;
                    }
                    auto part_iter = table_iter->second.find(part.pid());
                    if (part_iter != table_iter->second.end()) {
                        if (part_meta.offset() < part_iter->second) {
                            part_iter->second = part_meta.offset();
                        }
                    } else {
                        table_iter->second.insert(std::make_pair(part.pid(), part_meta.offset()));
                    }
                }
            }
        }
    }
    PDLOG(INFO, "start make snapshot");
    for (const auto& table : table_infos) {
        if (table.second->storage_mode() != common::kMemory) {
            continue;
        }
        auto table_iter = table_part_offset.find(table.second->name());
        if (table_iter == table_part_offset.end()) {
            continue;
        }
        for (const auto& part : table.second->table_partition()) {
            auto part_iter = table_iter->second.find(part.pid());
            if (part_iter == table_iter->second.end()) {
                continue;
            }
            for (const auto& part_meta : part.partition_meta()) {
                if (part_meta.is_alive()) {
                    auto client_iter = tablet_ptr_map.find(part_meta.endpoint());
                    if (client_iter != tablet_ptr_map.end()) {
                        thread_pool_.AddTask(boost::bind(&TabletClient::MakeSnapshot, client_iter->second->client_, table.second->tid(), part.pid(), part_iter->second, std::shared_ptr<rtidb::api::TaskInfo>()));
                    }
                }
            }
            std::string msg;
            for (const auto& ns : ns_client) {
                ns.second->MakeSnapshot(table.second->name(), part.pid(), part_iter->second, msg);
            }
        }
    }
    PDLOG(INFO, "make snapshot finished");
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
    }

}

void NameServerImpl::UpdateTableStatus() {
    std::map<std::string, std::shared_ptr<TabletInfo>> tablet_ptr_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                continue;
            }
            tablet_ptr_map.insert(std::make_pair(kv.first, kv.second));
        }
    }
    std::unordered_map<std::string, ::rtidb::api::TableStatus> pos_response;
    pos_response.reserve(16);
    for (const auto& kv : tablet_ptr_map) {
        ::rtidb::api::GetTableStatusResponse tablet_status_response;
        if (!kv.second->client_->GetTableStatus(tablet_status_response)) {
            PDLOG(WARNING, "get table status failed! endpoint[%s]", kv.first.c_str());
            continue;
        }
        for (int pos = 0; pos < tablet_status_response.all_table_status_size(); pos++) {
            std::string key = std::to_string(tablet_status_response.all_table_status(pos).tid()) + "_" +
                              std::to_string(tablet_status_response.all_table_status(pos).pid()) + "_" +
                              kv.first;
            pos_response.insert(std::make_pair(key, tablet_status_response.all_table_status(pos)));
        }
    }
    if (pos_response.empty()) {
        PDLOG(DEBUG, "pos_response is empty");
    } else {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : table_info_) {
            uint32_t tid = kv.second->tid();
            std::string first_index_col;
            for (int idx = 0; idx < kv.second->column_desc_size(); idx++) {
                if (kv.second->column_desc(idx).add_ts_idx()) {
                    first_index_col = kv.second->column_desc(idx).name();
                    break;
                }
            }
            for (int idx = 0; idx < kv.second->column_desc_v1_size(); idx++) {
                if (kv.second->column_desc_v1(idx).add_ts_idx()) {
                    first_index_col = kv.second->column_desc_v1(idx).name();
                    break;
                }
            }
            if(kv.second->column_key_size() > 0) {
                first_index_col = kv.second->column_key(0).index_name();
            }
            for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
                uint32_t pid = kv.second->table_partition(idx).pid();
                ::rtidb::nameserver::TablePartition* table_partition =
                    kv.second->mutable_table_partition(idx);
                ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta_field =
                    table_partition->mutable_partition_meta();
                for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                    std::string endpoint = kv.second->table_partition(idx).partition_meta(meta_idx).endpoint();
                    bool tablet_has_partition = false;
                    ::rtidb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(meta_idx);
                    std::string pos_key = std::to_string(tid) + "_" + std::to_string(pid) + "_" + endpoint;
                    auto pos_response_iter = pos_response.find(pos_key);
                    if (pos_response_iter != pos_response.end()) {
                        const ::rtidb::api::TableStatus& table_status = pos_response_iter->second;
                        partition_meta->set_offset(table_status.offset());
                        partition_meta->set_record_byte_size(table_status.record_byte_size() +
                                    table_status.record_idx_byte_size());
                        uint64_t record_cnt = table_status.record_cnt();
                        if (!first_index_col.empty()) {
                            for (int pos = 0; pos < table_status.ts_idx_status_size(); pos++) {
                                if (table_status.ts_idx_status(pos).idx_name() == first_index_col) {
                                    record_cnt = 0;
                                    for (int seg_idx = 0; seg_idx < table_status.ts_idx_status(pos).seg_cnts_size(); seg_idx++) {
                                        record_cnt += table_status.ts_idx_status(pos).seg_cnts(seg_idx);
                                    }
                                    break;
                                }
                            }
                        }
                        partition_meta->set_record_cnt(record_cnt);
                        partition_meta->set_diskused(table_status.diskused());
                        if (kv.second->table_partition(idx).partition_meta(meta_idx).is_alive() &&
                                kv.second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                                table_partition->set_record_cnt(record_cnt);
                                table_partition->set_record_byte_size(table_status.record_byte_size() +
                                    table_status.record_idx_byte_size());
                                table_partition->set_diskused(table_status.diskused());
                        }
                        tablet_has_partition = true;
                    }
                    partition_meta->set_tablet_has_partition(tablet_has_partition);
                }
            }
        }
    }
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_table_status_interval, boost::bind(&NameServerImpl::UpdateTableStatus, this));
    }
}

int NameServerImpl::CreateDelReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kDelReplicaOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create op data error. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateDelReplicaOPTask(op_data) < 0) {
        PDLOG(WARNING, "create delreplica op task failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add delreplica op. op_id[%lu] table[%s] pid[%u] endpoint[%s]",
                op_index_, name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateDelReplicaOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (leader_endpoint == endpoint) {
        PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kDelReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDelTableInfoTask(name, pid, endpoint, op_index, ::rtidb::api::OPType::kDelReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create deltableinfo task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(endpoint, op_index, ::rtidb::api::OPType::kDelReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u] endpoint[%s]",
                        tid, pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DelReplica op task ok. table[%s] pid[%u] endpoint[%s]",
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateDelReplicaRemoteOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_index, 
            ::rtidb::api::OPType::kDelReplicaRemoteOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", 
                name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDelTableInfoTask(name, pid, endpoint, op_index, ::rtidb::api::OPType::kDelReplicaRemoteOP, 1);
    if (!task) {
        PDLOG(WARNING, "create deltableinfo task failed. table[%s] pid[%u] endpoint[%s]", 
                name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DelReplica op task ok. table[%s] pid[%u] endpoint[%s]", 
            name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateOfflineReplicaOP(const std::string& name, uint32_t pid, 
                const std::string& endpoint, uint32_t concurrency) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kOfflineReplicaOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create op data failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateOfflineReplicaTask(op_data) < 0) {
        PDLOG(WARNING, "create offline replica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add kOfflineReplicaOP. op_id[%lu] table[%s] pid[%u] endpoint[%s]",
                op_index_, name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateOfflineReplicaTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    uint64_t op_index = op_data->op_info_.op_id();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "no alive leader for table %s pid %u", name.c_str(), pid);
        return -1;
    } else {
        if (leader_endpoint == endpoint) {
            PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
            return -1;
        }
        std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_index,
                    ::rtidb::api::OPType::kOfflineReplicaOP, tid, pid, endpoint);
        if (!task) {
            PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]",
                            name.c_str(), pid, endpoint.c_str());
            return -1;
        }
        op_data->task_list_.push_back(task);
        task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, false,
                    op_index, ::rtidb::api::OPType::kOfflineReplicaOP);
        if (!task) {
            PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                            name.c_str(), pid, endpoint.c_str());
            return -1;
        }
        op_data->task_list_.push_back(task);
        PDLOG(INFO, "create OfflineReplica task ok. table[%s] pid[%u] endpoint[%s]",
                     name.c_str(), pid, endpoint.c_str());
    }

    return 0;
}

int NameServerImpl::CreateChangeLeaderOP(const std::string& name, uint32_t pid,
            const std::string& candidate_leader, bool need_restore, uint32_t concurrency) {
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    //TODO use get healthy follower method
    std::vector<std::string> follower_endpoint;
    std::vector<::rtidb::common::EndpointAndTid> remote_follower_endpoint;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                std::string endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (!iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    auto tablets_iter = tablets_.find(endpoint);
                    if (tablets_iter != tablets_.end() &&
                        tablets_iter->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                        follower_endpoint.push_back(endpoint);
                    } else {
                        PDLOG(WARNING, "endpoint[%s] is offline. table[%s] pid[%u]",
                                        endpoint.c_str(), name.c_str(), pid);
                    }
                }
            }
        }
        for (int i = 0; i < iter->second->table_partition(idx).remote_partition_meta_size(); i++) {
            if (iter->second->table_partition(idx).remote_partition_meta(i).is_alive()) {
                ::rtidb::common::EndpointAndTid et;
                std::string endpoint = iter->second->table_partition(idx).remote_partition_meta(i).endpoint();
                uint32_t tid = iter->second->table_partition(idx).remote_partition_meta(i).remote_tid();
                et.set_endpoint(endpoint);
                et.set_tid(tid);
                remote_follower_endpoint.push_back(et);
            }
        }
        break;
    }

    if (need_restore && !candidate_leader.empty()
            && std::find(follower_endpoint.begin(), follower_endpoint.end(), candidate_leader) == follower_endpoint.end()) {
        follower_endpoint.push_back(candidate_leader);
    }
    if (follower_endpoint.empty()) {
        PDLOG(INFO, "table not found follower. name[%s] pid[%u]", name.c_str(), pid);
        return 0;
    }
    if (!candidate_leader.empty() && std::find(follower_endpoint.begin(), follower_endpoint.end(), candidate_leader) == follower_endpoint.end()) {
        PDLOG(WARNING, "candidate_leader[%s] is not in followers. name[%s] pid[%u]", candidate_leader.c_str(), name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    ChangeLeaderData change_leader_data;
    change_leader_data.set_name(name);
    change_leader_data.set_tid(tid);
    change_leader_data.set_pid(pid);
    for (const auto& endpoint : follower_endpoint) {
        change_leader_data.add_follower(endpoint);
    }
    for (const auto &endpoint : remote_follower_endpoint) {
        change_leader_data.add_remote_follower()->CopyFrom(endpoint);
    }
    if (!candidate_leader.empty()) {
        change_leader_data.set_candidate_leader(candidate_leader);
    }
    std::string value;
    change_leader_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kChangeLeaderOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ChangeLeaderOP data error. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    if (CreateChangeLeaderOPTask(op_data) < 0) {
        PDLOG(WARNING, "create ChangeLeaderOP task failed. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add changeleader op. op_id[%lu] table[%s] pid[%u]",
                op_data->op_info_.op_id(), name.c_str(), pid);
    return 0;
}

int NameServerImpl::CreateChangeLeaderOPTask(std::shared_ptr<OPData> op_data) {
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]",
                        op_data->op_info_.op_id(), op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = change_leader_data.name();
    uint32_t tid = change_leader_data.tid();
    uint32_t pid = change_leader_data.pid();
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < change_leader_data.follower_size(); idx++) {
        follower_endpoint.push_back(change_leader_data.follower(idx));
    }
    std::shared_ptr<Task> task = CreateSelectLeaderTask(
                op_data->op_info_.op_id(), ::rtidb::api::OPType::kChangeLeaderOP,
                name, tid, pid, follower_endpoint);
    if (!task) {
        PDLOG(WARNING, "create selectleader task failed. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateChangeLeaderTask(op_data->op_info_.op_id(),
                ::rtidb::api::OPType::kChangeLeaderOP, name, pid);
    if (!task) {
        PDLOG(WARNING, "create changeleader task failed. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdateLeaderInfoTask(op_data->op_info_.op_id(),
                ::rtidb::api::OPType::kChangeLeaderOP, name, pid);
    if (!task) {
        PDLOG(WARNING, "create updateleaderinfo task failed. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ChangeLeader op task ok. name[%s] pid[%u]", name.c_str(), pid);
    return 0;
}

void NameServerImpl::OnLocked() {
    PDLOG(INFO, "become the leader name server");
    bool ok = Recover();
    if (!ok) {
        PDLOG(WARNING, "recover failed");
        //TODO fail to recover discard the lock
    }
    running_.store(true, std::memory_order_release);
    task_thread_pool_.DelayTask(FLAGS_get_task_status_interval, boost::bind(&NameServerImpl::UpdateTaskStatus, this, false));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::UpdateTableStatus, this));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::ProcessTask, this));
    thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
    task_thread_pool_.DelayTask(FLAGS_get_replica_status_interval, boost::bind(&NameServerImpl::CheckClusterInfo, this));
    task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
}

void NameServerImpl::OnLostLock() {
    PDLOG(INFO, "become the stand by name sever");
    running_.store(false, std::memory_order_release);
}

int NameServerImpl::CreateRecoverTableOP(const std::string& name, uint32_t pid,
            const std::string& endpoint, bool is_leader, uint64_t offset_delta, uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_is_leader(is_leader);
    recover_table_data.set_offset_delta(offset_delta);
    recover_table_data.set_concurrency(concurrency);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kRecoverTableOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create RecoverTableOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateRecoverTableOPTask(op_data) < 0) {
        PDLOG(WARNING, "create recover table op task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create RecoverTable op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]",
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateRecoverTableOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    RecoverTableData recover_table_data;
    if (!recover_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse recover_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    bool is_leader = recover_table_data.is_leader();
    uint32_t concurrency = recover_table_data.concurrency();
    if (!is_leader) {
        std::string leader_endpoint;
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
            return -1;
        }
        uint32_t tid = iter->second->tid();
        if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
            PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
            return -1;
        }
        if (leader_endpoint == endpoint) {
            PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
            return -1;
        }
        std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_data->op_info_.op_id(),
                    ::rtidb::api::OPType::kRecoverTableOP, tid, pid, endpoint);
        if (!task) {
            PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]",
                            name.c_str(), pid, endpoint.c_str());
            return -1;
        }
        op_data->task_list_.push_back(task);
    }
    std::shared_ptr<Task> task = CreateRecoverTableTask(op_data->op_info_.op_id(),
            ::rtidb::api::OPType::kRecoverTableOP, name, pid, endpoint, offset_delta, concurrency);
    if (!task) {
        PDLOG(WARNING, "create RecoverTable task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create RecoverTable task ok. name[%s] pid[%u] endpoint[%s]",
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

std::shared_ptr<Task> NameServerImpl::CreateRecoverTableTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                const std::string& name, uint32_t pid, const std::string& endpoint,
                uint64_t offset_delta, uint32_t concurrency) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kRecoverTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::RecoverEndpointTable, this, name, pid,
            endpoint, offset_delta, concurrency, task->task_info_);
    return task;
}

void NameServerImpl::RecoverEndpointTable(const std::string& name, uint32_t pid, std::string& endpoint,
            uint64_t offset_delta, uint32_t concurrency, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    uint32_t tid = 0;
    std::shared_ptr<TabletInfo> leader_tablet_ptr;
    std::shared_ptr<TabletInfo> tablet_ptr;
    bool has_follower = true;
    ::rtidb::common::StorageMode storage_mode = ::rtidb::common::StorageMode::kMemory;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        tid = iter->second->tid();
        storage_mode = iter->second->storage_mode();
        for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
            if (iter->second->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
                const PartitionMeta& partition_meta = iter->second->table_partition(idx).partition_meta(meta_idx);
                if (partition_meta.is_leader()) {
                    if (partition_meta.is_alive()) {
                        std::string leader_endpoint = partition_meta.endpoint();
                        auto tablet_iter = tablets_.find(leader_endpoint);
                        if (tablet_iter == tablets_.end()) {
                            PDLOG(WARNING, "can not find the leader endpoint[%s]'s client. op_id[%lu]", leader_endpoint.c_str(), task_info->op_id());
                            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                            return;
                        }
                        leader_tablet_ptr = tablet_iter->second;
                        if (leader_tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                            PDLOG(WARNING, "leader endpoint [%s] is offline. op_id[%lu]", leader_endpoint.c_str(), task_info->op_id());
                            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                            return;
                        }
                    } else if (endpoint == OFFLINE_LEADER_ENDPOINT) {
                        endpoint = partition_meta.endpoint();
                        PDLOG(INFO, "use endpoint[%s] to replace[%s], tid[%u] pid[%u]",
                                            endpoint.c_str(), OFFLINE_LEADER_ENDPOINT.c_str(), tid, pid);
                    }
                }
                if (partition_meta.endpoint() == endpoint) {
                    if (partition_meta.is_alive()) {
                        PDLOG(INFO, "endpoint[%s] is alive, need not recover. name[%s] pid[%u]",
                                        endpoint.c_str(), name.c_str(), pid);
                        task_info->set_status(::rtidb::api::TaskStatus::kDone);
                        return;
                    }
                    auto tablet_iter = tablets_.find(endpoint);
                    if (tablet_iter == tablets_.end()) {
                        PDLOG(WARNING, "can not find the endpoint[%s]'s client. op_id[%lu]", endpoint.c_str(), task_info->op_id());
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                    tablet_ptr = tablet_iter->second;
                    if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                        PDLOG(WARNING, "endpoint [%s] is offline. op_id[%lu]", endpoint.c_str(), task_info->op_id());
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                    if (iter->second->table_partition(idx).partition_meta_size() == 1) {
                        has_follower = false;
                        break;
                    }
                }
            }
            break;
        }
    }
    if ((has_follower && !leader_tablet_ptr) || !tablet_ptr) {
        PDLOG(WARNING, "not has tablet. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]",
                        name.c_str(), tid, pid, endpoint.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    bool has_table = false;
    bool is_leader = false;
    uint64_t term = 0;
    uint64_t offset = 0;
    if (!tablet_ptr->client_->GetTermPair(tid, pid, storage_mode, term, offset, has_table, is_leader)) {
        PDLOG(WARNING, "GetTermPair failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", 
                        name.c_str(), tid, pid, endpoint.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    if (!has_follower) {
        std::lock_guard<std::mutex> lock(mu_);
        if (has_table) {
            CreateUpdatePartitionStatusOP(name, pid, endpoint, true, true, task_info->op_id(), concurrency);
        } else {
            CreateReLoadTableOP(name, pid, endpoint, task_info->op_id(), concurrency);
        }
        task_info->set_status(::rtidb::api::TaskStatus::kDone);
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                    task_info->op_id(),
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
        return;
    }
    if (has_table && is_leader) {
        if (!tablet_ptr->client_->ChangeRole(tid, pid, false, 0)) {
            PDLOG(WARNING, "change role failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", 
                            name.c_str(), tid, pid, endpoint.c_str(), task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "change to follower. name[%s] tid[%u] pid[%u] endpoint[%s]",
                    name.c_str(), tid, pid, endpoint.c_str());
    }
    if (!has_table) {
        if (!tablet_ptr->client_->DeleteBinlog(tid, pid, storage_mode)) {
            PDLOG(WARNING, "delete binlog failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]",
                            name.c_str(), tid, pid, endpoint.c_str(), task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "delete binlog ok. name[%s] tid[%u] pid[%u] endpoint[%s]",
                            name.c_str(), tid, pid, endpoint.c_str());
    }
    int ret_code = MatchTermOffset(name, pid, has_table, term, offset);
    if (ret_code < 0) {
        PDLOG(WARNING, "match error. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]",
                        name.c_str(), tid, pid, endpoint.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ::rtidb::api::Manifest manifest;
    if (!leader_tablet_ptr->client_->GetManifest(tid, pid, storage_mode, manifest)) {
        PDLOG(WARNING, "get manifest failed. name[%s] tid[%u] pid[%u] op_id[%lu]",
                name.c_str(), tid, pid, task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    PDLOG(INFO, "offset[%lu] manifest offset[%lu]. name[%s] tid[%u] pid[%u]",
                 offset,  manifest.offset(), name.c_str(), tid, pid);
    if (has_table) {
        if (ret_code == 0 && offset >= manifest.offset()) {
            CreateReAddReplicaSimplifyOP(name, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        } else {
            CreateReAddReplicaWithDropOP(name, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        }
    } else {
        if (ret_code == 0 && offset >= manifest.offset()) {
            CreateReAddReplicaNoSendOP(name, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        } else {
            CreateReAddReplicaOP(name, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        }
    }
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "recover table task run success. name[%s] tid[%u] pid[%u]",
                name.c_str(), tid, pid);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                task_info->op_id(),
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

int NameServerImpl::CreateReAddReplicaOP(const std::string& name, uint32_t pid,
            const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (CreateReAddReplicaTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;

    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]",
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaTask(std::shared_ptr<OPData> op_data) {
    RecoverTableData recover_table_data;
    if (!recover_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse recover_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaOP, name,
                tid, pid, ttl, seg_cnt, false, pos->second->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateCheckBinlogSyncProgressTask(op_index,
                ::rtidb::api::OPType::kReAddReplicaOP, name, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true,
                op_index, ::rtidb::api::OPType::kReAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica op task ok. name[%s] pid[%u] endpoint[%s]",
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaWithDropOP(const std::string& name, uint32_t pid,
        const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaWithDropOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReAddReplicaWithDropOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReAddReplicaWithDropTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaWithDropOP task error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica with drop op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]",
                 op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaWithDropTask(std::shared_ptr<OPData> op_data) {
    RecoverTableData recover_table_data;
    if (!recover_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse recover_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(endpoint, op_index, ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, name,
                tid, pid, ttl, seg_cnt, false, pos->second->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateCheckBinlogSyncProgressTask(op_index,
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, name, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true,
                op_index, ::rtidb::api::OPType::kReAddReplicaWithDropOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ReAddReplicaWithDrop task ok. name[%s] pid[%u] endpoint[%s]",
                name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaNoSendOP(const std::string& name, uint32_t pid,
            const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaNoSendOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReAddReplicaNoSendOP data failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (CreateReAddReplicaNoSendTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaNoSendOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica no send op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]",
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaNoSendTask(std::shared_ptr<OPData> op_data) {
    RecoverTableData recover_table_data;
    if (!recover_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse recover_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, name,
                tid, pid, ttl, seg_cnt, false, pos->second->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateCheckBinlogSyncProgressTask(op_index,
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, name, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true,
                op_index, ::rtidb::api::OPType::kReAddReplicaNoSendOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica no send task ok. name[%s] pid[%u] endpoint[%s]",
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::GetLeader(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info, uint32_t pid, std::string& leader_endpoint) {
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader() &&
                    table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                leader_endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                return 0;
            }
        }
        break;
    }
    return -1;
}

int NameServerImpl::CreateReAddReplicaSimplifyOP(const std::string& name, uint32_t pid,
            const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id, uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaSimplifyOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReAddReplicaSimplifyOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReAddReplicaSimplifyTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaSimplifyOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica simplify op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]",
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;

}

int NameServerImpl::CreateReAddReplicaSimplifyTask(std::shared_ptr<OPData> op_data) {
    RecoverTableData recover_table_data;
    if (!recover_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse recover_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateAddReplicaTask(leader_endpoint, op_index,
                ::rtidb::api::OPType::kReAddReplicaSimplifyOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateCheckBinlogSyncProgressTask(op_index,
                ::rtidb::api::OPType::kReAddReplicaSimplifyOP, name, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true,
                op_index, ::rtidb::api::OPType::kReAddReplicaSimplifyOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica simplify task ok. name[%s] pid[%u] endpoint[%s]",
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::DropTableRemoteOP(const std::string& name, 
        const std::string& alias,  
        uint64_t parent_id, 
        uint32_t concurrency) {
    std::string value = alias;
    uint32_t pid = UINT32_MAX;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kDropTableRemoteOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create DropTableRemoteOP data error. table[%s] pid[%u] alias[%s]",
                name.c_str(), pid, alias.c_str());
        return -1;
    }
    if (DropTableRemoteTask(op_data) < 0) {
        PDLOG(WARNING, "create DropTableRemote task failed. table[%s] pid[%u] alias[%s]",
                        name.c_str(), pid, alias.c_str());
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] alias[%s]", 
                        name.c_str(), pid, alias.c_str());
        return -1;
    }
    PDLOG(INFO, "create DropTableRemote op ok. op_id[%lu] name[%s] pid[%u] alias[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, alias.c_str());
    return 0;
}

int NameServerImpl::DropTableRemoteTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string alias = op_data->op_info_.data();
    auto it = nsc_.find(alias);
    if (it == nsc_.end()) {
        PDLOG(WARNING, "replica cluster [%s] is not online", alias.c_str());
        return -1;
    }
    std::shared_ptr<Task> task = DropTableRemoteTask(name, alias, 
            op_data->op_info_.op_id(), ::rtidb::api::OPType::kDropTableRemoteOP);
    if (!task) {
        PDLOG(WARNING, "create DropTableRemote task failed. table[%s] pid[%u]", name.c_str(), op_data->op_info_.pid());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DropTableRemote task ok. name[%s] pid[%u] alias[%s]", 
            name.c_str(), op_data->op_info_.pid(), alias.c_str());
    return 0;
}

int NameServerImpl::CreateTableRemoteOP(const ::rtidb::nameserver::TableInfo& table_info, 
        const ::rtidb::nameserver::TableInfo& remote_table_info,
        const std::string& alias,  
        uint64_t parent_id, 
        uint32_t concurrency) {
    CreateTableData create_table_data;
    create_table_data.set_alias(alias);
    ::rtidb::nameserver::TableInfo* table_info_p = create_table_data.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    ::rtidb::nameserver::TableInfo* remote_table_info_p = create_table_data.mutable_remote_table_info();
    remote_table_info_p->CopyFrom(remote_table_info);
    std::string value;
    create_table_data.SerializeToString(&value);
    std::string name = table_info.name();
    uint32_t pid = UINT32_MAX;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kCreateTableRemoteOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create CreateTableRemoteOP data error. table[%s] pid[%u] alias[%s]",
                name.c_str(), pid, alias.c_str());
        return -1;
    }
    if (CreateTableRemoteTask(op_data) < 0) {
        PDLOG(WARNING, "create CreateTableRemote task failed. table[%s] pid[%u] alias[%s]",
                        table_info.name().c_str(), pid, alias.c_str());
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] alias[%s]", 
                        table_info.name().c_str(), pid, alias.c_str());
        return -1;
    }
    PDLOG(INFO, "create CreateTableRemote op ok. op_id[%lu] name[%s] pid[%u] alias[%s]", 
                op_data->op_info_.op_id(), table_info.name().c_str(), pid, alias.c_str());
    return 0;
}

int NameServerImpl::CreateTableRemoteTask(std::shared_ptr<OPData> op_data) {
    CreateTableData create_table_data;
    if (!create_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse create_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string alias = create_table_data.alias();
    ::rtidb::nameserver::TableInfo remote_table_info = create_table_data.remote_table_info();
    auto it = nsc_.find(alias);
    if (it == nsc_.end()) {
        PDLOG(WARNING, "replica cluster [%s] is not online", alias.c_str());
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateTableRemoteTask(remote_table_info, alias, 
            op_index, ::rtidb::api::OPType::kCreateTableRemoteOP);
    if (!task) {
        PDLOG(WARNING, "create CreateTableRemote task failed. table[%s] pid[%u]", remote_table_info.name().c_str(), op_data->op_info_.pid());
        return -1;
    }
    op_data->task_list_.push_back(task);

    ::rtidb::nameserver::TableInfo table_info = create_table_data.table_info();
    uint32_t tid = table_info.tid();
    uint32_t remote_tid = remote_table_info.tid();
    std::string name = table_info.name();
    for (int idx = 0; idx < remote_table_info.table_partition_size(); idx++) {
        ::rtidb::nameserver::TablePartition table_partition = remote_table_info.table_partition(idx);
        uint32_t pid = table_partition.pid();
        for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
            if (table_partition.partition_meta(meta_idx).is_leader()) {
                ::rtidb::nameserver::PartitionMeta partition_meta = table_partition.partition_meta(meta_idx);
                std::string endpoint = partition_meta.endpoint();
                std::string leader_endpoint;
                std::shared_ptr<::rtidb::nameserver::TableInfo> table_info_tmp = std::make_shared<::rtidb::nameserver::TableInfo> (table_info);
                if (GetLeader(table_info_tmp, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
                    PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
                    return -1;
                }
                task = CreateAddReplicaRemoteTask(leader_endpoint, op_index, 
                        ::rtidb::api::OPType::kCreateTableRemoteOP, tid, remote_tid, pid, endpoint, idx);
                if (!task) {
                    PDLOG(WARNING, "create addreplica task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]",
                            tid, remote_tid, pid);
                    return -1;
                }
                op_data->task_list_.push_back(task);
                task = CreateAddTableInfoTask(alias, endpoint, name, partition_meta.remote_tid(), pid,
                        op_index, ::rtidb::api::OPType::kCreateTableRemoteOP);
                if (!task) {
                    PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
                    return -1;
                }
                op_data->task_list_.push_back(task);
                break;
            }
        }
    }

    PDLOG(INFO, "create CreateTableRemote task ok. name[%s] pid[%u] alias[%s]", 
            remote_table_info.name().c_str(), op_data->op_info_.pid(), alias.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableOP(const std::string& name, uint32_t pid, 
            const std::string& endpoint, uint64_t parent_id, uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReLoadTableOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReLoadTableOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReLoadTableTask(op_data) < 0) {
        PDLOG(WARNING, "create ReLoadTable task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create ReLoadTableOP op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]",
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableOP(const std::string& name, uint32_t pid, 
            const std::string& endpoint, uint64_t parent_id, uint32_t concurrency, 
            uint64_t remote_op_id, uint64_t& rep_cluster_op_id) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReLoadTableOP, value, op_data, name, pid, parent_id, remote_op_id) < 0) {
        PDLOG(WARNING, "create ReLoadTableOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReLoadTableTask(op_data) < 0) {
        PDLOG(WARNING, "create ReLoadTable task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    rep_cluster_op_id = op_data->op_info_.op_id(); //for multi cluster 
    PDLOG(INFO, "create ReLoadTableOP op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::shared_ptr<Task> task = CreateLoadTableTask(endpoint, op_data->op_info_.op_id(),
                ::rtidb::api::OPType::kReLoadTableOP, name,
                tid, pid, ttl, seg_cnt, true, pos->second->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, true, true,
                op_data->op_info_.op_id(), ::rtidb::api::OPType::kReLoadTableOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ReLoadTable task ok. name[%s] pid[%u] endpoint[%s]",
                name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateUpdatePartitionStatusOP(const std::string& name, uint32_t pid,
                const std::string& endpoint, bool is_leader, bool is_alive, uint64_t parent_id, uint32_t concurrency) {
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    EndpointStatusData endpoint_status_data;
    endpoint_status_data.set_endpoint(endpoint);
    endpoint_status_data.set_is_leader(is_leader);
    endpoint_status_data.set_is_alive(is_alive);
    std::string value;
    endpoint_status_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kUpdatePartitionStatusOP, value, op_data, name, pid, parent_id) < 0) {
        PDLOG(WARNING, "create UpdatePartitionStatusOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateUpdatePartitionStatusOPTask(op_data) < 0) {
        PDLOG(WARNING, "create UpdatePartitionStatusOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create UpdatePartitionStatusOP op ok."
                 "op_id[%lu] name[%s] pid[%u] endpoint[%s] is_leader[%d] is_alive[%d] concurrency[%u]",
                 op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str(), is_leader, is_alive, concurrency);
    return 0;
}

int NameServerImpl::CreateUpdatePartitionStatusOPTask(std::shared_ptr<OPData> op_data) {
    EndpointStatusData endpoint_status_data;
    if (!endpoint_status_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse endpont_status_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = endpoint_status_data.endpoint();
    bool is_leader = endpoint_status_data.is_leader();
    bool is_alive = endpoint_status_data.is_alive();
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    std::shared_ptr<Task> task = CreateUpdatePartitionStatusTask(name, pid, endpoint, is_leader, is_alive,
                op_data->op_info_.op_id(), ::rtidb::api::OPType::kUpdatePartitionStatusOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create UpdatePartitionStatusOP task ok."
                 "name[%s] pid[%u] endpoint[%s] is_leader[%d] is_alive[%d]",
                 name.c_str(), pid, endpoint.c_str(), is_leader, is_alive);
    return 0;
}

int NameServerImpl::MatchTermOffset(const std::string& name, uint32_t pid, bool has_table, uint64_t term, uint64_t offset) {
    if (!has_table && offset == 0) {
        PDLOG(INFO, "has not table, offset is zero. name[%s] pid[%u]", name.c_str(), pid);
        return 1;
    }
    std::map<uint64_t, uint64_t> term_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
            return -1;
        }
        for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
            if (iter->second->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int term_idx = 0; term_idx < iter->second->table_partition(idx).term_offset_size(); term_idx++) {
                term_map.insert(std::make_pair(iter->second->table_partition(idx).term_offset(term_idx).term(),
                            iter->second->table_partition(idx).term_offset(term_idx).offset()));
            }
            break;
        }
    }
    auto iter = term_map.find(term);
    if (iter == term_map.end()) {
        PDLOG(WARNING, "not found term[%lu] in table_info. name[%s] pid[%u]",
                        term, name.c_str(), pid);
        return 1;
    } else if (iter->second > offset) {
        if (term_map.rbegin()->second == offset + 1) {
            PDLOG(INFO, "term[%lu] offset[%lu] has matched. name[%s] pid[%u]",
                            term, offset, name.c_str(), pid);
            return 0;
        }
        PDLOG(INFO, "offset is not matched. name[%s] pid[%u] term[%lu] term start offset[%lu] cur offset[%lu]",
                        name.c_str(), pid, term, iter->second, offset);
        return 1;
    }
    iter++;
    if (iter == term_map.end()) {
        PDLOG(INFO, "cur term[%lu] is the last one. name[%s] pid[%u]",
                        term, name.c_str(), pid);
        return 0;
    }
    if (iter->second <= offset) {
        PDLOG(INFO, "term[%lu] offset not matched. name[%s] pid[%u] offset[%lu]",
                        term, name.c_str(), pid, offset);
        return 1;
    }
    PDLOG(INFO, "term[%lu] offset has matched. name[%s] pid[%u] offset[%lu]",
                    term, name.c_str(), pid, offset);
    return 0;
}

void NameServerImpl::WrapTaskFun(const boost::function<bool ()>& fun, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!fun()) {
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        PDLOG(WARNING, "task[%s] run failed. op_id[%lu]",
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str(), task_info->op_id());
    }
    task_rpc_version_.fetch_add(1, std::memory_order_acq_rel);
    task_info->set_is_rpc_send(true);
}

std::shared_ptr<Task> NameServerImpl::CreateMakeSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid, uint64_t end_offset) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kMakeSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::MakeSnapshot, it->second->client_, tid, pid, end_offset, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreatePauseSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kPauseSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::PauseSnapshot, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateRecoverSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kRecoverSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::RecoverSnapshot, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateSendSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t remote_tid, 
                    uint32_t pid, const std::string& des_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kSendSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::SendSnapshot, it->second->client_, 
            tid, remote_tid, pid, des_endpoint, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::DropTableRemoteTask(const std::string& name, 
        const std::string& alias, 
        uint64_t op_index, 
        ::rtidb::api::OPType op_type) {
    auto it = nsc_.find(alias);
    if (it == nsc_.end()) {
        return std::shared_ptr<Task>();
    }
    std::shared_ptr<Task> task = std::make_shared<Task>(it->second->client_->GetEndpoint(), std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDropTableRemote);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(it->second->client_->GetEndpoint());

    boost::function<bool ()> fun = boost::bind(&NameServerImpl::DropTableRemote, this, *(task->task_info_), name, it->second);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateTableRemoteTask(const ::rtidb::nameserver::TableInfo& table_info, 
        const std::string& alias, 
        uint64_t op_index, 
        ::rtidb::api::OPType op_type) {
    auto it = nsc_.find(alias);
    if (it == nsc_.end()) {
        return std::shared_ptr<Task>();
    }
    std::shared_ptr<Task> task = std::make_shared<Task>(it->second->client_->GetEndpoint(), std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kCreateTableRemote);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(it->second->client_->GetEndpoint());

    boost::function<bool ()> fun = boost::bind(&NameServerImpl::CreateTableRemote, this, *(task->task_info_), table_info, it->second);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateLoadTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, const std::string& name,
                    uint32_t tid, uint32_t pid, uint64_t ttl, uint32_t seg_cnt, bool is_leader,
                    ::rtidb::common::StorageMode storage_mode) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kLoadTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);

    ::rtidb::common::StorageMode cur_storage_mode = ::rtidb::common::StorageMode::kMemory;
    if (storage_mode == ::rtidb::common::StorageMode::kSSD) {
        cur_storage_mode = ::rtidb::common::StorageMode::kSSD;
    } else if (storage_mode == ::rtidb::common::StorageMode::kHDD) {
        cur_storage_mode = ::rtidb::common::StorageMode::kHDD;
    }
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_name(name);
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_ttl(ttl);
    table_meta.set_seg_cnt(seg_cnt);
    table_meta.set_storage_mode(cur_storage_mode);
    if (is_leader) {
        table_meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    } else {
        table_meta.set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    boost::function<bool ()> fun = boost::bind(&TabletClient::LoadTable, it->second->client_,
                table_meta, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateLoadTableRemoteTask(const std::string& alias, 
        const std::string& name,
        const std::string& endpoint,
        uint32_t pid,
        uint64_t op_index, 
        ::rtidb::api::OPType op_type) {
    auto it = nsc_.find(alias);
    if (it == nsc_.end()) {
        return std::shared_ptr<Task>();
    }
    std::shared_ptr<Task> task = std::make_shared<Task>(it->second->client_->GetEndpoint(), std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kLoadTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(it->second->client_->GetEndpoint());
    
    boost::function<bool ()> fun = boost::bind(&NsClient::LoadTable, std::atomic_load_explicit(&it->second->client_, std::memory_order_relaxed), 
                name, endpoint, pid, zone_info_, *(task->task_info_));
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddReplicaRemoteTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t remote_tid,
                    uint32_t pid, const std::string& des_endpoint, uint64_t task_id) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        PDLOG(WARNING, "provide endpoint [%s] not found", endpoint.c_str());
        return std::shared_ptr<Task>();
    }
    if (it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "provide endpoint [%s] is not healthy", endpoint.c_str());
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddReplica);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    if (task_id != INVALID_PARENT_ID) {
        task->task_info_->set_task_id(task_id); 
    }
    boost::function<bool ()> fun = boost::bind(&TabletClient::AddReplica, it->second->client_, tid, pid, 
                des_endpoint, remote_tid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddReplicaNSRemoteTask(const std::string& alias, 
        const std::string& name, 
        const std::vector<std::string>& endpoint_vec,
        uint32_t pid,
        uint64_t op_index, 
        ::rtidb::api::OPType op_type) {
    auto it = nsc_.find(alias);
    if (it == nsc_.end()) {
        return std::shared_ptr<Task>();
    }
    std::shared_ptr<Task> task = std::make_shared<Task>(it->second->client_->GetEndpoint(), std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddReplicaNSRemote);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(it->second->client_->GetEndpoint());
    boost::function<bool ()> fun = boost::bind(&NsClient::AddReplicaNS, std::atomic_load_explicit(&it->second->client_, std::memory_order_relaxed), name, endpoint_vec, pid,  
             zone_info_, *(task->task_info_));
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddReplicaTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
                    const std::string& des_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddReplica);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::AddReplica, it->second->client_, tid, pid,
                des_endpoint, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddTableInfoTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::AddTableInfo, this, name, endpoint, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddTableInfoTask(const std::string& alias, 
        const std::string& endpoint,
        const std::string& name, 
        uint32_t remote_tid,
        uint32_t pid, 
        uint64_t op_index, 
        ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::AddTableInfo, this, alias, endpoint, name, remote_tid, pid, task->task_info_);
    return task;
}

void NameServerImpl::AddTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() == pid) {
            ::rtidb::nameserver::TablePartition* table_partition = iter->second->mutable_table_partition(idx);
            for (int meta_idx = 0; meta_idx< table_partition->partition_meta_size(); meta_idx++) {
                if (table_partition->partition_meta(meta_idx).endpoint() == endpoint) {
                    PDLOG(WARNING, "follower already exists pid[%u] table[%s] endpoint[%s] op_id[%lu]", pid, name.c_str(), endpoint.c_str(), task_info->op_id());
                    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                    return;
                }
            }
            ::rtidb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
            partition_meta->set_endpoint(endpoint);
            partition_meta->set_is_leader(false);
            partition_meta->set_is_alive(false);
            break;
        }
    }
    std::string table_value;
    iter->second->SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s] op_id[%lu]", 
                        zk_table_data_path_.c_str(), name.c_str(), table_value.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
        return;         
    }
    PDLOG(INFO, "update table node[%s/%s]. value is [%s]", 
                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::AddTableInfo(const std::string& alias, 
        const std::string& endpoint,
        const std::string& name, 
        uint32_t remote_tid,
        uint32_t pid, 
        std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ::rtidb::nameserver::TableInfo table_info(*(iter->second));
    for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
        if (table_info.table_partition(idx).pid() == pid) {
            ::rtidb::nameserver::TablePartition* table_partition_ptr = table_info.mutable_table_partition(idx);
            bool is_exist = false;
            int meta_idx = 0;
            for (; meta_idx < table_partition_ptr->remote_partition_meta_size(); meta_idx++) {
                if (table_partition_ptr->remote_partition_meta(meta_idx).endpoint() == endpoint) {
                    is_exist = true;
                    break;
                }
            }
            PartitionMeta* meta = NULL;
            if (is_exist) {
                PDLOG(INFO, "remote follower already exists pid[%u] table[%s] endpoint[%s] op_id[%lu]", pid, name.c_str(), endpoint.c_str(), task_info->op_id());
                meta = table_partition_ptr->mutable_remote_partition_meta(meta_idx);
            } else {
                meta = table_partition_ptr->add_remote_partition_meta();
            }
            meta->set_endpoint(endpoint);
            meta->set_remote_tid(remote_tid);
            meta->set_is_leader(false);
            meta->set_is_alive(true);
            meta->set_alias(alias);
            break;
        }
    }
    std::string table_value;
    table_info.SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s] op_id[%lu]",
                        zk_table_data_path_.c_str(), name.c_str(), table_value.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    PDLOG(INFO, "update table node[%s/%s]. value is [%s]",
                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
    
    iter->second->CopyFrom(table_info);
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                task_info->op_id(),
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

std::shared_ptr<Task> NameServerImpl::CreateDelReplicaTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
                    const std::string& follower_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDelReplica);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::DelReplica, it->second->client_, tid, pid,
                follower_endpoint, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateDropTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDropTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(endpoint);
    boost::function<bool ()> fun = boost::bind(&TabletClient::DropTable, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateCheckBinlogSyncProgressTask(uint64_t op_index,
        ::rtidb::api::OPType op_type, const std::string& name, uint32_t pid,
        const std::string& follower, uint64_t offset_delta) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kCheckBinlogSyncProgress);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::CheckBinlogSyncProgress, this, name, pid,
        follower, offset_delta, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdateTableInfoTask(const std::string& src_endpoint,
                    const std::string& name, uint32_t pid, const std::string& des_endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdateTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdateTableInfo, this, src_endpoint, name, pid,
            des_endpoint, task->task_info_);
    return task;
}

void NameServerImpl::CheckBinlogSyncProgress(const std::string& name, uint32_t pid,
                const std::string& follower, uint64_t offset_delta, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    if (task_info->status() != ::rtidb::api::TaskStatus::kDoing) {
        PDLOG(WARNING, "task status is[%s], exit task. op_id[%lu], task_type[%s]",
                        ::rtidb::api::TaskStatus_Name(task_info->status()).c_str(),
                        task_info->op_id(),
                        ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
        return;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    uint64_t leader_offset = 0;
    uint64_t follower_offset = 0;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            const PartitionMeta& meta = iter->second->table_partition(idx).partition_meta(meta_idx);
            if (!meta.tablet_has_partition()) {
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(WARNING, "tablet has not partition, update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                                task_info->op_id(), ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
                return;
            }
            if (!meta.has_offset()) {
                continue;
            }
            if (meta.is_leader() && meta.is_alive()) {
                leader_offset = meta.offset();
            } else if (meta.endpoint() == follower) {
                follower_offset = meta.offset();
            }
        }
        if (leader_offset <= follower_offset + offset_delta) {
            task_info->set_status(::rtidb::api::TaskStatus::kDone);
            PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s], leader_offset[%lu], follower_offset[%lu]",
                        task_info->op_id(),
                        ::rtidb::api::TaskType_Name(task_info->task_type()).c_str(),
                        leader_offset, follower_offset);
            return;
        }
        break;
    }
    PDLOG(INFO, "op_id[%lu], task_type[%s],leader_offset[%lu], follower_offset[%lu] offset_delta[%lu]",
                    task_info->op_id(),
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str(),
                    leader_offset, follower_offset, offset_delta);
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_table_status_interval,
                boost::bind(&NameServerImpl::CheckBinlogSyncProgress, this, name, pid, follower, offset_delta, task_info));
    }
}

void NameServerImpl::UpdateTableInfo(const std::string& src_endpoint, const std::string& name, uint32_t pid,
                const std::string& des_endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        int src_endpoint_index = -1;
        int des_endpoint_index = -1;
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            std::string endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();
            if (endpoint == src_endpoint) {
                src_endpoint_index = meta_idx;
            } else if (endpoint == des_endpoint) {
                des_endpoint_index = meta_idx;
            }
        }
        if (src_endpoint_index < 0) {
            PDLOG(WARNING, "has not found src_endpoint[%s]. name[%s] pid[%u] op_id[%lu]",
                            src_endpoint.c_str(), name.c_str(), pid, task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        ::rtidb::nameserver::TablePartition* table_partition =
                    iter->second->mutable_table_partition(idx);
        ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta_field =
                    table_partition->mutable_partition_meta();
        if (des_endpoint_index < 0) {
            // use src_endpoint's meta when the meta of des_endpoint is not exist
            PDLOG(INFO, "des_endpoint meta is not exist, use src_endpoint's meta."
                        "src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
            ::rtidb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(src_endpoint_index);
            partition_meta->set_endpoint(des_endpoint);
            partition_meta->set_is_alive(true);
            partition_meta->set_is_leader(false);
        } else {
            ::rtidb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(des_endpoint_index);
            partition_meta->set_is_alive(true);
            partition_meta->set_is_leader(false);
            PDLOG(INFO, "remove partition[%u] in endpoint[%s]. name[%s]",
                        pid, src_endpoint.c_str(), name.c_str());
            partition_meta_field->DeleteSubrange(src_endpoint_index, 1);
        }
        break;
    }
    std::string table_value;
    iter->second->SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s] op_id[%lu]",
                        zk_table_data_path_.c_str(), name.c_str(), table_value.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    PDLOG(INFO, "update table node[%s/%s]. value is [%s]",
                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    NotifyTableChanged();
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                task_info->op_id(),
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

std::shared_ptr<Task> NameServerImpl::CreateDelTableInfoTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t flag) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDelTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::DelTableInfo, this, name, endpoint, pid, task->task_info_, flag);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateDelTableInfoTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDelTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::DelTableInfo, this, name, endpoint, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdatePartitionStatusTask(const std::string& name,
                    uint32_t pid, const std::string& endpoint, bool is_leader, bool is_alive,
                    uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdatePartitionStatus);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdatePartitionStatus, this, name, endpoint,
                        pid, is_leader, is_alive, task->task_info_);
    return task;
}

void NameServerImpl::DelTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
   return DelTableInfo(name, endpoint, pid, task_info, 0); 
}

void NameServerImpl::DelTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                std::shared_ptr<::rtidb::api::TaskInfo> task_info, uint32_t for_remote) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ::rtidb::nameserver::TableInfo table_info(*(iter->second));
    for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
        if (table_info.table_partition(idx).pid() != pid) {
            continue;
        }
        bool has_found = false;
        if (for_remote == 1) {
            for (int meta_idx = 0; meta_idx < table_info.table_partition(idx).remote_partition_meta_size(); meta_idx++) {
                if (table_info.table_partition(idx).remote_partition_meta(meta_idx).endpoint() == endpoint) {
                    ::rtidb::nameserver::TablePartition* table_partition = table_info.mutable_table_partition(idx);
                    ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta = 
                        table_partition->mutable_remote_partition_meta();
                    PDLOG(INFO, "remove pid[%u] in table[%s]. endpoint is[%s]", 
                            pid, name.c_str(), endpoint.c_str());
                    partition_meta->DeleteSubrange(meta_idx, 1);
                    has_found = true;
                    break;
                }
            }
        } else {
            for (int meta_idx = 0; meta_idx < table_info.table_partition(idx).partition_meta_size(); meta_idx++) {
                if (table_info.table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    ::rtidb::nameserver::TablePartition* table_partition = 
                        table_info.mutable_table_partition(idx);
                    ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta = 
                        table_partition->mutable_partition_meta();
                    PDLOG(INFO, "remove pid[%u] in table[%s]. endpoint is[%s]", 
                            pid, name.c_str(), endpoint.c_str());
                    partition_meta->DeleteSubrange(meta_idx, 1);
                    has_found = true;
                    break;
                }
            }
        }
        if (!has_found) {
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            PDLOG(INFO, "not found endpoint[%s] in partition_meta. name[%s] pid[%u] op_id[%lu]",
                         endpoint.c_str(), name.c_str(), pid, task_info->op_id());
            return;
        }
        break;
    }
    std::string table_value;
    table_info.SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s] op_id[%lu]",
                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    PDLOG(INFO, "update table node[%s/%s]. value is [%s]",
            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
    iter->second->CopyFrom(table_info);
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    NotifyTableChanged();
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
            task_info->op_id(),
            ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::UpdatePartitionStatus(const std::string& name, const std::string& endpoint, uint32_t pid,
                bool is_leader, bool is_alive, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::rtidb::nameserver::TablePartition* table_partition =
                        iter->second->mutable_table_partition(idx);
                ::rtidb::nameserver::PartitionMeta* partition_meta =
                        table_partition->mutable_partition_meta(meta_idx);
                partition_meta->set_is_leader(is_leader);
                partition_meta->set_is_alive(is_alive);
                std::string table_value;
                iter->second->SerializeToString(&table_value);
                if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
                    PDLOG(WARNING, "update table node[%s/%s] failed! value[%s] op_id[%lu]",
                                    zk_table_data_path_.c_str(), name.c_str(), table_value.c_str(), task_info->op_id());
                    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                    return;
                }
                NotifyTableChanged();
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(INFO, "update table node[%s/%s]. value is [%s]",
                                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
                PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                            task_info->op_id(),
                            ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
                return;
            }
        }
        break;
    }
    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
    PDLOG(WARNING, "name[%s] endpoint[%s] pid[%u] is not exist. op_id[%lu]",
                    name.c_str(), endpoint.c_str(), pid, task_info->op_id());
}

void NameServerImpl::UpdateTableAliveStatus(RpcController* controller,
        const UpdateTableAliveRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(301);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string name = request->name();
    std::string endpoint = request->endpoint();
    if (tablets_.find(endpoint) == tablets_.end()) {
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        response->set_code(302);
        response->set_msg("endpoint is not exist");
        return;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "table [%s] is not exist", name.c_str());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> cur_table_info(iter->second->New());
    cur_table_info->CopyFrom(*(iter->second));
    bool has_update = false;
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (request->has_pid() && cur_table_info->table_partition(idx).pid() != request->pid()) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < cur_table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (cur_table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::rtidb::nameserver::TablePartition* table_partition =
                        cur_table_info->mutable_table_partition(idx);
                ::rtidb::nameserver::PartitionMeta* partition_meta =
                        table_partition->mutable_partition_meta(meta_idx);
                partition_meta->set_is_alive(request->is_alive());
                std::string is_alive = request->is_alive() ? "true" : "false";
                PDLOG(INFO, "update status[%s]. name[%s] endpoint[%s] pid[%u]",
                            is_alive.c_str(), name.c_str(), endpoint.c_str(), iter->second->table_partition(idx).pid());
                has_update = true;
                break;
            }
        }
    }
    if (has_update) {
        std::string table_value;
        cur_table_info->SerializeToString(&table_value);
        if (zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            NotifyTableChanged();
            iter->second = cur_table_info;
            PDLOG(INFO, "update alive status ok. name[%s] endpoint[%s]", name.c_str(), endpoint.c_str());
            response->set_code(0);
            response->set_msg("ok");
            return;
        } else {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]",
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            response->set_msg("set zk failed");
            response->set_code(304);
        }
    } else {
        response->set_msg("no pid has update");
        response->set_code(321);
    }
}

int NameServerImpl::UpdateEndpointTableAlive(const std::string& endpoint, bool is_alive) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return 0;
    }
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::TablePartition>* table_partition =
                    kv.second->mutable_table_partition();
        bool has_update = false;
        for (int idx = 0; idx < table_partition->size(); idx++) {
            ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta>* partition_meta =
                    table_partition->Mutable(idx)->mutable_partition_meta();;
            uint32_t alive_cnt = 0;
            for (int meta_idx = 0; meta_idx < partition_meta->size(); meta_idx++) {
                ::rtidb::nameserver::PartitionMeta* cur_partition_meta = partition_meta->Mutable(meta_idx);
                if (cur_partition_meta->is_alive()) {
                    alive_cnt++;
                }
            }
            if (alive_cnt == 1 && !is_alive) {
                PDLOG(INFO, "alive_cnt is one, should not set alive to false. name[%s] pid[%u] endpoint[%s] is_alive[%d]",
                            kv.first.c_str(), table_partition->Get(idx).pid(), endpoint.c_str(), is_alive);
                continue;

            }
            for (int meta_idx = 0; meta_idx < partition_meta->size(); meta_idx++) {
                ::rtidb::nameserver::PartitionMeta* cur_partition_meta = partition_meta->Mutable(meta_idx);
                if (cur_partition_meta->endpoint() == endpoint) {
                    cur_partition_meta->set_is_alive(is_alive);
                    has_update = true;
                }
            }
        }
        if (has_update) {
            std::string table_value;
            kv.second->SerializeToString(&table_value);
            if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + kv.first, table_value)) {
                PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]",
                                zk_table_data_path_.c_str(), kv.first.c_str(), table_value.c_str());
                return -1;
            }
            PDLOG(INFO, "update success. table[%s] endpoint[%s] is_alive[%d]", kv.first.c_str(), endpoint.c_str(), is_alive);
        }
    }
    NotifyTableChanged();
    return 0;
}

std::shared_ptr<Task> NameServerImpl::CreateSelectLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t tid, uint32_t pid,
                    std::vector<std::string>& follower_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kSelectLeader);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::SelectLeader, this, name, tid, pid, follower_endpoint, task->task_info_);
    PDLOG(INFO, "create SelectLeader task success. name[%s] tid[%u] pid[%u]", name.c_str(), tid, pid);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateChangeLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kChangeLeader);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::ChangeLeader, this, task->task_info_);
    PDLOG(INFO, "create ChangeLeader task success. name[%s] pid[%u]", name.c_str(), pid);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdateLeaderInfoTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdateLeaderInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdateLeaderInfo, this, task->task_info_);
    PDLOG(INFO, "create UpdateLeaderInfo task success. name[%s] pid[%u]", name.c_str(), pid);
    return task;
}

std::shared_ptr<OPData> NameServerImpl::FindRunningOP(uint64_t op_id) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        if (op_list.front()->op_info_.op_id() == op_id) {
            return op_list.front();
        }
    }
    return std::shared_ptr<OPData>();
}

void NameServerImpl::SelectLeader(const std::string& name, uint32_t tid, uint32_t pid,
            std::vector<std::string>& follower_endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (auto_failover_.load(std::memory_order_acquire)) {
            auto iter = table_info_.find(name);
            if (iter == table_info_.end()) {
                task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
                return;
            }
            for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
                if (iter->second->table_partition(idx).pid() != pid) {
                    continue;
                }
                for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
                    if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive() &&
                            iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        PDLOG(WARNING, "leader is alive, need not changeleader. table name[%s] pid[%u] op_id[%lu]",
                                        name.c_str(), pid, task_info->op_id());
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                }
                break;
            }
        }
        if (!zk_client_->SetNodeValue(zk_term_node_, std::to_string(term_ + 2))) {
            PDLOG(WARNING, "update leader id  node failed. table name[%s] pid[%u] op_id[%lu]", name.c_str(), pid, task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        cur_term = term_ + 1;
        term_ += 2;
    }
    // select the max offset endpoint as leader
    uint64_t max_offset = 0;
    std::vector<std::string> leader_endpoint_vec;
    for (const auto& endpoint : follower_endpoint) {
        std::shared_ptr<TabletInfo> tablet_ptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = tablets_.find(endpoint);
            if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {

                PDLOG(WARNING, "endpoint[%s] is offline. table[%s] pid[%u]  op_id[%lu]",
                                endpoint.c_str(), name.c_str(), pid, task_info->op_id());
                task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                return;
            }
            tablet_ptr = it->second;
        }
        uint64_t offset = 0;
        if (!tablet_ptr->client_->FollowOfNoOne(tid, pid, cur_term, offset)) {
            PDLOG(WARNING, "followOfNoOne failed. tid[%u] pid[%u] endpoint[%s] op_id[%lu]",
                            tid, pid, endpoint.c_str(), task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "FollowOfNoOne ok. term[%lu] offset[%lu] name[%s] tid[%u] pid[%u] endpoint[%s]",
                     cur_term, offset, name.c_str(), tid, pid, endpoint.c_str());
        if (offset > max_offset || leader_endpoint_vec.empty()) {
            max_offset = offset;
            leader_endpoint_vec.clear();
            leader_endpoint_vec.push_back(endpoint);
        } else if (offset == max_offset) {
            leader_endpoint_vec.push_back(endpoint);
        }
    }
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. name[%s] pid[%u] data[%s] op_id[%lu]",
                        name.c_str(), pid, op_data->op_info_.data().c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    std::string leader_endpoint;
    if (change_leader_data.has_candidate_leader()) {
        std::string candidate_leader = change_leader_data.candidate_leader();
        if (std::find(leader_endpoint_vec.begin(), leader_endpoint_vec.end(), candidate_leader) != leader_endpoint_vec.end()) {
            leader_endpoint = candidate_leader;
        } else {
            PDLOG(WARNING, "select leader failed, candidate_leader[%s] is not in leader_endpoint_vec. tid[%u] pid[%u] op_id[%lu]",
                            candidate_leader.c_str(), tid, pid, task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
    } else {
        leader_endpoint = leader_endpoint_vec[rand_.Next() % leader_endpoint_vec.size()];
    }
    change_leader_data.set_leader(leader_endpoint);
    change_leader_data.set_offset(max_offset);
    change_leader_data.set_term(cur_term + 1);
    std::string value;
    change_leader_data.SerializeToString(&value);
    op_data->op_info_.set_data(value);
    PDLOG(INFO, "new leader is[%s]. name[%s] tid[%u] pid[%u] offset[%lu]",
                leader_endpoint.c_str(), name.c_str(), tid, pid, max_offset);
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                task_info->op_id(),
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
 }

void NameServerImpl::ChangeLeader(std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]",
                        task_info->op_id(), op_data->op_info_.data().c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    std::string leader_endpoint = change_leader_data.leader();
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < change_leader_data.follower_size(); idx++) {
        follower_endpoint.push_back(change_leader_data.follower(idx));
    }
    std::shared_ptr<TabletInfo> tablet_ptr;
    uint64_t cur_term = change_leader_data.term();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(leader_endpoint);
        if (iter == tablets_.end() || iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            PDLOG(WARNING, "endpoint[%s] is offline", leader_endpoint.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        follower_endpoint.erase(std::find(follower_endpoint.begin(), follower_endpoint.end(), leader_endpoint));
        tablet_ptr = iter->second;
    }
    std::vector<::rtidb::common::EndpointAndTid> endpoint_tid;
    for (const auto& e: change_leader_data.remote_follower()) {
        endpoint_tid.push_back(e);
    }
    if (!tablet_ptr->client_->ChangeRole(change_leader_data.tid(), change_leader_data.pid(), true,
                                         follower_endpoint, cur_term, &endpoint_tid)) {
        PDLOG(WARNING, "change leader failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]",
                        change_leader_data.name().c_str(), change_leader_data.tid(),
                        change_leader_data.pid(), leader_endpoint.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    PDLOG(INFO, "change leader ok. name[%s] tid[%u] pid[%u] leader[%s] term[%lu]",
                change_leader_data.name().c_str(), change_leader_data.tid(),
                change_leader_data.pid(), leader_endpoint.c_str(), cur_term);
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                task_info->op_id(),
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::UpdateTTL(RpcController* controller,
        const ::rtidb::nameserver::UpdateTTLRequest* request,
        ::rtidb::nameserver::UpdateTTLResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::shared_ptr<TableInfo> table = GetTableInfo(request->name());
    if (!table) {
        PDLOG(WARNING, "table with name %s does not exist", request->name().c_str());
        response->set_code(101);
        response->set_msg("table is not exist");
        return;
    }
    // validation
    ::rtidb::api::TTLType old_ttl_type = ::rtidb::api::TTLType::kAbsoluteTime;
    ::rtidb::api::TTLType new_ttl_type = ::rtidb::api::TTLType::kAbsoluteTime;
    uint64_t abs_ttl = request->value();
    uint64_t lat_ttl = 0;
    if (table->has_ttl_desc()) {
        old_ttl_type = table->ttl_desc().ttl_type();
    } else if (table->ttl_type() == "kLatestTime") {
        old_ttl_type = ::rtidb::api::TTLType::kLatestTime;
    }
    if (request->has_ttl_desc()) {
        new_ttl_type = request->ttl_desc().ttl_type();
        abs_ttl = request->ttl_desc().abs_ttl();
        lat_ttl = request->ttl_desc().lat_ttl();
    } else if (request->ttl_type() == "kLatestTime") {
        new_ttl_type = ::rtidb::api::TTLType::kLatestTime;
        abs_ttl = 0;
        lat_ttl = request->value();
    }
    if (old_ttl_type != new_ttl_type) {
        PDLOG(WARNING, "table ttl type mismatch, expect %s but %s",::rtidb::api::TTLType_Name(old_ttl_type).c_str(),
            ::rtidb::api::TTLType_Name(new_ttl_type).c_str());
        response->set_code(112);
        response->set_msg("ttl type mismatch");
        return;
    }
    std::string ts_name;
    if (request->has_ts_name() && request->ts_name().size() > 0) {
        ts_name = request->ts_name();
        bool has_found = false;
        for (int i = 0; i < table->column_desc_v1_size(); i++) {
            if (table->column_desc_v1(i).is_ts_col()
                    && table->column_desc_v1(i).name() == ts_name) {
                has_found = true;
                break;
            }
        }
        if (!has_found) {
            PDLOG(WARNING, "ts name %s not found in table %s",
                    ts_name.c_str(), request->name().c_str());
            response->set_code(137);
            response->set_msg("ts name not found");
            return;
        }
    }
    // update the tablet
    bool all_ok = true;
    for (int32_t i = 0; i < table->table_partition_size(); i++) {
        if (!all_ok) {
            break;
        }
        const TablePartition& table_partition = table->table_partition(i);
        for (int32_t j = 0; j < table_partition.partition_meta_size(); j++) {
            const PartitionMeta& meta = table_partition.partition_meta(j);
            all_ok = all_ok && UpdateTTLOnTablet(meta.endpoint(), table->tid(),
                    table_partition.pid(), new_ttl_type, abs_ttl, lat_ttl, ts_name);
        }
    }
    if (!all_ok) {
        response->set_code(322);
        response->set_msg("fail to update ttl from tablet");
        return;
    }
    TableInfo table_info;
    std::lock_guard<std::mutex> lock(mu_);
    table_info.CopyFrom(*table);
    if (ts_name.empty()) {
        table_info.set_ttl(request->value());
        ::rtidb::api::TTLDesc* ttl_desc = table_info.mutable_ttl_desc();
        ttl_desc->set_abs_ttl(abs_ttl);
        ttl_desc->set_lat_ttl(lat_ttl);
        ttl_desc->set_ttl_type(new_ttl_type);
    } else {
        for (int i = 0; i < table_info.column_desc_v1_size(); i++) {
            if (table_info.column_desc_v1(i).is_ts_col()
                    && table_info.column_desc_v1(i).name() == ts_name) {
                ::rtidb::common::ColumnDesc* column_desc = table_info.mutable_column_desc_v1(i);
                column_desc->set_ttl(request->value());
                column_desc->set_abs_ttl(abs_ttl);
                column_desc->set_lat_ttl(lat_ttl);
            }
        }
    }
    // update zookeeper
    std::string table_value;
    table_info.SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + table->name(), table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]",
                        zk_table_data_path_.c_str(), table->name().c_str(), table_value.c_str());
        response->set_code(304);
        response->set_msg("set zk failed");
        return;
    }
    table->CopyFrom(table_info);
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::UpdateLeaderInfo(std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]",
                        task_info->op_id(), op_data->op_info_.data().c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    std::string leader_endpoint = change_leader_data.leader();
    std::string name = change_leader_data.name();
    uint32_t pid = change_leader_data.pid();

    std::lock_guard<std::mutex> lock(mu_);
    auto table_iter = table_info_.find(name);
    if (table_iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    int old_leader_index = -1;
    int new_leader_index = -1;
    for (int idx = 0; idx < table_iter->second->table_partition_size(); idx++) {
        if (table_iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_iter->second->table_partition(idx).partition_meta(meta_idx).is_leader() &&
                    table_iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                old_leader_index = meta_idx;
            } else if (table_iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == leader_endpoint) {
                new_leader_index = meta_idx;
            }
        }
        ::rtidb::nameserver::TablePartition* table_partition =
                table_iter->second->mutable_table_partition(idx);
        if (old_leader_index >= 0) {
            ::rtidb::nameserver::PartitionMeta* old_leader_meta =
                    table_partition->mutable_partition_meta(old_leader_index);
            old_leader_meta->set_is_alive(false);
        }
        if (new_leader_index < 0) {
            PDLOG(WARNING, "endpoint[%s] is not exist. name[%s] pid[%u] op_id[%lu]",
                            leader_endpoint.c_str(),  name.c_str(), pid, task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        ::rtidb::nameserver::PartitionMeta* new_leader_meta =
                table_partition->mutable_partition_meta(new_leader_index);
        new_leader_meta->set_is_leader(true);
        ::rtidb::nameserver::TermPair* term_offset = table_partition->add_term_offset();
        term_offset->set_term(change_leader_data.term());
        term_offset->set_offset(change_leader_data.offset() + 1);
        std::string table_value;
        table_iter->second->SerializeToString(&table_value);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s] op_id[%lu]",
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str(), task_info->op_id());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "change leader success. name[%s] pid[%u] new leader[%s]",
                    name.c_str(), pid, leader_endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kDone);
        // notify client to update table partition information
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                    task_info->op_id(),
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
        NotifyTableChanged();
        return;
    }
    PDLOG(WARNING, "partition[%u] is not exist. name[%s] op_id[%lu]", pid, name.c_str(), task_info->op_id());
    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
}

void NameServerImpl::NotifyTableChanged() {
    std::string value;
    bool ok = zk_client_->GetNodeValue(zk_table_changed_notify_node_, value);
    if (!ok) {
        PDLOG(WARNING, "get zk table changed notify node value failed");
        return;
    }
    uint64_t counter = std::stoull(value) + 1;
    ok = zk_client_->SetNodeValue(zk_table_changed_notify_node_, std::to_string(counter));
    if (!ok) {
        PDLOG(WARNING, "incr zk table changed notify node value failed");
    }
    PDLOG(INFO, "notify table changed ok, update counter from %s to %lu", value.c_str(), counter);
}

std::shared_ptr<TableInfo> NameServerImpl::GetTableInfo(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<TableInfo> table;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>>::iterator it = table_info_.find(name);
    if (it == table_info_.end()) {
        return table;
    }
    table = it->second;
    return table;
}

std::shared_ptr<TabletInfo> NameServerImpl::GetTabletInfo(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<TabletInfo> tablet;
    std::map<std::string, std::shared_ptr<TabletInfo>>::iterator it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        return tablet;
    }
    tablet = it->second;
    return tablet;
}

bool NameServerImpl::UpdateTTLOnTablet(const std::string& endpoint,
        int32_t tid, int32_t pid, const ::rtidb::api::TTLType& type,
        uint64_t abs_ttl, uint64_t lat_ttl, const std::string& ts_name) {
    std::shared_ptr<TabletInfo> tablet = GetTabletInfo(endpoint);
    if (!tablet) {
        PDLOG(WARNING, "tablet with endpoint %s is not found", endpoint.c_str());
        return false;
    }

    if (!tablet->client_) {
        PDLOG(WARNING, "tablet with endpoint %s has not client", endpoint.c_str());
        return false;
    }
    bool ok = tablet->client_->UpdateTTL(tid, pid, type, abs_ttl, lat_ttl, ts_name);
    if (!ok) {
        PDLOG(WARNING, "fail to update ttl with tid %d, pid %d, abs_ttl %lu, lat_ttl %lu, endpoint %s", tid, pid, abs_ttl, lat_ttl, endpoint.c_str());
    }else {
        PDLOG(INFO, "update ttl with tid %d pid %d abs_ttl %lu, lat_ttl %lu endpoint %s ok", tid, pid, abs_ttl, lat_ttl, endpoint.c_str());
    }
    return ok;
}

void NameServerImpl::AddReplicaCluster(RpcController* controller,
        const ClusterAddress* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) != kLEADER) {
        response->set_code(454);
        response->set_msg("cur nameserver is not leader mode");
        PDLOG(WARNING, "cur nameserver is not leader mode");
        return;
    }
    int code = 0;
    std::string rpc_msg("ok");
    do {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (nsc_.find(request->alias()) != nsc_.end()) {
                code = 400;
                rpc_msg = "replica cluster alias duplicate";
                break;
            }
        }
        std::shared_ptr<::rtidb::nameserver::ClusterInfo> cluster_info = std::make_shared<::rtidb::nameserver::ClusterInfo>(*request);
        if ((code = cluster_info->Init(rpc_msg)) != 0) {
            PDLOG(WARNING, "%s init failed, error: %s", request->alias().c_str(), rpc_msg.c_str());
            break;
        }
        std::vector<::rtidb::nameserver::TableInfo> tables;
        if (!std::atomic_load_explicit(&cluster_info->client_, std::memory_order_relaxed)->ShowTable("", tables, rpc_msg)) {
            rpc_msg = "showtable error when add replica cluster";
            code = 455;
            break;
        }
        {
            if (!tables.empty()) {
                decltype(tablets_) tablets;
                {
                    std::lock_guard<std::mutex> lock(mu_);
                    auto it = tablets_.begin();
                    for (; it != tablets_.end(); it++) {
                        if (it->second->state_ != api::kTabletHealthy) {
                            continue;
                        }
                        tablets.insert(std::make_pair(it->first, it->second));
                    }
                }
                std::map<std::string, std::map<uint32_t, std::map<uint32_t, uint64_t>>> tablet_part_offset;
                for (auto it = tablets.begin(); it != tablets.end(); it++) {
                    std::map<uint32_t, std::map<uint32_t, uint64_t>> value;
                    bool ok = it->second->client_->GetAllSnapshotOffset(value);
                    if (ok) {
                        tablet_part_offset.insert(std::make_pair(it->second->client_->GetEndpoint(), value));
                    }
                }
                std::lock_guard<std::mutex> lock(mu_);
                if (!CompareTableInfo(tables)) {
                    PDLOG(WARNING, "compare table info error");
                    rpc_msg = "compare table info error";
                    code = 567;
                    break;
                }
                if (!CompareSnapshotOffset(tables, rpc_msg, code, tablet_part_offset)) {
                    break;
                }

            }
        }
        if (!cluster_info->AddReplicaClusterByNs(request->alias(), zone_info_.zone_name(), zone_info_.zone_term(), rpc_msg)) {
            code = 300;
            break;
        }
        std::string cluster_value, value;
        request->SerializeToString(&cluster_value);
        if (zk_client_->GetNodeValue(zk_zone_data_path_ + "/replica/" + request->alias(), value)) {
            if (!zk_client_->SetNodeValue(zk_zone_data_path_ + "/replica/" + request->alias(), cluster_value)) {
                PDLOG(WARNING, "write replica cluster to zk failed, alias: %s", request->alias().c_str());
                code = 304;
                rpc_msg = "set zk failed";
                break;
            }
        } else {
            if (!zk_client_->CreateNode(zk_zone_data_path_ + "/replica/" + request->alias(), cluster_value)) {
                PDLOG(WARNING, "write replica cluster to zk failed, alias: %s", request->alias().c_str());
                code = 450;
                rpc_msg = "create zk failed";
                break;
            }
        }
        cluster_info->state_.store(kClusterHealthy, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(mu_);
            nsc_.insert(std::make_pair(request->alias(), cluster_info));
        }
        thread_pool_.AddTask(boost::bind(&NameServerImpl::CheckSyncExistTable, this, request->alias(), tables, std::atomic_load_explicit(&cluster_info->client_, std::memory_order_relaxed)));
        thread_pool_.AddTask(boost::bind(&NameServerImpl::CheckSyncTable, this, request->alias(), tables, std::atomic_load_explicit(&cluster_info->client_, std::memory_order_relaxed)));
    } while (0);

    response->set_code(code);
    response->set_msg(rpc_msg);
}

void NameServerImpl::AddReplicaClusterByNs(RpcController* controller,
        const ::rtidb::nameserver::ReplicaClusterByNsRequest* request,
        ::rtidb::nameserver::AddReplicaClusterByNsResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t code = 0;
    std::string rpc_msg = "accept";

    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) == kLEADER) {
        response->set_code(568);
        response->set_msg("cur nameserver is leader cluster");
        PDLOG(WARNING, "cur nameserver is leader cluster");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    PDLOG(DEBUG, "request zone name is: %s, term is: %lu %d,", request->zone_info().zone_name().c_str(), request->zone_info().zone_term(), zone_info_.mode());
    PDLOG(DEBUG, "cur zone name is: %s", zone_info_.zone_name().c_str());
    do {
        if ((mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
            if (request->zone_info().replica_alias() != zone_info_.replica_alias()) {
                code = 402;
                rpc_msg = "not same replica name";
                break;
            }
            if (request->zone_info().zone_name() == zone_info_.zone_name()) {
                if (request->zone_info().zone_term() < zone_info_.zone_term()) {
                    code = 406;
                    rpc_msg = "term le cur term";
                    break;
                }
                if (request->zone_info().zone_term() == zone_info_.zone_term()) {
                    code = 408;
                    rpc_msg = "already join zone";
                    break;
                }
            } else {
                code = 407;
                rpc_msg = "zone name not equal";
                break;
            }
        }
        std::string zone_info;
        request->zone_info().SerializeToString(&zone_info);
        if (zk_client_->IsExistNode(zk_zone_data_path_ + "/follower") > 0) {
            if (!zk_client_->CreateNode(zk_zone_data_path_ + "/follower", zone_info)) {
                PDLOG(WARNING, "write follower to zk failed, alias: %s", request->zone_info().replica_alias().c_str());
                code = 450;
                rpc_msg = "create zk failed";
                break;
            } 
        } else {
            if (!zk_client_->SetNodeValue(zk_zone_data_path_ + "/follower", zone_info)) {
                code = 304;
                rpc_msg = "set zk failed";
                PDLOG(WARNING, "set zk failed, save follower value failed");
                break;
            }
        }
        mode_.store(request->zone_info().mode(), std::memory_order_release);
        zone_info_.CopyFrom(request->zone_info());
    } while (0);
    thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
    response->set_code(code);
    response->set_msg(rpc_msg);
}
void NameServerImpl::ShowReplicaCluster(RpcController* controller,
        const GeneralRequest* request,
        ShowReplicaClusterResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) == kFOLLOWER) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader, is follower cluster");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);

    for (auto it = nsc_.begin(); it != nsc_.end(); ++it) {
        auto* status = response->add_replicas();
        auto replica = status->mutable_replica();
        replica->set_alias(it->first);
        replica->set_zk_path(it->second->cluster_add_.zk_path());
        replica->set_zk_endpoints(it->second->cluster_add_.zk_endpoints());
        status->set_state(ClusterStatus_Name(it->second->state_.load(std::memory_order_relaxed)));
        status->set_age(::baidu::common::timer::get_micros() / 1000 - it->second->ctime_);
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::RemoveReplicaCluster(RpcController* controller,
        const ::rtidb::nameserver::RemoveReplicaOfRequest* request,
        ::rtidb::nameserver::GeneralResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_relaxed) == kFOLLOWER)) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    int code = 0;
    std::string rpc_msg = "ok";
    std::shared_ptr<::rtidb::client::NsClient> c_ptr;
    ClusterStatus state = kClusterHealthy;
    do {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = nsc_.find(request->alias());
        if (it == nsc_.end()) {
            code = 404;
            rpc_msg = "replica name not found";
            PDLOG(WARNING, "replica name [%s] not found when remove replica clsuter", request->alias().c_str());
            break;
        }
        state = it->second->state_.load(std::memory_order_relaxed);
        for (auto iter = it->second->last_status.begin(); iter != it->second->last_status.end(); iter++) {
            for (auto part_iter = iter->second.begin(); part_iter != iter->second.end(); part_iter++) {
                for (auto meta : part_iter->partition_meta()) {
                    if (meta.endpoint().empty()) {
                        break;
                    }
                    DelReplicaRemoteOP(meta.endpoint(), iter->first, part_iter->pid());
                }
            }
        }
        if (!zk_client_->DeleteNode(zk_zone_data_path_ + "/replica/" + request->alias())) {
            code = 452;
            rpc_msg = "del zk failed";
            PDLOG(WARNING, "del replica zk node [%s] failed, when remove repcluster", request->alias().c_str());
            break;
        }
        c_ptr = std::atomic_load_explicit(&it->second->client_, std::memory_order_relaxed);
        nsc_.erase(it);
        PDLOG(INFO, "success remove replica cluster [%s]", request->alias().c_str());
    } while(0);
    if ((code == 0) && (state == kClusterHealthy)) {
        if (!c_ptr->RemoveReplicaClusterByNs(request->alias(), zone_info_.zone_name(), zone_info_.zone_term(), code, rpc_msg)) {
            PDLOG(WARNING, "send remove replica cluster request to replica clsute failed");
        }
    }
    response->set_code(code);
    response->set_msg(rpc_msg);
    return;
}

void NameServerImpl::RemoveReplicaClusterByNs(RpcController* controller,
        const ::rtidb::nameserver::ReplicaClusterByNsRequest* request,
        ::rtidb::nameserver::GeneralResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t code = 0;
    std::string rpc_msg = "ok";

    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) != kFOLLOWER) {
        response->set_code(405);
        response->set_msg("this is not follower");
        return;
    }
    do {
        std::lock_guard<std::mutex> lock(mu_);
        if (request->zone_info().replica_alias() != zone_info_.replica_alias()) {
            code = 402;
            rpc_msg = "not same replica name";
            break;
        }
        if (request->zone_info().zone_name() == zone_info_.zone_name()) {
            if (request->zone_info().zone_term() < zone_info_.zone_term()) {
                code = 406;
                rpc_msg = "term le cur term";
                break;
            }
        } else {
            code = 407;
            rpc_msg = "zone name not equal";
            break;
        }
        std::string value;
        ZoneInfo zone_info;
        zone_info.CopyFrom(request->zone_info());
        zone_info.set_mode(kNORMAL);
        zone_info.set_zone_name(FLAGS_endpoint + FLAGS_zk_root_path);
        zone_info.set_replica_alias("");
        zone_info.set_zone_term(1);
        zone_info.SerializeToString(&value);
        if (!zk_client_->SetNodeValue(zk_zone_data_path_ + "/follower", value)) {
            code = 304;
            rpc_msg = "set zk failed";
            PDLOG(WARNING, "set zk failed, save follower value failed");
            break;
        }
        mode_.store(zone_info.mode(), std::memory_order_release);
        zone_info_.CopyFrom(zone_info);
    } while (0);
    thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
    response->set_code(code);
    response->set_msg(rpc_msg);
    return;
}

void NameServerImpl::CheckClusterInfo() {
    do {
        decltype(nsc_) tmp_nsc;
        {

            std::lock_guard<std::mutex> lock(mu_);
            if (nsc_.size() < 1) {
                break;
            }
            for (auto i : nsc_) {
                if (i.second->state_.load(std::memory_order_relaxed) == kClusterHealthy) {
                    tmp_nsc.insert(std::make_pair(i.first, i.second));
                }
            }
        }
        for (const auto& i : tmp_nsc) {
            i.second->CheckZkClient();
        }
        std::string msg;
        for (auto i : tmp_nsc) {
            std::vector<::rtidb::nameserver::TableInfo> tables;
            if (!std::atomic_load_explicit(&i.second->client_, std::memory_order_relaxed)->ShowTable("", tables, msg)) {
                PDLOG(WARNING, "check %s showtable has error: %s", i.first.c_str(), msg.c_str());
                continue;
            }
            std::lock_guard<std::mutex> lock(mu_);
            if ((tables.size() > 0) && !CompareTableInfo(tables)) {
                // todo :: add cluster statsu, need show in showreplica
                PDLOG(WARNING, "compare %s table info has error", i.first.c_str());
                continue;
            }
            CheckTableInfo(i.second, tables);
        }
    } while(0);

    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_replica_status_interval, boost::bind(&NameServerImpl::CheckClusterInfo, this));
    }
}

void NameServerImpl::SwitchMode(::google::protobuf::RpcController* controller,
        const ::rtidb::nameserver::SwitchModeRequest* request,
        ::rtidb::nameserver::GeneralResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (request->sm() >= kFOLLOWER) {
        response->set_code(505);
        response->set_msg("unkown server status");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == request->sm()) {
        response->set_code(0);
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kLEADER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (nsc_.size() > 0) {
            response->set_code(555);
            response->set_msg("zone not empty");
            return;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    decltype(zone_info_) zone_info = zone_info_;
    zone_info.set_mode(request->sm());
    std::string value;
    zone_info.SerializeToString(&value);
    if (zk_client_->IsExistNode(zk_zone_data_path_ + "/follower") > 0) {
        if (!zk_client_->CreateNode(zk_zone_data_path_ + "/follower", value)) {
            PDLOG(WARNING, "write follower to zk failed");
            response->set_code(450);
            response->set_msg("create zk failed");
            return;
        }
    } else {
        if (!zk_client_->SetNodeValue(zk_zone_data_path_ + "/follower", value)) {
            PDLOG(WARNING, "set zk failed, save follower value failed");
            response->set_code(304);
            response->set_msg("set zk failed");
            return;
        }
    }
    PDLOG(INFO, "current cluster mode is [%s]", ServerMode_Name(zone_info_.mode()).c_str());
    zone_info_.set_mode(request->sm());
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        // notify table leave follower mode, leader table will be writeable.
        mode_.store(request->sm(), std::memory_order_release);
        thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
    } else {
        mode_.store(request->sm(), std::memory_order_release);
    }
    PDLOG(INFO, "set new cluster mode [%s]", ServerMode_Name(request->sm()).c_str());
    response->set_code(0);
    return;
}

void NameServerImpl::SyncTable(RpcController* controller,
        const SyncTableRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(300);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) != kLEADER) {
        response->set_code(454);
        response->set_msg("cur nameserver is not leader mode");
        PDLOG(WARNING, "cur nameserver is not leader mode");
        return;
    }
    int code = 0;
    std::string msg = "ok";
    std::string name = request->name();
    std::string cluster_alias = request->cluster_alias();
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    do {
        std::shared_ptr<::rtidb::client::NsClient> client;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto iter = table_info_.find(name);
            if (iter == table_info_.end()) {
                response->set_code(100);
                response->set_msg("table is not exist!");
                PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
                return;
            }
            table_info = iter->second;
            auto it = nsc_.find(cluster_alias);
            if(it == nsc_.end()) {
                code = 404;
                msg = "replica name not found";
                PDLOG(WARNING, "replica name [%s] not found when synctable [%s]", cluster_alias.c_str(), name.c_str());
                break;
            }
            if (it->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                code = 507;
                msg = "replica cluster not healthy";
                PDLOG(WARNING, "replica cluster [%s] not healthy when syntable [%s]", cluster_alias.c_str(), name.c_str());
                break;
            }
            client = std::atomic_load_explicit(&it->second->client_, std::memory_order_relaxed);
        }
        std::vector<::rtidb::nameserver::TableInfo> tables;
        if (!client->ShowTable("", tables, msg)) {
            code = 455;
            msg = "showtable error when synctable";
            PDLOG(WARNING, "replica cluster [%s] showtable error when synctable [%s]", cluster_alias.c_str(), name.c_str());
            break;
        }
        std::vector<std::string> table_name_vec;
        for (auto& rkv : tables) {
            table_name_vec.push_back(rkv.name());
        }
        if (request->has_pid()) {
            if (std::find(table_name_vec.begin(), table_name_vec.end(), name) != table_name_vec.end()) {
                PDLOG(INFO, "table [%s] already exists in replica cluster [%s]", name.c_str(), cluster_alias.c_str());
                uint32_t pid = request->pid();
                if (SyncExistTable(name, tables, *table_info, pid, code, msg) < 0) {
                    break;
                }
            } else {
                PDLOG(INFO, "table [%s] does not exist in replica cluster [%s]", name.c_str(), cluster_alias.c_str());
                code = 508;
                msg = "replica cluster has no table, do not need pid";
                PDLOG(WARNING, "replica cluster has no table [%s], do not need pid", name.c_str()); 
                break;
            }
        } else {
            for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
                ::rtidb::nameserver::TablePartition table_partition_local = table_info->table_partition(idx);
                for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                    if (table_partition_local.partition_meta(midx).is_leader() &&
                            (!table_partition_local.partition_meta(midx).is_alive())) {
                        code = 509;
                        msg = "local table has a no alive leader partition";
                        PDLOG(WARNING, "local table [%s] pid [%u] has a no alive leader partition", 
                                table_info->name().c_str(), table_partition_local.pid());
                        response->set_code(code);
                        response->set_msg(msg);
                        return;
                    }
                }
            }
            if (std::find(table_name_vec.begin(), table_name_vec.end(), name) != table_name_vec.end()) {
                PDLOG(INFO, "table [%s] already exists in replica cluster [%s]", name.c_str(), cluster_alias.c_str());
                if (SyncExistTable(name, tables, *table_info, UINT32_MAX, code, msg) < 0) {
                    break;
                }
            } else {
                PDLOG(INFO, "table [%s] does not exist in replica cluster [%s]", name.c_str(), cluster_alias.c_str());
                ::rtidb::nameserver::TableInfo table_info_r(*table_info); 
                //get remote table_info: tid and leader partition info
                std::string msg;
                if (!client->CreateRemoteTableInfo(zone_info_, table_info_r, msg)) {
                    code = 510;
                    msg = "create remote table info failed";
                    PDLOG(WARNING, "create remote table_info error, wrong msg is [%s]", msg.c_str()); 
                    break;
                }
                std::lock_guard<std::mutex> lock(mu_);
                for (int idx = 0; idx < table_info_r.table_partition_size(); idx++) {
                    ::rtidb::nameserver::TablePartition table_partition = table_info_r.table_partition(idx);
                    if (AddReplicaRemoteOP(cluster_alias, table_info_r.name(), table_partition,
                                table_info_r.tid(), table_partition.pid()) < 0) {
                        code = 511;
                        msg = "create AddReplicaRemoteOP failed";
                        PDLOG(INFO, "create AddReplicaRemoteOP failed. table[%s] pid[%u]", name.c_str(), table_partition.pid());
                        response->set_code(code);
                        response->set_msg(msg);
                        return;
                    }
                }
            }
        }
    }while(0);
    response->set_code(code);
    response->set_msg(msg);
}

int NameServerImpl::SyncExistTable(const std::string& name,
        const std::vector<::rtidb::nameserver::TableInfo> tables_remote, 
        const ::rtidb::nameserver::TableInfo& table_info_local, 
        uint32_t pid, 
        int& code,
        std::string& msg) {
    std::vector<::rtidb::nameserver::TableInfo> table_vec;
    ::rtidb::nameserver::TableInfo table_info_remote;
    for (const auto& table : tables_remote) {
        if (table.name() == name) {
            table_vec.push_back(table);
            table_info_remote = table;
            break;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!CompareTableInfo(table_vec)) {
            PDLOG(WARNING, "compare table info error");
            msg = "compare table info error";
            code = 567;
            return -1;
        }
        //TODO: after merge feat/110-binlog-vs-snapshot-offset into develop
        /**
          if (!CompareSnapshotOffset(table_vec, msg, code, tablet_part_offset)) {
          return -1;
          }
          */
    }
    std::vector<uint32_t> pid_vec;
    if (pid == UINT32_MAX) {
        for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
            pid_vec.push_back(table_info_remote.table_partition(idx).pid());
        }
    } else {
        pid_vec.push_back(pid);
    }
    for (const auto& cur_pid : pid_vec) {
        bool has_pid = false;
        for (int idx = 0; idx < table_info_local.table_partition_size(); idx++) {
            ::rtidb::nameserver::TablePartition table_partition_local = table_info_local.table_partition(idx);
            if (table_partition_local.pid() == cur_pid) {
                has_pid = true;
                for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                    if (table_partition_local.partition_meta(midx).is_leader() &&
                            (!table_partition_local.partition_meta(midx).is_alive())) {
                        code = 509;
                        msg = "local table has a no alive leader partition";
                        PDLOG(WARNING, "table [%s] pid [%u] has a no alive leader partition", 
                                name.c_str(), table_partition_local.pid());
                        return -1;
                    }
                }
                break;
            }
        }
        if (!has_pid) {
            code = 512;
            msg = "table has no current pid";
            PDLOG(WARNING, "table [%s] has no pid [%u]", 
                    name.c_str(), cur_pid);
            return -1;
        }
        //remote table
        for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
            ::rtidb::nameserver::TablePartition table_partition = table_info_remote.table_partition(idx);
            if (table_partition.pid() == cur_pid) {
                for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                    if (table_partition.partition_meta(midx).is_leader()) { 
                        if (!table_partition.partition_meta(midx).is_alive()) {
                            code = 514;
                            msg = "remote table has a no alive leader partition";
                            PDLOG(WARNING, "remote table [%s] has a no alive leader partition pid[%u]", 
                                    name.c_str(), cur_pid);
                            return -1;
                        }
                    }
                }
                break;
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& cur_pid : pid_vec) {
            for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
                ::rtidb::nameserver::TablePartition table_partition = table_info_remote.table_partition(idx);
                if (table_partition.pid() == cur_pid) {
                    for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                        if (table_partition.partition_meta(midx).is_leader() && 
                                table_partition.partition_meta(midx).is_alive()) {
                            if (AddReplicaSimplyRemoteOP(name, table_partition.partition_meta(midx).endpoint(), 
                                        table_info_remote.tid(), cur_pid) < 0) {
                                PDLOG(WARNING, "create AddReplicasSimplyRemoteOP failed. table[%s] pid[%u]", 
                                        name.c_str(), cur_pid);
                                code = 513;
                                msg = "create AddReplicasSimplyRemoteOP failed";
                                return -1;
                            }
                        }
                    }
                    break;
                }
            }
        }
    }
    return 0;
}

void NameServerImpl::DistributeTabletMode() {
    if (!running_.load(std::memory_order_acquire)) {
        return;
    }
    decltype(tablets_) tmp_tablets;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& tablet : tablets_) {
            if (tablet.second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                continue;
            }
            tmp_tablets.insert(std::make_pair(tablet.first, tablet.second));
        }
    }
    bool mode = mode_.load(std::memory_order_acquire) == kFOLLOWER ? true : false;
    for (const auto& tablet : tmp_tablets) {
        if (!tablet.second->client_->SetMode(mode)) {
            PDLOG(WARNING, "set tablet %s mode failed!", tablet.first.c_str());
        }
    }

}
bool NameServerImpl::CreateTableRemote(const ::rtidb::api::TaskInfo& task_info,
        const ::rtidb::nameserver::TableInfo& table_info, 
        const std::shared_ptr<::rtidb::nameserver::ClusterInfo> cluster_info) {
    return cluster_info->CreateTableRemote(task_info, table_info, zone_info_);
}

bool NameServerImpl::DropTableRemote(const ::rtidb::api::TaskInfo& task_info, 
        const std::string& name, 
        const std::shared_ptr<::rtidb::nameserver::ClusterInfo> cluster_info) {
    auto iter = cluster_info->last_status.find(name);
    if (iter != cluster_info->last_status.end()) {
        cluster_info->last_status.erase(iter);
    }
    return cluster_info->DropTableRemote(task_info, name, zone_info_);
}

void NameServerImpl::MakeTablePartitionSnapshot(uint32_t pid, uint64_t end_offset, std::shared_ptr<::rtidb::nameserver::TableInfo> table_info) {
    for(const auto& part : table_info->table_partition()) {
        if (part.pid() != pid) {
            continue;
        }
        for(const auto& meta : part.partition_meta()) {
            if (!meta.is_alive()) {
                continue;
            }
            std::shared_ptr<TabletClient> client;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto tablet_iter = tablets_.find(meta.endpoint());
                if (tablet_iter == tablets_.end()) {
                    PDLOG(WARNING, "tablet[%s] not found in tablets", meta.endpoint().c_str());
                    continue;
                }
                client = tablet_iter->second->client_;
            }
            client->MakeSnapshot(table_info->tid(), pid, end_offset,std::shared_ptr<rtidb::api::TaskInfo>());
        }
    }
}

}
}
