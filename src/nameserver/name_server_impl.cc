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

#include "nameserver/name_server_impl.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <random>
#include <set>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "nameserver/system_table.h"
#include "sdk/db_sdk.h"
#include "statistics/query_response_time/deploy_query_response_time.h"
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

#include <utility>

#include "base/glog_wrapper.h"
#include "base/proto_util.h"
#include "base/status.h"
#include "base/strings.h"
#include "boost/algorithm/string.hpp"
#include "boost/bind.hpp"
#include "codec/row_codec.h"
#include "gflags/gflags.h"
#include "schema/index_util.h"
#include "schema/schema_adapter.h"

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_string(zk_auth_schema);
DECLARE_string(zk_cert);
DECLARE_string(tablet);
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
DECLARE_uint32(get_table_status_interval);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_uint32(check_binlog_sync_progress_delta);
DECLARE_uint32(name_server_op_execute_timeout);
DECLARE_uint32(get_replica_status_interval);
DECLARE_int32(make_snapshot_time);
DECLARE_int32(make_snapshot_check_interval);
DECLARE_bool(use_name);
DECLARE_bool(enable_distsql);

namespace openmldb {
namespace nameserver {

using ::openmldb::base::ReturnCode;

const std::string OFFLINE_LEADER_ENDPOINT = "OFFLINE_LEADER_ENDPOINT";  // NOLINT
constexpr uint8_t MAX_ADD_TABLE_FIELD_COUNT = 63;
constexpr uint32_t SEQ_TASK_CHECK_INTERVAL = 100;  // 100ms

void NameServerImpl::CheckSyncExistTable(const std::string& alias,
                                         const std::vector<::openmldb::nameserver::TableInfo>& tables_remote,
                                         const std::shared_ptr<::openmldb::client::NsClient> ns_client) {
    for (const TableInfo& table_info_remote : tables_remote) {
        std::string name = table_info_remote.name();
        std::string db = table_info_remote.db();

        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info_local;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (!GetTableInfoUnlock(name, db, &table_info_local)) {
                PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
                continue;
            }
        }
        bool is_continue = false;
        // remote table
        for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
            const ::openmldb::nameserver::TablePartition& table_partition = table_info_remote.table_partition(idx);
            for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                if (table_partition.partition_meta(midx).is_leader() &&
                    (!table_partition.partition_meta(midx).is_alive())) {
                    PDLOG(WARNING,
                          "remote table [%s] has a no alive leader partition "
                          "pid[%u]",
                          name.c_str(), table_partition.pid());
                    is_continue = true;
                    break;
                }
            }
        }
        if (is_continue) {
            PDLOG(WARNING, "table [%s] does not sync to replica cluster [%s]", name.c_str(), alias.c_str());
            continue;
        }
        for (int idx = 0; idx < table_info_local->table_partition_size(); idx++) {
            const ::openmldb::nameserver::TablePartition& table_partition_local =
                table_info_local->table_partition(idx);
            for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                if (table_partition_local.partition_meta(midx).is_leader() &&
                    (!table_partition_local.partition_meta(midx).is_alive())) {
                    PDLOG(WARNING, "table [%s] pid [%u] has a no alive leader partition", name.c_str(),
                          table_partition_local.pid());
                    is_continue = true;
                    break;
                }
            }
        }
        if (is_continue) {
            PDLOG(WARNING, "table [%s] does not sync to replica cluster [%s]", name.c_str(), alias.c_str());
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
                const ::openmldb::nameserver::TablePartition& table_partition = table_info_remote.table_partition(idx);
                uint32_t cur_pid = table_partition.pid();
                for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                    if (table_partition.partition_meta(midx).is_leader() &&
                        table_partition.partition_meta(midx).is_alive()) {
                        if (AddReplicaSimplyRemoteOP(alias, name, db, table_partition.partition_meta(midx).endpoint(),
                                                     table_info_remote.tid(), cur_pid) < 0) {
                            PDLOG(WARNING,
                                  "create AddReplicasSimplyRemoteOP failed. "
                                  "table[%s] pid[%u] alias[%s]",
                                  name.c_str(), cur_pid, alias.c_str());
                            break;
                        }
                    }
                }
            }
        }
    }
}

void NameServerImpl::TableInfoToVec(
    const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_infos,
    const std::vector<uint32_t>& table_tid_vec, std::vector<::openmldb::nameserver::TableInfo>* local_table_info_vec) {
    for (const auto& kv : table_infos) {
        if (std::find(table_tid_vec.begin(), table_tid_vec.end(), kv.second->tid()) == table_tid_vec.end()) {
            bool has_no_alive_leader_partition = false;
            for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
                const ::openmldb::nameserver::TablePartition& table_partition_local = kv.second->table_partition(idx);
                for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                    if (table_partition_local.partition_meta(midx).is_leader() &&
                        (!table_partition_local.partition_meta(midx).is_alive())) {
                        has_no_alive_leader_partition = true;
                        PDLOG(WARNING,
                              "table [%s] pid [%u] has a no alive leader "
                              "partition",
                              kv.second->name().c_str(), table_partition_local.pid());
                        break;
                    }
                }
                if (has_no_alive_leader_partition) {
                    break;
                }
            }
            if (!has_no_alive_leader_partition) {
                local_table_info_vec->push_back(*(kv.second));
            }
        }
    }
}

void NameServerImpl::CheckSyncTable(const std::string& alias,
                                    const std::vector<::openmldb::nameserver::TableInfo> tables,
                                    const std::shared_ptr<::openmldb::client::NsClient> ns_client) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info_.empty() && db_table_info_.empty()) {
            PDLOG(INFO, "leader cluster has no table");
            return;
        }
    }
    std::vector<uint32_t> table_tid_vec;
    for (auto& rkv : tables) {
        table_tid_vec.push_back(rkv.tid());
    }
    std::vector<::openmldb::nameserver::TableInfo> local_table_info_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        TableInfoToVec(table_info_, table_tid_vec, &local_table_info_vec);
        for (const auto& kv : db_table_info_) {
            TableInfoToVec(kv.second, table_tid_vec, &local_table_info_vec);
        }
    }
    for (const auto& table_tmp : local_table_info_vec) {
        ::openmldb::nameserver::TableInfo table_info(table_tmp);
        // get remote table_info: tid and leader partition info
        std::string msg;
        if (!ns_client->CreateRemoteTableInfo(zone_info_, table_info, msg)) {
            PDLOG(WARNING, "create remote table_info erro, wrong msg is [%s]", msg.c_str());
            return;
        }
        std::lock_guard<std::mutex> lock(mu_);
        for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
            const ::openmldb::nameserver::TablePartition& table_partition = table_info.table_partition(idx);
            AddReplicaRemoteOP(alias, table_info.name(), table_info.db(), table_partition, table_info.tid(),
                               table_partition.pid());
        }
    }
}

void NameServerImpl::CheckTableInfo(std::shared_ptr<ClusterInfo>& ci,
                                    const std::vector<::openmldb::nameserver::TableInfo>& tables) {
    for (const auto& table : tables) {
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
        if (!GetTableInfoUnlock(table.name(), table.db(), &table_info)) {
            PDLOG(WARNING, "table [%u][%s] not found in table_info", table.tid(), table.name().c_str());
            continue;
        }
        auto status_iter = ci->last_status[table.db()].find(table.name());
        if (status_iter == ci->last_status[table.db()].end()) {
            std::vector<TablePartition> tbs;
            for (const auto& part : table_info->table_partition()) {
                for (const auto& meta : part.remote_partition_meta()) {
                    if (meta.alias() == ci->cluster_add_.alias()) {
                        TablePartition tb;
                        tb.set_pid(part.pid());
                        PartitionMeta* m = tb.add_partition_meta();
                        m->CopyFrom(meta);
                        tbs.push_back(tb);
                        break;
                    }
                }
            }
            if (tbs.size() != table.partition_num()) {
                continue;
            }
            ci->last_status[table.db()].insert(std::make_pair(table.name(), tbs));
        } else {
            // cache endpoint
            std::set<uint32_t> parts;
            for (const auto& part : table_info->table_partition()) {
                for (auto& meta : part.partition_meta()) {
                    if (meta.is_leader() && meta.is_alive()) {
                        parts.insert(part.pid());
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
                    continue;  // leader partition is offline, can't add talbe
                               // replica
                }
                for (auto& meta : part.partition_meta()) {
                    if (meta.is_leader() && meta.is_alive()) {
                        auto iter = part_refer.find(part.pid());
                        if (iter == part_refer.end()) {
                            PDLOG(WARNING, "table [%s] pid [%u] not found", table.name().c_str(), part.pid());
                            break;
                        }
                        if (iter->second->partition_meta_size() < 1) {
                            PDLOG(WARNING, "table [%s] pid [$u] meta size is %d", table.name().c_str(), part.pid(),
                                  iter->second->partition_meta_size());
                            break;
                        }
                        std::string endpoint = iter->second->partition_meta(0).endpoint();
                        if (meta.endpoint() == endpoint) {
                            break;
                        }
                        PDLOG(INFO, "table [%s] pid[%u] will remove endpoint %s", table.name().c_str(), part.pid(),
                              endpoint.c_str());
                        DelReplicaRemoteOP(endpoint, table.name(), table.db(), part.pid());
                        iter->second->clear_partition_meta();
                        iter->second->add_partition_meta()->CopyFrom(meta);

                        PDLOG(INFO, "table [%s] pid[%u] will add remote endpoint %s", table.name().c_str(), part.pid(),
                              meta.endpoint().c_str());
                        AddReplicaSimplyRemoteOP(ci->cluster_add_.alias(), table.name(), table.db(), meta.endpoint(),
                                                 table.tid(), part.pid());
                        break;
                    }
                }
            }
        }
    }
}

bool NameServerImpl::CompareSnapshotOffset(
    const std::vector<TableInfo>& tables, std::string& msg, int& code,
    std::map<std::string, std::map<uint32_t, std::map<uint32_t, uint64_t>>>& table_part_offset) {
    for (const auto& table : tables) {
        // iter == table_info_.end() is impossible, because CompareTableInfo has
        // checked it
        std::map<uint32_t, uint64_t> pid_offset;
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
        if (!GetTableInfoUnlock(table.name(), table.db(), &table_info)) {
            PDLOG(WARNING, "table [%s] not found in table_info", table.name().c_str());
            return false;
        }
        int32_t tid = table_info->tid();
        for (const auto& part : table_info->table_partition()) {
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
                        PDLOG(WARNING, "tid [%u] pid [%u] not found on tablet %s", tid, part.pid(),
                              meta.endpoint().c_str());
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
                        PDLOG(WARNING,
                              "table [%s] pid [%u] offset less than local "
                              "table snapshot",
                              table.name().c_str(), part.pid());
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

bool NameServerImpl::CompareTableInfo(const std::vector<::openmldb::nameserver::TableInfo>& tables, bool period_check) {
    for (auto& table : tables) {
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
        if (!GetTableInfoUnlock(table.name(), table.db(), &table_info)) {
            PDLOG(WARNING, "table [%s] not found in table_info_", table.name().c_str());
            if (period_check) {
                continue;
            }
            return false;
        }
        if (table.table_partition_size() != table_info->table_partition_size()) {
            PDLOG(WARNING, "table [%s] partition num not equal, remote [%d] local [%d]", table.name().c_str(),
                  table.table_partition_size(), table_info->table_partition_size());
            return false;
        }
        if (table.compress_type() != table_info->compress_type()) {
            PDLOG(WARNING, "table [%s] compress type not equal", table.name().c_str());
            return false;
        }
        if (table.column_desc_size() != table_info->column_desc_size()) {
            PDLOG(WARNING, "table [%s] column desc size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < table_info->column_desc_size(); i++) {
                std::string name = table_info->column_desc(i).name();
                std::string value;
                table_info->column_desc(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& column : table.column_desc()) {
                auto iter = tmp_map.find(column.name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING,
                          "table [%s] not found column desc [%s] in local "
                          "cluster",
                          table.name().c_str(), column.name().c_str());
                    return false;
                }
                if (column.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(),
                          column.name().c_str());
                    return false;
                }
            }
        }
        if (table.column_desc_size() != table_info->column_desc_size()) {
            PDLOG(WARNING, "table [%s] column desc v1 size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < table_info->column_desc_size(); i++) {
                std::string name = table_info->column_desc(i).name();
                std::string value;
                table_info->column_desc(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& column_v1 : table.column_desc()) {
                auto iter = tmp_map.find(column_v1.name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING,
                          "table [%s] not found column desc [%s] in local "
                          "cluster",
                          table.name().c_str(), column_v1.name().c_str());
                    return false;
                }
                if (column_v1.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(),
                          column_v1.name().c_str());
                    return false;
                }
            }
        }
        if (table.column_key_size() != table_info->column_key_size()) {
            PDLOG(WARNING, "table [%s] column key size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < table_info->column_key_size(); i++) {
                std::string name = table_info->column_key(i).index_name();
                std::string value;
                table_info->column_key(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& key : table.column_key()) {
                auto iter = tmp_map.find(key.index_name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING,
                          "table [%s] not found column desc [%s] in local "
                          "cluster",
                          table.name().c_str(), key.index_name().c_str());
                    return false;
                }
                if (key.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(),
                          key.index_name().c_str());
                    return false;
                }
            }
        }
        if (table.added_column_desc_size() != table_info->added_column_desc_size()) {
            PDLOG(WARNING, "table [%s] added column desc size not equal", table.name().c_str());
            return false;
        }
        {
            std::map<std::string, std::string> tmp_map;
            for (int i = 0; i < table_info->added_column_desc_size(); i++) {
                std::string name = table_info->added_column_desc(i).name();
                std::string value;
                table_info->added_column_desc(i).SerializeToString(&value);
                tmp_map.insert(std::make_pair(name, value));
            }
            for (auto& added_column : table.added_column_desc()) {
                auto iter = tmp_map.find(added_column.name());
                if (iter == tmp_map.end()) {
                    PDLOG(WARNING,
                          "table [%s] not found column desc [%s] in local "
                          "cluster",
                          table.name().c_str(), added_column.name().c_str());
                    return false;
                }
                if (added_column.SerializeAsString() != iter->second) {
                    PDLOG(WARNING, "table [%s] column desc [%s] not equal", table.name().c_str(),
                          added_column.name().c_str());
                    return false;
                }
            }
        }
    }
    return true;
}

NameServerImpl::NameServerImpl()
    : zk_client_(nullptr),
      dist_lock_(nullptr),
      thread_pool_(1),
      task_thread_pool_(FLAGS_name_server_task_pool_size),
      rand_(0xdeadbeef),
      startup_mode_(::openmldb::type::StartupMode::kStandalone) {}

NameServerImpl::~NameServerImpl() {
    running_.store(false, std::memory_order_release);
    thread_pool_.Stop(true);
    task_thread_pool_.Stop(true);
    if (dist_lock_ != NULL) {
        dist_lock_->Stop();
        delete dist_lock_;
    }
    delete zk_client_;
}

// become name server leader
bool NameServerImpl::Recover() {
    if (startup_mode_ == ::openmldb::type::StartupMode::kStandalone) {
        PDLOG(INFO, "skip recover in standalone mode");
        return true;
    }
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        PDLOG(WARNING, "get endpoints node failed!");
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);

        std::string value;
        if (zk_client_->GetNodeValue(zk_path_.zone_data_path_ + "/follower", value)) {
            zone_info_.ParseFromString(value);
            mode_.store(zone_info_.mode(), std::memory_order_release);
            PDLOG(WARNING, "recover zone info : %s", value.c_str());
        }
        UpdateTablets(endpoints);
        value.clear();
        if (!zk_client_->GetNodeValue(zk_path_.table_index_node_, value)) {
            if (!zk_client_->CreateNode(zk_path_.table_index_node_, "1")) {
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
        if (!zk_client_->GetNodeValue(zk_path_.term_node_, value)) {
            if (!zk_client_->CreateNode(zk_path_.term_node_, "1")) {
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
        if (!zk_client_->GetNodeValue(zk_path_.op_index_node_, value)) {
            if (!zk_client_->CreateNode(zk_path_.op_index_node_, "0")) {
                PDLOG(WARNING, "create op index node failed!");
                return false;
            }
            op_index_ = 0;
            PDLOG(INFO, "init op_index[%u]", op_index_);
        } else {
            op_index_ = std::stoull(value);
            PDLOG(INFO, "recover op_index[%u]", op_index_);
        }
        value.clear();
        if (!zk_client_->GetNodeValue(zk_path_.table_changed_notify_node_, value)) {
            if (!zk_client_->CreateNode(zk_path_.table_changed_notify_node_, "1")) {
                PDLOG(WARNING, "create zk table changed notify node failed");
                return false;
            }
        }
        value.clear();
        if (!zk_client_->GetNodeValue(zk_path_.globalvar_changed_notify_node_, value)) {
            if (!zk_client_->CreateNode(zk_path_.globalvar_changed_notify_node_, "1")) {
                PDLOG(WARNING, "create globalvar changed notify node failed");
                return false;
            }
        }
        if (!zk_client_->GetNodeValue(zk_path_.auto_failover_node_, value)) {
            auto_failover_.load(std::memory_order_acquire) ? value = "true" : value = "false";
            if (!zk_client_->CreateNode(zk_path_.auto_failover_node_, value)) {
                PDLOG(WARNING, "create auto failover node failed!");
                return false;
            }
            PDLOG(INFO, "set zk_auto_failover_node[%s]", value.c_str());
        } else {
            value == "true" ? auto_failover_.store(true, std::memory_order_release)
                            : auto_failover_.store(false, std::memory_order_release);
            PDLOG(INFO, "get zk_auto_failover_node[%s]", value.c_str());
        }
        if (!RecoverDb()) {
            PDLOG(WARNING, "recover db failed!");
            return false;
        }
        if (!RecoverTableInfo()) {
            PDLOG(WARNING, "recover table info failed!");
            return false;
        }
        if (!RecoverProcedureInfo()) {
            PDLOG(WARNING, "recover store procedure info failed!");
            return false;
        }
        UpdateSdkEpMap();
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
    UpdateRealEpMapToTablet(false);
    if (FLAGS_use_name) {
        UpdateRemoteRealEpMap();
    }
    UpdateTaskStatus(true);
    if (!RecoverExternalFunction()) {
        return false;
    }
    return true;
}

bool NameServerImpl::RecoverExternalFunction() {
    std::vector<std::string> functions;
    if (zk_client_->IsExistNode(zk_path_.external_function_path_) == 0) {
        if (!zk_client_->GetChildren(zk_path_.external_function_path_, functions)) {
            LOG(WARNING) << "fail to get function list with path " << zk_path_.external_function_path_;
            return false;
        }
    }
    external_fun_.clear();
    if (functions.empty()) {
        return true;
    }
    for (const auto& name : functions) {
        std::string value;
        if (!zk_client_->GetNodeValue(zk_path_.external_function_path_ + "/" + name, value)) {
            LOG(WARNING) << "fail to get function data. function: " << name;
            continue;
        }
        auto fun = std::make_shared<::openmldb::common::ExternalFun>();
        if (!fun->ParseFromString(value)) {
            LOG(WARNING) << "fail to parse external function. function: " << name << " value: " << value;
            continue;
        }
        external_fun_.emplace(name, fun);
        LOG(INFO) << "recover function " << name;
    }
    return true;
}

bool NameServerImpl::RecoverDb() {
    databases_.clear();
    std::vector<std::string> db_vec;
    if (!zk_client_->GetChildren(zk_path_.db_path_, db_vec)) {
        if (zk_client_->IsExistNode(zk_path_.db_path_) > 0) {
            PDLOG(WARNING, "db node does not exist");
            return true;
        }
        PDLOG(WARNING, "get db failed!");
        return false;
    }
    PDLOG(INFO, "recover db num[%d]", db_vec.size());
    databases_.insert(db_vec.begin(), db_vec.end());
    return true;
}

void NameServerImpl::RecoverOfflineTablet() {
    offline_endpoint_map_.clear();
    for (const auto& tablet : tablets_) {
        if (tablet.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
            offline_endpoint_map_.insert(std::make_pair(tablet.first, tablet.second->ctime_));
            thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval,
                                   boost::bind(&NameServerImpl::OnTabletOffline, this, tablet.first, false));
            PDLOG(INFO, "recover offlinetablet. endpoint %s", tablet.first.c_str());
        }
    }
}

void NameServerImpl::RecoverClusterInfo() {
    nsc_.clear();
    std::vector<std::string> cluster_vec;
    if (!zk_client_->GetChildren(zk_path_.zone_data_path_ + "/replica", cluster_vec)) {
        if (zk_client_->IsExistNode(zk_path_.zone_data_path_ + "/replica") > 0) {
            PDLOG(WARNING, "cluster info node does not exist");
            return;
        }
        PDLOG(WARNING, "get cluster info failed!");
        return;
    }
    PDLOG(INFO, "need to recover cluster info[%d]", cluster_vec.size());

    std::string value, rpc_msg;
    for (const auto& alias : cluster_vec) {
        value.clear();
        if (!zk_client_->GetNodeValue(zk_path_.zone_data_path_ + "/replica/" + alias, value)) {
            PDLOG(WARNING, "get cluster info failed! name[%s]", alias.c_str());
            continue;
        }

        ::openmldb::nameserver::ClusterAddress cluster_add;
        cluster_add.ParseFromString(value);
        std::shared_ptr<::openmldb::nameserver::ClusterInfo> cluster_info =
            std::make_shared<::openmldb::nameserver::ClusterInfo>(cluster_add);
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
    db_table_info_.clear();
    std::vector<std::string> table_vec;
    std::vector<std::string> db_table_vec;
    if (!zk_client_->GetChildren(zk_path_.table_data_path_, table_vec)) {
        if (zk_client_->IsExistNode(zk_path_.table_data_path_) > 0) {
            PDLOG(WARNING, "table data node does not exist");
        } else {
            PDLOG(WARNING, "get table name failed!");
            return false;
        }
    }
    PDLOG(INFO, "need to recover default table num[%d]", table_vec.size());
    for (const auto& table_name : table_vec) {
        std::string table_name_node = zk_path_.table_data_path_ + "/" + table_name;
        std::string value;
        if (!zk_client_->GetNodeValue(table_name_node, value)) {
            PDLOG(WARNING, "get table info failed! name[%s] table node[%s]", table_name.c_str(),
                  table_name_node.c_str());
            continue;
        }
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info =
            std::make_shared<::openmldb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            PDLOG(WARNING, "parse table info failed! name[%s] value[%s] value size[%d]", table_name.c_str(),
                  value.c_str(), value.length());
            continue;
        }
        table_info_.insert(std::make_pair(table_name, table_info));
        PDLOG(INFO, "recover table[%s] success", table_name.c_str());
    }
    if (!zk_client_->GetChildren(zk_path_.db_table_data_path_, db_table_vec)) {
        if (zk_client_->IsExistNode(zk_path_.db_table_data_path_) > 0) {
            PDLOG(WARNING, "db table data node does not exist");
        } else {
            PDLOG(WARNING, "get db table id failed!");
            return false;
        }
    }
    PDLOG(INFO, "need to recover db table num[%d]", db_table_vec.size());
    for (const auto& tid : db_table_vec) {
        std::string tid_node = zk_path_.db_table_data_path_ + "/" + tid;
        std::string value;
        if (!zk_client_->GetNodeValue(tid_node, value)) {
            PDLOG(WARNING, "get db table info failed! tid[%s] table node[%s]", tid.c_str(), tid_node.c_str());
            continue;
        }
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info =
            std::make_shared<::openmldb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            PDLOG(WARNING, "parse table info failed! tid[%s] value[%s] value size[%d]", tid.c_str(), value.c_str(),
                  value.length());
            continue;
        }
        if (databases_.find(table_info->db()) != databases_.end()) {
            db_table_info_[table_info->db()].insert(std::make_pair(table_info->name(), table_info));
            LOG(INFO) << "recover table tid " << tid << " with name " << table_info->name() << " in db "
                      << table_info->db();
        } else {
            LOG(WARNING) << "table " << table_info->name() << " not exist on recovering in db  " << table_info->db();
        }
    }
    return true;
}

bool NameServerImpl::RecoverOPTask() {
    for (auto& op_list : task_vec_) {
        op_list.clear();
    }
    std::vector<std::string> op_vec;
    if (!zk_client_->GetChildren(zk_path_.op_data_path_, op_vec)) {
        if (zk_client_->IsExistNode(zk_path_.op_data_path_) > 0) {
            PDLOG(WARNING, "op data node does not exist");
            return true;
        }
        PDLOG(WARNING, "get op failed!");
        return false;
    }
    PDLOG(INFO, "need to recover op num[%d]", op_vec.size());
    for (const auto& op_id : op_vec) {
        std::string op_node = zk_path_.op_data_path_ + "/" + op_id;
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
        if (op_data->op_info_.task_status() == ::openmldb::api::TaskStatus::kDone) {
            DEBUGLOG("op status is kDone. op_id[%lu]", op_data->op_info_.op_id());
            continue;
        }
        if (op_data->op_info_.task_status() == ::openmldb::api::TaskStatus::kCanceled) {
            DEBUGLOG("op status is kCanceled. op_id[%lu]", op_data->op_info_.op_id());
            continue;
        }
        std::string op_type_str = ::openmldb::api::OPType_Name(op_data->op_info_.op_type());
        switch (op_data->op_info_.op_type()) {
            case ::openmldb::api::OPType::kMakeSnapshotOP:
                if (CreateMakeSnapshotOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kAddReplicaOP:
                if (CreateAddReplicaOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kChangeLeaderOP:
                if (CreateChangeLeaderOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kMigrateOP:
                if (CreateMigrateTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kRecoverTableOP:
                if (CreateRecoverTableOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kOfflineReplicaOP:
                if (CreateOfflineReplicaTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kDelReplicaOP:
                if (CreateDelReplicaOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kReAddReplicaOP:
                if (CreateReAddReplicaTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kReAddReplicaNoSendOP:
                if (CreateReAddReplicaNoSendTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kReAddReplicaWithDropOP:
                if (CreateReAddReplicaWithDropTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kReAddReplicaSimplifyOP:
                if (CreateReAddReplicaSimplifyTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kReLoadTableOP:
                if (CreateReLoadTableTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kUpdatePartitionStatusOP:
                if (CreateUpdatePartitionStatusOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kCreateTableRemoteOP:
                if (CreateTableRemoteTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kDropTableRemoteOP:
                if (DropTableRemoteTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kDelReplicaRemoteOP:
                if (CreateDelReplicaRemoteOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kAddReplicaSimplyRemoteOP:
                if (CreateAddReplicaSimplyRemoteOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kAddReplicaRemoteOP:
                if (CreateAddReplicaRemoteOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            case ::openmldb::api::OPType::kAddIndexOP:
                if (!CreateAddIndexOPTask(op_data).OK()) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", op_type_str.c_str(), op_id);
                    continue;
                }
                break;
            default:
                PDLOG(WARNING, "unsupport recover op[%s]! op_id[%lu]", op_type_str.c_str(), op_id);
                continue;
        }
        if (!SkipDoneTask(op_data)) {
            PDLOG(WARNING, "SkipDoneTask task failed. op_id[%lu] task_index[%u]", op_data->op_info_.op_id(),
                  op_data->op_info_.task_index());
            continue;
        }
        if (op_data->op_info_.task_status() == ::openmldb::api::TaskStatus::kFailed ||
            op_data->op_info_.task_status() == ::openmldb::api::TaskStatus::kCanceled) {
            done_op_list_.push_back(op_data);
        } else {
            uint32_t idx = 0;
            if (op_data->op_info_.for_replica_cluster() == 1) {
                idx = op_data->op_info_.vec_idx();
                PDLOG(INFO,
                      "current task is for replica cluster, op_index [%lu] "
                      "op_type[%s]",
                      op_data->op_info_.op_id(), ::openmldb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
            } else {
                idx = op_data->op_info_.pid() % task_vec_.size();
                if (op_data->op_info_.has_vec_idx() && op_data->op_info_.vec_idx() < task_vec_.size()) {
                    idx = op_data->op_info_.vec_idx();
                }
            }
            task_vec_[idx].push_back(op_data);
        }
        PDLOG(INFO, "recover op[%s] success. op_id[%lu]",
              ::openmldb::api::OPType_Name(op_data->op_info_.op_type()).c_str(), op_data->op_info_.op_id());
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
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request.name(), request.db(), &table_info)) {
        PDLOG(WARNING, "get table info failed! name[%s]", request.name().c_str());
        return -1;
    }
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
    auto task = CreateTask<MakeSnapshotTaskMeta>(op_data->op_info_.op_id(), ::openmldb::api::OPType::kMakeSnapshotOP,
                                                 endpoint, tid, pid, end_offset);
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
    std::string op_type = ::openmldb::api::OPType_Name(op_data->op_info_.op_type());
    if (op_data->op_info_.task_status() == ::openmldb::api::kInited) {
        PDLOG(INFO, "op_id[%lu] op_type[%s] status is kInited, need not skip", op_id, op_type.c_str());
        return true;
    }
    uint32_t task_index = op_data->op_info_.task_index();
    if (op_data->task_list_.empty()) {
        PDLOG(WARNING, "skip task failed, task_list is empty. op_id[%lu] op_type[%s]", op_id, op_type.c_str());
        return false;
    }
    if (task_index > op_data->task_list_.size() - 1) {
        PDLOG(WARNING, "skip task failed. op_id[%lu] op_type[%s] task_index[%u]", op_id, op_type.c_str(), task_index);
        return false;
    }
    for (uint32_t idx = 0; idx < task_index; idx++) {
        op_data->task_list_.pop_front();
    }
    if (!op_data->task_list_.empty()) {
        std::shared_ptr<Task> task = op_data->task_list_.front();
        PDLOG(INFO, "cur task[%s]. op_id[%lu] op_type[%s]",
              ::openmldb::api::TaskType_Name(task->task_info_->task_type()).c_str(), op_id, op_type.c_str());
        if (op_data->op_info_.task_status() == ::openmldb::api::TaskStatus::kFailed) {
            task->task_info_->set_status(::openmldb::api::TaskStatus::kFailed);
            return true;
        }
        switch (task->task_info_->task_type()) {
            case ::openmldb::api::TaskType::kSelectLeader:
            case ::openmldb::api::TaskType::kUpdateLeaderInfo:
            case ::openmldb::api::TaskType::kUpdatePartitionStatus:
            case ::openmldb::api::TaskType::kUpdateTableInfo:
            case ::openmldb::api::TaskType::kRecoverTable:
            case ::openmldb::api::TaskType::kAddTableInfo:
            case ::openmldb::api::TaskType::kCheckBinlogSyncProgress:
                // execute the task again
                task->task_info_->set_status(::openmldb::api::TaskStatus::kInited);
                break;
            default:
                task->task_info_->set_status(::openmldb::api::TaskStatus::kDoing);
        }
    }
    return true;
}

void NameServerImpl::UpdateTabletsLocked(const std::vector<std::string>& endpoints) {
    std::lock_guard<std::mutex> lock(mu_);
    UpdateTablets(endpoints);
}

void NameServerImpl::UpdateTablets(const std::vector<std::string>& endpoints) {
    std::set<std::string> alive;
    std::vector<std::string> tablet_endpoints;
    std::string nearline_tablet_endpoint;
    for (const auto& endpoint : endpoints) {
        std::string cur_endpoint = endpoint;
        if (boost::starts_with(cur_endpoint, ::openmldb::base::NEARLINE_PREFIX)) {
            nearline_tablet_endpoint = endpoint.substr(::openmldb::base::NEARLINE_PREFIX.size());
            cur_endpoint = nearline_tablet_endpoint;
        } else {
            tablet_endpoints.push_back(cur_endpoint);
        }
        auto it = real_ep_map_.find(cur_endpoint);
        if (FLAGS_use_name) {
            std::string real_ep;
            if (!zk_client_->GetNodeValue(FLAGS_zk_root_path + "/map/names/" + cur_endpoint, real_ep)) {
                PDLOG(WARNING, "get tablet names value failed. endpint %s", cur_endpoint.c_str());
                continue;
            }
            if (it == real_ep_map_.end()) {
                real_ep_map_.emplace(cur_endpoint, real_ep);
            } else {
                it->second = real_ep;
            }
        } else if (it == real_ep_map_.end()) {
            real_ep_map_.emplace(cur_endpoint, cur_endpoint);
        }
    }

    auto it = tablet_endpoints.begin();
    for (; it != tablet_endpoints.end(); ++it) {
        alive.insert(*it);
        Tablets::iterator tit = tablets_.find(*it);
        // register a new tablet
        if (tit == tablets_.end()) {
            std::shared_ptr<TabletInfo> tablet = std::make_shared<TabletInfo>();
            tablet->state_ = ::openmldb::type::EndpointState::kHealthy;
            if (FLAGS_use_name) {
                auto real_ep_map_it = real_ep_map_.find(*it);
                if (real_ep_map_it == real_ep_map_.end()) {
                    PDLOG(WARNING, "fail to get real endpoint. endpoint %s", it->c_str());
                    continue;
                }
                tablet->client_ = std::make_shared<::openmldb::client::TabletClient>(*it, real_ep_map_it->second, true);
            } else {
                tablet->client_ = std::make_shared<::openmldb::client::TabletClient>(*it, "", true);
            }
            if (tablet->client_->Init() != 0) {
                PDLOG(WARNING, "tablet client init error. endpoint[%s]", it->c_str());
                continue;
            }
            tablet->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            tablets_.insert(std::make_pair(*it, tablet));
            PDLOG(INFO, "add tablet client. endpoint[%s]", it->c_str());
            NotifyTableChanged(::openmldb::type::NotifyType::kTable);
        } else {
            if (tit->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                if (FLAGS_use_name) {
                    auto real_ep_map_it = real_ep_map_.find(*it);
                    if (real_ep_map_it == real_ep_map_.end()) {
                        PDLOG(WARNING, "fail to get real endpoint. endpoint %s", it->c_str());
                        continue;
                    }
                    // TODO(denglong) guarantee threadsafe
                    tit->second->client_ =
                        std::make_shared<::openmldb::client::TabletClient>(*it, real_ep_map_it->second, true);
                    if (tit->second->client_->Init() != 0) {
                        PDLOG(WARNING, "tablet client init error. endpoint[%s]", tit->first.c_str());
                        continue;
                    }
                }
                tit->second->state_ = ::openmldb::type::EndpointState::kHealthy;
                tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
                PDLOG(INFO, "tablet is online. endpoint[%s]", tit->first.c_str());
                thread_pool_.AddTask(boost::bind(&NameServerImpl::OnTabletOnline, this, tit->first));
            }
        }
        PDLOG(INFO, "healthy tablet with endpoint[%s]", it->c_str());
    }
    // handle offline tablet
    for (Tablets::iterator tit = tablets_.begin(); tit != tablets_.end(); ++tit) {
        if (alive.find(tit->first) == alive.end() && tit->second->state_ == ::openmldb::type::EndpointState::kHealthy) {
            // tablet offline
            PDLOG(INFO, "offline tablet with endpoint[%s]", tit->first.c_str());
            tit->second->state_ = ::openmldb::type::EndpointState::kOffline;
            tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            if (offline_endpoint_map_.find(tit->first) == offline_endpoint_map_.end()) {
                offline_endpoint_map_.insert(std::make_pair(tit->first, tit->second->ctime_));
                if (running_.load(std::memory_order_acquire)) {
                    thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval,
                                           boost::bind(&NameServerImpl::OnTabletOffline, this, tit->first, false));
                }
            } else {
                offline_endpoint_map_[tit->first] = tit->second->ctime_;
            }
        }
    }
    thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
    thread_pool_.AddTask(boost::bind(&NameServerImpl::UpdateRealEpMapToTablet, this, true));
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
        if (!startup_flag && tit->second->state_ == ::openmldb::type::EndpointState::kHealthy) {
            PDLOG(INFO, "endpoint %s is healthy, need not offline endpoint", endpoint.c_str());
            return;
        }
        if (table_info_.empty() && db_table_info_.empty()) {
            PDLOG(INFO, "endpoint %s has no table, need not offline endpoint", endpoint.c_str());
            return;
        }
        uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
        if (!startup_flag && cur_time < iter->second + FLAGS_tablet_heartbeat_timeout) {
            thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval,
                                   boost::bind(&NameServerImpl::OnTabletOffline, this, endpoint, false));
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
            PDLOG(WARNING,
                  "cannot find endpoint %s in offline endpoint map. need not "
                  "recover",
                  endpoint.c_str());
            return;
        }
        if (!zk_client_->GetNodeValue(zk_path_.root_path_ + "/nodes/" + endpoint, value)) {
            PDLOG(WARNING, "get tablet node value failed");
            offline_endpoint_map_.erase(iter);
            return;
        }
        if (table_info_.empty() && db_table_info_.empty()) {
            PDLOG(INFO, "endpoint %s has no table, need not recover endpoint", endpoint.c_str());
            offline_endpoint_map_.erase(iter);
            return;
        }
        if (!boost::starts_with(value, "startup_")) {
            uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
            if (cur_time < iter->second + FLAGS_tablet_heartbeat_timeout) {
                PDLOG(INFO,
                      "need not recover. endpoint[%s] cur_time[%lu] "
                      "offline_time[%lu]",
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

void NameServerImpl::RecoverEndpointDBInternal(
    const std::string& endpoint, bool need_restore, uint32_t concurrency,
    const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_info) {
    for (const auto& kv : table_info) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid = kv.second->table_partition(idx).pid();
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (kv.second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    if (kv.second->table_partition(idx).partition_meta(meta_idx).is_alive() &&
                        kv.second->table_partition(idx).partition_meta_size() > 1) {
                        PDLOG(INFO,
                              "table[%s] pid[%u] endpoint[%s] is alive, need "
                              "not recover",
                              kv.first.c_str(), pid, endpoint.c_str());
                        break;
                    }
                    PDLOG(INFO, "recover table[%s] pid[%u] endpoint[%s]", kv.first.c_str(), pid, endpoint.c_str());
                    bool is_leader = false;
                    if (kv.second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        is_leader = true;
                    }
                    uint64_t offset_delta = need_restore ? 0 : FLAGS_check_binlog_sync_progress_delta;
                    CreateRecoverTableOP(kv.first, kv.second->db(), pid, endpoint, is_leader, offset_delta,
                                         concurrency);
                    if (need_restore && is_leader) {
                        PDLOG(INFO, "restore table[%s] pid[%u] endpoint[%s]", kv.first.c_str(), pid, endpoint.c_str());
                        CreateChangeLeaderOP(kv.first, kv.second->db(), pid, endpoint, need_restore, concurrency);
                        CreateRecoverTableOP(kv.first, kv.second->db(), pid, OFFLINE_LEADER_ENDPOINT, true,
                                             FLAGS_check_binlog_sync_progress_delta, concurrency);
                    }
                    break;
                }
            }
        }
    }
}

void NameServerImpl::RecoverEndpointInternal(const std::string& endpoint, bool need_restore, uint32_t concurrency) {
    std::lock_guard<std::mutex> lock(mu_);
    RecoverEndpointDBInternal(endpoint, need_restore, concurrency, table_info_);
    for (const auto& kv : db_table_info_) {
        RecoverEndpointDBInternal(endpoint, need_restore, concurrency, kv.second);
    }
    // recover global variable after tablet restart
    std::shared_ptr<TableInfo> table_info;
    if (!GetTableInfoUnlock(GLOBAL_VARIABLES, INFORMATION_SCHEMA_DB, &table_info)) {
        PDLOG(WARNING, "global variable table does not exist!");
        return;
    }
    bool exist_globalvar = false;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                exist_globalvar = true;
                break;
            }
        }
    }
    if (!exist_globalvar) {
        NotifyTableChanged(::openmldb::type::NotifyType::kGlobalVar);
    }
}

void NameServerImpl::ShowTablet(RpcController* controller, const ShowTabletRequest* request,
                                ShowTabletResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    Tablets::iterator it = tablets_.begin();
    for (; it != tablets_.end(); ++it) {
        TabletStatus* status = response->add_tablets();
        status->set_endpoint(it->first);
        if (FLAGS_use_name) {
            auto n_it = real_ep_map_.find(it->first);
            if (n_it == real_ep_map_.end()) {
                status->set_real_endpoint("-");
            } else {
                status->set_real_endpoint(n_it->second);
            }
        }
        status->set_state(::openmldb::type::EndpointState_Name(it->second->state_));
        status->set_age(::baidu::common::timer::get_micros() / 1000 - it->second->ctime_);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

base::Status NameServerImpl::InsertUserRecord(const std::string& host, const std::string& user,
                                              const std::string& password) {
    std::shared_ptr<TableInfo> table_info;
    if (!GetTableInfo(USER_INFO_NAME, INTERNAL_DB, &table_info)) {
        return {ReturnCode::kTableIsNotExist, "user table does not exist"};
    }

    std::vector<std::string> row_values;
    row_values.push_back(host);
    row_values.push_back(user);
    row_values.push_back(password);
    row_values.push_back("");  // password_last_changed
    row_values.push_back("");  // password_expired_time
    row_values.push_back("");  // create_time
    row_values.push_back("");  // update_time
    row_values.push_back("");  // account_type
    row_values.push_back("");  // privileges
    row_values.push_back("");  // extra_info

    std::string encoded_row;
    codec::RowCodec::EncodeRow(row_values, table_info->column_desc(), 1, encoded_row);
    std::vector<std::pair<std::string, uint32_t>> dimensions;
    dimensions.push_back({host + "|" + user, 0});

    uint32_t tid = table_info->tid();
    auto table_partition = table_info->table_partition(0);  // only one partition for system table
    for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
        if (table_partition.partition_meta(meta_idx).is_leader() &&
            table_partition.partition_meta(meta_idx).is_alive()) {
            uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
            std::string endpoint = table_partition.partition_meta(meta_idx).endpoint();
            auto table_ptr = GetTablet(endpoint);
            if (!table_ptr->client_->Put(tid, 0, cur_ts, encoded_row, dimensions).OK()) {
                return {ReturnCode::kPutFailed, "failed to create initial user entry"};
            }
            break;
        }
    }
    return {};
}

bool NameServerImpl::Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint,
                          const std::string& real_endpoint) {
    if (zk_cluster.empty() && FLAGS_tablet.empty()) {
        PDLOG(WARNING, "zk cluster disabled and tablet is empty");
        return false;
    }
    endpoint_ = endpoint;
    running_.store(false, std::memory_order_release);
    if (!zk_cluster.empty()) {
        startup_mode_ = ::openmldb::type::StartupMode::kCluster;
        zk_path_.zk_cluster_ = zk_cluster;
        zk_path_.root_path_ = zk_path;
        std::string zk_table_path = zk_path + "/table";
        std::string zk_sp_path = zk_path + "/store_procedure";
        zk_path_.table_index_node_ = zk_table_path + "/table_index";
        zk_path_.table_data_path_ = zk_table_path + "/table_data";
        zk_path_.db_path_ = zk_path + "/db";
        zk_path_.db_table_data_path_ = zk_table_path + "/db_table_data";
        zk_path_.db_sp_data_path_ = zk_sp_path + "/db_sp_data";
        zk_path_.term_node_ = zk_table_path + "/term";
        std::string zk_op_path = zk_path + "/op";
        zk_path_.op_index_node_ = zk_op_path + "/op_index";
        zk_path_.op_data_path_ = zk_op_path + "/op_data";
        zk_path_.offline_endpoint_lock_node_ = zk_path + "/offline_endpoint_lock";
        std::string zk_config_path = zk_path + "/config";
        zk_path_.zone_data_path_ = zk_path + "/cluster";
        zk_path_.auto_failover_node_ = zk_config_path + "/auto_failover";
        zk_path_.table_changed_notify_node_ = zk_table_path + "/notify";
        zk_path_.globalvar_changed_notify_node_ = zk_path + "/notify/global_variable";
        zk_path_.external_function_path_ = zk_path + "/data/function";
        zone_info_.set_mode(kNORMAL);
        zone_info_.set_zone_name(endpoint + zk_path);
        zone_info_.set_replica_alias("");
        zone_info_.set_zone_term(1);
        LOG(INFO) << "zone name " << zone_info_.zone_name();
        zk_client_ = new ZkClient(zk_cluster, real_endpoint, FLAGS_zk_session_timeout, endpoint, zk_path,
                                  FLAGS_zk_auth_schema, FLAGS_zk_cert);
        if (!zk_client_->Init()) {
            PDLOG(WARNING, "fail to init zookeeper with cluster[%s]", zk_cluster.c_str());
            return false;
        }
        std::string value;
        std::vector<std::string> endpoints;
        if (!zk_client_->GetNodes(endpoints)) {
            zk_client_->CreateNode(zk_path + "/nodes", "");
        } else {
            UpdateTablets(endpoints);
        }
        zk_client_->WatchNodes(boost::bind(&NameServerImpl::UpdateTabletsLocked, this, _1));
        bool ok = zk_client_->WatchNodes();
        if (!ok) {
            PDLOG(WARNING, "fail to watch nodes");
            return false;
        }
        if (zk_client_->IsExistNode(zk_path_.external_function_path_) != 0) {
            if (!zk_client_->CreateNode(zk_path_.external_function_path_, "")) {
                LOG(WARNING) << "fail to create function node " << zk_path_.external_function_path_;
                return false;
            }
        }
        session_term_ = zk_client_->GetSessionTerm();

        thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
        dist_lock_ = new DistLock(zk_path + "/leader", zk_client_, boost::bind(&NameServerImpl::OnLocked, this),
                                  boost::bind(&NameServerImpl::OnLostLock, this), endpoint);
        dist_lock_->Lock();

    } else {
        const std::string& tablet_endpoint = FLAGS_tablet;
        startup_mode_ = ::openmldb::type::StartupMode::kStandalone;
        std::shared_ptr<TabletInfo> tablet = std::make_shared<TabletInfo>();
        tablet->state_ = ::openmldb::type::EndpointState::kHealthy;
        tablet->client_ = std::make_shared<::openmldb::client::TabletClient>(tablet_endpoint, "", true);
        if (tablet->client_->Init() != 0) {
            PDLOG(WARNING, "tablet client init error. endpoint[%s]", tablet_endpoint.c_str());
        }
        tablet->ctime_ = ::baidu::common::timer::get_micros() / 1000;
        tablets_.insert(std::make_pair(tablet_endpoint, tablet));
        PDLOG(INFO, "add tablet client. endpoint[%s]", tablet_endpoint.c_str());
        OnLocked();
    }
    mode_.store(kNORMAL, std::memory_order_release);
    auto_failover_.store(FLAGS_auto_failover, std::memory_order_release);
    task_rpc_version_.store(0, std::memory_order_relaxed);
    if (FLAGS_use_name) {
        auto n_it = real_ep_map_.find(FLAGS_endpoint);
        if (n_it == real_ep_map_.end()) {
            real_ep_map_.insert(std::make_pair(FLAGS_endpoint, real_endpoint));
        } else {
            n_it->second = real_endpoint;
        }
    }
    task_vec_.resize(FLAGS_name_server_task_max_concurrency + FLAGS_name_server_task_concurrency_for_replica_cluster);
    task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval,
                                boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    while (!GetTableInfo(::openmldb::nameserver::USER_INFO_NAME, ::openmldb::nameserver::INTERNAL_DB, &table_info)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return true;
}

bool NameServerImpl::Init(const std::string& real_endpoint) {
    return Init(FLAGS_zk_cluster, FLAGS_zk_root_path, FLAGS_endpoint, real_endpoint);
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
            PDLOG(WARNING, "watch node failed");
        }
    }
    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
}

int NameServerImpl::UpdateTaskStatus(bool is_recover_op) {
    std::map<std::string, std::shared_ptr<TabletClient>> client_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                DEBUGLOG("tablet[%s] is not Healthy", iter->first.c_str());
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
                    if (task->task_info_->status() != ::openmldb::api::kDoing) {
                        continue;
                    }
                    if (task->task_info_->has_endpoint() && task->task_info_->endpoint() == iter->first) {
                        PDLOG(WARNING,
                              "tablet is offline. update task status from[kDoing] to[kFailed]. "
                              "op_id[%lu], task_type[%s] endpoint[%s]",
                              op_data->op_info_.op_id(),
                              ::openmldb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                              iter->first.c_str());
                        task->task_info_->set_status(::openmldb::api::kFailed);
                    }
                }
            } else {
                client_map.insert(std::make_pair(iter->first, iter->second->client_));
            }
        }
    }
    uint64_t last_task_rpc_version = task_rpc_version_.load(std::memory_order_acquire);
    for (auto iter = client_map.begin(); iter != client_map.end(); ++iter) {
        ::openmldb::api::TaskStatusResponse response;
        // get task status from tablet
        if (iter->second->GetTaskStatus(response)) {
            std::lock_guard<std::mutex> lock(mu_);
            if (last_task_rpc_version != task_rpc_version_.load(std::memory_order_acquire)) {
                DEBUGLOG("task_rpc_version mismatch");
                break;
            }
            std::string endpoint = iter->first;
            for (const auto& op_list : task_vec_) {
                UpdateTask(op_list, endpoint, is_recover_op, response);
            }
        }
    }
    UpdateTaskStatusRemote(is_recover_op);
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_task_status_interval,
                                    boost::bind(&NameServerImpl::UpdateTaskStatus, this, false));
    }
    return 0;
}

int NameServerImpl::UpdateTaskStatusRemote(bool is_recover_op) {
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        return 0;
    }
    std::map<std::string, std::shared_ptr<::openmldb::client::NsClient>> client_map;
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
            client_map.emplace(iter->first,
                               std::atomic_load_explicit(&iter->second->client_, std::memory_order_relaxed));
        }
    }
    uint64_t last_task_rpc_version = task_rpc_version_.load(std::memory_order_acquire);
    for (auto iter = client_map.begin(); iter != client_map.end(); ++iter) {
        ::openmldb::api::TaskStatusResponse response;
        // get task status from replica cluster
        if (iter->second->GetTaskStatus(response)) {
            std::lock_guard<std::mutex> lock(mu_);
            if (last_task_rpc_version != task_rpc_version_.load(std::memory_order_acquire)) {
                DEBUGLOG("task_rpc_version mismatch");
                break;
            }
            std::string endpoint = iter->second->GetEndpoint();
            uint32_t index = 0;
            for (const auto& op_list : task_vec_) {
                index++;
                if (index <= FLAGS_name_server_task_max_concurrency) {
                    continue;
                }
                UpdateTask(op_list, endpoint, is_recover_op, response);
            }
        } else {
            if (response.has_msg()) {
                PDLOG(WARNING, "get task status faild : [%s]", response.msg().c_str());
            }
        }
    }
    return 0;
}

int NameServerImpl::UpdateTask(const std::list<std::shared_ptr<OPData>>& op_list, const std::string& endpoint,
                               bool is_recover_op, const ::openmldb::api::TaskStatusResponse& response) {
    if (op_list.empty()) {
        return -1;
    }
    std::shared_ptr<OPData> op_data = op_list.front();
    if (op_data->task_list_.empty()) {
        return -1;
    }
    std::shared_ptr<Task> task = op_data->task_list_.front();
    if (task->task_info_->status() != ::openmldb::api::kDoing) {
        return -1;
    }
    task->UpdateTaskStatus(response.task(), endpoint, is_recover_op);
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
        task->UpdateStatusFromSubTask();
        if (task->GetStatus() == ::openmldb::api::kDone) {
            uint32_t cur_task_index = op_data->op_info_.task_index();
            op_data->op_info_.set_task_index(cur_task_index + 1);
            std::string value;
            op_data->op_info_.SerializeToString(&value);
            std::string node = absl::StrCat(zk_path_.op_data_path_, "/", op_data->op_info_.op_id());
            if (zk_client_->SetNodeValue(node, value)) {
                PDLOG(INFO, "set zk status value success. node[%s] value[%s]", node.c_str(), value.c_str());
                op_data->task_list_.pop_front();
                continue;
            }
            // revert task index
            op_data->op_info_.set_task_index(cur_task_index);
            PDLOG(WARNING, "set zk status value failed! node[%s] op_id[%lu] op_type[%s] task_index[%u]", node.c_str(),
                  op_data->GetOpId(), op_data->GetReadableType().c_str(), op_data->op_info_.task_index());
        }
    }
    return 0;
}

void NameServerImpl::UpdateTaskMapStatus(uint64_t remote_op_id, uint64_t op_id,
                                         const ::openmldb::api::TaskStatus& status) {
    auto iter = task_map_.find(remote_op_id);
    if (iter == task_map_.end()) {
        DEBUGLOG("op [%lu] is not in task_map_", remote_op_id);
        return;
    }
    for (auto& task_info : iter->second) {
        for (int idx = 0; idx < task_info->rep_cluster_op_id_size(); idx++) {
            uint64_t rep_cluster_op_id = task_info->rep_cluster_op_id(idx);
            if (rep_cluster_op_id == op_id) {
                if (status == ::openmldb::api::kFailed || status == ::openmldb::api::kCanceled) {
                    task_info->set_status(status);
                    if (status == ::openmldb::api::kFailed) {
                        DEBUGLOG("update task status from[kDoing] to[kFailed]. op_id[%lu], task_type[%s]",
                                 task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
                    } else {
                        DEBUGLOG("update task status from[kDoing] to[kCanceled]. op_id[%lu], task_type[%s]",
                                 task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
                    }
                }
                if (idx == task_info->rep_cluster_op_id_size() - 1) {
                    if (status == ::openmldb::api::kDone && task_info->status() != ::openmldb::api::kFailed &&
                        task_info->status() != ::openmldb::api::kCanceled) {
                        task_info->set_status(status);
                        DEBUGLOG("update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                                 task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
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
                    UpdateTaskMapStatus(op_data->op_info_.remote_op_id(), op_data->op_info_.op_id(),
                                        ::openmldb::api::TaskStatus::kDone);
                }
            } else {
                std::shared_ptr<Task> task = op_data->task_list_.front();
                if (task->task_info_->status() == ::openmldb::api::kFailed ||
                    op_data->op_info_.task_status() == ::openmldb::api::kCanceled) {
                    done_task_vec.push_back(op_data->op_info_.op_id());
                    // for multi cluster -- leader cluster judge
                    if (op_data->op_info_.for_replica_cluster() == 1) {
                        done_task_vec_remote.push_back(op_data->op_info_.op_id());
                    }
                    // for multi cluster -- replica cluster judge
                    PDLOG(WARNING, "task failed or canceled. op_id[%lu], task_type[%s]", task->task_info_->op_id(),
                          ::openmldb::api::TaskType_Name(task->task_info_->task_type()).c_str());
                    if (op_data->op_info_.has_remote_op_id()) {
                        UpdateTaskMapStatus(op_data->op_info_.remote_op_id(), op_data->op_info_.op_id(),
                                            task->task_info_->status());
                    }
                }
            }
        }
        if (done_task_vec.empty()) {
            return 0;
        }
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                DEBUGLOG("tablet[%s] is not Healthy", iter->first.c_str());
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
        DEBUGLOG("tablet[%s] delete op success", (*iter)->GetEndpoint().c_str());
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
    std::vector<std::shared_ptr<::openmldb::client::NsClient>> client_vec;
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
        DEBUGLOG("replica cluster[%s] delete op success", (*iter)->GetEndpoint().c_str());
    }
    return 0;
}

void NameServerImpl::DeleteTask(const std::vector<uint64_t>& done_task_vec) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto op_id : done_task_vec) {
        std::shared_ptr<OPData> op_data;
        uint32_t index = 0;
        std::list<std::shared_ptr<OPData>>::iterator iter;
        for (uint32_t idx = 0; idx < task_vec_.size(); idx++) {
            if (task_vec_[idx].empty()) {
                continue;
            }
            for (iter = task_vec_[idx].begin(); iter != task_vec_[idx].end(); iter++) {
                if ((*iter)->GetOpId() == op_id) {
                    op_data = *iter;
                    index = idx;
                    break;
                }
            }
            if (op_data) {
                break;
            }
        }
        if (!op_data) {
            PDLOG(WARNING, "has not found op[%lu] in running op", op_id);
            continue;
        }
        std::string node = absl::StrCat(zk_path_.op_data_path_, "/", op_id);
        if (!op_data->task_list_.empty() && op_data->task_list_.front()->GetStatus() == ::openmldb::api::kFailed) {
            op_data->SetTaskStatus(::openmldb::api::kFailed);
            op_data->op_info_.set_end_time(::baidu::common::timer::now_time());
            PDLOG(WARNING, "set op[%s] status failed. op_id[%lu]", op_data->GetReadableType().c_str(), op_id);
            std::string value;
            op_data->op_info_.SerializeToString(&value);
            if (!zk_client_->SetNodeValue(node, value)) {
                PDLOG(WARNING, "set zk status value failed. node[%s] value[%s]", node.c_str(), value.c_str());
            }
            done_op_list_.push_back(op_data);
            task_vec_[index].erase(iter);
            PDLOG(INFO, "delete op[%lu] in running op", op_id);
        } else {
            if (zk_client_->DeleteNode(node)) {
                PDLOG(INFO, "delete zk op node[%s] success.", node.c_str());
                op_data->op_info_.set_end_time(::baidu::common::timer::now_time());
                if (op_data->GetTaskStatus() == ::openmldb::api::kDoing) {
                    op_data->SetTaskStatus(::openmldb::api::kDone);
                    op_data->task_list_.clear();
                }
                done_op_list_.push_back(op_data);
                task_vec_[index].erase(iter);
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
                if (op_data->task_list_.empty() || op_data->GetTaskStatus() == ::openmldb::api::kFailed ||
                    op_data->GetTaskStatus() == ::openmldb::api::kCanceled) {
                    continue;
                }
                if (op_data->GetTaskStatus() == ::openmldb::api::kInited) {
                    op_data->op_info_.set_start_time(::baidu::common::timer::now_time());
                    op_data->SetTaskStatus(::openmldb::api::kDoing);
                    std::string value;
                    op_data->op_info_.SerializeToString(&value);
                    std::string node = absl::StrCat(zk_path_.op_data_path_, "/", op_data->GetOpId());
                    if (!zk_client_->SetNodeValue(node, value)) {
                        PDLOG(WARNING, "set zk op status value failed. node[%s] value[%s]", node.c_str(),
                              value.c_str());
                        op_data->SetTaskStatus(::openmldb::api::kInited);
                        continue;
                    }
                }
                std::shared_ptr<Task> task = op_data->task_list_.front();
                if (task->GetStatus() == ::openmldb::api::kFailed) {
                    PDLOG(WARNING, "task[%s] run failed, terminate op[%s]. op_id[%lu]", task->GetReadableType().c_str(),
                          task->GetReadableOpType().c_str(), task->GetOpId());
                } else if (task->task_info_->status() == ::openmldb::api::kInited) {
                    DEBUGLOG("run task. opid[%lu] op_type[%s] task_type[%s]", task->GetOpId(),
                             task->GetReadableOpType().c_str(), task->GetReadableType().c_str());
                    task_thread_pool_.AddTask(task->fun_);
                    task->SetStatus(::openmldb::api::kDoing);
                } else if (task->GetStatus() == ::openmldb::api::kDoing) {
                    uint64_t cur_ts = ::baidu::common::timer::now_time();
                    if (cur_ts - op_data->op_info_.start_time() > FLAGS_name_server_op_execute_timeout / 1000) {
                        PDLOG(INFO,
                              "The execution time of op is too long. opid[%lu] op_type[%s] cur task_type[%s] "
                              "start_time[%lu] cur_time[%lu]",
                              task->GetOpId(), task->GetReadableOpType().c_str(), task->GetReadableType().c_str(),
                              op_data->op_info_.start_time(), cur_ts);
                        cv_.wait_for(lock, std::chrono::milliseconds(FLAGS_name_server_task_wait_time));
                    }
                }
            }
        }
        UpdateZKTaskStatus();
        DeleteTask();
    }
}

void NameServerImpl::ConnectZK(RpcController* controller, const ConnectZKRequest* request, GeneralResponse* response,
                               Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (zk_client_->Reconnect()) {
        if (session_term_ != zk_client_->GetSessionTerm()) {
            if (zk_client_->WatchNodes()) {
                session_term_ = zk_client_->GetSessionTerm();
                PDLOG(INFO, "watch node ok");
            }
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        PDLOG(INFO, "connect zk ok");
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kConnectZkFailed);
    response->set_msg("connect zk failed");
}

void NameServerImpl::DisConnectZK(RpcController* controller, const DisConnectZKRequest* request,
                                  GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    zk_client_->CloseZK();
    OnLostLock();
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    PDLOG(INFO, "disconnect zk ok");
}

void NameServerImpl::GetTablePartition(RpcController* controller, const GetTablePartitionRequest* request,
                                       GetTablePartitionResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string name = request->name();
    std::string db = request->db();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist", name.c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        ::openmldb::nameserver::TablePartition* table_partition = response->mutable_table_partition();
        table_partition->CopyFrom(table_info->table_partition(idx));
        break;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::SetTablePartition(RpcController* controller, const SetTablePartitionRequest* request,
                                       GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::string name = request->name();
    std::string db = request->db();
    uint32_t pid = request->table_partition().pid();
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist", name.c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> cur_table_info(table_info->New());
    cur_table_info->CopyFrom(*table_info);
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (cur_table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        ::openmldb::nameserver::TablePartition* table_partition = cur_table_info->mutable_table_partition(idx);
        table_partition->Clear();
        table_partition->CopyFrom(request->table_partition());
        if (!UpdateZkTableNode(cur_table_info)) {
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
            response->set_msg("set zk failed");
            return;
        }
        table_info->CopyFrom(*cur_table_info);
        break;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::MakeSnapshotNS(RpcController* controller, const MakeSnapshotNSRequest* request,
                                    GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist", request->name().c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (request->offset() > 0) {
        if (table_info->storage_mode() != common::kMemory) {
            PDLOG(WARNING,
                  "table[%s] is not memory table, can't do snapshot with end "
                  "offset",
                  request->name().c_str());
            response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
            response->set_msg("table is not memory table, can't do snapshot with end offset");
            return;
        } else {
            thread_pool_.AddTask(boost::bind(&NameServerImpl::MakeTablePartitionSnapshot, this, request->pid(),
                                             request->offset(), table_info));
            response->set_code(::openmldb::base::ReturnCode::kOk);
            return;
        }
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    request->SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kMakeSnapshotOP, value, op_data, request->name(), request->db(),
                     request->pid()) < 0) {
        response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
        response->set_msg("set zk failed");
        PDLOG(WARNING, "create makesnapshot op data error. name[%s] pid[%u]", request->name().c_str(), request->pid());
        return;
    }
    if (CreateMakeSnapshotOPTask(op_data) < 0) {
        response->set_code(::openmldb::base::ReturnCode::kCreateOpFailed);
        response->set_msg("create op failed");
        PDLOG(WARNING, "create makesnapshot op task failed. name[%s] pid[%u]", request->name().c_str(), request->pid());
        return;
    }
    if (AddOPData(op_data) < 0) {
        response->set_code(::openmldb::base::ReturnCode::kAddOpDataFailed);
        response->set_msg("add op data failed");
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u]", request->name().c_str(), request->pid());
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    PDLOG(INFO, "add makesnapshot op ok. op_id[%lu] name[%s] pid[%u]", op_data->op_info_.op_id(),
          request->name().c_str(), request->pid());
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
            if (kv.second->state_ == ::openmldb::type::EndpointState::kHealthy) {
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
        PDLOG(WARNING, "healthy endpoint num[%u] is less than replica_num[%u]", endpoint_pid_bucked.size(),
              replica_num);
        return -1;
    }
    if (replica_num < 1) {
        PDLOG(WARNING, "replica_num less than 1 that is illegal, replica_num[%u]", replica_num);
        return -1;
    }
    std::map<std::string, uint64_t> endpoint_leader = endpoint_pid_bucked;
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>* cur_table_info = &table_info_;
        if (FLAGS_enable_distsql && !table_info.db().empty()) {
            auto it = db_table_info_.find(table_info.db());
            if (it != db_table_info_.end()) {
                cur_table_info = &(it->second);
            }
        }
        for (const auto& iter : *cur_table_info) {
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
    for (const auto& iter : endpoint_pid_bucked) {
        endpoint_vec.push_back(iter.first);
        if (iter.second < min) {
            min = iter.second;
            pos = index;
        }
        index++;
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(endpoint_vec.begin(), endpoint_vec.end(), g);
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
    PDLOG(INFO, "set table partition ok. name[%s] partition_num[%u] replica_num[%u]", table_info.name().c_str(),
          partition_num, replica_num);
    return 0;
}

base::Status NameServerImpl::CreateTableOnTablet(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info,
                                                 bool is_leader, uint64_t term,
                                                 std::map<uint32_t, std::vector<std::string>>* endpoint_map) {
    ::openmldb::type::CompressType compress_type = ::openmldb::type::CompressType::kNoCompress;
    if (table_info->compress_type() == ::openmldb::type::kSnappy) {
        compress_type = ::openmldb::type::CompressType::kSnappy;
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_db(table_info->db());
    table_meta.set_name(table_info->name());
    table_meta.set_tid(static_cast<::google::protobuf::int32>(table_info->tid()));
    table_meta.set_seg_cnt(static_cast<::google::protobuf::int32>(table_info->seg_cnt()));
    table_meta.set_compress_type(compress_type);
    table_meta.set_storage_mode(table_info->storage_mode());
    table_meta.set_base_table_tid(table_info->base_table_tid());
    if (table_info->has_key_entry_max_height()) {
        table_meta.set_key_entry_max_height(table_info->key_entry_max_height());
    }
    for (int idx = 0; idx < table_info->column_desc_size(); idx++) {
        table_meta.add_column_desc()->CopyFrom(table_info->column_desc(idx));
    }
    for (int idx = 0; idx < table_info->column_key_size(); idx++) {
        table_meta.add_column_key()->CopyFrom(table_info->column_key(idx));
    }
    for (const auto& table_partition : table_info->table_partition()) {
        auto partition = table_meta.add_table_partition();
        partition->set_pid(table_partition.pid());
        for (const auto& partition_meta : table_partition.partition_meta()) {
            auto meta = partition->add_partition_meta();
            meta->set_endpoint(partition_meta.endpoint());
            meta->set_is_leader(partition_meta.is_leader());
            meta->set_is_alive(true);
        }
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        table_meta.set_pid(static_cast<::google::protobuf::int32>(pid));
        table_meta.clear_replicas();
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader() != is_leader) {
                continue;
            }
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            auto tablet_ptr = GetTablet(endpoint);
            if (!tablet_ptr) {
                PDLOG(WARNING, "endpoint[%s] cannot find client", endpoint.c_str());
                return {base::ReturnCode::kServerConnError, absl::StrCat("endpoint ", endpoint, " cannot find client")};
            }
            if (is_leader) {
                auto table_partition = table_info->mutable_table_partition(idx);
                auto term_pair = table_partition->add_term_offset();
                term_pair->set_term(term);
                term_pair->set_offset(0);
                table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
                table_meta.set_term(term);
                for (const auto& e : (*endpoint_map)[pid]) {
                    table_meta.add_replicas(e);
                }
            } else {
                auto iter = endpoint_map->find(pid);
                if (iter == endpoint_map->end()) {
                    iter = endpoint_map->emplace(pid, std::vector<std::string>()).first;
                }
                iter->second.push_back(endpoint);
                table_meta.set_mode(::openmldb::api::TableMode::kTableFollower);
            }
            if (auto status = tablet_ptr->client_->CreateTable(table_meta); !status.OK()) {
                PDLOG(WARNING, "create table failed. tid[%u] pid[%u] endpoint[%s] msg[%s]", table_info->tid(), pid,
                      endpoint.c_str(), status.GetMsg().c_str());
                return status;
            }
            PDLOG(INFO, "create table success. tid[%u] pid[%u] endpoint[%s] idx[%d]", table_info->tid(), pid,
                  endpoint.c_str(), idx);
        }
    }
    return {};
}

int NameServerImpl::DropTableOnTablet(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info) {
    uint32_t tid = table_info->tid();
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            auto tablet_ptr = GetTablet(endpoint);
            if (!tablet_ptr) {
                PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                continue;
            }
            if (!tablet_ptr->client_->DropTable(tid, pid)) {
                PDLOG(WARNING, "drop table failed. tid[%u] pid[%u] endpoint[%s]", tid, pid, endpoint.c_str());
            } else {
                PDLOG(INFO, "drop table success. tid[%u] pid[%u] endpoint[%s]", tid, pid, endpoint.c_str());
            }
        }
    }
    return 0;
}

void NameServerImpl::ConfSet(RpcController* controller, const ConfSetRequest* request, GeneralResponse* response,
                             Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string key = request->conf().key();
    std::string value = request->conf().value();
    if (key.empty() || value.empty()) {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "key[%s] value[%s]", key.c_str(), value.c_str());
        return;
    }
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value != "true" && value != "false") {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "invalid value[%s]", request->conf().value().c_str());
        return;
    }
    if (key == "auto_failover") {
        if (!zk_client_->SetNodeValue(zk_path_.auto_failover_node_, value)) {
            PDLOG(WARNING, "set auto_failover_node failed!");
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
            response->set_msg("set zk failed");
            return;
        }
        if (value == "true") {
            auto_failover_.store(true, std::memory_order_release);
        } else {
            auto_failover_.store(false, std::memory_order_release);
        }
    } else {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "unsupport set key[%s]", key.c_str());
        return;
    }
    PDLOG(INFO, "config set ok. key[%s] value[%s]", key.c_str(), value.c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::ConfGet(RpcController* controller, const ConfGetRequest* request, ConfGetResponse* response,
                             Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    ::openmldb::nameserver::Pair* conf = response->add_conf();
    conf->set_key("auto_failover");
    auto_failover_.load(std::memory_order_acquire) ? conf->set_value("true") : conf->set_value("false");

    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::ChangeLeader(RpcController* controller, const ChangeLeaderRequest* request,
                                  GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::string name = request->name();
    std::string db = request->db();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist", name.c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (pid > (uint32_t)table_info->table_partition_size() - 1) {
        PDLOG(WARNING, "pid[%u] does not exist, table[%s]", pid, name.c_str());
        response->set_code(::openmldb::base::ReturnCode::kPidIsNotExist);
        response->set_msg("pid does not exist");
        return;
    }
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        if (table_info->table_partition(idx).partition_meta_size() == 1) {
            PDLOG(WARNING, "table[%s] pid[%u] has no followers, cannot change leader", name.c_str(),
                  table_info->table_partition(idx).pid());
            response->set_code(::openmldb::base::ReturnCode::kNoFollower);
            response->set_msg("no follower");
            return;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                if (!table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    follower_endpoint.push_back(table_info->table_partition(idx).partition_meta(meta_idx).endpoint());
                } else if (!request->has_candidate_leader()) {
                    PDLOG(WARNING,
                          "leader is alive, cannot change leader. table[%s] "
                          "pid[%u]",
                          name.c_str(), pid);
                    response->set_code(::openmldb::base::ReturnCode::kLeaderIsAlive);
                    response->set_msg("leader is alive");
                    return;
                }
            }
        }
        break;
    }
    if (follower_endpoint.empty()) {
        response->set_code(::openmldb::base::ReturnCode::kNoAliveFollower);
        response->set_msg("no alive follower");
        PDLOG(WARNING, "no alive follower. table[%s] pid[%u]", name.c_str(), pid);
        return;
    }
    std::string candidate_leader;
    if (request->has_candidate_leader() && request->candidate_leader() != "auto") {
        candidate_leader = request->candidate_leader();
    }
    if (CreateChangeLeaderOP(name, db, pid, candidate_leader, false) < 0) {
        response->set_code(::openmldb::base::ReturnCode::kCreateOpFailed);
        response->set_msg("create op failed");
        PDLOG(WARNING, "change leader failed. name[%s] pid[%u]", name.c_str(), pid);
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::OfflineEndpoint(RpcController* controller, const OfflineEndpointRequest* request,
                                     GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    uint32_t concurrency = FLAGS_name_server_task_concurrency;
    if (request->has_concurrency()) {
        if (request->concurrency() > FLAGS_name_server_task_max_concurrency) {
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
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
            response->set_code(::openmldb::base::ReturnCode::kEndpointIsNotExist);
            response->set_msg("endpoint does not exist");
            PDLOG(WARNING, "endpoint[%s] does not exist", endpoint.c_str());
            return;
        }
    }
    OfflineEndpointInternal(endpoint, concurrency);
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::OfflineEndpointDBInternal(
    const std::string& endpoint, uint32_t concurrency,
    const std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& table_info) {
    for (const auto& kv : table_info) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid = kv.second->table_partition(idx).pid();
            if (kv.second->table_partition(idx).partition_meta_size() == 1 &&
                kv.second->table_partition(idx).partition_meta(0).endpoint() == endpoint) {
                PDLOG(INFO, "table[%s] pid[%u] has no followers", kv.first.c_str(), pid);
                CreateUpdatePartitionStatusOP(kv.first, kv.second->db(), pid, endpoint, true, false, INVALID_PARENT_ID,
                                              concurrency);
                continue;
            }
            std::string alive_leader;
            int endpoint_index = -1;
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                const ::openmldb::nameserver::PartitionMeta& partition_meta =
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
            const ::openmldb::nameserver::PartitionMeta& partition_meta =
                kv.second->table_partition(idx).partition_meta(endpoint_index);
            if (partition_meta.is_leader() || alive_leader.empty()) {
                // leader partition lost
                if (alive_leader.empty() || alive_leader == endpoint) {
                    PDLOG(INFO, "table[%s] pid[%u] change leader", kv.first.c_str(), pid);
                    CreateChangeLeaderOP(kv.first, kv.second->db(), pid, "", false, concurrency);
                } else {
                    PDLOG(INFO, "table[%s] pid[%u] need not change leader", kv.first.c_str(), pid);
                }
            } else {
                CreateOfflineReplicaOP(kv.first, kv.second->db(), pid, endpoint, concurrency);
            }
        }
    }
}

void NameServerImpl::OfflineEndpointInternal(const std::string& endpoint, uint32_t concurrency) {
    std::lock_guard<std::mutex> lock(mu_);
    OfflineEndpointDBInternal(endpoint, concurrency, table_info_);
    for (const auto& kv : db_table_info_) {
        OfflineEndpointDBInternal(endpoint, concurrency, kv.second);
    }
}

void NameServerImpl::RecoverEndpoint(RpcController* controller, const RecoverEndpointRequest* request,
                                     GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    uint32_t concurrency = FLAGS_name_server_task_concurrency;
    if (request->has_concurrency()) {
        if (request->concurrency() > FLAGS_name_server_task_max_concurrency) {
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
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
            response->set_code(::openmldb::base::ReturnCode::kEndpointIsNotExist);
            response->set_msg("endpoint does not exist");
            PDLOG(WARNING, "endpoint[%s] does not exist", endpoint.c_str());
            return;
        } else if (iter->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
            response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
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
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::RecoverTable(RpcController* controller, const RecoverTableRequest* request,
                                  GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::string name = request->name();
    std::string db = request->db();
    std::string endpoint = request->endpoint();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        response->set_code(::openmldb::base::ReturnCode::kEndpointIsNotExist);
        response->set_msg("endpoint does not exist");
        PDLOG(WARNING, "endpoint[%s] does not exist", endpoint.c_str());
        return;
    } else if (it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", endpoint.c_str());
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist", name.c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    bool has_found = false;
    bool is_leader = false;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                if (table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    PDLOG(WARNING,
                          "status is alive, need not recover. name[%s] pid[%u] "
                          "endpoint[%s]",
                          name.c_str(), pid, endpoint.c_str());
                    response->set_code(::openmldb::base::ReturnCode::kPartitionIsAlive);
                    response->set_msg("table is alive, need not recover");
                    return;
                }
                if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    is_leader = true;
                }
                has_found = true;
            }
        }
        break;
    }
    if (!has_found) {
        PDLOG(WARNING, "not found table[%s] pid[%u] in endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        response->set_code(::openmldb::base::ReturnCode::kPidIsNotExist);
        response->set_msg("pid does not exist");
        return;
    }
    CreateRecoverTableOP(name, db, pid, endpoint, is_leader, FLAGS_check_binlog_sync_progress_delta,
                         FLAGS_name_server_task_concurrency);
    PDLOG(INFO, "recover table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::DeleteOP(RpcController* controller, const DeleteOPRequest* request, GeneralResponse* response,
                              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (!request->has_op_id() && (request->status() == ::openmldb::api::TaskStatus::kInited ||
                                  request->status() == ::openmldb::api::TaskStatus::kDoing)) {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
        response->set_msg("cannot delete the Inited OP");
        PDLOG(WARNING, "cannot delete the Inited OP");
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    auto need_delete = [](const DeleteOPRequest* request, const ::openmldb::api::OPInfo& op_info) -> bool {
        if (request->has_op_id()) {
            if (op_info.op_id() != request->op_id()) {
                return false;
            }
        } else if (op_info.task_status() != request->status() || (request->has_db() && request->db() != op_info.db())) {
            return false;
        }
        return true;
    };
    auto delete_zk_op = [](ZkClient* zk_client, const std::string& path, uint64_t op_id) -> bool {
        std::string node = absl::StrCat(path, "/", op_id);
        if (zk_client->DeleteNode(node)) {
            PDLOG(INFO, "delete zk op node[%s] success.", node.c_str());
        } else {
            PDLOG(WARNING, "delete zk op_node failed. node[%s]", node.c_str());
            return false;
        }
        return true;
    };
    std::lock_guard<std::mutex> lock(mu_);
    for (auto iter = done_op_list_.begin(); iter != done_op_list_.end();) {
        const auto& op_info = (*iter)->op_info_;
        if (need_delete(request, op_info)) {
            if (op_info.task_status() != api::TaskStatus::kDone &&
                !delete_zk_op(zk_client_, zk_path_.op_data_path_, op_info.op_id())) {
                response->set_code(base::ReturnCode::kDelZkFailed);
                response->set_msg("delete zk op_node failed");
                return;
            }
            iter = done_op_list_.erase(iter);
            if (request->has_op_id()) {
                return;
            }
            continue;
        }
        iter++;
    }
    for (auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        for (auto iter = op_list.begin(); iter != op_list.end();) {
            const auto& op_info = (*iter)->op_info_;
            if (need_delete(request, op_info)) {
                if (op_info.task_status() != api::TaskStatus::kDone &&
                    !delete_zk_op(zk_client_, zk_path_.op_data_path_, op_info.op_id())) {
                    response->set_code(base::ReturnCode::kDelZkFailed);
                    response->set_msg("delete zk op_node failed");
                    return;
                }
                iter = op_list.erase(iter);
                if (request->has_op_id()) {
                    return;
                }
                continue;
            }
            iter++;
        }
    }
    if (request->has_op_id()) {
        response->set_code(base::ReturnCode::kDeleteFailed);
        response->set_msg("op id does not exist");
        PDLOG(WARNING, "op id %lu does not exist", request->op_id());
    }
}

void NameServerImpl::CancelOP(RpcController* controller, const CancelOPRequest* request, GeneralResponse* response,
                              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    bool find_op = false;
    std::vector<std::shared_ptr<TabletClient>> client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& op_list : task_vec_) {
            if (op_list.empty()) {
                continue;
            }
            for (auto& op_data : op_list) {
                if (op_data->op_info_.op_id() == request->op_id()) {
                    if (op_data->op_info_.task_status() == ::openmldb::api::kInited ||
                        (op_data->op_info_.task_status() == ::openmldb::api::kDoing)) {
                        op_data->op_info_.set_task_status(::openmldb::api::kCanceled);
                        for (auto& task : op_data->task_list_) {
                            task->task_info_->set_status(::openmldb::api::kCanceled);
                        }
                        find_op = true;
                    }
                    break;
                }
            }
        }
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                DEBUGLOG("tablet[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            client_vec.push_back(iter->second->client_);
        }
    }
    if (find_op) {
        for (const auto& client : client_vec) {
            if (!client->CancelOP(request->op_id())) {
                PDLOG(WARNING, "tablet[%s] cancel op failed", client->GetEndpoint().c_str());
                continue;
            }
            DEBUGLOG("tablet[%s] cancel op success", client->GetEndpoint().c_str());
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        PDLOG(INFO, "op[%lu] is canceled!", request->op_id());
    } else {
        response->set_code(::openmldb::base::ReturnCode::kOpStatusIsNotKdoingOrKinited);
        response->set_msg("op status is not kDoing or kInited");
        PDLOG(WARNING, "op[%lu] status is not kDoing or kInited", request->op_id());
    }
}

void NameServerImpl::ShowOPStatus(RpcController* controller, const ShowOPStatusRequest* request,
                                  ShowOPStatusResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::map<uint64_t, std::shared_ptr<OPData>> op_map;
    std::lock_guard<std::mutex> lock(mu_);
    DeleteDoneOP();
    for (const auto& op_data : done_op_list_) {
        if (request->has_op_id() && op_data->op_info_.op_id() != request->op_id()) {
            continue;
        }
        if (request->has_db() && op_data->op_info_.db() != request->db()) {
            continue;
        }
        if (request->has_name() && op_data->op_info_.name() != request->name()) {
            continue;
        }
        if (request->has_pid() && op_data->op_info_.pid() != request->pid()) {
            continue;
        }
        op_map.emplace(op_data->op_info_.op_id(), op_data);
    }
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        for (const auto& op_data : op_list) {
            if (request->has_op_id() && op_data->op_info_.op_id() != request->op_id()) {
                continue;
            }
            if (request->has_name() && op_data->op_info_.name() != request->name()) {
                continue;
            }
            if (request->has_db() && op_data->op_info_.db() != request->db()) {
                continue;
            }
            if (request->has_pid() && op_data->op_info_.pid() != request->pid()) {
                continue;
            }
            op_map.emplace(op_data->op_info_.op_id(), op_data);
        }
    }
    for (const auto& kv : op_map) {
        OPStatus* op_status = response->add_op_status();
        op_status->set_op_id(kv.second->op_info_.op_id());
        op_status->set_op_type(::openmldb::api::OPType_Name(kv.second->op_info_.op_type()));
        op_status->set_name(kv.second->op_info_.name());
        op_status->set_db(kv.second->op_info_.db());
        op_status->set_pid(kv.second->op_info_.pid());
        op_status->set_status(::openmldb::api::TaskStatus_Name(kv.second->op_info_.task_status()));
        op_status->set_for_replica_cluster(kv.second->op_info_.for_replica_cluster());
        if (kv.second->task_list_.empty() || kv.second->op_info_.task_status() == ::openmldb::api::kInited) {
            op_status->set_task_type("-");
        } else {
            std::shared_ptr<Task> task = kv.second->task_list_.front();
            op_status->set_task_type(::openmldb::api::TaskType_Name(task->task_info_->task_type()));
        }
        op_status->set_start_time(kv.second->op_info_.start_time());
        op_status->set_end_time(kv.second->op_info_.end_time());
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::ShowDbTable(const std::map<std::string, std::shared_ptr<TableInfo>>& table_infos,
                                 const ShowTableRequest* request, ShowTableResponse* response) {
    for (const auto& kv : table_infos) {
        if (request->has_name() && request->name() != kv.first) {
            continue;
        }
        ::openmldb::nameserver::TableInfo* table_info = response->add_table_info();
        table_info->CopyFrom(*(kv.second));
        table_info->clear_column_key();
        for (const auto& column_key : kv.second->column_key()) {
            if (!column_key.flag()) {
                ::openmldb::common::ColumnKey* ck = table_info->add_column_key();
                ck->CopyFrom(column_key);
            }
        }
    }
}

void NameServerImpl::ShowTable(RpcController* controller, const ShowTableRequest* request, ShowTableResponse* response,
                               Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        if (request->has_name() && request->name() != kv.first) {
            continue;
        }
        ::openmldb::nameserver::TableInfo* table_info = response->add_table_info();
        table_info->CopyFrom(*(kv.second));
        table_info->clear_column_key();
        for (const auto& column_key : kv.second->column_key()) {
            if (!column_key.flag()) {
                ::openmldb::common::ColumnKey* ck = table_info->add_column_key();
                ck->CopyFrom(column_key);
            }
        }
    }
    if (request->show_all()) {
        for (const auto& db_it : db_table_info_) {
            ShowDbTable(db_it.second, request, response);
        }
    } else if (!request->db().empty()) {
        auto db_it = db_table_info_.find(request->db());
        if (db_it != db_table_info_.end()) {
            ShowDbTable(db_it->second, request, response);
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::DropTableFun(const DropTableRequest* request, GeneralResponse* response,
                                  std::shared_ptr<::openmldb::nameserver::TableInfo> table_info) {
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_zone_info() && request->has_task_info() && request->task_info().IsInitialized()) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<uint64_t> rep_cluster_op_id_vec;
            if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kDropTableRemote, task_ptr,
                          rep_cluster_op_id_vec) < 0) {
                response->set_code(::openmldb::base::ReturnCode::kAddTaskInReplicaClusterNsFailed);
                response->set_msg("add task in replica cluster ns failed");
                return;
            }
            PDLOG(INFO, "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] task_status [%s]",
                  task_ptr->op_id(), ::openmldb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                  ::openmldb::api::TaskStatus_Name(task_ptr->status()).c_str());
        }
        task_thread_pool_.AddTask(
            boost::bind(&NameServerImpl::DropTableInternel, this, *request, *response, table_info, task_ptr));
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
    } else {
        DropTableInternel(*request, *response, table_info, task_ptr);
        response->set_code(response->code());
        response->set_msg(response->msg());
    }
}

::openmldb::base::Status NameServerImpl::CheckZoneInfo(const ::openmldb::nameserver::ZoneInfo& zone_info) {
    std::lock_guard<std::mutex> lock(mu_);
    if (zone_info.zone_name() != zone_info_.zone_name() || zone_info.zone_term() != zone_info_.zone_term()) {
        PDLOG(WARNING,
              "zone_info mismathch, expect zone name[%s], zone term [%lu], "
              "but zone name [%s], zone term [%u]",
              zone_info_.zone_name().c_str(), zone_info_.zone_term(), zone_info.zone_name().c_str(),
              zone_info.zone_term());
        return {::openmldb::base::ReturnCode::kZoneInfoMismathch, "zone_info mismathch"};
    }
    return {};
}

void NameServerImpl::DropTable(RpcController* controller, const DropTableRequest* request, GeneralResponse* response,
                               Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    }
    // if table is associated with deployment, drop it fail
    if (!request->db().empty()) {
        std::lock_guard<std::mutex> lock(mu_);
        auto db_iter = db_table_sp_map_.find(request->db());
        if (db_iter != db_table_sp_map_.end()) {
            auto& table_sp_map = db_iter->second;
            auto table_iter = table_sp_map.find(request->name());
            if (table_iter != table_sp_map.end()) {
                const auto& sp_vec = table_iter->second;
                if (!sp_vec.empty()) {
                    std::stringstream ss;
                    ss << "table has associated deployment: ";
                    for (uint32_t i = 0; i < sp_vec.size(); i++) {
                        ss << sp_vec[i].first << "." << sp_vec[i].second;
                        if (i != sp_vec.size() - 1) {
                            ss << ", ";
                        }
                    }
                    std::string err_msg = ss.str();
                    response->set_code(::openmldb::base::ReturnCode::kDropTableError);
                    response->set_msg(err_msg);
                    LOG(WARNING) << err_msg;
                    return;
                }
            }
        }
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfo(request->name(), request->db(), &table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist!");
        PDLOG(WARNING, "table[%s.%s] does not exist!", request->db().c_str(), request->name().c_str());
        return;
    }
    DropTableFun(request, response, table_info);
}

void NameServerImpl::DropTableInternel(const DropTableRequest& request, GeneralResponse& response,
                                       std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                                       std::shared_ptr<::openmldb::api::TaskInfo> task_ptr) {
    const std::string& name = request.name();
    const std::string& db = request.db();
    std::map<uint32_t, std::map<std::string, std::shared_ptr<TabletClient>>> pid_endpoint_map;
    uint32_t tid = table_info->tid();
    int code = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (!table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    PDLOG(WARNING, "table[%s] is not alive. pid[%u] endpoint[%s]", name.c_str(),
                          table_info->table_partition(idx).pid(), endpoint.c_str());
                    continue;
                }
                auto tablets_iter = tablets_.find(endpoint);
                // check tablet if exist
                if (tablets_iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    continue;
                }
                // check tablet healthy
                if (tablets_iter->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    continue;
                }
                uint32_t pid = table_info->table_partition(idx).pid();
                auto map_iter = pid_endpoint_map.find(pid);
                if (map_iter == pid_endpoint_map.end()) {
                    pid_endpoint_map.emplace(pid, std::map<std::string, std::shared_ptr<TabletClient>>());
                }
                pid_endpoint_map[pid].emplace(endpoint, tablets_iter->second->client_);
            }
        }
    }
    for (const auto& pkv : pid_endpoint_map) {
        for (const auto& kv : pkv.second) {
            if (!kv.second->DropTable(tid, pkv.first)) {
                PDLOG(WARNING, "drop table failed. tid[%u] pid[%u] endpoint[%s]", tid, pkv.first, kv.first.c_str());
                code = base::ReturnCode::kDropTableError;  // if drop table failed, return error
                continue;
            }
            PDLOG(INFO, "drop table. tid[%u] pid[%u] endpoint[%s]", tid, pkv.first, kv.first.c_str());
        }
    }
    std::vector<uint64_t> id_vec;  // for cancel op
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!request.db().empty()) {
            if (IsClusterMode() && !zk_client_->DeleteNode(zk_path_.db_table_data_path_ + "/" + std::to_string(tid))) {
                PDLOG(WARNING, "delete db table node[%s/%u] failed!", zk_path_.db_table_data_path_.c_str(), tid);
                code = base::ReturnCode::kSetZkFailed;
            } else {
                PDLOG(INFO, "delete table node[%s/%u]", zk_path_.db_table_data_path_.c_str(), tid);
                db_table_info_[db].erase(name);
            }
        } else {
            if (IsClusterMode() && !zk_client_->DeleteNode(zk_path_.table_data_path_ + "/" + name)) {
                PDLOG(WARNING, "delete table node[%s/%s] failed!", zk_path_.table_data_path_.c_str(), name.c_str());
                code = base::ReturnCode::kSetZkFailed;
            } else {
                PDLOG(INFO, "delete table node[%s/%s]", zk_path_.table_data_path_.c_str(), name.c_str());
                table_info_.erase(name);
            }
        }
        if (!nsc_.empty()) {
            for (auto kv : nsc_) {
                if (kv.second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                    PDLOG(INFO, "cluster[%s] is not Healthy", kv.first.c_str());
                    continue;
                }
                if (DropTableRemoteOP(name, db, kv.first, INVALID_PARENT_ID,
                                      FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
                    PDLOG(WARNING, "create DropTableRemoteOP for replica cluster failed, table_name: %s, alias: %s",
                          name.c_str(), kv.first.c_str());
                    code = base::ReturnCode::kCreateDroptableremoteopForReplicaClusterFailed;
                    continue;
                }
            }
        }

        for (auto& op_list : task_vec_) {
            if (op_list.empty()) {
                continue;
            }
            for (auto& op_data : op_list) {
                if (op_data->op_info_.for_replica_cluster() == 1 ||
                    (task_ptr && task_ptr->op_id() == op_data->op_info_.op_id())) {
                    continue;
                }
                if (op_data->op_info_.db() == db && op_data->op_info_.name() == name) {
                    if (op_data->op_info_.task_status() == ::openmldb::api::kInited ||
                        (op_data->op_info_.task_status() == ::openmldb::api::kDoing)) {
                        op_data->op_info_.set_task_status(::openmldb::api::kCanceled);
                        for (auto& task : op_data->task_list_) {
                            task->task_info_->set_status(::openmldb::api::kCanceled);
                        }
                        id_vec.push_back(op_data->op_info_.op_id());
                        PDLOG(INFO, "cancel op %lu", op_data->op_info_.op_id());
                    }
                }
            }
        }
    }
    if (!id_vec.empty()) {
        std::set<std::string> endpoint_set;
        for (const auto& pkv : pid_endpoint_map) {
            for (const auto& kv : pkv.second) {
                if (endpoint_set.find(kv.first) != endpoint_set.end()) {
                    continue;
                }
                endpoint_set.insert(kv.first);
                for (auto op_id : id_vec) {
                    if (!kv.second->CancelOP(op_id)) {
                        PDLOG(WARNING, "tablet[%s] cancel op [%lu] failed", kv.first.c_str(), op_id);
                    }
                }
            }
        }
    }
    response.set_code(code);
    code == 0 ? response.set_msg("ok") : response.set_msg("drop table error");
    if (task_ptr) {
        if (code != 0) {
            task_ptr->set_status(::openmldb::api::TaskStatus::kFailed);
        } else {
            task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
        }
    }
    if (IsClusterMode()) {
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
    }
}

bool NameServerImpl::AddFieldToTablet(const std::vector<openmldb::common::ColumnDesc>& cols,
                                      std::shared_ptr<TableInfo> table_info, openmldb::common::VersionPair* new_pair) {
    std::set<std::string> endpoint_set;
    std::map<std::string, std::shared_ptr<TabletClient>> tablet_client_map;
    for (const auto& part : table_info->table_partition()) {
        for (const auto& meta : part.partition_meta()) {
            if (tablet_client_map.find(meta.endpoint()) != tablet_client_map.end()) {
                continue;
            }
            std::shared_ptr<TabletInfo> tablet = GetTabletInfo(meta.endpoint());
            if (!tablet) {
                continue;
            }
            if (!tablet->Health()) {
                LOG(WARNING) << "endpoint[" << meta.endpoint() << "] is offline";
                return false;
            }
            tablet_client_map.insert(std::make_pair(meta.endpoint(), tablet->client_));
        }
    }
    const std::string& name = table_info->name();
    int32_t version_id = 1;
    if (table_info->schema_versions_size() > 0) {
        int32_t versions_size = table_info->schema_versions_size();
        const auto& pair = table_info->schema_versions(versions_size - 1);
        version_id = pair.id();
    }
    if (version_id >= UINT8_MAX) {
        LOG(WARNING) << "reach max version " << UINT8_MAX << " table " << name;
        return false;
    }
    uint32_t field_count = table_info->column_desc_size() + table_info->added_column_desc_size();
    version_id++;
    new_pair->set_id(version_id);
    new_pair->set_field_count(field_count + cols.size());

    uint32_t tid = table_info->tid();
    std::string msg;
    std::vector<openmldb::common::ColumnDesc> new_cols;
    for (auto it = tablet_client_map.begin(); it != tablet_client_map.end(); it++) {
        if (!it->second->UpdateTableMetaForAddField(tid, cols, *new_pair, msg)) {
            LOG(WARNING) << "update table_meta on endpoint[" << it->first << "for add table field failed! err: " << msg;
            return false;
        }
        LOG(INFO) << "update table_meta on endpoint[" << it->first << "] for add table field success! version is "
                  << version_id << " columns size is " << field_count << " for table " << table_info->name();
    }
    return true;
}

void NameServerImpl::AddTableField(RpcController* controller, const AddTableFieldRequest* request,
                                   GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    const std::string& name = request->name();
    const std::string& db = request->db();
    std::map<std::string, std::shared_ptr<TabletClient>> tablet_client_map;
    std::shared_ptr<TableInfo> table_info;
    std::string schema;
    std::set<std::string> endpoint_set;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!GetTableInfoUnlock(name, db, &table_info)) {
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table doesn't exist!");
            LOG(WARNING) << "table[" << name << "] doesn't exist!";
            return;
        }
        if (table_info->added_column_desc_size() >= MAX_ADD_TABLE_FIELD_COUNT) {
            response->set_code(ReturnCode::kTheCountOfAddingFieldIsMoreThan63);
            response->set_msg("the count of adding field is more than 63");
            LOG(WARNING) << "the count of adding field is more than 63 in table " << name;
            return;
        }
        // judge if field exists in table_info
        const std::string& col_name = request->column_desc().name();
        if (table_info->column_desc_size() > 0) {
            for (const auto& column : table_info->column_desc()) {
                if (column.name() == col_name) {
                    response->set_code(ReturnCode::kFieldNameRepeatedInTableInfo);
                    response->set_msg("field name repeated in table_info!");
                    LOG(WARNING) << "field name[" << col_name << "] repeated in table_info!";
                    return;
                }
            }
        }
        for (const auto& column : table_info->added_column_desc()) {
            if (column.name() == col_name) {
                response->set_code(ReturnCode::kFieldNameRepeatedInTableInfo);
                response->set_msg("field name repeated in table_info!");
                LOG(WARNING) << "field name[" << col_name << "] repeated in table_info!";
                return;
            }
        }
        // 1.update tablet tableMeta
    }
    openmldb::common::VersionPair new_pair;
    std::vector<openmldb::common::ColumnDesc> cols{request->column_desc()};
    bool ok = AddFieldToTablet(cols, table_info, &new_pair);
    if (!ok) {
        response->set_code(ReturnCode::kFailToUpdateTablemetaForAddingField);
        response->set_msg("fail to update tableMeta for adding field");
        LOG(WARNING) << "update tablemeta fail";
        return;
    }
    // update zk node
    std::shared_ptr<TableInfo> table_info_zk(table_info->New());
    table_info_zk->CopyFrom(*table_info);
    ::openmldb::common::ColumnDesc* added_column_desc_zk = table_info_zk->add_added_column_desc();
    added_column_desc_zk->CopyFrom(request->column_desc());
    openmldb::common::VersionPair* add_pair = table_info_zk->add_schema_versions();
    add_pair->CopyFrom(new_pair);
    if (!UpdateZkTableNodeWithoutNotify(table_info_zk.get())) {
        response->set_code(ReturnCode::kSetZkFailed);
        response->set_msg("set zk failed!");
        LOG(WARNING) << "set zk failed! table " << name << " db " << db;
        return;
    }
    {
        // 2.update ns table_info_
        std::lock_guard<std::mutex> lock(mu_);
        ::openmldb::common::ColumnDesc* added_column_desc = table_info->add_added_column_desc();
        added_column_desc->CopyFrom(request->column_desc());
        openmldb::common::VersionPair* added_version_pair = table_info->add_schema_versions();
        added_version_pair->CopyFrom(new_pair);
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
    }
    response->set_code(ReturnCode::kOk);
    response->set_msg("ok");
    LOG(INFO) << "add field success, table " << name << " db " << db;
}

void NameServerImpl::DeleteOPTask(RpcController* controller, const ::openmldb::api::DeleteTaskRequest* request,
                                  ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (int idx = 0; idx < request->op_id_size(); idx++) {
        auto iter = task_map_.find(request->op_id(idx));
        if (iter == task_map_.end()) {
            continue;
        }
        if (!iter->second.empty()) {
            PDLOG(INFO, "delete op task. op_id[%lu] op_type[%s] task_num[%u]", request->op_id(idx),
                  ::openmldb::api::OPType_Name(iter->second.front()->op_type()).c_str(), iter->second.size());
            iter->second.clear();
        }
        task_map_.erase(iter);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::GetTaskStatus(RpcController* controller, const ::openmldb::api::TaskStatusRequest* request,
                                   ::openmldb::api::TaskStatusResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : task_map_) {
        for (const auto& task_info : kv.second) {
            ::openmldb::api::TaskInfo* task = response->add_task();
            task->CopyFrom(*task_info);
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::LoadTable(RpcController* controller, const LoadTableRequest* request, GeneralResponse* response,
                               Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    }
    std::string name = request->name();
    std::string db = request->db();
    std::string endpoint = request->endpoint();
    uint32_t pid = request->pid();

    if (request->has_zone_info() && request->has_task_info() && request->task_info().IsInitialized()) {
        std::lock_guard<std::mutex> lock(mu_);
        uint64_t rep_cluster_op_id = INVALID_PARENT_ID;
        if (CreateReLoadTableOP(name, db, pid, endpoint, INVALID_PARENT_ID, FLAGS_name_server_task_concurrency,
                                request->task_info().op_id(), rep_cluster_op_id) < 0) {
            PDLOG(WARNING, "create load table op failed, table_name: %s, endpoint: %s", name.c_str(), endpoint.c_str());
            response->set_code(::openmldb::base::ReturnCode::kCreateOpFailed);
            response->set_msg("create op failed");
            return;
        }
        std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
        std::vector<uint64_t> rep_cluster_op_id_vec = {rep_cluster_op_id};
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kLoadTable, task_ptr, rep_cluster_op_id_vec) <
            0) {
            response->set_code(::openmldb::base::ReturnCode::kAddTaskInReplicaClusterNsFailed);
            response->set_msg("add task in replica cluster ns failed");
            return;
        }
        PDLOG(INFO,
              "add task in replica cluster ns success, op_id [%lu] task_tpye "
              "[%s] task_status [%s]",
              task_ptr->op_id(), ::openmldb::api::TaskType_Name(task_ptr->task_type()).c_str(),
              ::openmldb::api::TaskStatus_Name(task_ptr->status()).c_str());
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
    } else {
        PDLOG(WARNING, "request has no zone_info or task_info!");
        response->set_code(::openmldb::base::ReturnCode::kRequestHasNoZoneInfoOrTaskInfo);
        response->set_msg("request has no zone_info or task_info");
    }
}

// for multi cluster createtable
void NameServerImpl::CreateTableInfoSimply(RpcController* controller, const CreateTableInfoRequest* request,
                                           CreateTableInfoResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    } else {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotReplicaCluster);
        response->set_msg("nameserver is not replica cluster");
        PDLOG(WARNING, "nameserver is not replica cluster");
        return;
    }

    ::openmldb::nameserver::TableInfo* table_info = response->mutable_table_info();
    table_info->CopyFrom(request->table_info());
    uint32_t tablets_size = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::openmldb::type::EndpointState::kHealthy) {
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
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "pid is not start with zero and consecutive");
            return;
        }
    } else {
        if (SetPartitionInfo(*table_info) < 0) {
            response->set_code(::openmldb::base::ReturnCode::kSetPartitionInfoFailed);
            response->set_msg("set partition info failed");
            PDLOG(WARNING, "set partition info failed");
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_path_.table_index_node_, std::to_string(table_index_ + 1))) {
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
            response->set_msg("set zk failed");
            PDLOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
            return;
        }
        table_index_++;
        table_info->set_tid(table_index_);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

// for multi cluster addreplica
void NameServerImpl::CreateTableInfo(RpcController* controller, const CreateTableInfoRequest* request,
                                     CreateTableInfoResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    } else {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotReplicaCluster);
        response->set_msg("nameserver is not replica cluster");
        PDLOG(WARNING, "nameserver is not  replica cluster");
        return;
    }

    ::openmldb::nameserver::TableInfo* table_info = response->mutable_table_info();
    table_info->CopyFrom(request->table_info());
    uint32_t tablets_size = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::openmldb::type::EndpointState::kHealthy) {
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
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            response->set_msg("invalid parameter");
            PDLOG(WARNING, "pid is not start with zero and consecutive");
            return;
        }
    } else {
        if (SetPartitionInfo(*table_info) < 0) {
            response->set_code(::openmldb::base::ReturnCode::kSetPartitionInfoFailed);
            response->set_msg("set partition info failed");
            PDLOG(WARNING, "set partition info failed");
            return;
        }
    }

    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_path_.table_index_node_, std::to_string(table_index_ + 1))) {
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
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
        ::openmldb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            table_partition->clear_term_offset();
            ::openmldb::nameserver::TermPair* term_pair = table_partition->add_term_offset();
            term_pair->set_term(cur_term);
            term_pair->set_offset(0);
            break;
        }
    }
    // zk table_info
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info_zk(table_info->New());
    table_info_zk->CopyFrom(*table_info);
    for (int idx = 0; idx < table_info_zk->table_partition_size(); idx++) {
        ::openmldb::nameserver::PartitionMeta leader_partition_meta;
        ::openmldb::nameserver::TablePartition* table_partition = table_info_zk->mutable_table_partition(idx);
        for (int meta_idx = 0; meta_idx < table_info_zk->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_partition->partition_meta(meta_idx).is_leader() &&
                table_partition->partition_meta(meta_idx).is_alive()) {
                ::openmldb::nameserver::PartitionMeta* partition_meta =
                    table_partition->mutable_partition_meta(meta_idx);
                partition_meta->set_is_alive(false);
                leader_partition_meta = *partition_meta;
                // clear follower partition_meta
                table_partition->clear_partition_meta();
                ::openmldb::nameserver::PartitionMeta* partition_meta_ptr = table_partition->add_partition_meta();
                partition_meta_ptr->CopyFrom(leader_partition_meta);
                break;
            }
        }
    }
    if (SetTableInfo(table_info_zk)) {
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
    } else {
        response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
        response->set_msg("set zk failed");
    }
}

bool NameServerImpl::SetTableInfo(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info) {
    std::string table_value;
    table_info->SerializeToString(&table_value);
    if (!table_info->db().empty()) {
        if (!zk_client_->CreateNode(zk_path_.db_table_data_path_ + "/" + std::to_string(table_info->tid()),
                                    table_value)) {
            PDLOG(WARNING, "create db table node[%s/%u] failed! value[%s] value_size[%u]",
                  zk_path_.db_table_data_path_.c_str(), table_info->tid(), table_value.c_str(), table_value.length());
            return false;
        }
        PDLOG(INFO, "create db table node[%s/%u] success! value[%s] value_size[%u]",
              zk_path_.db_table_data_path_.c_str(), table_info->tid(), table_value.c_str(), table_value.length());
        {
            std::lock_guard<std::mutex> lock(mu_);
            db_table_info_[table_info->db()].insert(std::make_pair(table_info->name(), table_info));
            NotifyTableChanged(::openmldb::type::NotifyType::kTable);
        }
    } else {
        if (!zk_client_->CreateNode(zk_path_.table_data_path_ + "/" + table_info->name(), table_value)) {
            PDLOG(WARNING, "create table node[%s/%s] failed! value[%s] value_size[%u]",
                  zk_path_.table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str(),
                  table_value.length());
            return false;
        }
        PDLOG(INFO, "create table node[%s/%s] success! value[%s] value_size[%u]", zk_path_.table_data_path_.c_str(),
              table_info->name().c_str(), table_value.c_str(), table_value.length());
        {
            std::lock_guard<std::mutex> lock(mu_);
            table_info_.insert(std::make_pair(table_info->name(), table_info));
            NotifyTableChanged(::openmldb::type::NotifyType::kTable);
        }
    }
    return true;
}

void NameServerImpl::CreateTable(RpcController* controller, const CreateTableRequest* request,
                                 GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        base::SetResponseStatus(base::ReturnCode::kNameserverIsNotLeader, "nameserver is not leader", response);
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            base::SetResponseStatus(base::ReturnCode::kNoZoneInfo,
                                    "nameserver is for follower cluster, and request has no zone info", response);
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info(request->table_info().New());
    table_info->CopyFrom(request->table_info());
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!table_info->db().empty()) {
            if (databases_.find(table_info->db()) == databases_.end()) {
                base::SetResponseStatus(base::ReturnCode::kDatabaseNotFound, "database not found", response);
                PDLOG(WARNING, "database[%s] not found", table_info->db().c_str());
                return;
            } else {
                auto table_infos = db_table_info_[table_info->db()];
                if (table_infos.find(table_info->name()) != table_infos.end()) {
                    if (request->create_if_not_exist()) {
                        base::SetResponseOK(response);
                    } else {
                        base::SetResponseStatus(base::ReturnCode::kTableAlreadyExists, "table already exists",
                                                response);
                        PDLOG(WARNING, "table[%s] already exists", table_info->name().c_str());
                    }
                    return;
                }
            }
        } else if (table_info_.find(table_info->name()) != table_info_.end()) {
            if (request->create_if_not_exist()) {
                base::SetResponseOK(response);
            } else {
                base::SetResponseStatus(base::ReturnCode::kTableAlreadyExists, "table already exists", response);
                PDLOG(WARNING, "table[%s] already exists", table_info->name().c_str());
            }
            return;
        }
    }

    if (table_info->column_key_size() == 0) {
        // if no column_key, add one which key is the first column which is not float or double
        // the logic should be the same as 'create table xx(xx,index(key=<auto_selected_col>)) xx;'
        // Ref NsClient::TransformToTableDef
        schema::IndexUtil::AddDefaultIndex(table_info.get());
    }

    if (!IsClusterMode()) {
        table_info->set_partition_num(1);
        table_info->set_replica_num(1);
    }
    auto status = schema::SchemaAdapter::CheckTableMeta(*table_info);
    if (!status.OK()) {
        PDLOG(WARNING, status.msg.c_str());
        base::SetResponseStatus(base::ReturnCode::kInvalidParameter, "check TableMeta failed! " + status.msg, response);
        return;
    }
    if (!request->has_zone_info()) {
        if (!schema::IndexUtil::FillColumnKey(table_info.get())) {
            base::SetResponseStatus(base::ReturnCode::kInvalidParameter, "fill column key failed", response);
            PDLOG(WARNING, "fill column key failed");
            return;
        }
        if (table_info->table_partition_size() > 0) {
            std::set<uint32_t> pid_set;
            for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
                pid_set.insert(table_info->table_partition(idx).pid());
            }
            auto iter = pid_set.rbegin();
            if (*iter != static_cast<uint32_t>(table_info->table_partition_size()) - 1) {
                base::SetResponseStatus(base::ReturnCode::kInvalidParameter, "invalid parameter", response);
                PDLOG(WARNING, "pid is not start with zero and consecutive");
                return;
            }
        } else {
            if (SetPartitionInfo(*table_info) < 0) {
                base::SetResponseStatus(base::ReturnCode::kSetPartitionInfoFailed, "set partition info failed",
                                        response);
                PDLOG(WARNING, "set partition info failed");
                return;
            }
        }
    }
    uint32_t tid = 0;
    if (request->has_zone_info()) {
        tid = table_info->tid();
    } else {
        if (!AllocateTableId(&tid)) {
            base::SetResponseStatus(base::ReturnCode::kCreateTableFailed, "allocate table id failed!", response);
            LOG(WARNING) << response->msg() << " table: " << table_info->name();
            return;
        }
        // tid in ::openmldb::api::TableMeta is int32
        if (static_cast<::google::protobuf::int32>(tid) <= 0) {
            base::SetResponseStatus(base::ReturnCode::kCreateTableFailed, "allocated table id is invalid!", response);
            LOG(WARNING) << response->msg() << " table: " << table_info->name();
            return;
        }
        table_info->set_tid(tid);
    }
    uint64_t cur_term = GetTerm();
    if (request->has_zone_info() && request->has_task_info() && request->task_info().IsInitialized()) {
        std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<uint64_t> rep_cluster_op_id_vec;
            if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kCreateTableRemote, task_ptr,
                          rep_cluster_op_id_vec) < 0) {
                base::SetResponseStatus(base::ReturnCode::kAddTaskInReplicaClusterNsFailed,
                                        "add task in replica cluster ns failed", response);
                return;
            }
            PDLOG(INFO, "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] task_status [%s]",
                  task_ptr->op_id(), ::openmldb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                  ::openmldb::api::TaskStatus_Name(task_ptr->status()).c_str());
        }
        task_thread_pool_.AddTask(
            boost::bind(&NameServerImpl::CreateTableInternel, this, *response, table_info, cur_term, tid, task_ptr));
        base::SetResponseOK(response);
    } else {
        std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
        CreateTableInternel(*response, table_info, cur_term, tid, task_ptr);
        response->set_code(response->code());
        response->set_msg(response->msg());
    }
}

void NameServerImpl::TruncateTable(RpcController* controller, const TruncateTableRequest* request,
                                   TruncateTableResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string& db = request->db();
    const std::string& name = request->name();
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
            PDLOG(WARNING, "table[%s] does not exist in db [%s]", name.c_str(), db.c_str());
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table does not exist");
            return;
        }
        if (IsExistActiveOp(db, name)) {
            PDLOG(WARNING, "there is active op. db [%s] name [%s]", db.c_str(), name.c_str());
            response->set_code(::openmldb::base::ReturnCode::kOPAlreadyExists);
            response->set_msg("there is active op");
            return;
        }
    }
    uint32_t tid = table_info->tid();
    for (const auto& partition : table_info->table_partition()) {
        uint32_t offset = 0;
        for (const auto& partition_meta : partition.partition_meta()) {
            if (partition_meta.offset() != offset) {
                if (offset == 0) {
                    offset = partition_meta.offset();
                } else {
                    PDLOG(WARNING, "table[%s] partition [%d] offset mismatch", name.c_str(), partition.pid());
                    response->set_code(::openmldb::base::ReturnCode::kOffsetMismatch);
                    response->set_msg("partition offset mismatch");
                    return;
                }
            }
        }
    }
    for (const auto& partition : table_info->table_partition()) {
        uint32_t pid = partition.pid();
        for (const auto& partition_meta : partition.partition_meta()) {
            const auto& endpoint = partition_meta.endpoint();
            auto tablet_ptr = GetTablet(endpoint);
            if (!tablet_ptr) {
                PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                response->set_code(::openmldb::base::ReturnCode::kGetTabletFailed);
                response->set_msg("fail to get client, endpint " + endpoint);
                return;
            }
            auto status = tablet_ptr->client_->TruncateTable(tid, pid);
            if (!status.OK()) {
                PDLOG(WARNING, "truncate failed, tid[%u] pid[%u] endpoint[%s] msg [%s]", tid, pid, endpoint.c_str(),
                      status.GetMsg().c_str());
                response->set_code(::openmldb::base::ReturnCode::kTruncateTableFailed);
                response->set_msg(status.GetMsg());
                return;
            }
        }
    }
    PDLOG(INFO, "truncate success, db[%s] name[%s]", db.c_str(), name.c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

bool NameServerImpl::SaveTableInfo(std::shared_ptr<TableInfo> table_info) {
    std::string table_value;
    table_info->SerializeToString(&table_value);
    if (table_info->db().empty()) {
        if (!zk_client_->CreateNode(zk_path_.table_data_path_ + "/" + table_info->name(), table_value)) {
            PDLOG(WARNING, "create object table node[%s/%s] failed!", zk_path_.table_data_path_.c_str(),
                  table_info->name().c_str());
            return false;
        }
        PDLOG(INFO, "create table node[%s/%s] success!", zk_path_.table_data_path_.c_str(), table_info->name().c_str());
    } else {
        if (!zk_client_->CreateNode(zk_path_.db_table_data_path_ + "/" + std::to_string(table_info->tid()),
                                    table_value)) {
            PDLOG(WARNING, "create object db table node[%s/%s] failed!", zk_path_.db_table_data_path_.c_str(),
                  table_info->name().c_str());
            return false;
        }
        PDLOG(INFO, "create db table node[%s/%s] success!", zk_path_.db_table_data_path_.c_str(),
              table_info->name().c_str());
    }
    return true;
}

void NameServerImpl::RefreshTablet(uint32_t tid) {
    Tablets tablets;
    {
        std::lock_guard<std::mutex> lock(mu_);
        tablets = tablets_;
    }
    for (const auto& kv : tablets) {
        if (kv.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
            PDLOG(WARNING, "endpoint [%s] is offline", kv.first.c_str());
            continue;
        }
        kv.second->client_->Refresh(tid);
    }
}

void NameServerImpl::CreateTableInternel(GeneralResponse& response,
                                         std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                                         uint64_t cur_term, uint32_t tid,
                                         std::shared_ptr<::openmldb::api::TaskInfo> task_ptr) {
    std::map<uint32_t, std::vector<std::string>> endpoint_map;
    do {
        auto status = CreateTableOnTablet(table_info, false, cur_term, &endpoint_map);
        if (!status.OK()) {
            base::SetResponseStatus(status, &response);
            PDLOG(WARNING, "create table failed. name[%s] tid[%u] msg[%s]", table_info->name().c_str(), tid,
                  status.GetMsg().c_str());
            break;
        }
        status = CreateTableOnTablet(table_info, true, cur_term, &endpoint_map);
        if (!status.OK()) {
            base::SetResponseStatus(status, &response);
            PDLOG(WARNING, "create table failed. name[%s] tid[%u] msg[%s]", table_info->name().c_str(), tid,
                  status.GetMsg().c_str());
            break;
        }
        if (!IsClusterMode()) {
            std::lock_guard<std::mutex> lock(mu_);
            if (!table_info->db().empty()) {
                db_table_info_[table_info->db()].insert(std::make_pair(table_info->name(), table_info));
            } else {
                table_info_.insert(std::make_pair(table_info->name(), table_info));
            }
            PDLOG(INFO, "create table %s success", table_info->name().c_str());
        } else {
            if (SetTableInfo(table_info)) {
                if (task_ptr) {
                    task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
                    PDLOG(INFO, "set task type success, op_id [%lu] task_tpye [%s] task_status [%s]", task_ptr->op_id(),
                          ::openmldb::api::TaskType_Name(task_ptr->task_type()).c_str(),
                          ::openmldb::api::TaskStatus_Name(task_ptr->status()).c_str());
                }
            } else {
                response.set_code(::openmldb::base::ReturnCode::kSetZkFailed);
                response.set_msg("set zk failed");
                break;
            }
            RefreshTablet(table_info->tid());
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
                    ::openmldb::nameserver::TableInfo remote_table_info(*table_info);
                    std::string msg;
                    if (!std::atomic_load_explicit(&kv.second->client_, std::memory_order_relaxed)
                             ->CreateRemoteTableInfoSimply(zone_info_, remote_table_info, msg)) {
                        PDLOG(WARNING, "create remote table_info erro, wrong msg is [%s]", msg.c_str());
                        response.set_code(::openmldb::base::ReturnCode::kCreateRemoteTableInfoFailed);
                        response.set_msg("create remote table info failed");
                        break;
                    }
                    std::lock_guard<std::mutex> lock(mu_);
                    if (CreateTableRemoteOP(*table_info, remote_table_info, kv.first, INVALID_PARENT_ID,
                                            FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
                        PDLOG(WARNING,
                              "create CreateTableRemoteOP for replica cluster "
                              "failed, table_name: %s, alias: %s",
                              table_info->name().c_str(), kv.first.c_str());
                        response.set_code(::openmldb::base::ReturnCode::kCreateTableForReplicaClusterFailed);
                        response.set_msg("create CreateTableRemoteOP for replica cluster failed");
                        break;
                    }
                }
                if (response.code() != 0) {
                    break;
                }
            }
        }
        response.set_code(::openmldb::base::ReturnCode::kOk);
        response.set_msg("ok");
        return;
    } while (0);
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::openmldb::api::TaskStatus::kFailed);
    }
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::DropTableOnTablet, this, table_info));
}

// called by function CheckTableInfo and SyncTable
int NameServerImpl::AddReplicaSimplyRemoteOP(const std::string& alias, const std::string& name, const std::string& db,
                                             const std::string& endpoint, uint32_t remote_tid, uint32_t pid) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return -1;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist", name.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    AddReplicaData data;
    data.set_name(name);
    data.set_db(db);
    data.set_pid(pid);
    data.set_endpoint(endpoint);
    data.set_remote_tid(remote_tid);
    data.set_alias(alias);
    std::string value;
    data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kAddReplicaSimplyRemoteOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateAddReplicaSimplyRemoteOPTask(op_data) < 0) {
        PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
        PDLOG(WARNING, "add AddReplicaOP data failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add AddReplicasSimplyRemoteOP ok. op_id[%lu] table[%s] pid[%u]", op_data->op_info_.op_id(),
          name.c_str(), pid);
    return 0;
}

int NameServerImpl::CreateAddReplicaSimplyRemoteOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaData add_replica_data;
    if (!add_replica_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse add_replica_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(add_replica_data.name(), add_replica_data.db(), &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", add_replica_data.name().c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t pid = add_replica_data.pid();
    std::string alias = add_replica_data.alias();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", add_replica_data.name().c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kAddReplicaSimplyRemoteOP;
    auto task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid,
                                               add_replica_data.endpoint(), add_replica_data.remote_tid());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]", tid,
              add_replica_data.remote_tid(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddTableInfoTaskMeta>(op_index, op_type, add_replica_data.name(), add_replica_data.db(), pid,
                                            add_replica_data.endpoint(), alias, add_replica_data.remote_tid());
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create AddReplicaSimplyRemoteOP task ok. tid[%u] pid[%u] endpoint[%s]", tid, pid,
          add_replica_data.endpoint().c_str());
    return 0;
}

int NameServerImpl::AddReplicaRemoteOP(const std::string& alias, const std::string& name, const std::string& db,
                                       const ::openmldb::nameserver::TablePartition& table_partition,
                                       uint32_t remote_tid, uint32_t pid) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    AddReplicaData data;
    data.set_alias(alias);
    data.set_name(name);
    data.set_db(db);
    data.set_pid(pid);
    data.set_remote_tid(remote_tid);
    ::openmldb::nameserver::TablePartition* table_partition_ptr = data.mutable_table_partition();
    table_partition_ptr->CopyFrom(table_partition);

    std::string value;
    data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kAddReplicaRemoteOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateAddReplicaRemoteOPTask(op_data) < 0) {
        PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] ", name.c_str(), pid);
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
        PDLOG(WARNING, "add AddReplicaOP data failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add AddReplicaRemoteOP ok. op_id[%lu] table[%s] pid[%u]", op_data->op_info_.op_id(), name.c_str(),
          pid);
    return 0;
}

int NameServerImpl::CreateAddReplicaRemoteOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaData add_replica_data;
    if (!add_replica_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse add_replica_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(add_replica_data.name(), add_replica_data.db(), &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", add_replica_data.name().c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t pid = add_replica_data.pid();
    uint32_t remote_tid = add_replica_data.remote_tid();
    std::string name = add_replica_data.name();
    std::string db = add_replica_data.db();
    std::string alias = add_replica_data.alias();
    ::openmldb::nameserver::TablePartition table_partition = add_replica_data.table_partition();
    std::string endpoint;
    for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
        if (table_partition.partition_meta(meta_idx).is_leader()) {
            endpoint = table_partition.partition_meta(meta_idx).endpoint();
            break;
        }
    }

    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kAddReplicaRemoteOP;
    auto task = CreateTask<PauseSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<SendSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, remote_tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING,
              "create sendsnapshot task failed. leader cluster tid[%u] replica "
              "cluster tid[%u] pid[%u]",
              tid, remote_tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateLoadTableRemoteTask(alias, name, db, endpoint, pid, op_index,
                                     ::openmldb::api::OPType::kAddReplicaRemoteOP);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u]", tid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint, remote_tid);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]", tid,
              remote_tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateTask<RecoverSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
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
        task = CreateTask<AddReplicaNSRemoteTaskMeta>(op_index, op_type, name, alias, endpoint_vec, pid);
        if (!task) {
            PDLOG(WARNING,
                  "create addreplicaNS remote task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]", tid,
                  remote_tid, pid);
            return -1;
        }
        op_data->task_list_.push_back(task);
    }

    task = CreateTask<AddTableInfoTaskMeta>(op_index, op_type, name, db, pid, endpoint, alias, remote_tid);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    PDLOG(INFO, "create AddReplicaRemoteOP task ok. tid[%u] pid[%u] endpoint[%s]", tid, pid, endpoint.c_str());
    return 0;
}

void NameServerImpl::AddReplicaNS(RpcController* controller, const AddReplicaNSRequest* request,
                                  GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
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
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", request->endpoint().c_str());
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        PDLOG(WARNING, "table[%s] does not exist", request->name().c_str());
        return;
    }
    if (*(pid_group.rbegin()) > (uint32_t)table_info->table_partition_size() - 1) {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
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
                response->set_code(::openmldb::base::ReturnCode::kPidAlreadyExists);
                char msg[100];
                sprintf(msg, "pid %u is exist in %s",  // NOLINT
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
        if (CreateOPData(::openmldb::api::OPType::kAddReplicaOP, value, op_data, request->name(), request->db(), pid) <
            0) {
            PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]", request->name().c_str(), pid);
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
            response->set_msg("set zk failed");
            return;
        }
        if (CreateAddReplicaOPTask(op_data) < 0) {
            PDLOG(WARNING,
                  "create AddReplicaOP task failed. table[%s] pid[%u] "
                  "endpoint[%s]",
                  request->name().c_str(), pid, request->endpoint().c_str());
            response->set_code(::openmldb::base::ReturnCode::kCreateOpFailed);
            response->set_msg("create op failed");
            return;
        }
        if (AddOPData(op_data, 1) < 0) {
            response->set_code(::openmldb::base::ReturnCode::kAddOpDataFailed);
            response->set_msg("add op data failed");
            PDLOG(WARNING, "add op data failed. table[%s] pid[%u]", request->name().c_str(), pid);
            return;
        }
        PDLOG(INFO, "add addreplica op ok. op_id[%lu] table[%s] pid[%u]", op_data->op_info_.op_id(),
              request->name().c_str(), pid);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::AddReplicaNSFromRemote(RpcController* controller, const AddReplicaNSRequest* request,
                                            GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    uint32_t pid = request->pid();
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", request->endpoint().c_str());
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        PDLOG(WARNING, "table[%s] does not exist", request->name().c_str());
        return;
    }
    if (pid > (uint32_t)table_info->table_partition_size() - 1) {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
        response->set_msg("invalid parameter");
        PDLOG(WARNING, "max pid is greater than partition size. table[%s]", request->name().c_str());
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (pid == table_info->table_partition(idx).pid()) {
            for (int group_idx = 0; group_idx < request->endpoint_group_size(); group_idx++) {
                for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                    if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() ==
                        request->endpoint_group(group_idx)) {
                        response->set_code(::openmldb::base::ReturnCode::kPidAlreadyExists);
                        char msg[100];
                        sprintf(msg, "pid %u is exist in %s",  // NOLINT
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
        if (CreateOPData(::openmldb::api::OPType::kAddReplicaOP, value, op_data, request->name(), request->db(), pid,
                         INVALID_PARENT_ID, request->task_info().op_id()) < 0) {
            PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]", request->name().c_str(), pid);
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
            response->set_msg("set zk failed");
            return;
        }
        if (CreateAddReplicaOPTask(op_data) < 0) {
            PDLOG(WARNING,
                  "create AddReplicaOP task failed. table[%s] pid[%u] "
                  "endpoint[%s]",
                  request->name().c_str(), pid, endpoint.c_str());
            response->set_code(::openmldb::base::ReturnCode::kCreateOpFailed);
            response->set_msg("create op failed");
            return;
        }
        if (AddOPData(op_data, 1) < 0) {
            response->set_code(::openmldb::base::ReturnCode::kAddOpDataFailed);
            response->set_msg("add op data failed");
            PDLOG(WARNING, "add op data failed. table[%s] pid[%u]", request->name().c_str(), pid);
            return;
        }
        rep_cluster_op_id_vec.push_back(op_data->op_info_.op_id());  // for multi cluster
        PDLOG(INFO, "add addreplica op ok. op_id[%lu] table[%s] pid[%u]", op_data->op_info_.op_id(),
              request->name().c_str(), pid);
    }
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kAddReplicaNSRemote, task_ptr,
                  rep_cluster_op_id_vec) < 0) {
        response->set_code(::openmldb::base::ReturnCode::kAddTaskInReplicaClusterNsFailed);
        response->set_msg("add task in replica cluster ns failed");
        return;
    }
    PDLOG(INFO,
          "add task in replica cluster ns success, op_id [%lu] task_tpye [%s] "
          "task_status [%s]",
          task_ptr->op_id(), ::openmldb::api::TaskType_Name(task_ptr->task_type()).c_str(),
          ::openmldb::api::TaskStatus_Name(task_ptr->status()).c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

int NameServerImpl::CreateAddReplicaOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto it = tablets_.find(request.endpoint());
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", request.endpoint().c_str());
        return -1;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request.name(), request.db(), &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", request.name().c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t pid = request.pid();
    uint32_t seg_cnt = table_info->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kAddReplicaOP;
    auto task = CreateTask<PauseSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<SendSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<LoadTableTaskMeta>(op_index, op_type, request.endpoint(), request.name(), tid, pid, seg_cnt,
                                         false, table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<RecoverSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddTableInfoTaskMeta>(op_index, op_type, request.name(), request.db(), pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CheckBinlogSyncProgressTaskMeta>(op_index, op_type, request.name(), request.db(), pid,
                                                       request.endpoint(), FLAGS_check_binlog_sync_progress_delta);
    if (!task) {
        PDLOG(WARNING, "create checkbinlogsyncprogress task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, request.name(), request.db(), pid,
                                                     request.endpoint(), false, true);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]",
              request.name().c_str(), pid, request.endpoint().c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create AddReplicaOP task ok. tid[%u] pid[%u] endpoint[%s]", tid, pid, request.endpoint().c_str());
    return 0;
}

void NameServerImpl::Migrate(RpcController* controller, const MigrateRequest* request, GeneralResponse* response,
                             Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto pos = tablets_.find(request->src_endpoint());
    if (pos == tablets_.end() || pos->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        response->set_code(::openmldb::base::ReturnCode::kSrcEndpointIsNotExistOrNotHealthy);
        response->set_msg("src_endpoint does not exist or not healthy");
        PDLOG(WARNING, "src_endpoint[%s] does not exist or not healthy", request->src_endpoint().c_str());
        return;
    }
    pos = tablets_.find(request->des_endpoint());
    if (pos == tablets_.end() || pos->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        response->set_code(::openmldb::base::ReturnCode::kDesEndpointIsNotExistOrNotHealthy);
        response->set_msg("des_endpoint does not exist or not healthy");
        PDLOG(WARNING, "des_endpoint[%s] does not exist or not healthy", request->des_endpoint().c_str());
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        PDLOG(WARNING, "table[%s] does not exist", request->name().c_str());
        return;
    }
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
            sprintf(error_msg,  // NOLINT
                    "leader endpoint is empty. name[%s] pid[%u]", request->name().c_str(), pid);
            has_error = true;
            break;
        }
        if (leader_endpoint == request->src_endpoint()) {
            sprintf(error_msg,  // NOLINT
                    "cannot migrate leader. name[%s] pid[%u]", request->name().c_str(), pid);
            has_error = true;
            break;
        }
        auto it = tablets_.find(leader_endpoint);
        if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
            sprintf(error_msg,  // NOLINT
                    "leader[%s] is offline. name[%s] pid[%u]", leader_endpoint.c_str(), request->name().c_str(), pid);
            has_error = true;
            break;
        }
        if (!has_found_src_endpoint) {
            sprintf(  // NOLINT
                error_msg, "src_endpoint[%s] has not partition[%u]. name[%s]", request->src_endpoint().c_str(), pid,
                request->name().c_str());
            has_error = true;
            break;
        }
        if (has_found_des_endpoint) {
            sprintf(error_msg,  // NOLINT
                    "partition[%u] is already in des_endpoint[%s]. name[%s]", pid, request->des_endpoint().c_str(),
                    request->name().c_str());
            has_error = true;
            break;
        }
    }
    if (has_error) {
        response->set_code(::openmldb::base::ReturnCode::kMigrateFailed);
        response->set_msg(error_msg);
        PDLOG(WARNING, "%s", error_msg);
        return;
    }
    for (int i = 0; i < request->pid_size(); i++) {
        uint32_t pid = request->pid(i);
        CreateMigrateOP(request->src_endpoint(), request->name(), request->db(), pid, request->des_endpoint());
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

int NameServerImpl::CreateMigrateOP(const std::string& src_endpoint, const std::string& name, const std::string& db,
                                    uint32_t pid, const std::string& des_endpoint) {
    std::shared_ptr<OPData> op_data;
    MigrateInfo migrate_info;
    migrate_info.set_src_endpoint(src_endpoint);
    migrate_info.set_des_endpoint(des_endpoint);
    std::string value;
    migrate_info.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kMigrateOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING,
              "create migrate op data failed. src_endpoint[%s] name[%s] "
              "pid[%u] des_endpoint[%s]",
              src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    if (CreateMigrateTask(op_data) < 0) {
        PDLOG(WARNING,
              "create migrate op task failed. src_endpoint[%s] name[%s] "
              "pid[%u] des_endpoint[%s]",
              src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING,
              "add migrate op data failed. src_endpoint[%s] name[%s] pid[%u] "
              "des_endpoint[%s]",
              src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    PDLOG(INFO,
          "add migrate op ok. op_id[%lu] src_endpoint[%s] name[%s] pid[%u] "
          "des_endpoint[%s]",
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
    std::string db = op_data->op_info_.db();
    uint32_t pid = op_data->op_info_.pid();
    std::string src_endpoint = migrate_info.src_endpoint();
    std::string des_endpoint = migrate_info.des_endpoint();
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "get table info failed! name[%s]", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    auto it = tablets_.find(leader_endpoint);
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "leader[%s] is not online", leader_endpoint.c_str());
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kMigrateOP;
    auto task = CreateTask<PauseSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u] endpoint[%s]", tid, pid,
              leader_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<SendSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, tid, pid, des_endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", tid, pid,
              leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<RecoverSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", tid, pid,
              leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<LoadTableTaskMeta>(op_index, op_type, des_endpoint, name, tid, pid, table_info->seg_cnt(), false,
                                         table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u] endpoint[%s]", tid, pid, des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, des_endpoint);
    if (!task) {
        PDLOG(WARNING,
              "create addreplica task failed. tid[%u] pid[%u] endpoint[%s] "
              "des_endpoint[%s]",
              tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddTableInfoTaskMeta>(op_index, op_type, name, db, pid, des_endpoint);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", tid, pid,
              leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CheckBinlogSyncProgressTaskMeta>(op_index, op_type, name, db, pid, des_endpoint,
                                                       FLAGS_check_binlog_sync_progress_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<DelReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, src_endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. tid[%u] pid[%u] leader[%s] follower[%s]", tid, pid,
              leader_endpoint.c_str(), src_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdateTableInfoTaskMeta>(op_index, op_type, name, db, pid, src_endpoint, des_endpoint);
    if (!task) {
        PDLOG(WARNING, "create update table info task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", tid, pid,
              src_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<DropTableTaskMeta>(op_index, op_type, src_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u] endpoint[%s]", tid, pid, src_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create migrate op task ok. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", src_endpoint.c_str(),
          name.c_str(), pid, des_endpoint.c_str());
    return 0;
}

void NameServerImpl::DelReplicaNS(RpcController* controller, const DelReplicaNSRequest* request,
                                  GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
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
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        PDLOG(WARNING, "table[%s] does not exist", request->name().c_str());
        return;
    }
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
        response->set_msg("tablet is not healthy");
        PDLOG(WARNING, "tablet[%s] is not healthy", request->endpoint().c_str());
        return;
    }
    if (*(pid_group.rbegin()) > (uint32_t)table_info->table_partition_size() - 1) {
        response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
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
            response->set_code(::openmldb::base::ReturnCode::kPidIsNotExist);
            sprintf(msg, "pid %u is not in %s",  // NOLINT
                    table_info->table_partition(idx).pid(), request->endpoint().c_str());
            response->set_msg(msg);
            PDLOG(WARNING, "table %s %s", request->name().c_str(), msg);
            return;
        } else if (is_leader) {
            char msg[100];
            response->set_code(::openmldb::base::ReturnCode::kTableIsLeader);
            sprintf(msg, "can not del leader. pid %u endpoint %s",  // NOLINT
                    table_info->table_partition(idx).pid(), request->endpoint().c_str());
            response->set_msg(msg);
            PDLOG(WARNING, "table %s %s", request->name().c_str(), msg);
            return;
        }
    }
    for (auto pid : pid_group) {
        if (CreateDelReplicaOP(request->name(), request->db(), pid, request->endpoint()) < 0) {
            response->set_code(::openmldb::base::ReturnCode::kCreateOpFailed);
            response->set_msg("create op failed");
            return;
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

int NameServerImpl::DelReplicaRemoteOP(const std::string& endpoint, const std::string& name, const std::string& db,
                                       uint32_t pid) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::openmldb::api::OPType::kDelReplicaRemoteOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create op data error. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateDelReplicaRemoteOPTask(op_data) < 0) {
        PDLOG(WARNING, "create delreplica op task failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, FLAGS_name_server_task_concurrency_for_replica_cluster) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add delreplica op. op_id[%lu] table[%s] pid[%u] endpoint[%s]", op_index_, name.c_str(), pid,
          endpoint.c_str());
    return 0;
}

int NameServerImpl::AddOPTask(const ::openmldb::api::TaskInfo& task_info, ::openmldb::api::TaskType task_type,
                              std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr,
                              std::vector<uint64_t> rep_cluster_op_id_vec) {
    if (FindTask(task_info.op_id(), task_info.task_type())) {
        PDLOG(WARNING, "task is running. op_id[%lu] op_type[%s] task_type[%s]", task_info.op_id(),
              ::openmldb::api::OPType_Name(task_info.op_type()).c_str(),
              ::openmldb::api::TaskType_Name(task_info.task_type()).c_str());
        return -1;
    }
    task_ptr.reset(task_info.New());
    task_ptr->CopyFrom(task_info);
    task_ptr->set_status(::openmldb::api::TaskStatus::kDoing);
    for (auto op_id : rep_cluster_op_id_vec) {
        task_ptr->add_rep_cluster_op_id(op_id);
    }
    auto iter = task_map_.find(task_info.op_id());
    if (iter == task_map_.end()) {
        task_map_.insert(std::make_pair(task_info.op_id(), std::list<std::shared_ptr<::openmldb::api::TaskInfo>>()));
    }
    task_map_[task_info.op_id()].push_back(task_ptr);
    if (task_info.task_type() != task_type) {
        PDLOG(WARNING, "task type is not match. type is[%s]",
              ::openmldb::api::TaskType_Name(task_info.task_type()).c_str());
        task_ptr->set_status(::openmldb::api::TaskStatus::kFailed);
        return -1;
    }
    return 0;
}

std::shared_ptr<::openmldb::api::TaskInfo> NameServerImpl::FindTask(uint64_t op_id,
                                                                    ::openmldb::api::TaskType task_type) {
    auto iter = task_map_.find(op_id);
    if (iter == task_map_.end()) {
        return std::shared_ptr<::openmldb::api::TaskInfo>();
    }
    for (auto& task : iter->second) {
        if (task->op_id() == op_id && task->task_type() == task_type) {
            return task;
        }
    }
    return std::shared_ptr<::openmldb::api::TaskInfo>();
}

std::shared_ptr<openmldb::nameserver::ClusterInfo> NameServerImpl::GetHealthCluster(const std::string& alias) {
    auto iter = nsc_.find(alias);
    if (iter == nsc_.end() || iter->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
        return std::shared_ptr<openmldb::nameserver::ClusterInfo>();
    }
    return iter->second;
}

int NameServerImpl::CreateOPData(::openmldb::api::OPType op_type, const std::string& value,
                                 std::shared_ptr<OPData>& op_data, const std::string& name, const std::string& db,
                                 uint32_t pid, uint64_t parent_id, uint64_t remote_op_id) {
    if (!zk_client_->SetNodeValue(zk_path_.op_index_node_, std::to_string(op_index_ + 1))) {
        PDLOG(WARNING, "set op index node failed! op_index[%lu]", op_index_);
        return -1;
    }
    op_index_++;
    op_data = std::make_shared<OPData>();
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(op_type);
    op_data->op_info_.set_task_index(0);
    op_data->op_info_.set_data(value);
    op_data->op_info_.set_task_status(::openmldb::api::kInited);
    op_data->op_info_.set_name(name);
    op_data->op_info_.set_db(db);
    op_data->op_info_.set_pid(pid);
    op_data->op_info_.set_parent_id(parent_id);
    if (remote_op_id != INVALID_PARENT_ID) {
        op_data->op_info_.set_remote_op_id(remote_op_id);
    }
    return 0;
}

int NameServerImpl::AddOPData(const std::shared_ptr<OPData>& op_data, uint32_t concurrency) {
    uint32_t idx = 0;
    if (op_data->op_info_.for_replica_cluster() == 1) {
        if (op_data->op_info_.pid() == INVALID_PID) {
            idx = FLAGS_name_server_task_max_concurrency +
                  (::openmldb::base::hash64(op_data->op_info_.name()) % concurrency);
        } else {
            idx = FLAGS_name_server_task_max_concurrency + (rand_.Next() % concurrency);
        }
    } else {
        idx = op_data->op_info_.pid() % task_vec_.size();
        if (concurrency < task_vec_.size() && concurrency > 0) {
            idx = op_data->op_info_.pid() % concurrency;
        }
    }
    op_data->op_info_.set_vec_idx(idx);
    std::string value;
    op_data->op_info_.SerializeToString(&value);
    std::string node = absl::StrCat(zk_path_.op_data_path_, "/", op_data->GetOpId());
    if (!zk_client_->CreateNode(node, value)) {
        PDLOG(WARNING, "create op node[%s] failed. op_index[%lu] op_type[%s]", node.c_str(), op_data->GetOpId(),
              op_data->GetReadableType().c_str());
        return -1;
    }
    uint64_t parent_id = op_data->op_info_.parent_id();
    if (parent_id != INVALID_PARENT_ID) {
        std::list<std::shared_ptr<OPData>>::iterator iter = task_vec_[idx].begin();
        for (; iter != task_vec_[idx].end(); iter++) {
            if ((*iter)->op_info_.op_id() == parent_id) {
                break;
            }
        }
        if (iter != task_vec_[idx].end()) {
            iter++;
            task_vec_[idx].insert(iter, op_data);
        } else {
            PDLOG(WARNING, "not found parent_id[%lu] with index[%u]. add op[%lu] failed, op_type[%s]", parent_id, idx,
                  op_data->GetOpId(), op_data->GetReadableType().c_str());
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
        if (op_data->op_info_.task_status() == ::openmldb::api::TaskStatus::kFailed) {
            std::string node = absl::StrCat(zk_path_.op_data_path_, "/", op_data->GetOpId());
            if (zk_client_->DeleteNode(node)) {
                PDLOG(INFO, "delete zk op node[%s] success.", node.c_str());
                op_data->task_list_.clear();
            } else {
                PDLOG(WARNING, "delete zk op_node failed. op_id[%lu] node[%s]", op_data->GetOpId(), node.c_str());
                break;
            }
        }
        PDLOG(INFO, "done_op_list size[%u] is greater than the max_op_num[%u], delete op[%lu]", done_op_list_.size(),
              (uint32_t)FLAGS_max_op_num, op_data->GetOpId());
        done_op_list_.pop_front();
    }
}

void NameServerImpl::SchedMakeSnapshot() {
    if (!running_.load(std::memory_order_acquire) || mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval,
                                    boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
        return;
    }
    int now_hour = ::openmldb::base::GetNowHour();
    if (now_hour != FLAGS_make_snapshot_time) {
        task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval,
                                    boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
        return;
    }
    std::map<std::string, std::shared_ptr<TabletInfo>> tablet_ptr_map;
    std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>> table_infos;
    std::map<std::string, std::shared_ptr<::openmldb::nameserver::NsClient>> ns_client;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info_.size() < 1) {
            task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval,
                                        boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
            return;
        }
        for (const auto& kv : tablets_) {
            if (kv.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                continue;
            }
            tablet_ptr_map.insert(std::make_pair(kv.first, kv.second));
        }
        for (auto iter = nsc_.begin(); iter != nsc_.end(); ++iter) {
            if (iter->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                PDLOG(INFO, "cluster[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            ns_client.insert(std::make_pair(
                iter->first, std::atomic_load_explicit(&iter->second->client_, std::memory_order_relaxed)));
        }
        for (auto iter = table_info_.begin(); iter != table_info_.end(); ++iter) {
            table_infos.insert(std::make_pair(iter->first, iter->second));
        }
    }
    std::map<std::string, std::map<uint32_t, uint64_t>> table_part_offset;
    {
        std::vector<TableInfo> tables;
        std::vector<std::string> delete_map;
        std::string msg;
        for (const auto& ns : ns_client) {
            if (!ns.second->ShowAllTable(tables, msg)) {
                delete_map.push_back(ns.first);
                continue;
            }
            for (const auto& table : tables) {
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
        auto table_iter = table_part_offset.find(table.second->name());
        if (table_iter == table_part_offset.end()) {
            continue;
        }
        for (const auto& part : table.second->table_partition()) {
            auto part_iter = table_iter->second.find(part.pid());
            if (part_iter == table_iter->second.end()) {
                continue;
            }
            if (part_iter->second < 1) {
                PDLOG(WARNING, "table %s pid %u snapshot offset is %lu, too small, skip makesnapshot",
                      table.second->name().c_str(), part.pid(), part_iter->second);
                continue;
            }
            PDLOG(INFO, "table %s pid %u specify snapshot offset is %lu", table.second->name().c_str(), part.pid(),
                  part_iter->second);
            for (const auto& part_meta : part.partition_meta()) {
                if (part_meta.is_alive()) {
                    auto client_iter = tablet_ptr_map.find(part_meta.endpoint());
                    if (client_iter != tablet_ptr_map.end()) {
                        thread_pool_.AddTask(boost::bind(&TabletClient::MakeSnapshot, client_iter->second->client_,
                                                         table.second->tid(), part.pid(), part_iter->second,
                                                         std::shared_ptr<openmldb::api::TaskInfo>()));
                    }
                }
            }
            std::string msg;
            for (const auto& ns : ns_client) {
                ns.second->MakeSnapshot(table.second->name(), table.second->db(), part.pid(), part_iter->second, msg);
            }
        }
    }
    PDLOG(INFO, "make snapshot finished");
    task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval + 60 * 60 * 1000,
                                boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
}

void NameServerImpl::UpdateTableStatus() {
    std::map<std::string, std::shared_ptr<TabletInfo>> tablet_ptr_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                continue;
            }
            tablet_ptr_map.insert(std::make_pair(kv.first, kv.second));
        }
    }
    std::unordered_map<std::string, ::openmldb::api::TableStatus> pos_response;
    pos_response.reserve(16);
    for (const auto& kv : tablet_ptr_map) {
        ::openmldb::api::GetTableStatusResponse tablet_status_response;
        if (auto st = kv.second->client_->GetTableStatus(tablet_status_response); !st.OK()) {
            PDLOG(WARNING, "get table status failed! endpoint[%s], %s", kv.first.c_str(), st.GetMsg());
            continue;
        }
        for (int pos = 0; pos < tablet_status_response.all_table_status_size(); pos++) {
            std::string key = absl::StrCat(tablet_status_response.all_table_status(pos).tid(), "_",
                                           tablet_status_response.all_table_status(pos).pid(), "_", kv.first);
            pos_response.emplace(key, tablet_status_response.all_table_status(pos));
        }
    }
    if (pos_response.empty()) {
        DEBUGLOG("pos_response is empty");
    } else {
        UpdateTableStatusFun(table_info_, pos_response);
        for (const auto& kv : db_table_info_) {
            UpdateTableStatusFun(kv.second, pos_response);
        }
    }
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_table_status_interval,
                                    boost::bind(&NameServerImpl::UpdateTableStatus, this));
    }
}

void NameServerImpl::UpdateTableStatusFun(
    const std::map<std::string, std::shared_ptr<TableInfo>>& table_info_map,
    const std::unordered_map<std::string, ::openmldb::api::TableStatus>& pos_response) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_map) {
        uint32_t tid = kv.second->tid();
        std::string first_index_col;
        if (kv.second->column_key_size() > 0) {
            first_index_col = kv.second->column_key(0).index_name();
        }
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid = kv.second->table_partition(idx).pid();
            auto table_partition = kv.second->mutable_table_partition(idx);
            auto partition_meta_field = table_partition->mutable_partition_meta();
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                std::string endpoint = kv.second->table_partition(idx).partition_meta(meta_idx).endpoint();
                bool tablet_has_partition = false;
                auto partition_meta = partition_meta_field->Mutable(meta_idx);
                std::string pos_key = absl::StrCat(tid, "_", pid, "_", endpoint);
                auto pos_response_iter = pos_response.find(pos_key);
                if (pos_response_iter != pos_response.end()) {
                    const ::openmldb::api::TableStatus& table_status = pos_response_iter->second;
                    partition_meta->set_offset(table_status.offset());
                    partition_meta->set_record_byte_size(table_status.record_byte_size() +
                                                         table_status.record_idx_byte_size());
                    uint64_t record_cnt = table_status.record_cnt();
                    if (!first_index_col.empty()) {
                        for (int pos = 0; pos < table_status.ts_idx_status_size(); pos++) {
                            if (table_status.ts_idx_status(pos).idx_name() == first_index_col) {
                                record_cnt = 0;
                                for (int seg_idx = 0; seg_idx < table_status.ts_idx_status(pos).seg_cnts_size();
                                     seg_idx++) {
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

int NameServerImpl::CreateDelReplicaOP(const std::string& name, const std::string& db, uint32_t pid,
                                       const std::string& endpoint) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::openmldb::api::OPType::kDelReplicaOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create op data error. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateDelReplicaOPTask(op_data) < 0) {
        PDLOG(WARNING, "create delreplica op task failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add delreplica op. op_id[%lu] table[%s] pid[%u] endpoint[%s]", op_index_, name.c_str(), pid,
          endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateDelReplicaOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (leader_endpoint == endpoint) {
        PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kDelReplicaOP;
    auto task = CreateTask<DelReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<DelTableInfoTaskMeta>(op_index, op_type, name, db, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create deltableinfo task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<DropTableTaskMeta>(op_index, op_type, endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u] endpoint[%s]", tid, pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DelReplica op task ok. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateDelReplicaRemoteOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kDelReplicaRemoteOP;
    auto task = CreateTask<DelReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<DelTableInfoTaskMeta>(op_index, op_type, name, db, pid, endpoint, 1);
    if (!task) {
        PDLOG(WARNING, "create deltableinfo task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DelReplica op task ok. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateOfflineReplicaOP(const std::string& name, const std::string& db, uint32_t pid,
                                           const std::string& endpoint, uint32_t concurrency) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::openmldb::api::OPType::kOfflineReplicaOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create op data failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateOfflineReplicaTask(op_data) < 0) {
        PDLOG(WARNING, "create offline replica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add kOfflineReplicaOP. op_id[%lu] table[%s] pid[%u] endpoint[%s]", op_index_, name.c_str(), pid,
          endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateOfflineReplicaTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
    uint32_t pid = op_data->op_info_.pid();
    uint64_t op_index = op_data->op_info_.op_id();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "no alive leader for table %s pid %u", name.c_str(), pid);
        return -1;
    } else {
        if (leader_endpoint == endpoint) {
            PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
            return -1;
        }
        auto op_type = ::openmldb::api::OPType::kOfflineReplicaOP;
        auto task = CreateTask<DelReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
        if (!task) {
            PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
                  endpoint.c_str());
            return -1;
        }
        op_data->task_list_.push_back(task);
        task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, false, false);
        if (!task) {
            PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
                  pid, endpoint.c_str());
            return -1;
        }
        op_data->task_list_.push_back(task);
        PDLOG(INFO, "create OfflineReplica task ok. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
    }

    return 0;
}

int NameServerImpl::CreateChangeLeaderOP(const std::string& name, const std::string& db, uint32_t pid,
                                         const std::string& candidate_leader, bool need_restore, uint32_t concurrency) {
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    std::vector<std::string> follower_endpoint;
    std::vector<::openmldb::common::EndpointAndTid> remote_follower_endpoint;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (!table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    auto tablets_iter = tablets_.find(endpoint);
                    if (tablets_iter != tablets_.end() &&
                        tablets_iter->second->state_ == ::openmldb::type::EndpointState::kHealthy) {
                        follower_endpoint.push_back(endpoint);
                    } else {
                        PDLOG(WARNING, "endpoint[%s] is offline. table[%s] pid[%u]", endpoint.c_str(), name.c_str(),
                              pid);
                    }
                }
            }
        }
        for (int i = 0; i < table_info->table_partition(idx).remote_partition_meta_size(); i++) {
            if (table_info->table_partition(idx).remote_partition_meta(i).is_alive()) {
                ::openmldb::common::EndpointAndTid et;
                std::string endpoint = table_info->table_partition(idx).remote_partition_meta(i).endpoint();
                uint32_t tid = table_info->table_partition(idx).remote_partition_meta(i).remote_tid();
                et.set_endpoint(endpoint);
                et.set_tid(tid);
                remote_follower_endpoint.push_back(et);
            }
        }
        break;
    }

    if (need_restore && !candidate_leader.empty() &&
        std::find(follower_endpoint.begin(), follower_endpoint.end(), candidate_leader) == follower_endpoint.end()) {
        follower_endpoint.push_back(candidate_leader);
    }
    if (follower_endpoint.empty()) {
        PDLOG(INFO, "table not found follower. name[%s] pid[%u]", name.c_str(), pid);
        return 0;
    }
    if (!candidate_leader.empty() &&
        std::find(follower_endpoint.begin(), follower_endpoint.end(), candidate_leader) == follower_endpoint.end()) {
        PDLOG(WARNING, "candidate_leader[%s] is not in followers. name[%s] pid[%u]", candidate_leader.c_str(),
              name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    ChangeLeaderData change_leader_data;
    change_leader_data.set_name(name);
    change_leader_data.set_db(db);
    change_leader_data.set_tid(tid);
    change_leader_data.set_pid(pid);
    for (const auto& endpoint : follower_endpoint) {
        change_leader_data.add_follower(endpoint);
    }
    for (const auto& endpoint : remote_follower_endpoint) {
        change_leader_data.add_remote_follower()->CopyFrom(endpoint);
    }
    if (!candidate_leader.empty()) {
        change_leader_data.set_candidate_leader(candidate_leader);
    }
    std::string value;
    change_leader_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kChangeLeaderOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create ChangeLeaderOP data error. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateChangeLeaderOPTask(op_data) < 0) {
        PDLOG(WARNING, "create ChangeLeaderOP task failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add changeleader op. op_id[%lu] table[%s] pid[%u]", op_data->op_info_.op_id(), name.c_str(), pid);
    return 0;
}

int NameServerImpl::CreateChangeLeaderOPTask(std::shared_ptr<OPData> op_data) {
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]", op_data->op_info_.op_id(),
              op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = change_leader_data.name();
    uint32_t tid = change_leader_data.tid();
    uint32_t pid = change_leader_data.pid();
    std::string db = change_leader_data.db();
    uint64_t op_index = op_data->op_info_.op_id();
    ::openmldb::api::OPType op_type = ::openmldb::api::OPType::kChangeLeaderOP;
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < change_leader_data.follower_size(); idx++) {
        follower_endpoint.push_back(change_leader_data.follower(idx));
    }
    auto task = CreateTask<SelectLeaderTaskMeta>(op_index, op_type, name, db, tid, pid, follower_endpoint);
    if (!task) {
        PDLOG(WARNING, "create selectleader task failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "create SelectLeader task success. name[%s] tid[%u] pid[%u]", name.c_str(), tid, pid);
    op_data->task_list_.push_back(task);
    task = CreateTask<ChangeLeaderTaskMeta>(op_index, op_type);
    if (!task) {
        PDLOG(WARNING, "create changeleader task failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "create ChangeLeader task success. name[%s] pid[%u]", name.c_str(), pid);
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdateLeaderInfoTaskMeta>(op_index, op_type);
    if (!task) {
        PDLOG(WARNING, "create updateleaderinfo task failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "create UpdateLeaderInfo task success. name[%s] pid[%u]", name.c_str(), pid);
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ChangeLeader op task ok. name[%s] pid[%u]", name.c_str(), pid);
    return 0;
}

void NameServerImpl::OnLocked() {
    if (!Recover()) {
        PDLOG(WARNING, "recover failed");
    }
    CreateDatabaseOrExit(INTERNAL_DB);
    if (IsClusterMode()) {
        if (tablets_.size() < FLAGS_system_table_replica_num) {
            LOG(ERROR) << "tablet num " << tablets_.size() << " is less then system table replica num "
                       << FLAGS_system_table_replica_num;
            exit(1);
        }

        if (FLAGS_system_table_replica_num > 0 && db_table_info_[INTERNAL_DB].count(JOB_INFO_NAME) == 0) {
            CreateSystemTableOrExit(SystemTableType::kJobInfo);
        }
    }
    if (FLAGS_system_table_replica_num > 0 && db_table_info_[INTERNAL_DB].count(USER_INFO_NAME) == 0) {
        CreateSystemTableOrExit(SystemTableType::kUser);
        InsertUserRecord("%", "root", "1e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    if (FLAGS_system_table_replica_num > 0 && db_table_info_[INTERNAL_DB].count(PRE_AGG_META_NAME) == 0) {
        CreateSystemTableOrExit(SystemTableType::kPreAggMetaInfo);
    }

    CreateDatabaseOrExit(PRE_AGG_DB);

    CreateDatabaseOrExit(INFORMATION_SCHEMA_DB);
    auto& table_infos = db_table_info_[INFORMATION_SCHEMA_DB];
    // TODO(ace): create table if not exists
    if (FLAGS_system_table_replica_num > 0) {
        if (table_infos.count(GLOBAL_VARIABLES) == 0) {
            CreateSystemTableOrExit(SystemTableType::kGlobalVariable);
            InitGlobalVarTable();
        }
        if (auto iter = table_infos.find(DEPLOY_RESPONSE_TIME); iter == table_infos.end()) {
            CreateSystemTableOrExit(SystemTableType::kDeployResponseTime);
        } else if (iter->second->column_desc(2).data_type() != type::DataType::kBigInt) {
            LOG(WARNING) << "the result of count in DEPLOY_RESPONSE_TIME may overflow as the type is int32";
        }
    }

    running_.store(true, std::memory_order_release);
    task_thread_pool_.DelayTask(FLAGS_get_task_status_interval,
                                boost::bind(&NameServerImpl::UpdateTaskStatus, this, false));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::UpdateTableStatus, this));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::ProcessTask, this));
    thread_pool_.AddTask(boost::bind(&NameServerImpl::DistributeTabletMode, this));
    task_thread_pool_.DelayTask(FLAGS_get_replica_status_interval,
                                boost::bind(&NameServerImpl::CheckClusterInfo, this));
    task_thread_pool_.DelayTask(FLAGS_make_snapshot_check_interval,
                                boost::bind(&NameServerImpl::SchedMakeSnapshot, this));
}

void NameServerImpl::OnLostLock() {
    PDLOG(INFO, "become the stand by name sever");
    running_.store(false, std::memory_order_release);
}

int NameServerImpl::CreateRecoverTableOP(const std::string& name, const std::string& db, uint32_t pid,
                                         const std::string& endpoint, bool is_leader, uint64_t offset_delta,
                                         uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_is_leader(is_leader);
    recover_table_data.set_offset_delta(offset_delta);
    recover_table_data.set_concurrency(concurrency);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kRecoverTableOP, value, op_data, name, db, pid) < 0) {
        PDLOG(WARNING, "create RecoverTableOP data error. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (CreateRecoverTableOPTask(op_data) < 0) {
        PDLOG(WARNING,
              "create recover table op task failed. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create RecoverTable op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", op_data->op_info_.op_id(),
          name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateRecoverTableOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
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
    auto op_type = ::openmldb::api::OPType::kRecoverTableOP;
    uint64_t op_index = op_data->op_info_.op_id();
    if (!is_leader) {
        std::string leader_endpoint;
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
        if (!GetTableInfoUnlock(name, db, &table_info)) {
            PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
            return -1;
        }
        uint32_t tid = table_info->tid();
        if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
            PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
            return -1;
        }
        if (leader_endpoint == endpoint) {
            PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
            return -1;
        }
        auto task = CreateTask<DelReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
        if (!task) {
            PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
                  endpoint.c_str());
            return -1;
        }
        op_data->task_list_.push_back(task);
    }
    auto task = CreateTask<RecoverTableTaskMeta>(op_index, op_type, name, db, pid, endpoint, offset_delta, concurrency);
    if (!task) {
        PDLOG(WARNING, "create RecoverTable task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create RecoverTable task ok. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    return 0;
}

void NameServerImpl::RecoverEndpointTable(const std::string& name, const std::string& db, uint32_t pid,
                                          std::string& endpoint, uint64_t offset_delta, uint32_t concurrency,
                                          std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    uint32_t tid = 0;
    std::shared_ptr<TabletInfo> leader_tablet_ptr;
    std::shared_ptr<TabletInfo> tablet_ptr;
    bool has_follower = true;
    common::StorageMode storage_mode = common::kMemory;
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
        if (!GetTableInfoUnlock(name, db, &table_info)) {
            PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        tid = table_info->tid();
        storage_mode = table_info->storage_mode();
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            if (table_info->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                const PartitionMeta& partition_meta = table_info->table_partition(idx).partition_meta(meta_idx);
                if (partition_meta.is_leader()) {
                    if (partition_meta.is_alive()) {
                        std::string leader_endpoint = partition_meta.endpoint();
                        auto tablet_iter = tablets_.find(leader_endpoint);
                        if (tablet_iter == tablets_.end()) {
                            PDLOG(WARNING, "can not find the leader endpoint[%s]'s client. op_id[%lu]",
                                  leader_endpoint.c_str(), task_info->op_id());
                            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                            return;
                        }
                        leader_tablet_ptr = tablet_iter->second;
                        if (leader_tablet_ptr->state_ != ::openmldb::type::EndpointState::kHealthy) {
                            PDLOG(WARNING, "leader endpoint [%s] is offline. op_id[%lu]", leader_endpoint.c_str(),
                                  task_info->op_id());
                            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                            return;
                        }
                    } else if (endpoint == OFFLINE_LEADER_ENDPOINT) {
                        endpoint = partition_meta.endpoint();
                        PDLOG(INFO, "use endpoint[%s] to replace[%s], tid[%u] pid[%u]", endpoint.c_str(),
                              OFFLINE_LEADER_ENDPOINT.c_str(), tid, pid);
                    }
                }
                if (partition_meta.endpoint() == endpoint) {
                    if (partition_meta.is_alive()) {
                        PDLOG(INFO, "endpoint[%s] is alive, need not recover. name[%s] pid[%u]", endpoint.c_str(),
                              name.c_str(), pid);
                        task_info->set_status(::openmldb::api::TaskStatus::kDone);
                        return;
                    }
                    auto tablet_iter = tablets_.find(endpoint);
                    if (tablet_iter == tablets_.end()) {
                        PDLOG(WARNING, "can not find the endpoint[%s]'s client. op_id[%lu]", endpoint.c_str(),
                              task_info->op_id());
                        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                        return;
                    }
                    tablet_ptr = tablet_iter->second;
                    if (tablet_ptr->state_ != ::openmldb::type::EndpointState::kHealthy) {
                        PDLOG(WARNING, "endpoint [%s] is offline. op_id[%lu]", endpoint.c_str(), task_info->op_id());
                        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                        return;
                    }
                    if (table_info->table_partition(idx).partition_meta_size() == 1) {
                        has_follower = false;
                        break;
                    }
                }
            }
            break;
        }
    }
    if ((has_follower && !leader_tablet_ptr) || !tablet_ptr) {
        PDLOG(WARNING, "not has tablet. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", name.c_str(), tid, pid,
              endpoint.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    bool has_table = false;
    bool is_leader = false;
    uint64_t term = 0;
    uint64_t offset = 0;
    if (!tablet_ptr->client_->GetTermPair(tid, pid, storage_mode, term, offset, has_table, is_leader)) {
        PDLOG(WARNING, "GetTermPair failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", name.c_str(), tid, pid,
              endpoint.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    if (!has_follower) {
        std::lock_guard<std::mutex> lock(mu_);
        if (has_table) {
            CreateUpdatePartitionStatusOP(name, db, pid, endpoint, true, true, task_info->op_id(), concurrency);
        } else {
            CreateReLoadTableOP(name, db, pid, endpoint, task_info->op_id(), concurrency);
        }
        task_info->set_status(::openmldb::api::TaskStatus::kDone);
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
              ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
        return;
    }
    if (has_table && is_leader) {
        if (!tablet_ptr->client_->ChangeRole(tid, pid, false, 0)) {
            PDLOG(WARNING, "change role failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", name.c_str(), tid,
                  pid, endpoint.c_str(), task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "change to follower. name[%s] tid[%u] pid[%u] endpoint[%s]", name.c_str(), tid, pid,
              endpoint.c_str());
    }
    if (!has_table) {
        if (!tablet_ptr->client_->DeleteBinlog(tid, pid, storage_mode)) {
            PDLOG(WARNING, "delete binlog failed. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", name.c_str(), tid,
                  pid, endpoint.c_str(), task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "delete binlog ok. name[%s] tid[%u] pid[%u] endpoint[%s]", name.c_str(), tid, pid,
              endpoint.c_str());
    }
    int ret_code = MatchTermOffset(name, db, pid, has_table, term, offset);
    if (ret_code < 0) {
        PDLOG(WARNING, "match error. name[%s] tid[%u] pid[%u] endpoint[%s] op_id[%lu]", name.c_str(), tid, pid,
              endpoint.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    ::openmldb::api::Manifest manifest;
    if (!leader_tablet_ptr->client_->GetManifest(tid, pid, storage_mode, manifest)) {
        PDLOG(WARNING, "get manifest failed. name[%s] tid[%u] pid[%u] op_id[%lu]", name.c_str(), tid, pid,
              task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    PDLOG(INFO, "offset[%lu] manifest offset[%lu]. name[%s] tid[%u] pid[%u]", offset, manifest.offset(), name.c_str(),
          tid, pid);
    if (has_table) {
        if (ret_code == 0 && offset >= manifest.offset()) {
            CreateReAddReplicaSimplifyOP(name, db, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        } else {
            CreateReAddReplicaWithDropOP(name, db, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        }
    } else {
        if (ret_code == 0 && offset >= manifest.offset()) {
            CreateReAddReplicaNoSendOP(name, db, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        } else {
            CreateReAddReplicaOP(name, db, pid, endpoint, offset_delta, task_info->op_id(), concurrency);
        }
    }
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "recover table task run success. name[%s] tid[%u] pid[%u]", name.c_str(), tid, pid);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

int NameServerImpl::CreateReAddReplicaOP(const std::string& name, const std::string& db, uint32_t pid,
                                         const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                         uint32_t concurrency) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kReAddReplicaOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP data error. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }

    if (CreateReAddReplicaTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", op_data->op_info_.op_id(),
          name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaTask(std::shared_ptr<OPData> op_data) {
    RecoverTableData recover_table_data;
    if (!recover_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse recover_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t seg_cnt = table_info->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kReAddReplicaOP;
    auto task = CreateTask<PauseSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<SendSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<LoadTableTaskMeta>(op_index, op_type, endpoint, name, tid, pid, seg_cnt, false,
                                         table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<RecoverSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CheckBinlogSyncProgressTaskMeta>(op_index, op_type, name, db, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, false, true);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
              pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica op task ok. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaWithDropOP(const std::string& name, const std::string& db, uint32_t pid,
                                                 const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                                 uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kReAddReplicaWithDropOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING,
              "create ReAddReplicaWithDropOP data error. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReAddReplicaWithDropTask(op_data) < 0) {
        PDLOG(WARNING,
              "create ReAddReplicaWithDropOP task error. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO,
          "create readdreplica with drop op ok. op_id[%lu] name[%s] pid[%u] "
          "endpoint[%s]",
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
    std::string db = op_data->op_info_.db();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t seg_cnt = table_info->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kReAddReplicaWithDropOP;
    auto task = CreateTask<PauseSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<DropTableTaskMeta>(op_index, op_type, endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<SendSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<LoadTableTaskMeta>(op_index, op_type, endpoint, name, tid, pid, seg_cnt, false,
                                         table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<RecoverSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CheckBinlogSyncProgressTaskMeta>(op_index, op_type, name, db, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, false, true);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
              pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ReAddReplicaWithDrop task ok. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
          endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaNoSendOP(const std::string& name, const std::string& db, uint32_t pid,
                                               const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                               uint32_t concurrency) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kReAddReplicaNoSendOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING,
              "create ReAddReplicaNoSendOP data failed. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (CreateReAddReplicaNoSendTask(op_data) < 0) {
        PDLOG(WARNING,
              "create ReAddReplicaNoSendOP task failed. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO,
          "create readdreplica no send op ok. op_id[%lu] name[%s] pid[%u] "
          "endpoint[%s]",
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
    std::string db = op_data->op_info_.db();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t seg_cnt = table_info->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kReAddReplicaNoSendOP;
    auto task = CreateTask<PauseSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<LoadTableTaskMeta>(op_index, op_type, endpoint, name, tid, pid, seg_cnt, false,
                                         table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<RecoverSnapshotTaskMeta>(op_index, op_type, leader_endpoint, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CheckBinlogSyncProgressTaskMeta>(op_index, op_type, name, db, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, false, true);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
              pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica no send task ok. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
          endpoint.c_str());
    return 0;
}

int NameServerImpl::GetLeader(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info, uint32_t pid,
                              std::string& leader_endpoint) {
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

int NameServerImpl::CreateReAddReplicaSimplifyOP(const std::string& name, const std::string& db, uint32_t pid,
                                                 const std::string& endpoint, uint64_t offset_delta, uint64_t parent_id,
                                                 uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    RecoverTableData recover_table_data;
    recover_table_data.set_endpoint(endpoint);
    recover_table_data.set_offset_delta(offset_delta);
    std::string value;
    recover_table_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kReAddReplicaSimplifyOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING,
              "create ReAddReplicaSimplifyOP data error. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReAddReplicaSimplifyTask(op_data) < 0) {
        PDLOG(WARNING,
              "create ReAddReplicaSimplifyOP task failed. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO,
          "create readdreplica simplify op ok. op_id[%lu] name[%s] pid[%u] "
          "endpoint[%s]",
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
    std::string db = op_data->op_info_.db();
    std::string endpoint = recover_table_data.endpoint();
    uint64_t offset_delta = recover_table_data.offset_delta();
    uint32_t pid = op_data->op_info_.pid();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kReAddReplicaSimplifyOP;
    auto task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CheckBinlogSyncProgressTaskMeta>(op_index, op_type, name, db, pid, endpoint, offset_delta);
    if (!task) {
        PDLOG(WARNING, "create CheckBinlogSyncProgressTask failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, false, true);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
              pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica simplify task ok. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
          endpoint.c_str());
    return 0;
}

int NameServerImpl::DropTableRemoteOP(const std::string& name, const std::string& db, const std::string& alias,
                                      uint64_t parent_id, uint32_t concurrency) {
    std::string value = alias;
    uint32_t pid = INVALID_PID;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::openmldb::api::OPType::kDropTableRemoteOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING, "create DropTableRemoteOP data error. table[%s] pid[%u] alias[%s]", name.c_str(), pid,
              alias.c_str());
        return -1;
    }
    if (DropTableRemoteTask(op_data) < 0) {
        PDLOG(WARNING, "create DropTableRemote task failed. table[%s] pid[%u] alias[%s]", name.c_str(), pid,
              alias.c_str());
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] alias[%s]", name.c_str(), pid, alias.c_str());
        return -1;
    }
    PDLOG(INFO, "create DropTableRemote op ok. op_id[%lu] name[%s] pid[%u] alias[%s]", op_data->op_info_.op_id(),
          name.c_str(), pid, alias.c_str());
    return 0;
}

int NameServerImpl::DropTableRemoteTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
    std::string alias = op_data->op_info_.data();
    std::shared_ptr<openmldb::nameserver::ClusterInfo> cluster = GetHealthCluster(alias);
    if (!cluster) {
        PDLOG(WARNING, "replica[%s] not available", alias.c_str());
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kDropTableRemoteOP;
    auto task = CreateTask<DropTableRemoteTaskMeta>(op_index, op_type, name, db, alias);
    if (!task) {
        PDLOG(WARNING, "create DropTableRemote task failed. table[%s] pid[%u]", name.c_str(), op_data->op_info_.pid());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DropTableRemote task ok. name[%s] pid[%u] alias[%s]", name.c_str(), op_data->op_info_.pid(),
          alias.c_str());
    return 0;
}

int NameServerImpl::CreateTableRemoteOP(const ::openmldb::nameserver::TableInfo& table_info,
                                        const ::openmldb::nameserver::TableInfo& remote_table_info,
                                        const std::string& alias, uint64_t parent_id, uint32_t concurrency) {
    CreateTableData create_table_data;
    create_table_data.set_alias(alias);
    create_table_data.mutable_table_info()->CopyFrom(table_info);
    create_table_data.mutable_remote_table_info()->CopyFrom(remote_table_info);
    std::string value;
    create_table_data.SerializeToString(&value);
    std::string name = table_info.name();
    std::string db = table_info.db();
    uint32_t pid = INVALID_PID;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::openmldb::api::OPType::kCreateTableRemoteOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING, "create CreateTableRemoteOP data error. table[%s] pid[%u] alias[%s]", name.c_str(), pid,
              alias.c_str());
        return -1;
    }
    if (CreateTableRemoteTask(op_data) < 0) {
        PDLOG(WARNING, "create CreateTableRemote task failed. table[%s] pid[%u] alias[%s]", table_info.name().c_str(),
              pid, alias.c_str());
        return -1;
    }
    op_data->op_info_.set_for_replica_cluster(1);
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] alias[%s]", table_info.name().c_str(), pid, alias.c_str());
        return -1;
    }
    PDLOG(INFO, "create CreateTableRemote op ok. op_id[%lu] name[%s] pid[%u] alias[%s]", op_data->op_info_.op_id(),
          table_info.name().c_str(), pid, alias.c_str());
    return 0;
}

int NameServerImpl::CreateTableRemoteTask(std::shared_ptr<OPData> op_data) {
    CreateTableData create_table_data;
    if (!create_table_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse create_table_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string alias = create_table_data.alias();
    auto remote_table_info = create_table_data.remote_table_info();
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = ::openmldb::api::OPType::kCreateTableRemoteOP;
    auto task = CreateTask<CreateTableRemoteTaskMeta>(op_index, op_type, remote_table_info, alias);
    if (!task) {
        PDLOG(WARNING, "create CreateTableRemote task failed. table[%s] pid[%u]", remote_table_info.name().c_str(),
              op_data->op_info_.pid());
        return -1;
    }
    op_data->task_list_.push_back(task);

    auto table_info = create_table_data.table_info();
    uint32_t tid = table_info.tid();
    uint32_t remote_tid = remote_table_info.tid();
    std::string name = table_info.name();
    std::string db = table_info.db();
    for (int idx = 0; idx < remote_table_info.table_partition_size(); idx++) {
        const ::openmldb::nameserver::TablePartition& table_partition = remote_table_info.table_partition(idx);
        uint32_t pid = table_partition.pid();
        for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
            if (table_partition.partition_meta(meta_idx).is_leader()) {
                const ::openmldb::nameserver::PartitionMeta& partition_meta = table_partition.partition_meta(meta_idx);
                const std::string& endpoint = partition_meta.endpoint();
                std::string leader_endpoint;
                std::shared_ptr<::openmldb::nameserver::TableInfo> table_info_tmp =
                    std::make_shared<::openmldb::nameserver::TableInfo>(table_info);
                if (GetLeader(table_info_tmp, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
                    PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
                    return -1;
                }
                task = CreateTask<AddReplicaTaskMeta>(op_index, op_type, leader_endpoint, tid, pid, endpoint,
                                                      remote_tid, idx);
                if (!task) {
                    PDLOG(WARNING,
                          "create addreplica task failed. leader cluster tid[%u] replica cluster tid[%u] pid[%u]", tid,
                          remote_tid, pid);
                    return -1;
                }
                op_data->task_list_.push_back(task);
                task = CreateTask<AddTableInfoTaskMeta>(op_index, op_type, name, db, pid, endpoint, alias, remote_tid);
                if (!task) {
                    PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
                    return -1;
                }
                op_data->task_list_.push_back(task);
                break;
            }
        }
    }

    PDLOG(INFO, "create CreateTableRemote task ok. name[%s] pid[%u] alias[%s]", remote_table_info.name().c_str(),
          op_data->op_info_.pid(), alias.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableOP(const std::string& name, const std::string& db, uint32_t pid,
                                        const std::string& endpoint, uint64_t parent_id, uint32_t concurrency) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::openmldb::api::OPType::kReLoadTableOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING, "create ReLoadTableOP data error. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (CreateReLoadTableTask(op_data) < 0) {
        PDLOG(WARNING, "create ReLoadTable task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create ReLoadTableOP op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", op_data->op_info_.op_id(),
          name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableOP(const std::string& name, const std::string& db, uint32_t pid,
                                        const std::string& endpoint, uint64_t parent_id, uint32_t concurrency,
                                        uint64_t remote_op_id, uint64_t& rep_cluster_op_id) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::openmldb::api::OPType::kReLoadTableOP, value, op_data, name, db, pid, parent_id, remote_op_id) <
        0) {
        PDLOG(WARNING, "create ReLoadTableOP data error. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (CreateReLoadTableTask(op_data) < 0) {
        PDLOG(WARNING, "create ReLoadTable task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid,
              endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    rep_cluster_op_id = op_data->op_info_.op_id();  // for multi cluster
    PDLOG(INFO, "create ReLoadTableOP op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", op_data->op_info_.op_id(),
          name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    std::string db = op_data->op_info_.db();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = table_info->tid();
    uint32_t seg_cnt = table_info->seg_cnt();
    auto op_type = ::openmldb::api::OPType::kReLoadTableOP;
    uint64_t op_index = op_data->op_info_.op_id();
    auto task = CreateTask<LoadTableTaskMeta>(op_index, op_type, endpoint, name, tid, pid, seg_cnt, true,
                                              table_info->storage_mode());
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, true, true);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
              pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ReLoadTable task ok. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateUpdatePartitionStatusOP(const std::string& name, const std::string& db, uint32_t pid,
                                                  const std::string& endpoint, bool is_leader, bool is_alive,
                                                  uint64_t parent_id, uint32_t concurrency) {
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    EndpointStatusData endpoint_status_data;
    endpoint_status_data.set_endpoint(endpoint);
    endpoint_status_data.set_is_leader(is_leader);
    endpoint_status_data.set_is_alive(is_alive);
    std::string value;
    endpoint_status_data.SerializeToString(&value);
    if (CreateOPData(::openmldb::api::OPType::kUpdatePartitionStatusOP, value, op_data, name, db, pid, parent_id) < 0) {
        PDLOG(WARNING,
              "create UpdatePartitionStatusOP data error. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateUpdatePartitionStatusOPTask(op_data) < 0) {
        PDLOG(WARNING,
              "create UpdatePartitionStatusOP task failed. table[%s] pid[%u] "
              "endpoint[%s]",
              name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO,
          "create UpdatePartitionStatusOP op ok."
          "op_id[%lu] name[%s] pid[%u] endpoint[%s] is_leader[%d] is_alive[%d] "
          "concurrency[%u]",
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
    std::string db = op_data->op_info_.db();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = endpoint_status_data.endpoint();
    bool is_leader = endpoint_status_data.is_leader();
    bool is_alive = endpoint_status_data.is_alive();
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    ::openmldb::api::OPType op_type = ::openmldb::api::OPType::kUpdatePartitionStatusOP;
    auto task =
        CreateTask<UpdatePartitionStatusTaskMeta>(op_index, op_type, name, db, pid, endpoint, is_leader, is_alive);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(),
              pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO,
          "create UpdatePartitionStatusOP task ok."
          "name[%s] pid[%u] endpoint[%s] is_leader[%d] is_alive[%d]",
          name.c_str(), pid, endpoint.c_str(), is_leader, is_alive);
    return 0;
}

int NameServerImpl::MatchTermOffset(const std::string& name, const std::string& db, uint32_t pid, bool has_table,
                                    uint64_t term, uint64_t offset) {
    if (!has_table && offset == 0) {
        PDLOG(INFO, "has not table, offset is zero. name[%s] pid[%u]", name.c_str(), pid);
        return 1;
    }
    std::map<uint64_t, uint64_t> term_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
        if (!GetTableInfoUnlock(name, db, &table_info)) {
            PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
            return -1;
        }
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            if (table_info->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int term_idx = 0; term_idx < table_info->table_partition(idx).term_offset_size(); term_idx++) {
                term_map.insert(std::make_pair(table_info->table_partition(idx).term_offset(term_idx).term(),
                                               table_info->table_partition(idx).term_offset(term_idx).offset()));
            }
            break;
        }
    }
    auto iter = term_map.find(term);
    if (iter == term_map.end()) {
        PDLOG(WARNING, "not found term[%lu] in table_info. name[%s] pid[%u]", term, name.c_str(), pid);
        return 1;
    } else if (iter->second > offset) {
        if (term_map.rbegin()->second == offset + 1) {
            PDLOG(INFO, "term[%lu] offset[%lu] has matched. name[%s] pid[%u]", term, offset, name.c_str(), pid);
            return 0;
        }
        PDLOG(INFO,
              "offset is not matched. name[%s] pid[%u] term[%lu] term start "
              "offset[%lu] cur offset[%lu]",
              name.c_str(), pid, term, iter->second, offset);
        return 1;
    }
    iter++;
    if (iter == term_map.end()) {
        PDLOG(INFO, "cur term[%lu] is the last one. name[%s] pid[%u]", term, name.c_str(), pid);
        return 0;
    }
    if (iter->second <= offset) {
        PDLOG(INFO, "term[%lu] offset not matched. name[%s] pid[%u] offset[%lu]", term, name.c_str(), pid, offset);
        return 1;
    }
    PDLOG(INFO, "term[%lu] offset has matched. name[%s] pid[%u] offset[%lu]", term, name.c_str(), pid, offset);
    return 0;
}

void NameServerImpl::WrapTaskFun(const boost::function<bool()>& fun,
                                 std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::string msg =
        absl::StrCat("op_id ", task_info->op_id(), " type ", ::openmldb::api::TaskType_Name(task_info->task_type()),
                     " ", Task::GetAdditionalMsg(*task_info));
    if (!fun()) {
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        PDLOG(WARNING, "task run failed. %s", msg.c_str());
    }
    PDLOG(INFO, "task starts running. %s", msg.c_str());
    task_rpc_version_.fetch_add(1, std::memory_order_acq_rel);
    task_info->set_is_rpc_send(true);
}

void NameServerImpl::WrapNormalTaskFun(const boost::function<base::Status()>& fun,
                                       std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::string msg =
        absl::StrCat("op_id ", task_info->op_id(), " type ", ::openmldb::api::TaskType_Name(task_info->task_type()),
                     " ", Task::GetAdditionalMsg(*task_info));
    auto status = fun();
    if (!status.OK()) {
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        PDLOG(WARNING, "task run failed. %s", msg.c_str());
    }
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "task run success. %s", msg.c_str());
}

std::shared_ptr<Task> NameServerImpl::CreateLoadTableRemoteTask(const std::string& alias, const std::string& name,
                                                                const std::string& db, const std::string& endpoint,
                                                                uint32_t pid, uint64_t op_index,
                                                                ::openmldb::api::OPType op_type) {
    std::shared_ptr<openmldb::nameserver::ClusterInfo> cluster = GetHealthCluster(alias);
    if (!cluster) {
        PDLOG(WARNING, "replica[%s] not available op_index[%lu]", alias.c_str(), op_index);
        return std::shared_ptr<Task>();
    }
    std::string cluster_endpoint =
        std::atomic_load_explicit(&cluster->client_, std::memory_order_relaxed)->GetEndpoint();
    std::shared_ptr<Task> task =
        std::make_shared<Task>(cluster_endpoint, std::make_shared<::openmldb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::openmldb::api::TaskType::kLoadTable);
    task->task_info_->set_status(::openmldb::api::TaskStatus::kInited);
    task->task_info_->set_endpoint(cluster_endpoint);

    boost::function<bool()> fun =
        boost::bind(&NsClient::LoadTable, std::atomic_load_explicit(&cluster->client_, std::memory_order_relaxed), name,
                    db, endpoint, pid, zone_info_, *(task->task_info_));
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

void NameServerImpl::AddTableInfo(const std::string& name, const std::string& db, const std::string& endpoint,
                                  uint32_t pid, std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> cur_table_info(table_info->New());
    cur_table_info->CopyFrom(*table_info);
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (cur_table_info->table_partition(idx).pid() == pid) {
            ::openmldb::nameserver::TablePartition* table_partition = cur_table_info->mutable_table_partition(idx);
            for (int meta_idx = 0; meta_idx < table_partition->partition_meta_size(); meta_idx++) {
                if (table_partition->partition_meta(meta_idx).endpoint() == endpoint) {
                    PDLOG(WARNING,
                          "follower already exists pid[%u] table[%s] "
                          "endpoint[%s] op_id[%lu]",
                          pid, name.c_str(), endpoint.c_str(), task_info->op_id());
                    task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                    return;
                }
            }
            ::openmldb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
            partition_meta->set_endpoint(endpoint);
            partition_meta->set_is_leader(false);
            partition_meta->set_is_alive(false);
            break;
        }
    }
    if (!UpdateZkTableNodeWithoutNotify(cur_table_info.get())) {
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    table_info->CopyFrom(*cur_table_info);
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::AddTableInfo(const std::string& alias, const std::string& endpoint, const std::string& name,
                                  const std::string& db, uint32_t remote_tid, uint32_t pid,
                                  std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() == pid) {
            ::openmldb::nameserver::TablePartition* table_partition_ptr = table_info->mutable_table_partition(idx);
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
                PDLOG(INFO,
                      "remote follower already exists pid[%u] table[%s] "
                      "endpoint[%s] op_id[%lu]",
                      pid, name.c_str(), endpoint.c_str(), task_info->op_id());
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
    if (!UpdateZkTableNodeWithoutNotify(table_info.get())) {
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::CheckBinlogSyncProgress(const std::string& name, const std::string& db, uint32_t pid,
                                             const std::string& follower, uint64_t offset_delta,
                                             std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    if (task_info->status() != ::openmldb::api::TaskStatus::kDoing) {
        PDLOG(WARNING, "task status is[%s], exit task. op_id[%lu], task_type[%s]",
              ::openmldb::api::TaskStatus_Name(task_info->status()).c_str(), task_info->op_id(),
              ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table %s in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    uint64_t leader_offset = 0;
    uint64_t follower_offset = 0;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        if (table_info->table_partition(idx).partition_meta_size() == 1) {
            task_info->set_status(openmldb::api::TaskStatus::kDone);
            LOG(INFO) << "no follower. update task status from [kDoing] to[kDone]. op_id[" << task_info->op_id()
                      << "], task_type[" << openmldb::api::TaskType_Name(task_info->task_type()) << "]";
            return;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            const PartitionMeta& meta = table_info->table_partition(idx).partition_meta(meta_idx);
            if (!meta.tablet_has_partition()) {
                task_info->set_status(::openmldb::api::TaskStatus::kDone);
                PDLOG(WARNING,
                      "tablet has not partition, update task status "
                      "from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                      task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
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
            task_info->set_status(::openmldb::api::TaskStatus::kDone);
            PDLOG(INFO,
                  "update task status from[kDoing] to[kDone]. op_id[%lu], "
                  "task_type[%s], leader_offset[%lu], follower_offset[%lu]",
                  task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str(), leader_offset,
                  follower_offset);
            return;
        }
        break;
    }
    PDLOG(INFO, "op_id[%lu], task_type[%s], leader_offset[%lu], follower_offset[%lu] offset_delta[%lu]",
          task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str(), leader_offset,
          follower_offset, offset_delta);
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_table_status_interval,
                                    boost::bind(&NameServerImpl::CheckBinlogSyncProgress, this, name, db, pid, follower,
                                                offset_delta, task_info));
    }
}

void NameServerImpl::UpdateTableInfo(const std::string& src_endpoint, const std::string& name, const std::string& db,
                                     uint32_t pid, const std::string& des_endpoint,
                                     std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table %s in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        int src_endpoint_index = -1;
        int des_endpoint_index = -1;
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            if (endpoint == src_endpoint) {
                src_endpoint_index = meta_idx;
            } else if (endpoint == des_endpoint) {
                des_endpoint_index = meta_idx;
            }
        }
        if (src_endpoint_index < 0) {
            PDLOG(WARNING, "has not found src_endpoint[%s]. name[%s] pid[%u] op_id[%lu]", src_endpoint.c_str(),
                  name.c_str(), pid, task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        ::openmldb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
        ::google::protobuf::RepeatedPtrField<::openmldb::nameserver::PartitionMeta>* partition_meta_field =
            table_partition->mutable_partition_meta();
        if (des_endpoint_index < 0) {
            // use src_endpoint's meta when the meta of des_endpoint is not
            // exist
            PDLOG(INFO,
                  "des_endpoint meta does not exist, use src_endpoint's meta."
                  "src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]",
                  src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
            ::openmldb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(src_endpoint_index);
            partition_meta->set_endpoint(des_endpoint);
            partition_meta->set_is_alive(true);
            partition_meta->set_is_leader(false);
        } else {
            ::openmldb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(des_endpoint_index);
            partition_meta->set_is_alive(true);
            partition_meta->set_is_leader(false);
            PDLOG(INFO, "remove partition[%u] in endpoint[%s]. name[%s]", pid, src_endpoint.c_str(), name.c_str());
            partition_meta_field->DeleteSubrange(src_endpoint_index, 1);
        }
        break;
    }
    if (!UpdateZkTableNode(table_info)) {
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::DelTableInfo(const std::string& name, const std::string& db, const std::string& endpoint,
                                  uint32_t pid, std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    return DelTableInfo(name, db, endpoint, pid, task_info, 0);
}

void NameServerImpl::DelTableInfo(const std::string& name, const std::string& db, const std::string& endpoint,
                                  uint32_t pid, std::shared_ptr<::openmldb::api::TaskInfo> task_info,
                                  uint32_t for_remote) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> cur_table_info(table_info->New());
    cur_table_info->CopyFrom(*table_info);
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (cur_table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        bool has_found = false;
        if (for_remote == 1) {
            for (int meta_idx = 0; meta_idx < cur_table_info->table_partition(idx).remote_partition_meta_size();
                 meta_idx++) {
                if (cur_table_info->table_partition(idx).remote_partition_meta(meta_idx).endpoint() == endpoint) {
                    auto table_partition = cur_table_info->mutable_table_partition(idx);
                    auto partition_meta = table_partition->mutable_remote_partition_meta();
                    PDLOG(INFO, "remove pid[%u] in table[%s]. endpoint is[%s]", pid, name.c_str(), endpoint.c_str());
                    partition_meta->DeleteSubrange(meta_idx, 1);
                    has_found = true;
                    break;
                }
            }
        } else {
            for (int meta_idx = 0; meta_idx < cur_table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (cur_table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    auto table_partition = cur_table_info->mutable_table_partition(idx);
                    auto partition_meta = table_partition->mutable_partition_meta();
                    PDLOG(INFO, "remove pid[%u] in table[%s]. endpoint is[%s]", pid, name.c_str(), endpoint.c_str());
                    partition_meta->DeleteSubrange(meta_idx, 1);
                    has_found = true;
                    break;
                }
            }
        }
        if (!has_found) {
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            PDLOG(INFO, "not found endpoint[%s] in partition_meta. name[%s] pid[%u] op_id[%lu]", endpoint.c_str(),
                  name.c_str(), pid, task_info->op_id());
            return;
        }
        break;
    }
    if (!UpdateZkTableNode(cur_table_info)) {
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    table_info->CopyFrom(*cur_table_info);
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::UpdatePartitionStatus(const std::string& name, const std::string& db, const std::string& endpoint,
                                           uint32_t pid, bool is_leader, bool is_alive,
                                           std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::openmldb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
                ::openmldb::nameserver::PartitionMeta* partition_meta =
                    table_partition->mutable_partition_meta(meta_idx);
                partition_meta->set_is_leader(is_leader);
                partition_meta->set_is_alive(is_alive);
                if (!UpdateZkTableNode(table_info)) {
                    task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                    return;
                }
                task_info->set_status(::openmldb::api::TaskStatus::kDone);
                PDLOG(INFO,
                      "update task status from[kDoing] to[kDone]. op_id[%lu], "
                      "task_type[%s]",
                      task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
                return;
            }
        }
        break;
    }
    task_info->set_status(::openmldb::api::TaskStatus::kFailed);
    PDLOG(WARNING, "name[%s] endpoint[%s] pid[%u] does not exist. op_id[%lu]", name.c_str(), endpoint.c_str(), pid,
          task_info->op_id());
}

void NameServerImpl::UpdateTableAliveStatus(RpcController* controller, const UpdateTableAliveRequest* request,
                                            GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kAutoFailoverIsEnabled);
        response->set_msg("auto_failover is enabled");
        PDLOG(WARNING, "auto_failover is enabled");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string name = request->name();
    std::string endpoint = request->endpoint();
    if (tablets_.find(endpoint) == tablets_.end()) {
        PDLOG(WARNING, "endpoint[%s] does not exist", endpoint.c_str());
        response->set_code(::openmldb::base::ReturnCode::kEndpointIsNotExist);
        response->set_msg("endpoint does not exist");
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(request->name(), request->db(), &table_info)) {
        PDLOG(WARNING, "table [%s] does not exist", name.c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> cur_table_info(table_info->New());
    cur_table_info->CopyFrom(*table_info);
    bool has_update = false;
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (request->has_pid() && cur_table_info->table_partition(idx).pid() != request->pid()) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < cur_table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (cur_table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::openmldb::nameserver::TablePartition* table_partition = cur_table_info->mutable_table_partition(idx);
                ::openmldb::nameserver::PartitionMeta* partition_meta =
                    table_partition->mutable_partition_meta(meta_idx);
                partition_meta->set_is_alive(request->is_alive());
                std::string is_alive = request->is_alive() ? "true" : "false";
                PDLOG(INFO, "update status[%s]. name[%s] endpoint[%s] pid[%u]", is_alive.c_str(), name.c_str(),
                      endpoint.c_str(), cur_table_info->table_partition(idx).pid());
                has_update = true;
                break;
            }
        }
    }
    if (has_update) {
        if (UpdateZkTableNode(cur_table_info)) {
            PDLOG(INFO, "update alive status ok. name[%s] endpoint[%s]", name.c_str(), endpoint.c_str());
            table_info->CopyFrom(*cur_table_info);
            response->set_code(::openmldb::base::ReturnCode::kOk);
            response->set_msg("ok");
        } else {
            response->set_msg("set zk failed");
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
        }
    } else {
        response->set_msg("no pid has update");
        response->set_code(::openmldb::base::ReturnCode::kNoPidHasUpdate);
    }
}

int NameServerImpl::UpdateEndpointTableAliveHandle(const std::string& endpoint, TableInfos& table_infos,
                                                   bool is_alive) {  // NOLINT
    for (const auto& kv : table_infos) {
        ::google::protobuf::RepeatedPtrField<TablePartition>* table_parts = kv.second->mutable_table_partition();
        bool has_update = false;
        for (int idx = 0; idx < table_parts->size(); idx++) {
            ::google::protobuf::RepeatedPtrField<PartitionMeta>* partition_meta =
                table_parts->Mutable(idx)->mutable_partition_meta();
            uint32_t alive_cnt = 0;
            for (int meta_idx = 0; meta_idx < partition_meta->size(); meta_idx++) {
                PartitionMeta* cur_partition_meta = partition_meta->Mutable(meta_idx);
                if (cur_partition_meta->is_alive()) {
                    alive_cnt++;
                }
            }
            if (alive_cnt == 1 && !is_alive) {
                LOG(INFO) << "alive_cnt is one, should not set alive to false. name[" << kv.first << "] pid ["
                          << table_parts->Get(idx).pid() << "] endpoint[" << endpoint << "] is_alive[" << is_alive
                          << "]";
                continue;
            }
            for (int meta_idx = 0; meta_idx < partition_meta->size(); meta_idx++) {
                PartitionMeta* cur_partition_meta = partition_meta->Mutable(meta_idx);
                if (cur_partition_meta->endpoint() == endpoint) {
                    cur_partition_meta->set_is_alive(is_alive);
                    has_update = true;
                }
            }
        }
        if (has_update) {
            if (!UpdateZkTableNodeWithoutNotify(kv.second.get())) {
                LOG(WARNING) << "update fail. table[" << kv.first << "] endpoint[" << endpoint << "] is_alive["
                             << is_alive << "]";
                return -1;
            }
            LOG(INFO) << "update success. table[" << kv.first << "] endpoint[" << endpoint << "] is_alive[" << is_alive
                      << "]";
        }
    }
    return 0;
}

int NameServerImpl::UpdateEndpointTableAlive(const std::string& endpoint, bool is_alive) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return 0;
    }
    std::lock_guard<std::mutex> lock(mu_);
    int ret = UpdateEndpointTableAliveHandle(endpoint, table_info_, is_alive);
    if (ret != 0) {
        return ret;
    }
    for (auto& kv : db_table_info_) {
        ret = UpdateEndpointTableAliveHandle(endpoint, kv.second, is_alive);
        if (ret != 0) {
            return ret;
        }
    }
    NotifyTableChanged(::openmldb::type::NotifyType::kTable);
    return 0;
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

void NameServerImpl::SelectLeader(const std::string& name, const std::string& db, uint32_t tid, uint32_t pid,
                                  std::vector<std::string>& follower_endpoint,
                                  std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. name[%s] pid[%u] data[%s] op_id[%lu]", name.c_str(), pid,
              op_data->op_info_.data().c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    if (change_leader_data.has_candidate_leader()) {
        if (std::find(follower_endpoint.begin(), follower_endpoint.end(), change_leader_data.candidate_leader()) ==
            follower_endpoint.end()) {
            PDLOG(WARNING, "candidate_leader[%s] is not follower. name[%s] pid[%u] op_id[%lu]",
                  change_leader_data.candidate_leader().c_str(), name.c_str(), pid, task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
    }
    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (auto_failover_.load(std::memory_order_acquire)) {
            std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
            if (!GetTableInfoUnlock(name, db, &table_info)) {
                task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
                return;
            }
            for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
                auto& partition = table_info->table_partition(idx);
                if (partition.pid() != pid) {
                    continue;
                }
                for (int meta_idx = 0; meta_idx < partition.partition_meta_size(); meta_idx++) {
                    if (partition.partition_meta(meta_idx).is_alive() &&
                        partition.partition_meta(meta_idx).is_leader()) {
                        PDLOG(WARNING, "leader is alive, need not changeleader. table name[%s] pid[%u] op_id[%lu]",
                              name.c_str(), pid, task_info->op_id());
                        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
                        return;
                    }
                }
                break;
            }
        }
        if (!zk_client_->SetNodeValue(zk_path_.term_node_, std::to_string(term_ + 2))) {
            PDLOG(WARNING, "update leader id  node failed. table name[%s] pid[%u] op_id[%lu]", name.c_str(), pid,
                  task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        cur_term = term_ + 1;
        term_ += 2;
    }
    // select the max offset endpoint as leader
    uint64_t max_offset = 0;
    std::vector<std::string> leader_endpoint_vec;
    std::map<std::string, std::shared_ptr<TabletInfo>> client_map;
    for (const auto& endpoint : follower_endpoint) {
        auto tablet_ptr = GetTablet(endpoint);
        if (!tablet_ptr) {
            PDLOG(WARNING, "endpoint[%s] is offline. table[%s] pid[%u]  op_id[%lu]", endpoint.c_str(), name.c_str(),
                  pid, task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        client_map.emplace(endpoint, tablet_ptr);
    }
    for (const auto& endpoint : follower_endpoint) {
        auto tablet_ptr = client_map[endpoint];
        uint64_t offset = 0;
        if (!tablet_ptr->client_->FollowOfNoOne(tid, pid, cur_term, offset)) {
            PDLOG(WARNING, "followOfNoOne failed. tid[%u] pid[%u] endpoint[%s] op_id[%lu]", tid, pid, endpoint.c_str(),
                  task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "FollowOfNoOne ok. term[%lu] offset[%lu] name[%s] tid[%u] pid[%u] endpoint[%s]", cur_term, offset,
              name.c_str(), tid, pid, endpoint.c_str());
        if (offset > max_offset || leader_endpoint_vec.empty()) {
            max_offset = offset;
            leader_endpoint_vec.clear();
            leader_endpoint_vec.push_back(endpoint);
        } else if (offset == max_offset) {
            leader_endpoint_vec.push_back(endpoint);
        }
    }
    std::string leader_endpoint;
    if (change_leader_data.has_candidate_leader()) {
        leader_endpoint = change_leader_data.candidate_leader();
    } else {
        leader_endpoint = leader_endpoint_vec[rand_.Next() % leader_endpoint_vec.size()];
    }
    change_leader_data.set_leader(leader_endpoint);
    change_leader_data.set_offset(max_offset);
    change_leader_data.set_term(cur_term + 1);
    std::string value;
    change_leader_data.SerializeToString(&value);
    op_data->op_info_.set_data(value);
    PDLOG(INFO, "new leader is[%s]. name[%s] tid[%u] pid[%u] offset[%lu]", leader_endpoint.c_str(), name.c_str(), tid,
          pid, max_offset);
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::ChangeLeader(std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]", task_info->op_id(),
              op_data->op_info_.data().c_str());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
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
        if (iter == tablets_.end() || iter->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
            PDLOG(WARNING, "endpoint[%s] is offline", leader_endpoint.c_str());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        follower_endpoint.erase(std::find(follower_endpoint.begin(), follower_endpoint.end(), leader_endpoint));
        tablet_ptr = iter->second;
    }
    std::vector<::openmldb::common::EndpointAndTid> endpoint_tid;
    for (const auto& e : change_leader_data.remote_follower()) {
        endpoint_tid.push_back(e);
    }
    if (!tablet_ptr->client_->ChangeRole(change_leader_data.tid(), change_leader_data.pid(), true, follower_endpoint,
                                         cur_term, &endpoint_tid)) {
        PDLOG(WARNING,
              "change leader failed. name[%s] tid[%u] pid[%u] endpoint[%s] "
              "op_id[%lu]",
              change_leader_data.name().c_str(), change_leader_data.tid(), change_leader_data.pid(),
              leader_endpoint.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    PDLOG(INFO, "change leader ok. name[%s] tid[%u] pid[%u] leader[%s] term[%lu]", change_leader_data.name().c_str(),
          change_leader_data.tid(), change_leader_data.pid(), leader_endpoint.c_str(), cur_term);
    task_info->set_status(::openmldb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
          ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::UpdateTTL(RpcController* controller, const ::openmldb::nameserver::UpdateTTLRequest* request,
                               ::openmldb::nameserver::UpdateTTLResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_acquire) == kFOLLOWER)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table;
    if (!GetTableInfo(request->name(), request->db(), &table)) {
        PDLOG(WARNING, "table with name %s does not exist", request->name().c_str());
        response->set_code(::openmldb::base::ReturnCode::kTableAlreadyExists);
        response->set_msg("table does not exist");
        return;
    }
    std::string index_name;
    if (request->has_index_name()) {
        index_name = request->index_name();
    }
    bool all_ok = true;
    for (int32_t i = 0; i < table->table_partition_size(); i++) {
        if (!all_ok) {
            break;
        }
        const TablePartition& table_partition = table->table_partition(i);
        for (int32_t j = 0; j < table_partition.partition_meta_size(); j++) {
            const PartitionMeta& meta = table_partition.partition_meta(j);
            all_ok = all_ok && UpdateTTLOnTablet(meta.endpoint(), table->tid(), table_partition.pid(), index_name,
                                                 request->ttl_desc());
        }
    }
    if (!all_ok) {
        response->set_code(::openmldb::base::ReturnCode::kFailToUpdateTtlFromTablet);
        response->set_msg("fail to update ttl from tablet");
        return;
    }
    TableInfo table_info;
    {
        std::lock_guard<std::mutex> lock(mu_);
        table_info.CopyFrom(*table);
    }
    auto column_keys = table_info.mutable_column_key();
    for (auto& column_key : *column_keys) {
        if (index_name.empty()) {
            column_key.mutable_ttl()->CopyFrom(request->ttl_desc());
        } else if (column_key.index_name() == index_name) {
            column_key.mutable_ttl()->CopyFrom(request->ttl_desc());
            break;
        }
    }
    // update zookeeper
    if (!UpdateZkTableNodeWithoutNotify(&table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
        response->set_msg("set zk failed");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        table->CopyFrom(table_info);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::UpdateLeaderInfo(std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]", task_info->op_id(),
              op_data->op_info_.data().c_str());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::string leader_endpoint = change_leader_data.leader();
    std::string name = change_leader_data.name();
    std::string db = change_leader_data.db();
    uint32_t pid = change_leader_data.pid();

    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "not found table[%s] in table_info map. op_id[%lu]", name.c_str(), task_info->op_id());
        task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        return;
    }
    int old_leader_index = -1;
    int new_leader_index = -1;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader() &&
                table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                old_leader_index = meta_idx;
            } else if (table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == leader_endpoint) {
                new_leader_index = meta_idx;
            }
        }
        ::openmldb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
        if (old_leader_index >= 0) {
            ::openmldb::nameserver::PartitionMeta* old_leader_meta =
                table_partition->mutable_partition_meta(old_leader_index);
            old_leader_meta->set_is_alive(false);
        }
        if (new_leader_index < 0) {
            PDLOG(WARNING, "endpoint[%s] does not exist. name[%s] pid[%u] op_id[%lu]", leader_endpoint.c_str(),
                  name.c_str(), pid, task_info->op_id());
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        ::openmldb::nameserver::PartitionMeta* new_leader_meta =
            table_partition->mutable_partition_meta(new_leader_index);
        new_leader_meta->set_is_leader(true);
        ::openmldb::nameserver::TermPair* term_offset = table_partition->add_term_offset();
        term_offset->set_term(change_leader_data.term());
        term_offset->set_offset(change_leader_data.offset() + 1);
        if (!UpdateZkTableNode(table_info)) {
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "change leader success. name[%s] pid[%u] new leader[%s]", name.c_str(), pid,
              leader_endpoint.c_str());
        task_info->set_status(::openmldb::api::TaskStatus::kDone);
        // notify client to update table partition information
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", task_info->op_id(),
              ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
        return;
    }
    PDLOG(WARNING, "partition[%u] does not exist. name[%s] op_id[%lu]", pid, name.c_str(), task_info->op_id());
    task_info->set_status(::openmldb::api::TaskStatus::kFailed);
}

void NameServerImpl::NotifyTableChanged(::openmldb::type::NotifyType type) {
    if (!IsClusterMode()) {
        return;
    }
    if (type == ::openmldb::type::NotifyType::kTable) {
        if (!zk_client_->Increment(zk_path_.table_changed_notify_node_)) {
            PDLOG(WARNING, "increment failed. node is %s", zk_path_.table_changed_notify_node_.c_str());
            return;
        }
        PDLOG(INFO, "notify table changed ok");
    } else if (type == ::openmldb::type::NotifyType::kGlobalVar) {
        if (!zk_client_->Increment(zk_path_.globalvar_changed_notify_node_)) {
            PDLOG(WARNING, "increment failed, node is %s", zk_path_.globalvar_changed_notify_node_.c_str());
            return;
        }
        PDLOG(INFO, "notify globalvar changed ok");
    } else {
        PDLOG(ERROR, "unsupport notify type");
    }
}

bool NameServerImpl::GetTableInfo(const std::string& table_name, const std::string& db_name,
                                  std::shared_ptr<TableInfo>* table_info) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetTableInfoUnlock(table_name, db_name, table_info);
}

bool NameServerImpl::GetTableInfoUnlock(const std::string& table_name, const std::string& db_name,
                                        std::shared_ptr<TableInfo>* table_info) {
    if (db_name.empty()) {
        auto it = table_info_.find(table_name);
        if (it == table_info_.end()) {
            return false;
        }
        *table_info = it->second;
    } else {
        auto db_it = db_table_info_.find(db_name);
        if (db_it == db_table_info_.end()) {
            return false;
        } else {
            auto it = db_it->second.find(table_name);
            if (it == db_it->second.end()) {
                return false;
            }
            *table_info = it->second;
        }
    }
    return true;
}

std::shared_ptr<TabletInfo> NameServerImpl::GetTabletInfo(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetTabletInfoWithoutLock(endpoint);
}

std::shared_ptr<TabletInfo> NameServerImpl::GetTabletInfoWithoutLock(const std::string& endpoint) {
    std::shared_ptr<TabletInfo> tablet;
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        return tablet;
    }
    tablet = it->second;
    return tablet;
}

std::shared_ptr<TabletInfo> NameServerImpl::GetHealthTabletInfoNoLock(const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || !it->second->Health()) {
        return std::shared_ptr<TabletInfo>();
    }
    return it->second;
}

bool NameServerImpl::UpdateTTLOnTablet(const std::string& endpoint, int32_t tid, int32_t pid,
                                       const std::string& index_name, const ::openmldb::common::TTLSt& ttl) {
    std::shared_ptr<TabletInfo> tablet = GetTabletInfo(endpoint);
    if (!tablet) {
        PDLOG(WARNING, "tablet with endpoint %s is not found", endpoint.c_str());
        return false;
    }

    if (!tablet->client_) {
        PDLOG(WARNING, "tablet with endpoint %s has not client", endpoint.c_str());
        return false;
    }
    bool ok = tablet->client_->UpdateTTL(tid, pid, ttl.ttl_type(), ttl.abs_ttl(), ttl.lat_ttl(), index_name);
    if (!ok) {
        PDLOG(WARNING, "fail to update ttl with tid %d, pid %d, abs_ttl %lu, lat_ttl %lu, endpoint %s", tid, pid,
              ttl.abs_ttl(), ttl.lat_ttl(), endpoint.c_str());
    } else {
        PDLOG(INFO, "update ttl with tid %d pid %d abs_ttl %lu, lat_ttl %lu endpoint %s ok", tid, pid, ttl.abs_ttl(),
              ttl.lat_ttl(), endpoint.c_str());
    }
    return ok;
}

void NameServerImpl::AddReplicaCluster(RpcController* controller, const ClusterAddress* request,
                                       GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) != kLEADER) {
        response->set_code(::openmldb::base::ReturnCode::kCurNameserverIsNotLeaderMdoe);
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
        std::shared_ptr<::openmldb::nameserver::ClusterInfo> cluster_info =
            std::make_shared<::openmldb::nameserver::ClusterInfo>(*request);
        if ((code = cluster_info->Init(rpc_msg)) != 0) {
            PDLOG(WARNING, "%s init failed, error: %s", request->alias().c_str(), rpc_msg.c_str());
            break;
        }
        std::vector<::openmldb::nameserver::TableInfo> tables;
        if (!std::atomic_load_explicit(&cluster_info->client_, std::memory_order_relaxed)
                 ->ShowAllTable(tables, rpc_msg)) {
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
                        if (it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
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
                if (!CompareTableInfo(tables, false)) {
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
        if (!cluster_info->AddReplicaClusterByNs(request->alias(), zone_info_.zone_name(), zone_info_.zone_term(),
                                                 rpc_msg)) {
            code = 300;
            break;
        }
        std::string cluster_value, value;
        request->SerializeToString(&cluster_value);
        if (zk_client_->GetNodeValue(zk_path_.zone_data_path_ + "/replica/" + request->alias(), value)) {
            if (!zk_client_->SetNodeValue(zk_path_.zone_data_path_ + "/replica/" + request->alias(), cluster_value)) {
                PDLOG(WARNING, "write replica cluster to zk failed, alias: %s", request->alias().c_str());
                code = 304;
                rpc_msg = "set zk failed";
                break;
            }
        } else {
            if (!zk_client_->CreateNode(zk_path_.zone_data_path_ + "/replica/" + request->alias(), cluster_value)) {
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
        thread_pool_.AddTask(boost::bind(&NameServerImpl::CheckSyncExistTable, this, request->alias(), tables,
                                         std::atomic_load_explicit(&cluster_info->client_, std::memory_order_relaxed)));
        thread_pool_.AddTask(boost::bind(&NameServerImpl::CheckSyncTable, this, request->alias(), tables,
                                         std::atomic_load_explicit(&cluster_info->client_, std::memory_order_relaxed)));
    } while (0);

    response->set_code(code);
    response->set_msg(rpc_msg);
}

void NameServerImpl::AddReplicaClusterByNs(RpcController* controller,
                                           const ::openmldb::nameserver::ReplicaClusterByNsRequest* request,
                                           ::openmldb::nameserver::AddReplicaClusterByNsResponse* response,
                                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t code = 0;
    std::string rpc_msg = "accept";

    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) == kLEADER) {
        response->set_code(::openmldb::base::ReturnCode::kCurNameserverIsLeaderCluster);
        response->set_msg("cur nameserver is leader cluster");
        PDLOG(WARNING, "cur nameserver is leader cluster");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    DEBUGLOG("request zone name is: %s, term is: %lu %d,", request->zone_info().zone_name().c_str(),
             request->zone_info().zone_term(), zone_info_.mode());
    DEBUGLOG("cur zone name is: %s", zone_info_.zone_name().c_str());
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
        if (zk_client_->IsExistNode(zk_path_.zone_data_path_ + "/follower") > 0) {
            if (!zk_client_->CreateNode(zk_path_.zone_data_path_ + "/follower", zone_info)) {
                PDLOG(WARNING, "write follower to zk failed, alias: %s", request->zone_info().replica_alias().c_str());
                code = 450;
                rpc_msg = "create zk failed";
                break;
            }
        } else {
            if (!zk_client_->SetNodeValue(zk_path_.zone_data_path_ + "/follower", zone_info)) {
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

void NameServerImpl::ShowCatalog(RpcController* controller, const ShowCatalogRequest* request,
                                 ShowCatalogResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::map<std::string, std::shared_ptr<TabletInfo>> tablet_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::openmldb::type::EndpointState::kHealthy) {
                tablet_map.emplace(kv.first, kv.second);
            }
        }
    }
    for (const auto& kv : tablet_map) {
        uint64_t version = 1;
        if (!kv.second->client_->GetCatalog(&version)) {
            response->set_code(::openmldb::base::ReturnCode::kRequestTabletFailed);
            response->set_msg("request tablet failed");
            PDLOG(WARNING, "request tablet failed");
            return;
        }
        auto catalog_info = response->add_catalog();
        catalog_info->set_endpoint(kv.first);
        catalog_info->set_version(version);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::ShowReplicaCluster(RpcController* controller, const GeneralRequest* request,
                                        ShowReplicaClusterResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) == kFOLLOWER) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
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
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::RemoveReplicaCluster(RpcController* controller,
                                          const ::openmldb::nameserver::RemoveReplicaOfRequest* request,
                                          ::openmldb::nameserver::GeneralResponse* response,
                                          ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire) || (mode_.load(std::memory_order_relaxed) == kFOLLOWER)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    int code = 0;
    std::string rpc_msg = "ok";
    std::shared_ptr<::openmldb::client::NsClient> c_ptr;
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
        for (auto db_iter = it->second->last_status.begin(); db_iter != it->second->last_status.end(); db_iter++) {
            for (auto iter = db_iter->second.begin(); iter != db_iter->second.end(); iter++) {
                for (auto part_iter = iter->second.begin(); part_iter != iter->second.end(); part_iter++) {
                    for (auto meta : part_iter->partition_meta()) {
                        if (meta.endpoint().empty()) {
                            break;
                        }
                        DelReplicaRemoteOP(meta.endpoint(), iter->first, db_iter->first, part_iter->pid());
                    }
                }
            }
        }
        if (!zk_client_->DeleteNode(zk_path_.zone_data_path_ + "/replica/" + request->alias())) {
            code = 452;
            rpc_msg = "del zk failed";
            PDLOG(WARNING, "del replica zk node [%s] failed, when remove repcluster", request->alias().c_str());
            break;
        }
        c_ptr = std::atomic_load_explicit(&it->second->client_, std::memory_order_relaxed);
        nsc_.erase(it);
        PDLOG(INFO, "success remove replica cluster [%s]", request->alias().c_str());
    } while (0);
    if ((code == 0) && (state == kClusterHealthy)) {
        if (!c_ptr->RemoveReplicaClusterByNs(request->alias(), zone_info_.zone_name(), zone_info_.zone_term(), code,
                                             rpc_msg)) {
            PDLOG(WARNING, "send remove replica cluster request to replica clsute failed");
        }
    }
    response->set_code(code);
    response->set_msg(rpc_msg);
    return;
}

void NameServerImpl::RemoveReplicaClusterByNs(RpcController* controller,
                                              const ::openmldb::nameserver::ReplicaClusterByNsRequest* request,
                                              ::openmldb::nameserver::GeneralResponse* response,
                                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t code = 0;
    std::string rpc_msg = "ok";

    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) != kFOLLOWER) {
        response->set_code(::openmldb::base::ReturnCode::kThisIsNotFollower);
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
        zone_info.set_zone_name(endpoint_ + zk_path_.root_path_);
        zone_info.set_replica_alias("");
        zone_info.set_zone_term(1);
        zone_info.SerializeToString(&value);
        if (!zk_client_->SetNodeValue(zk_path_.zone_data_path_ + "/follower", value)) {
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
            std::vector<::openmldb::nameserver::TableInfo> tables;
            if (!std::atomic_load_explicit(&i.second->client_, std::memory_order_relaxed)->ShowAllTable(tables, msg)) {
                PDLOG(WARNING, "check %s showtable has error: %s", i.first.c_str(), msg.c_str());
                continue;
            }
            std::lock_guard<std::mutex> lock(mu_);
            if ((tables.size() > 0) && !CompareTableInfo(tables, true)) {
                // todo :: add cluster statsu, need show in showreplica
                PDLOG(WARNING, "compare %s table info has error", i.first.c_str());
                continue;
            }
            CheckTableInfo(i.second, tables);
        }
    } while (0);

    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_replica_status_interval,
                                    boost::bind(&NameServerImpl::CheckClusterInfo, this));
    }
}

void NameServerImpl::SwitchMode(::google::protobuf::RpcController* controller,
                                const ::openmldb::nameserver::SwitchModeRequest* request,
                                ::openmldb::nameserver::GeneralResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("cur nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (request->sm() >= kFOLLOWER) {
        response->set_code(::openmldb::base::ReturnCode::kUnkownServerMode);
        response->set_msg("unkown server status");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == request->sm()) {
        response->set_code(::openmldb::base::ReturnCode::kOk);
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kLEADER) {
        std::lock_guard<std::mutex> lock(mu_);
        if (nsc_.size() > 0) {
            response->set_code(::openmldb::base::ReturnCode::kZoneNotEmpty);
            response->set_msg("zone not empty");
            return;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    decltype(zone_info_) zone_info = zone_info_;
    zone_info.set_mode(request->sm());
    std::string value;
    zone_info.SerializeToString(&value);
    if (zk_client_->IsExistNode(zk_path_.zone_data_path_ + "/follower") > 0) {
        if (!zk_client_->CreateNode(zk_path_.zone_data_path_ + "/follower", value)) {
            PDLOG(WARNING, "write follower to zk failed");
            response->set_code(::openmldb::base::ReturnCode::kCreateZkFailed);
            response->set_msg("create zk failed");
            return;
        }
    } else {
        if (!zk_client_->SetNodeValue(zk_path_.zone_data_path_ + "/follower", value)) {
            PDLOG(WARNING, "set zk failed, save follower value failed");
            response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
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
    response->set_code(::openmldb::base::ReturnCode::kOk);
    return;
}

void NameServerImpl::SyncTable(RpcController* controller, const SyncTableRequest* request, GeneralResponse* response,
                               Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_relaxed) != kLEADER) {
        response->set_code(::openmldb::base::ReturnCode::kCurNameserverIsNotLeaderMdoe);
        response->set_msg("cur nameserver is not leader mode");
        PDLOG(WARNING, "cur nameserver is not leader mode");
        return;
    }
    int code = 0;
    std::string msg = "ok";
    std::string name = request->name();
    std::string db = request->db();
    std::string cluster_alias = request->cluster_alias();
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    do {
        std::shared_ptr<::openmldb::client::NsClient> client;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (!GetTableInfoUnlock(name, db, &table_info)) {
                response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
                response->set_msg("table does not exist!");
                PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
                return;
            }
            auto it = nsc_.find(cluster_alias);
            if (it == nsc_.end()) {
                code = 404;
                msg = "replica name not found";
                PDLOG(WARNING, "replica name [%s] not found when synctable [%s]", cluster_alias.c_str(), name.c_str());
                break;
            }
            if (it->second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                code = 507;
                msg = "replica cluster not healthy";
                PDLOG(WARNING, "replica cluster [%s] not healthy when syntable [%s]", cluster_alias.c_str(),
                      name.c_str());
                break;
            }
            client = std::atomic_load_explicit(&it->second->client_, std::memory_order_relaxed);
        }
        std::vector<::openmldb::nameserver::TableInfo> tables;
        if (!client->ShowTable(name, db, false, tables, msg)) {
            code = 455;
            msg = "showtable error when synctable";
            PDLOG(WARNING, "replica cluster [%s] showtable error when synctable [%s]", cluster_alias.c_str(),
                  name.c_str());
            break;
        }
        std::vector<std::string> table_name_vec;
        for (auto& rkv : tables) {
            table_name_vec.push_back(rkv.name());
        }
        if (request->has_pid()) {
            if (std::find(table_name_vec.begin(), table_name_vec.end(), table_info->name()) != table_name_vec.end()) {
                PDLOG(INFO, "table [%s] [%u] already exists in replica cluster [%s]", name.c_str(), table_info->tid(),
                      cluster_alias.c_str());
                uint32_t pid = request->pid();
                if (SyncExistTable(cluster_alias, name, db, tables, *table_info, pid, code, msg) < 0) {
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
                const ::openmldb::nameserver::TablePartition& table_partition_local = table_info->table_partition(idx);
                for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                    if (table_partition_local.partition_meta(midx).is_leader() &&
                        (!table_partition_local.partition_meta(midx).is_alive())) {
                        code = 509;
                        msg = "local table has a no alive leader partition";
                        PDLOG(WARNING,
                              "local table [%s] pid [%u] has a no alive leader "
                              "partition",
                              table_info->name().c_str(), table_partition_local.pid());
                        response->set_code(code);
                        response->set_msg(msg);
                        return;
                    }
                }
            }
            if (std::find(table_name_vec.begin(), table_name_vec.end(), table_info->name()) != table_name_vec.end()) {
                PDLOG(INFO, "table [%s] [%u] already exists in replica cluster [%s]", name.c_str(), table_info->tid(),
                      cluster_alias.c_str());
                if (SyncExistTable(cluster_alias, name, db, tables, *table_info, INVALID_PID, code, msg) < 0) {
                    break;
                }
            } else {
                PDLOG(INFO, "table [%s] does not exist in replica cluster [%s]", name.c_str(), cluster_alias.c_str());
                ::openmldb::nameserver::TableInfo table_info_r(*table_info);
                // get remote table_info: tid and leader partition info
                std::string error;
                if (!client->CreateRemoteTableInfo(zone_info_, table_info_r, error)) {
                    code = 510;
                    msg = "create remote table info failed";
                    PDLOG(WARNING, "create remote table_info error, wrong msg is [%s]", error.c_str());
                    break;
                }
                std::lock_guard<std::mutex> lock(mu_);
                for (int idx = 0; idx < table_info_r.table_partition_size(); idx++) {
                    const ::openmldb::nameserver::TablePartition& table_partition = table_info_r.table_partition(idx);
                    if (AddReplicaRemoteOP(cluster_alias, table_info_r.name(), table_info_r.db(), table_partition,
                                           table_info_r.tid(), table_partition.pid()) < 0) {
                        code = 511;
                        msg = "create AddReplicaRemoteOP failed";
                        PDLOG(INFO,
                              "create AddReplicaRemoteOP failed. table[%s] "
                              "pid[%u]",
                              name.c_str(), table_partition.pid());
                        response->set_code(code);
                        response->set_msg(msg);
                        return;
                    }
                }
            }
        }
    } while (0);
    response->set_code(code);
    response->set_msg(msg);
}

int NameServerImpl::SyncExistTable(const std::string& alias, const std::string& name, const std::string& db,
                                   const std::vector<::openmldb::nameserver::TableInfo> tables_remote,
                                   const ::openmldb::nameserver::TableInfo& table_info_local, uint32_t pid, int& code,
                                   std::string& msg) {
    std::vector<::openmldb::nameserver::TableInfo> table_vec;
    ::openmldb::nameserver::TableInfo table_info_remote;
    for (const auto& table : tables_remote) {
        if (table.name() == name && table.db() == db) {
            table_vec.push_back(table);
            table_info_remote = table;
            break;
        }
    }
    {
        decltype(tablets_) tablets;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = tablets_.begin();
            for (; it != tablets_.end(); it++) {
                if (it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
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
        if (!CompareTableInfo(table_vec, false)) {
            PDLOG(WARNING, "compare table info error");
            msg = "compare table info error";
            code = 567;
            return -1;
        }
        if (!CompareSnapshotOffset(table_vec, msg, code, tablet_part_offset)) {
            return -1;
        }
    }
    std::vector<uint32_t> pid_vec;
    if (pid == INVALID_PID) {
        for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
            pid_vec.push_back(table_info_remote.table_partition(idx).pid());
        }
    } else {
        pid_vec.push_back(pid);
    }
    for (const auto& cur_pid : pid_vec) {
        bool has_pid = false;
        for (int idx = 0; idx < table_info_local.table_partition_size(); idx++) {
            const ::openmldb::nameserver::TablePartition& table_partition_local = table_info_local.table_partition(idx);
            if (table_partition_local.pid() == cur_pid) {
                has_pid = true;
                for (int midx = 0; midx < table_partition_local.partition_meta_size(); midx++) {
                    if (table_partition_local.partition_meta(midx).is_leader() &&
                        (!table_partition_local.partition_meta(midx).is_alive())) {
                        code = 509;
                        msg = "local table has a no alive leader partition";
                        PDLOG(WARNING,
                              "table [%s] pid [%u] has a no alive leader "
                              "partition",
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
            PDLOG(WARNING, "table [%s] has no pid [%u]", name.c_str(), cur_pid);
            return -1;
        }
        // remote table
        for (int idx = 0; idx < table_info_remote.table_partition_size(); idx++) {
            const ::openmldb::nameserver::TablePartition& table_partition = table_info_remote.table_partition(idx);
            if (table_partition.pid() == cur_pid) {
                for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                    if (table_partition.partition_meta(midx).is_leader()) {
                        if (!table_partition.partition_meta(midx).is_alive()) {
                            code = 514;
                            msg = "remote table has a no alive leader partition";
                            PDLOG(WARNING,
                                  "remote table [%s] has a no alive leader "
                                  "partition pid[%u]",
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
                const ::openmldb::nameserver::TablePartition& table_partition = table_info_remote.table_partition(idx);
                if (table_partition.pid() == cur_pid) {
                    for (int midx = 0; midx < table_partition.partition_meta_size(); midx++) {
                        if (table_partition.partition_meta(midx).is_leader() &&
                            table_partition.partition_meta(midx).is_alive()) {
                            if (AddReplicaSimplyRemoteOP(alias, name, db,
                                                         table_partition.partition_meta(midx).endpoint(),
                                                         table_info_remote.tid(), cur_pid) < 0) {
                                PDLOG(WARNING,
                                      "create AddReplicasSimplyRemoteOP "
                                      "failed. table[%s] pid[%u]",
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
            if (tablet.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
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

bool NameServerImpl::CreateTableRemote(const ::openmldb::api::TaskInfo& task_info,
                                       const ::openmldb::nameserver::TableInfo& table_info,
                                       const std::shared_ptr<::openmldb::nameserver::ClusterInfo> cluster_info) {
    return cluster_info->CreateTableRemote(task_info, table_info, zone_info_);
}

bool NameServerImpl::DropTableRemote(const ::openmldb::api::TaskInfo& task_info, const std::string& name,
                                     const std::string& db,
                                     const std::shared_ptr<::openmldb::nameserver::ClusterInfo> cluster_info) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto db_iter = cluster_info->last_status.find(db);
        if (db_iter != cluster_info->last_status.end()) {
            auto iter = db_iter->second.find(name);
            if (iter != db_iter->second.end()) {
                db_iter->second.erase(iter);
            }
        }
    }
    return cluster_info->DropTableRemote(task_info, name, db, zone_info_);
}

void NameServerImpl::MakeTablePartitionSnapshot(uint32_t pid, uint64_t end_offset,
                                                std::shared_ptr<::openmldb::nameserver::TableInfo> table_info) {
    for (const auto& part : table_info->table_partition()) {
        if (part.pid() != pid) {
            continue;
        }
        for (const auto& meta : part.partition_meta()) {
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
            client->MakeSnapshot(table_info->tid(), pid, end_offset, std::shared_ptr<openmldb::api::TaskInfo>());
        }
    }
}

void NameServerImpl::DeleteIndex(RpcController* controller, const DeleteIndexRequest* request,
                                 GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    int32_t index_pos = -1;
    std::map<std::string, std::shared_ptr<::openmldb::client::TabletClient>> tablet_client_map;
    if (!GetTableInfo(request->table_name(), request->db_name(), &table_info)) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist!");
        PDLOG(WARNING, "table[%s] does not exist!", request->table_name().c_str());
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info->column_key_size() == 0) {
            response->set_code(::openmldb::base::ReturnCode::kHasNotColumnKey);
            response->set_msg("table has not column key");
            PDLOG(WARNING, "table %s has not column key", request->table_name().c_str());
            return;
        }
        for (int i = 0; i < table_info->column_key_size(); i++) {
            if (table_info->column_key(i).index_name() == request->idx_name()) {
                if (table_info->column_key(i).flag() == 0) {
                    index_pos = i;
                }
                break;
            }
        }
        if (index_pos < 0) {
            response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
            response->set_msg("index doesn't exist!");
            PDLOG(WARNING, "index[%s]  doesn't exist!", request->idx_name().c_str());
            return;
        } else if (index_pos == 0) {
            response->set_code(::openmldb::base::ReturnCode::kDeleteIndexFailed);
            response->set_msg("index is primary key");
            PDLOG(WARNING, "index %s is primary key, cannot delete", request->idx_name().c_str());
            return;
        }
        for (const auto& kv : tablets_) {
            if (kv.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
                response->set_msg("tablet is offline!");
                PDLOG(WARNING, "tablet[%s] is offline!", kv.second->client_->GetEndpoint().c_str());
                return;
            }
            tablet_client_map.insert(std::make_pair(kv.second->client_->GetEndpoint(), kv.second->client_));
        }
    }
    for (const auto& table_partition : table_info->table_partition()) {
        for (const auto& partition_meta : table_partition.partition_meta()) {
            const std::string& endpoint = partition_meta.endpoint();
            if (!partition_meta.is_alive()) {
                response->set_code(::openmldb::base::ReturnCode::kTableHasNoAliveLeaderPartition);
                response->set_msg("partition is not alive!");
                PDLOG(WARNING, "partition[%s][%d] is not alive!", endpoint.c_str(), table_partition.pid());
                return;
            }
            if (tablet_client_map.find(endpoint) == tablet_client_map.end()) {
                response->set_code(::openmldb::base::ReturnCode::kTabletIsNotHealthy);
                response->set_msg("tablet is not healthy");
                PDLOG(WARNING, "endpoint %s is not healthy", endpoint.c_str());
                return;
            }
        }
    }
    bool delete_failed = false;
    for (const auto& table_partition : table_info->table_partition()) {
        for (const auto& partition_meta : table_partition.partition_meta()) {
            const std::string& endpoint = partition_meta.endpoint();
            std::string msg;
            if (!tablet_client_map[endpoint]->DeleteIndex(table_info->tid(), table_partition.pid(), request->idx_name(),
                                                          &msg)) {
                PDLOG(WARNING, "delete index failed. name %s pid %u endpoint %s msg %s", request->table_name().c_str(),
                      table_partition.pid(), endpoint.c_str(), msg.c_str());
                delete_failed = true;
            }
        }
    }
    table_info->mutable_column_key(index_pos)->set_flag(1);
    UpdateZkTableNode(table_info);
    if (delete_failed) {
        response->set_code(::openmldb::base::kDeleteIndexFailed);
        response->set_msg("delete failed");
    } else {
        response->set_code(0);
        response->set_msg("ok");
    }
    PDLOG(INFO, "delete index : table[%s] index[%s]", request->table_name().c_str(), request->idx_name().c_str());
}

bool NameServerImpl::UpdateZkTableNode(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info) {
    if (IsClusterMode() && UpdateZkTableNodeWithoutNotify(table_info.get())) {
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
        if (table_info->db() == INFORMATION_SCHEMA_DB && table_info->name() == GLOBAL_VARIABLES) {
            NotifyTableChanged(::openmldb::type::NotifyType::kGlobalVar);
        }
        return true;
    }
    return false;
}

bool NameServerImpl::UpdateZkTableNodeWithoutNotify(const TableInfo* table_info) {
    if (!IsClusterMode()) {
        return true;
    }
    std::string table_value;
    table_info->SerializeToString(&table_value);
    std::string temp_path;
    if (table_info->db().empty()) {
        temp_path = zk_path_.table_data_path_ + "/" + table_info->name();
    } else {
        temp_path = zk_path_.db_table_data_path_ + "/" + std::to_string(table_info->tid());
    }
    if (!zk_client_->SetNodeValue(temp_path, table_value)) {
        LOG(WARNING) << "update table node[" << temp_path << "] failed!";
        return false;
    }
    LOG(INFO) << "update table node[" << temp_path << "] success";
    return true;
}

base::Status NameServerImpl::AddMultiIndexs(
    const std::string& db, const std::string& name, std::shared_ptr<TableInfo> table_info,
    const ::google::protobuf::RepeatedPtrField<openmldb::common::ColumnKey>& column_keys) {
    auto status = schema::IndexUtil::CheckUnique(column_keys);
    if (!status.OK()) {
        return status;
    }
    std::map<std::string, ::openmldb::common::ColumnDesc> column_map;
    for (const auto& col : table_info->column_desc()) {
        column_map.emplace(col.name(), col);
    }

    status = schema::IndexUtil::CheckIndex(column_map, column_keys);
    if (!status.OK()) {
        return status;
    }
    std::vector<openmldb::common::ColumnKey> indexs;
    for (int idx = 0; idx < column_keys.size(); idx++) {
        if (schema::IndexUtil::IsExist(column_keys.Get(idx), table_info->column_key())) {
            return {ReturnCode::kIndexAlreadyExists, "index has already exist!"};
        }
        indexs.push_back(column_keys.Get(idx));
    }
    uint32_t tid = table_info->tid();
    std::set<std::string> endpoint_set;
    for (const auto& part : table_info->table_partition()) {
        uint32_t pid = part.pid();
        for (const auto& meta : part.partition_meta()) {
            std::shared_ptr<TabletInfo> tablet = GetTabletInfo(meta.endpoint());
            if (!tablet) {
                continue;
            }
            if (!tablet->Health()) {
                LOG(WARNING) << "endpoint[" << meta.endpoint() << "] is offline";
                return {base::ReturnCode::kError, "endpoint" + meta.endpoint() + ""};
            }
            if (!tablet->client_->AddMultiIndex(tid, pid, indexs, nullptr)) {
                LOG(WARNING) << "add index failed. tid " << tid << " pid " << pid << " endpoint " << meta.endpoint();
                return {base::ReturnCode::kError, "add index failed"};
            }
            endpoint_set.insert(meta.endpoint());
        }
    }
    table_info.reset();
    std::vector<std::shared_ptr<TabletClient>> tb_client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!GetTableInfoUnlock(name, db, &table_info)) {
            return {ReturnCode::kTableIsNotExist, "table does not exist!"};
        }
        for (int idx = 0; idx < column_keys.size(); idx++) {
            table_info->add_column_key()->CopyFrom(column_keys.Get(idx));
        }
        for (auto& kv : tablets_) {
            if (!kv.second->Health()) {
                LOG(WARNING) << "endpoint [" << kv.first << "] is offline";
                continue;
            }
            if (endpoint_set.count(kv.first) == 0) {
                tb_client_vec.push_back(kv.second->client_);
            }
        }
    }
    UpdateZkTableNode(table_info);
    for (const auto& tablet_client : tb_client_vec) {
        tablet_client->Refresh(tid);
    }
    PDLOG(INFO, "add index ok. table[%s] index num[%d]", name.c_str(), column_keys.size());
    return {};
}

void NameServerImpl::AddIndex(RpcController* controller, const AddIndexRequest* request, GeneralResponse* response,
                              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        base::SetResponseStatus(ReturnCode::kNameserverIsNotLeader, "nameserver is not leader", response);
        LOG(WARNING) << "cur nameserver is not leader";
        return;
    }
    const std::string& name = request->name();
    const std::string& db = request->db();
    std::shared_ptr<TableInfo> table_info;
    if (!GetTableInfo(name, db, &table_info)) {
        base::SetResponseStatus(ReturnCode::kTableIsNotExist, "table does not exist!", response);
        LOG(WARNING) << "table[" << db << "." << name << "] does not exist!";
        return;
    }
    std::vector<::openmldb::common::ColumnKey> column_key_vec;
    if (request->column_keys_size() > 0) {
        for (const auto& column_key : request->column_keys()) {
            column_key_vec.push_back(column_key);
        }
    } else {
        column_key_vec.push_back(request->column_key());
    }
    for (const auto& column_key : column_key_vec) {
        if (schema::IndexUtil::IsExist(column_key, table_info->column_key())) {
            base::SetResponseStatus(ReturnCode::kIndexAlreadyExists, "index has already exist!", response);
            LOG(WARNING) << "index " << column_key.index_name() << " has already exist! table " << name;
            return;
        }
    }
    std::map<std::string, ::openmldb::common::ColumnDesc> col_map;
    for (const auto& column_desc : table_info->column_desc()) {
        col_map.emplace(column_desc.name(), column_desc);
    }
    for (const auto& col : table_info->added_column_desc()) {
        col_map.emplace(col.name(), col);
    }
    std::map<std::string, openmldb::common::ColumnDesc> request_cols;
    for (const auto& col : request->cols()) {
        if (col.data_type() == ::openmldb::type::kFloat || col.data_type() == ::openmldb::type::kDouble) {
            base::SetResponseStatus(ReturnCode::kWrongColumnKey, "index col type cannot float or double", response);
            LOG(WARNING) << col.name() << " type is " << ::openmldb::type::DataType_Name(col.data_type())
                         << " it is not allow be index col";
            return;
        }
        request_cols.emplace(col.name(), col);
    }
    std::set<std::string> need_create_cols;
    std::vector<openmldb::common::ColumnDesc> add_cols;
    for (const auto& col_name : request->column_key().col_name()) {
        auto it = col_map.find(col_name);
        if (it == col_map.end()) {
            auto tit = request_cols.find(col_name);
            if (tit == request_cols.end()) {
                base::SetResponseStatus(ReturnCode::kWrongColumnKey, "miss column desc in the request", response);
                LOG(WARNING) << "miss column desc in the request";
                return;
            } else {
                if (need_create_cols.find(col_name) == need_create_cols.end()) {
                    need_create_cols.insert(col_name);
                    add_cols.push_back(tit->second);
                }
            }
        } else if (it->second.data_type() == ::openmldb::type::kFloat ||
                   it->second.data_type() == ::openmldb::type::kDouble) {
            base::SetResponseStatus(ReturnCode::kWrongColumnKey, "wrong column key!", response);
            LOG(WARNING) << "column_desc " << col_name << " has wrong type or not exist, table " << name;
            return;
        }
    }
    if (!add_cols.empty()) {
        openmldb::common::VersionPair new_pair;
        bool ok = AddFieldToTablet(add_cols, table_info, &new_pair);
        if (!ok) {
            base::SetResponseStatus(ReturnCode::kFailToUpdateTablemetaForAddingField,
                                    "fail to update tableMeta for adding field", response);
            LOG(WARNING) << "update tablemeta fail";
            return;
        }
        std::shared_ptr<TableInfo> table_info_zk(table_info->New());
        table_info_zk->CopyFrom(*table_info);
        for (const auto& col : add_cols) {
            openmldb::common::ColumnDesc* new_col = table_info_zk->add_added_column_desc();
            new_col->CopyFrom(col);
        }
        openmldb::common::VersionPair* add_pair = table_info_zk->add_schema_versions();
        add_pair->CopyFrom(new_pair);
        if (!UpdateZkTableNodeWithoutNotify(table_info_zk.get())) {
            base::SetResponseStatus(ReturnCode::kSetZkFailed, "set zk failed", response);
            LOG(WARNING) << "set zk failed! table " << name << " db " << db;
            return;
        }
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& col : add_cols) {
            openmldb::common::ColumnDesc* new_col = table_info->add_added_column_desc();
            new_col->CopyFrom(col);
        }
        openmldb::common::VersionPair* pair = table_info->add_schema_versions();
        pair->CopyFrom(new_pair);
    }
    if (auto status = schema::IndexUtil::CheckIndex(col_map, schema::IndexUtil::Convert2PB(column_key_vec));
        !status.OK()) {
        base::SetResponseStatus(ReturnCode::kCheckIndexFailed, status.msg, response);
        LOG(WARNING) << status.msg;
        return;
    }
    if (IsClusterMode() && !request->skip_load_data()) {
        std::lock_guard<std::mutex> lock(mu_);
        if (IsExistActiveOp(db, name, api::kAddIndexOP)) {
            LOG(WARNING) << "create AddIndexOP failed. there is already a task running. db " << db << " table " << name;
            base::SetResponseStatus(ReturnCode::kOPAlreadyExists, "there is already a task running", response);
            return;
        }
        auto status = CreateAddIndexOP(name, db, column_key_vec);
        if (!status.OK()) {
            LOG(WARNING) << "create AddIndexOP failed, table " << name << " msg " << status.GetMsg();
            base::SetResponseStatus(ReturnCode::kAddIndexFailed, "add index failed. msg " + status.GetMsg(), response);
            return;
        }
    } else {
        for (const auto& partition : table_info->table_partition()) {
            uint32_t pid = partition.pid();
            for (const auto& meta : partition.partition_meta()) {
                auto tablet_ptr = GetTablet(meta.endpoint());
                if (!tablet_ptr) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", meta.endpoint().c_str());
                    base::SetResponseStatus(ReturnCode::kTabletIsNotHealthy, "tablet does not exist", response);
                    return;
                }
                if (!tablet_ptr->client_->AddMultiIndex(table_info->tid(), pid, column_key_vec, nullptr)) {
                    base::SetResponseStatus(ReturnCode::kAddIndexFailed, "add index failed", response);
                    return;
                }
                if (!request->skip_load_data()) {
                    auto ret = tablet_ptr->client_->ExtractIndexData(table_info->tid(), pid,
                                                                     (uint32_t)table_info->table_partition_size(),
                                                                     column_key_vec, 0, false, nullptr);
                    if (!ret) {
                        base::SetResponseStatus(ReturnCode::kAddIndexFailed, "extract multi index failed", response);
                        return;
                    }
                }
            }
        }
        // no rollback now
        if (!AddIndexToTableInfo(name, db, column_key_vec, nullptr)) {
            base::SetResponseStatus(ReturnCode::kAddIndexFailed, "add to table info failed", response);
        }
    }
    base::SetResponseOK(response);
    LOG(INFO) << "add index. table[" << name << "] index count[" << column_key_vec.size() << "]";
}

bool NameServerImpl::AddIndexToTableInfo(const std::string& name, const std::string& db,
                                         const std::vector<::openmldb::common::ColumnKey>& column_key,
                                         std::shared_ptr<::openmldb::api::TaskInfo> task_info) {
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    std::lock_guard<std::mutex> lock(mu_);
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        PDLOG(WARNING, "table[%s] does not exist!", name.c_str());
        if (task_info) {
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        }
        return false;
    }
    for (const auto& cur_column_key : column_key) {
        int index_pos = schema::IndexUtil::GetPosition(cur_column_key, table_info->column_key());
        if (index_pos >= 0) {
            table_info->mutable_column_key(index_pos)->CopyFrom(cur_column_key);
        } else {
            table_info->add_column_key()->CopyFrom(cur_column_key);
        }
    }
    UpdateZkTableNode(table_info);
    // refresh tablet here, cuz this func may be called by task
    // if refresh failed, won't break the process of add index
    std::set<std::string> endpoint_set;
    for (const auto& part : table_info->table_partition()) {
        for (const auto& meta : part.partition_meta()) {
            endpoint_set.insert(meta.endpoint());
        }
    }
    // locked on top
    for (const auto& tablet : tablets_) {
        if (!tablet.second->Health()) {
            continue;
        }
        if (endpoint_set.count(tablet.first) == 0) {
            tablet.second->client_->Refresh(table_info->tid());
        }
    }

    PDLOG(INFO, "add index ok. table %s index cnt %d", name.c_str(), column_key.size());
    if (task_info) {
        task_info->set_status(::openmldb::api::TaskStatus::kDone);
    }
    return true;
}

base::Status NameServerImpl::CreateAddIndexOP(const std::string& name, const std::string& db,
                                              const std::vector<::openmldb::common::ColumnKey>& column_key) {
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        return {-1, "table does not exist"};
    }
    std::shared_ptr<OPData> op_data;
    AddIndexMeta add_index_meta;
    add_index_meta.set_name(name);
    add_index_meta.set_pid(0);
    add_index_meta.set_db(db);
    for (const auto& cur_column_key : column_key) {
        add_index_meta.add_column_keys()->CopyFrom(cur_column_key);
    }
    std::string value;
    add_index_meta.SerializeToString(&value);
    if (CreateOPData(api::kAddIndexOP, value, op_data, name, db, 0) < 0) {
        return {-1, absl::StrCat("create AddIndexOP data failed. table ", name)};
    }
    auto status = CreateAddIndexOPTask(op_data);
    if (!status.OK()) {
        return {-1, absl::StrCat("create AddIndexOP task failed. table ", name, " msg ", status.GetMsg())};
    }
    if (AddOPData(op_data, FLAGS_name_server_task_max_concurrency) < 0) {
        return {-1, absl::StrCat("add op data failed. name ", name)};
    }
    PDLOG(INFO, "create AddIndexOP op ok. op_id[%lu] name[%s]", op_data->GetOpId(), name.c_str());
    return {};
}

base::Status NameServerImpl::CreateAddIndexOPTask(std::shared_ptr<OPData> op_data) {
    AddIndexMeta add_index_meta;
    if (!add_index_meta.ParseFromString(op_data->op_info_.data())) {
        return {-1, absl::StrCat("parse AddIndexMeta failed. data ", op_data->op_info_.data())};
    }
    const std::string& name = op_data->op_info_.name();
    const std::string& db = op_data->op_info_.db();
    auto column_key_vec = schema::IndexUtil::Convert2Vector(add_index_meta.column_keys());
    uint64_t op_index = op_data->op_info_.op_id();
    auto op_type = api::kAddIndexOP;
    return FillAddIndexTask(op_index, op_type, name, db, column_key_vec, &op_data->task_list_);
}

base::Status NameServerImpl::FillAddIndexTask(uint64_t op_index, api::OPType op_type, const std::string& name,
                                              const std::string& db,
                                              const std::vector<::openmldb::common::ColumnKey>& column_key,
                                              std::list<std::shared_ptr<Task>>* task_list) {
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    if (!GetTableInfoUnlock(name, db, &table_info)) {
        return {-1, absl::StrCat("get table info failed, db ", db, " name ", name)};
    }
    uint32_t tid = table_info->tid();
    std::map<uint32_t, std::string> pid_endpoint_map;
    std::map<uint32_t, uint64_t> pid_offset_map;
    std::vector<std::string> endpoints;
    for (const auto& part : table_info->table_partition()) {
        for (const auto& meta : part.partition_meta()) {
            const std::string& ep = meta.endpoint();
            if (!meta.is_alive()) {
                return {-1, absl::StrCat(ep, " is not alive")};
            }
            auto it = tablets_.find(ep);
            if (it == tablets_.end() || !it->second->Health()) {
                return {-1, absl::StrCat(ep, " is not online")};
            }
            if (meta.is_leader()) {
                pid_endpoint_map.emplace(part.pid(), ep);
                pid_offset_map.emplace(part.pid(), meta.offset());
            }
            endpoints.push_back(ep);
        }
    }
    if (static_cast<int>(pid_endpoint_map.size()) != table_info->table_partition_size()) {
        return {-1, "get leader failed"};
    }
    int part_size = table_info->table_partition_size();
    auto task = CreateTask<AddIndexToTabletTaskMeta>(op_index, op_type, *table_info, column_key);
    if (!task) {
        return {-1, "create add index task failed"};
    }
    task_list->push_back(task);
    task = CreateTask<AddIndexToTableInfoTaskMeta>(op_index, op_type, name, db, column_key);
    if (!task) {
        return {-1, "create add index to table info task failed"};
    }
    task_list->push_back(task);
    task = CreateTask<ExtractIndexDataTaskMeta>(op_index, op_type, tid, part_size, column_key, pid_offset_map,
                                                pid_endpoint_map);
    if (!task) {
        return {-1, "create extract index task failed"};
    }
    task_list->push_back(task);
    task = CreateTask<SendIndexDataTaskMeta>(op_index, op_type, tid, pid_endpoint_map);
    if (!task) {
        return {-1, "create send index task failed"};
    }
    task_list->push_back(task);
    task = CreateTask<LoadIndexDataTaskMeta>(op_index, op_type, tid, part_size, pid_endpoint_map);
    if (!task) {
        return {-1, "create load index task failed"};
    }
    task_list->push_back(task);
    return {};
}

void NameServerImpl::RunSubTask(std::shared_ptr<Task> task) {
    for (const auto& cur_task : task->sub_task_) {
        PDLOG(INFO, "task starts running. op_id %lu task type %s %s", cur_task->GetOpId(),
              cur_task->GetReadableType().c_str(), cur_task->GetAdditionalMsg().c_str());
        cur_task->SetStatus(::openmldb::api::TaskStatus::kDoing);
        cur_task->fun_();
    }
}

void NameServerImpl::RunSeqTask(std::shared_ptr<Task> task) {
    if (task->seq_task_.empty()) {
        PDLOG(INFO, "update task status from %s to kDone. op_id %lu task_type %s %s", task->GetReadableStatus().c_str(),
              task->GetOpId(), task->GetReadableType().c_str(), task->GetAdditionalMsg().c_str());
        task->SetStatus(::openmldb::api::TaskStatus::kDone);
        return;
    }
    auto cur_task = task->seq_task_.front();
    auto task_status = cur_task->GetStatus();
    if (task_status == ::openmldb::api::TaskStatus::kInited) {
        PDLOG(INFO, "seq task starts running. op_id %lu task type %s %s", cur_task->GetOpId(),
              cur_task->GetReadableType().c_str(), cur_task->GetAdditionalMsg().c_str());
        cur_task->SetStatus(::openmldb::api::TaskStatus::kDoing);
        cur_task->fun_();
    } else if (task_status == ::openmldb::api::TaskStatus::kFailed ||
               task_status == ::openmldb::api::TaskStatus::kCanceled) {
        PDLOG(INFO, "update task status from %s to %s. op_id %lu task_type %s %s", task->GetReadableStatus().c_str(),
              cur_task->GetReadableStatus().c_str(), task->GetOpId(), task->GetReadableType().c_str(),
              task->GetAdditionalMsg().c_str());
        task->SetStatus(task_status);
        return;
    } else if (task_status == ::openmldb::api::TaskStatus::kDone) {
        task->seq_task_.pop_front();
    }
    task_thread_pool_.DelayTask(SEQ_TASK_CHECK_INTERVAL, boost::bind(&NameServerImpl::RunSeqTask, this, task));
}

void NameServerImpl::CreateDatabase(RpcController* controller, const CreateDatabaseRequest* request,
                                    GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    }
    auto status = CreateDatabase(request->db(), request->if_not_exists());
    SetResponseStatus(status, response);
}

base::Status NameServerImpl::CreateDatabase(const std::string& db_name, bool if_not_exists) {
    bool is_exists = true;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (databases_.find(db_name) == databases_.end()) {
            is_exists = false;
            databases_.insert(db_name);
        }
    }
    if (is_exists) {
        if (if_not_exists) {
            return {};
        }
        PDLOG(INFO, "database %s already exists", db_name.c_str());
        return {::openmldb::base::ReturnCode::kDatabaseAlreadyExists, "database already exists"};
    } else {
        if (IsClusterMode() && !zk_client_->CreateNode(zk_path_.db_path_ + "/" + db_name, "")) {
            PDLOG(WARNING, "create db node[%s/%s] failed!", zk_path_.db_path_.c_str(), db_name.c_str());
            return {::openmldb::base::ReturnCode::kSetZkFailed, "set zk failed"};
        }
        if (mode_.load(std::memory_order_acquire) == kLEADER) {
            decltype(nsc_) tmp_nsc;
            {
                std::lock_guard<std::mutex> lock(mu_);
                tmp_nsc = nsc_;
            }
            for (const auto& kv : tmp_nsc) {
                if (kv.second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                    PDLOG(WARNING, "cluster[%s] is not Healthy", kv.first.c_str());
                    continue;
                }
                auto status = std::atomic_load_explicit(&kv.second->client_, std::memory_order_relaxed)
                                  ->CreateDatabaseRemote(db_name, zone_info_);
                if (!status.OK()) {
                    PDLOG(WARNING, "create remote database failed, msg is [%s]", status.msg.c_str());
                    return status;
                }
            }
        }
        PDLOG(INFO, "create database %s success", db_name.c_str());
    }
    return {};
}

void NameServerImpl::UseDatabase(RpcController* controller, const UseDatabaseRequest* request,
                                 GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (databases_.find(request->db()) != databases_.end()) {
            response->set_code(::openmldb::base::ReturnCode::kOk);
            response->set_msg("ok");
        } else {
            response->set_code(::openmldb::base::ReturnCode::kDatabaseNotFound);
            response->set_msg("database not found");
        }
    }
}

void NameServerImpl::ShowDatabase(RpcController* controller, const GeneralRequest* request,
                                  ShowDatabaseResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& db : databases_) {
            if (db != INTERNAL_DB && db != INFORMATION_SCHEMA_DB && db != PRE_AGG_DB) {
                response->add_db(db);
            }
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::DropDatabase(RpcController* controller, const DropDatabaseRequest* request,
                                  GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kError);
        response->set_msg("cannot drop internal database");
        PDLOG(WARNING, "cannot drop internal database");
        return;
    }
    if (mode_.load(std::memory_order_acquire) == kFOLLOWER) {
        if (!request->has_zone_info()) {
            response->set_code(::openmldb::base::ReturnCode::kNoZoneInfo);
            response->set_msg("nameserver is for follower cluster, and request has no zone info");
            PDLOG(WARNING, "nameserver is for follower cluster, and request has no zone info");
            return;
        }
        auto status = CheckZoneInfo(request->zone_info());
        if (!status.OK()) {
            ::openmldb::base::SetResponseStatus(status, response);
            return;
        }
    }
    if (request->db() == INTERNAL_DB || request->db() == INFORMATION_SCHEMA_DB) {
        response->set_code(::openmldb::base::ReturnCode::kDatabaseNotFound);
        response->set_msg("database not found");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (databases_.find(request->db()) == databases_.end()) {
            response->set_code(::openmldb::base::ReturnCode::kDatabaseNotFound);
            response->set_msg("database not found");
            return;
        }
        auto db_it = db_table_info_.find(request->db());
        if (db_it != db_table_info_.end() && db_it->second.size() != 0) {
            response->set_code(::openmldb::base::ReturnCode::kDatabaseNotEmpty);
            response->set_msg("database not empty");
            return;
        }
        if (IsClusterMode()) {
            if (!zk_client_->DeleteNode(zk_path_.db_path_ + "/" + request->db())) {
                PDLOG(WARNING, "drop db node[%s/%s] failed!", zk_path_.db_path_.c_str(), request->db().c_str());
                response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
                response->set_msg("set zk failed");
                return;
            }
        }
        databases_.erase(request->db());
    }
    if (mode_.load(std::memory_order_acquire) == kLEADER) {
        decltype(nsc_) tmp_nsc;
        {
            std::lock_guard<std::mutex> lock(mu_);
            tmp_nsc = nsc_;
        }
        for (const auto& kv : tmp_nsc) {
            if (kv.second->state_.load(std::memory_order_relaxed) != kClusterHealthy) {
                PDLOG(WARNING, "cluster[%s] is not Healthy", kv.first.c_str());
                continue;
            }
            auto status = std::atomic_load_explicit(&kv.second->client_, std::memory_order_relaxed)
                              ->DropDatabaseRemote(request->db(), zone_info_);
            if (!status.OK()) {
                PDLOG(WARNING, "drop remote database failed, msg is [%s]", status.msg.c_str());
                ::openmldb::base::SetResponseStatus(status, response);
                return;
            }
        }
    }
    ::openmldb::base::SetResponseOK(response);
}

void NameServerImpl::SetSdkEndpoint(RpcController* controller, const SetSdkEndpointRequest* request,
                                    GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string server_name = request->server_name();
    std::string sdk_endpoint = request->sdk_endpoint();
    if (sdk_endpoint != "null") {
        std::string leader_path = FLAGS_zk_root_path + "/leader";
        // check sever name exist
        std::vector<std::string> children;
        if (!zk_client_->GetChildren(leader_path, children) || children.empty()) {
            PDLOG(WARNING, "get zk children failed");
            response->set_code(::openmldb::base::ReturnCode::kGetZkFailed);
            response->set_msg("get zk children failed");
            return;
        }
        std::set<std::string> endpoint_set;
        for (const auto& path : children) {
            std::string endpoint;
            std::string real_path = leader_path + "/" + path;
            if (!zk_client_->GetNodeValue(real_path, endpoint)) {
                PDLOG(WARNING, "get zk value failed");
                response->set_code(::openmldb::base::ReturnCode::kGetZkFailed);
                response->set_msg("get zk value failed");
                return;
            }
            endpoint_set.insert(endpoint);
        }
        bool has_found = true;
        do {
            if (std::find(endpoint_set.begin(), endpoint_set.end(), server_name) != endpoint_set.end()) {
                break;
            }
            std::lock_guard<std::mutex> lock(mu_);
            auto it = tablets_.find(server_name);
            if (it != tablets_.end() && it->second->state_ == ::openmldb::type::EndpointState::kHealthy) {
                break;
            }
            has_found = false;
        } while (0);
        if (!has_found) {
            response->set_code(::openmldb::base::ReturnCode::kServerNameNotFound);
            response->set_msg("server_name does not exist or offline");
            PDLOG(WARNING, "server_name[%s] does not exist or offline", server_name.c_str());
            return;
        }
        // check sdkendpoint duplicate
        std::lock_guard<std::mutex> lock(mu_);
        for (auto it = sdk_endpoint_map_.begin(); it != sdk_endpoint_map_.end(); ++it) {
            if (it->second == sdk_endpoint) {
                response->set_code(::openmldb::base::ReturnCode::kSdkEndpointDuplicate);
                response->set_msg("sdkendpoint duplicate");
                PDLOG(WARNING, "sdkendpoint[%s] duplicate", sdk_endpoint.c_str());
                return;
            }
        }
    }
    decltype(sdk_endpoint_map_) tmp_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        tmp_map = sdk_endpoint_map_;
    }
    std::string path = FLAGS_zk_root_path + "/map/sdkendpoints/" + server_name;
    if (sdk_endpoint != "null") {
        if (zk_client_->IsExistNode(path) != 0) {
            if (!zk_client_->CreateNode(path, sdk_endpoint)) {
                PDLOG(WARNING, "create zk node %s value %s failed", path.c_str(), sdk_endpoint.c_str());
                response->set_code(::openmldb::base::ReturnCode::kCreateZkFailed);
                response->set_msg("create zk failed");
                return;
            }
        } else {
            if (!zk_client_->SetNodeValue(path, sdk_endpoint)) {
                PDLOG(WARNING, "set zk node %s value %s failed", path.c_str(), sdk_endpoint.c_str());
                response->set_code(::openmldb::base::ReturnCode::kSetZkFailed);
                response->set_msg("set zk failed");
                return;
            }
        }
        auto iter = tmp_map.find(server_name);
        if (iter == tmp_map.end()) {
            tmp_map.insert(std::make_pair(server_name, sdk_endpoint));
        } else {
            iter->second = sdk_endpoint;
        }
    } else {
        if (!zk_client_->DeleteNode(path)) {
            response->set_code(::openmldb::base::ReturnCode::kDelZkFailed);
            response->set_msg("del zk failed");
            PDLOG(WARNING, "del zk node [%s] failed", path.c_str());
            return;
        }
        tmp_map.erase(server_name);
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        sdk_endpoint_map_.swap(tmp_map);
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
    }
    PDLOG(INFO, "SetSdkEndpoint success. server_name %s sdk_endpoint %s", server_name.c_str(), sdk_endpoint.c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void NameServerImpl::UpdateRealEpMapToTablet(bool check_running) {
    if (check_running && !running_.load(std::memory_order_acquire)) {
        return;
    }
    decltype(tablets_) tmp_tablets;
    decltype(real_ep_map_) tmp_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (real_ep_map_.empty()) {
            return;
        }
        for (const auto& tablet : tablets_) {
            if (tablet.second->state_ != ::openmldb::type::EndpointState::kHealthy) {
                continue;
            }
            tmp_tablets.insert(std::make_pair(tablet.first, tablet.second));
        }
        tmp_map = real_ep_map_;
        for (auto& pair : remote_real_ep_map_) {
            auto it = tmp_map.find(pair.first);
            if (it == tmp_map.end()) {
                tmp_map.insert(std::make_pair(pair.first, pair.second));
            } else {
                it->second = pair.second;
            }
        }
    }
    for (const auto& tablet : tmp_tablets) {
        if (!tablet.second->client_->UpdateRealEndpointMap(tmp_map)) {
            PDLOG(WARNING, "UpdateRealEndpointMap for tablet %s failed!", tablet.first.c_str());
        }
    }
}

void NameServerImpl::UpdateRemoteRealEpMap() {
    do {
        if (!running_.load(std::memory_order_acquire)) {
            break;
        }
        if (mode_.load(std::memory_order_relaxed) != kLEADER) {
            break;
        }
        if (!FLAGS_use_name) {
            break;
        }
        decltype(nsc_) tmp_nsc;
        decltype(remote_real_ep_map_) old_map;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (nsc_.empty()) {
                break;
            }
            for (auto& i : nsc_) {
                if (i.second->state_.load(std::memory_order_relaxed) == kClusterHealthy) {
                    tmp_nsc.insert(std::make_pair(i.first, i.second));
                }
            }
            old_map = remote_real_ep_map_;
        }
        decltype(remote_real_ep_map_) tmp_map;
        for (auto& i : tmp_nsc) {
            auto r_map = std::atomic_load_explicit(&i.second->remote_real_ep_map_, std::memory_order_acquire);
            for (auto& pair : *r_map) {
                auto it = tmp_map.find(pair.first);
                if (it == tmp_map.end()) {
                    tmp_map.insert(std::make_pair(pair.first, pair.second));
                } else {
                    it->second = pair.second;
                }
            }
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            remote_real_ep_map_.swap(tmp_map);
        }
        if (old_map != tmp_map) {
            thread_pool_.AddTask(boost::bind(&NameServerImpl::UpdateRealEpMapToTablet, this, true));
        }
    } while (false);
    task_thread_pool_.DelayTask(FLAGS_get_replica_status_interval,
                                boost::bind(&NameServerImpl::UpdateRemoteRealEpMap, this));
}

void NameServerImpl::ShowSdkEndpoint(RpcController* controller, const ShowSdkEndpointRequest* request,
                                     ShowSdkEndpointResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (sdk_endpoint_map_.empty()) {
        PDLOG(INFO, "sdk_endpoint_map is empty");
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    }
    for (const auto& kv : sdk_endpoint_map_) {
        TabletStatus* status = response->add_tablets();
        status->set_endpoint(kv.first);
        status->set_real_endpoint(kv.second);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

bool NameServerImpl::UpdateSdkEpMap() {
    sdk_endpoint_map_.clear();
    std::string path = FLAGS_zk_root_path + "/map/sdkendpoints";
    if (zk_client_->IsExistNode(path) != 0) {
        PDLOG(INFO, "/map/sdkendpoints node %s not exist", path.c_str());
        return true;
    } else {
        std::vector<std::string> children;
        if (!zk_client_->GetChildren(path, children) || children.empty()) {
            PDLOG(WARNING, "get zk children failed");
            return false;
        }
        for (const auto& child : children) {
            std::string real_ep;
            if (!zk_client_->GetNodeValue(path + "/" + child, real_ep)) {
                PDLOG(WARNING, "get zk value failed");
                return false;
            }
            sdk_endpoint_map_.insert(std::make_pair(child, real_ep));
        }
    }
    PDLOG(INFO, "update sdk_endpoint_map size[%d]", sdk_endpoint_map_.size());
    return true;
}

bool NameServerImpl::RegisterName() {
    if (FLAGS_use_name) {
        if (!zk_client_->RegisterName()) {
            return false;
        }
    }
    return true;
}

void NameServerImpl::CreateProcedure(RpcController* controller, const api::CreateProcedureRequest* request,
                                     GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    const std::string& sp_db_name = request->sp_info().db_name();
    const std::string& sp_name = request->sp_info().sp_name();
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (databases_.find(sp_db_name) == databases_.end()) {
            response->set_code(::openmldb::base::ReturnCode::kDatabaseNotFound);
            response->set_msg("database not found");
            PDLOG(WARNING, "database[%s] not found", sp_db_name.c_str());
            return;
        } else {
            const auto& sp_table_map = db_sp_table_map_[sp_db_name];
            auto sp_table_iter = sp_table_map.find(sp_name);
            if (sp_table_iter != sp_table_map.end()) {
                response->set_code(::openmldb::base::ReturnCode::kProcedureAlreadyExists);
                response->set_msg("store procedure already exists");
                PDLOG(WARNING, "store procedure[%s] already exists in db[%s]", sp_name.c_str(), sp_db_name.c_str());
                return;
            }
        }
    }
    auto status = CreateProcedureInternal(*request);
    base::SetResponseStatus(status, response);
}

base::Status NameServerImpl::CreateProcedureInternal(const api::CreateProcedureRequest& sp_request) {
    auto sp_info = std::make_shared<api::ProcedureInfo>(sp_request.sp_info());
    const std::string& sp_db_name = sp_info->db_name();
    const std::string& sp_name = sp_info->sp_name();
    const std::string sp_data_path = absl::StrCat(zk_path_.db_sp_data_path_, "/", sp_db_name, ".", sp_name);
    auto status = CreateProcedureOnTablet(sp_request);
    do {
        if (!status.OK()) {
            break;
        }
        if (IsClusterMode()) {
            std::string sp_value;
            sp_info->SerializeToString(&sp_value);
            std::string compressed;
            ::snappy::Compress(sp_value.c_str(), sp_value.length(), &compressed);
            if (!zk_client_->CreateNode(sp_data_path, compressed)) {
                PDLOG(WARNING, "create db store procedure node[%s] failed! value[%s] value size[%lu]",
                      sp_data_path.c_str(), sp_value.c_str(), compressed.length());
                status = {base::ReturnCode::kCreateZkFailed, "create zk node failed"};
                break;
            }
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto& sp_table_map = db_sp_table_map_[sp_db_name];
            for (const auto& depend_table : sp_info->tables()) {
                auto& table_sp_map = db_table_sp_map_[depend_table.db_name()];
                sp_table_map[sp_name].emplace_back(depend_table.db_name(), depend_table.table_name());
                table_sp_map[depend_table.table_name()].emplace_back(sp_db_name, sp_name);
            }
            db_sp_info_map_[sp_db_name][sp_name] = sp_info;
        }
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
        PDLOG(INFO, "create db store procedure success! db_name [%s] sp_name [%s] sql [%s]", sp_db_name.c_str(),
              sp_name.c_str(), sp_info->sql().c_str());
    } while (0);
    if (!status.OK()) {
        DropProcedureOnTablet(sp_db_name, sp_name);
    }
    return status;
}

base::Status NameServerImpl::CreateProcedureOnTablet(const ::openmldb::api::CreateProcedureRequest& sp_request) {
    std::vector<std::shared_ptr<TabletClient>> tb_client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& kv : tablets_) {
            if (!kv.second->Health()) {
                LOG(WARNING) << "endpoint [" << kv.first << "] is offline";
                continue;
            }
            tb_client_vec.push_back(kv.second->client_);
        }
    }
    DLOG(INFO) << "request timeout in ms: " << sp_request.timeout_ms();
    const auto& sp_info = sp_request.sp_info();
    for (auto tb_client : tb_client_vec) {
        auto status = tb_client->CreateProcedure(sp_request);
        if (!status.OK()) {
            return {base::ReturnCode::kCreateProcedureFailedOnTablet,
                    absl::StrCat("create procedure on tablet failed, sp ", sp_info.db_name(), ".", sp_info.sp_name(),
                                 ", endpoint: ", tb_client->GetEndpoint(), ", msg: ", status.GetMsg())};
        }
        DLOG(INFO) << "create procedure on tablet success. db_name: " << sp_info.db_name() << ", "
                   << "sp_name: " << sp_info.sp_name() << ", " << "sql: " << sp_info.sql()
                   << "endpoint: " << tb_client->GetEndpoint();
    }
    return {};
}

void NameServerImpl::DropProcedureOnTablet(const std::string& db_name, const std::string& sp_name) {
    std::vector<std::shared_ptr<TabletClient>> tb_client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& kv : tablets_) {
            if (!kv.second->Health()) {
                PDLOG(WARNING, "endpoint [%s] is offline", kv.first.c_str());
                continue;
            }
            tb_client_vec.push_back(kv.second->client_);
        }
    }

    for (auto tb_client : tb_client_vec) {
        if (!tb_client->DropProcedure(db_name, sp_name)) {
            PDLOG(WARNING, "drop procedure on tablet failed. db_name[%s], sp_name[%s], endpoint[%s]", db_name.c_str(),
                  sp_name.c_str(), tb_client->GetEndpoint().c_str());
            continue;
        }

        PDLOG(INFO, "drop procedure on tablet success. db_name[%s], sp_name[%s], endpoint[%s]", db_name.c_str(),
              sp_name.c_str(), tb_client->GetEndpoint().c_str());
    }
}

void NameServerImpl::DropProcedure(RpcController* controller, const api::DropProcedureRequest* request,
                                   GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    const std::string db_name = request->db_name();
    const std::string sp_name = request->sp_name();
    bool wrong = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto db_iter = db_sp_table_map_.find(db_name);
        if (db_iter == db_sp_table_map_.end()) {
            wrong = true;
        } else {
            const auto& sp_table_map = db_iter->second;
            if (sp_table_map.find(sp_name) == sp_table_map.end()) {
                wrong = true;
            }
        }
        if (wrong) {
            PDLOG(WARNING, "storage procedure not found! sp_name [%s]", sp_name.c_str());
            response->set_code(::openmldb::base::ReturnCode::kProcedureNotFound);
            response->set_msg("storage procedure not found!");
            return;
        }
    }
    DropProcedureOnTablet(db_name, sp_name);
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (IsClusterMode()) {
            std::string sp_data_path = zk_path_.db_sp_data_path_ + "/" + db_name + "." + sp_name;
            if (!zk_client_->DeleteNode(sp_data_path)) {
                PDLOG(WARNING, "delete storage procedure zk node[%s] failed!", sp_data_path.c_str());
                response->set_code(::openmldb::base::ReturnCode::kDelZkFailed);
                response->set_msg("delete storage procedure zk node failed");
                return;
            }
        }
        auto& sp_table_map = db_sp_table_map_[db_name];
        auto& db_table_pairs = sp_table_map[sp_name];
        auto db_sp_pair = std::make_pair(db_name, sp_name);
        // erase depend table from db_table_sp_map_ if there is no associated procedures.
        for (const auto& db_table : db_table_pairs) {
            auto& table_sp_map = db_table_sp_map_[db_table.first];
            auto& sp_vec = table_sp_map[db_table.second];
            sp_vec.erase(std::remove(sp_vec.begin(), sp_vec.end(), db_sp_pair), sp_vec.end());
            if (sp_vec.empty()) {
                table_sp_map.erase(db_table.second);
            }
        }
        sp_table_map.erase(sp_name);
        // if db's map is empty, delete the db
        db_sp_info_map_[db_name].erase(sp_name);
        if (db_sp_info_map_[db_name].empty()) {
            db_sp_info_map_.erase(db_name);
        }
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

std::function<std::unique_ptr<::openmldb::catalog::FullTableIterator>(const std::string& table_name)>
NameServerImpl::GetSystemTableIterator() {
    return [this](const std::string& table_name) -> std::unique_ptr<::openmldb::catalog::FullTableIterator> {
        std::shared_ptr<TableInfo> table_info;
        if (!GetTableInfo(table_name, INTERNAL_DB, &table_info)) {
            return nullptr;
        }
        auto tid = table_info->tid();
        auto table_partition = table_info->table_partition(0);  // only one partition for system table
        for (int meta_idx = 0; meta_idx < table_partition.partition_meta_size(); meta_idx++) {
            if (table_partition.partition_meta(meta_idx).is_leader() &&
                table_partition.partition_meta(meta_idx).is_alive()) {
                auto endpoint = table_partition.partition_meta(meta_idx).endpoint();
                auto table_ptr = GetTablet(endpoint);
                std::map<uint32_t, std::shared_ptr<::openmldb::client::TabletClient>> tablet_clients = {
                    {0, table_ptr->client_}};
                return std::make_unique<catalog::FullTableIterator>(tid, nullptr, tablet_clients);
            }
        }
        return nullptr;
    };
}

bool NameServerImpl::RecoverProcedureInfo() {
    db_table_sp_map_.clear();
    db_sp_table_map_.clear();
    db_sp_info_map_.clear();

    std::vector<std::string> db_sp_vec;
    if (!zk_client_->GetChildren(zk_path_.db_sp_data_path_, db_sp_vec)) {
        if (zk_client_->IsExistNode(zk_path_.db_sp_data_path_) != 0) {
            LOG(WARNING) << "zk_db_sp_data_path node [" << zk_path_.db_sp_data_path_ << "] does not exist";
            return true;
        } else {
            LOG(WARNING) << "get zk_db_sp_data_path [" << zk_path_.db_sp_data_path_ << "] children node failed!";
            return false;
        }
    }
    LOG(INFO) << "need to recover db store procedure num: " << db_sp_vec.size();
    for (const auto& node : db_sp_vec) {
        std::string sp_node = zk_path_.db_sp_data_path_ + "/" + node;
        std::string value;
        if (!zk_client_->GetNodeValue(sp_node, value)) {
            LOG(WARNING) << "get db store procedure info failed! sp node: " << sp_node;
            continue;
        }
        std::string uncompressed;
        ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);

        auto sp_info = std::make_shared<api::ProcedureInfo>();
        if (!sp_info->ParseFromString(uncompressed)) {
            LOG(WARNING) << "parse store procedure info failed! sp node: " << sp_node;
            continue;
        }
        const std::string& sp_db_name = sp_info->db_name();
        const std::string& sp_name = sp_info->sp_name();
        const std::string& sql = sp_info->sql();
        if (databases_.find(sp_db_name) != databases_.end()) {
            auto& sp_table_map = db_sp_table_map_[sp_db_name];
            for (const auto& depend_table : sp_info->tables()) {
                // sp_db_name
                //  -> sp_name
                //      -> (depend_table.db_name, depend_table.table_name)
                sp_table_map[sp_name].push_back(std::make_pair(depend_table.db_name(), depend_table.table_name()));
                auto& table_sp_map = db_table_sp_map_[depend_table.db_name()];
                // depend_table.db_name
                //      -> depend_table.table_name
                //          -> (sp_db_name, sp_name)
                table_sp_map[depend_table.table_name()].push_back(std::make_pair(sp_db_name, sp_name));
            }
            auto& sp_info_map = db_sp_info_map_[sp_db_name];
            sp_info_map.emplace(sp_name, sp_info);
            LOG(INFO) << "recover store procedure " << sp_name << " with sql " << sql << " in db " << sp_db_name;
        } else {
            LOG(WARNING) << "db " << sp_db_name << " not exist for sp " << sp_name;
        }
    }
    return true;
}

bool NameServerImpl::AllocateTableId(uint32_t* id) {
    if (id == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (IsClusterMode()) {
        if (!zk_client_->SetNodeValue(zk_path_.table_index_node_, std::to_string(table_index_ + 1))) {
            PDLOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
            return false;
        }
    }
    table_index_++;
    *id = table_index_;
    return true;
}

uint64_t NameServerImpl::GetTerm() const { return term_; }

void NameServerImpl::ShowProcedure(RpcController* controller, const api::ShowProcedureRequest* request,
                                   api::ShowProcedureResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_code(base::ReturnCode::kOk);
    response->set_msg("ok");

    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    // empty(unset or set empty string) means 'show all'
    auto& db_name = request->db_name();
    auto& sp_name = request->sp_name();

    {
        std::lock_guard<std::mutex> lock(mu_);
        // show all
        if (db_name.empty()) {
            for (auto& db : db_sp_info_map_) {
                auto& db_nm = db.first;
                for (auto& sp : db.second) {
                    auto& sp_nm = sp.first;
                    auto& info = sp.second;
                    DCHECK_EQ(db_nm, info->db_name());
                    DCHECK_EQ(sp_nm, info->sp_name());
                    auto add = response->add_sp_info();
                    add->CopyFrom(*info);
                }
            }
            return;
        }
        // only find one db
        if (db_sp_info_map_.find(db_name) == db_sp_info_map_.end()) {
            // db_sp_info_map_ has no entry of db, just means no sp in db, return empty sp info array.
            return;
        }
        auto& sp_map = db_sp_info_map_[db_name];
        // db_sp_info_map_ won't have empty map of one db
        DCHECK(!sp_map.empty()) << "db " << db_name << " 's sp map is empty";
        // if no sp name, show all sp in this db
        if (sp_name.empty()) {
            for (auto& sp : sp_map) {
                auto& sp_nm = sp.first;
                auto& info = sp.second;
                DCHECK_EQ(db_name, info->db_name());
                DCHECK_EQ(sp_nm, info->sp_name());
                auto add = response->add_sp_info();
                add->CopyFrom(*info);
            }
            return;
        }
        if (sp_map.find(sp_name) == sp_map.end()) {
            response->set_code(::openmldb::base::ReturnCode::kDatabaseNotFound);
            response->set_msg("not found");
            PDLOG(WARNING, "db %s sp[%s] not found", db_name, sp_name);
            return;
        }
        // only return the specified one
        auto add = response->add_sp_info();
        add->CopyFrom(*sp_map[sp_name]);
    }
}

std::shared_ptr<TabletInfo> NameServerImpl::GetTabletUnlock(const std::string& endpoint) {
    std::shared_ptr<TabletInfo> tablet_ptr;
    auto iter = tablets_.find(endpoint);
    // check tablet if exist
    if (iter == tablets_.end()) {
        return {};
    }
    tablet_ptr = iter->second;
    // check tablet healthy
    if (tablet_ptr->state_ != ::openmldb::type::EndpointState::kHealthy) {
        return {};
    }
    return tablet_ptr;
}

std::shared_ptr<TabletInfo> NameServerImpl::GetTablet(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetTabletUnlock(endpoint);
}

void NameServerImpl::CreateDatabaseOrExit(const std::string& db) {
    auto status = CreateDatabase(db, true);
    if (!status.OK() && status.code != ::openmldb::base::ReturnCode::kDatabaseAlreadyExists) {
        LOG(ERROR) << "create database failed. code=" << status.GetCode() << ", msg=" << status.GetMsg();
        exit(1);
    }
}

void NameServerImpl::CreateSystemTableOrExit(SystemTableType type) {
    auto status = CreateSystemTable(type);
    if (!status.OK()) {
        LOG(ERROR) << "create system table " << GetSystemTableName(type) << " failed. code=" << status.GetCode()
                   << ", msg=" << status.GetMsg();
        exit(1);
    }
}

base::Status NameServerImpl::CreateSystemTable(SystemTableType table_type) {
    auto table_info = SystemTable::GetTableInfo(table_type);
    if (!table_info) {
        LOG(WARNING) << "fail to get table info. name is " << GetSystemTableName(table_type);
        return {base::ReturnCode::kError, "nullptr"};
    }
    uint32_t tid = 0;
    if (!AllocateTableId(&tid)) {
        return {base::ReturnCode::kError, "allocate tid failed"};
    }
    table_info->set_tid(tid);
    if (SetPartitionInfo(*table_info) < 0) {
        LOG(WARNING) << "set partition info failed. name is " << GetSystemTableName(table_type);
        return {base::ReturnCode::kError, "set partition info failed"};
    }
    uint64_t cur_term = GetTerm();
    GeneralResponse response;
    CreateTableInternel(response, table_info, cur_term, tid, nullptr);
    if (response.code() != 0) {
        return {base::ReturnCode::kError, response.msg()};
    }
    LOG(INFO) << "create system table ok. name is " << GetSystemTableName(table_type);
    return {};
}

void NameServerImpl::UpdateOfflineTableInfo(::google::protobuf::RpcController* controller,
                                            const ::openmldb::nameserver::TableInfo* request,
                                            ::openmldb::nameserver::GeneralResponse* response,
                                            ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    base::SetResponseOK(response);

    // TableInfo request is good, we can check db name tid...
    const auto& db_name = request->db();
    const auto& table_name = request->name();
    if (db_name.empty() || table_name.empty() || !request->has_tid()) {
        base::SetResponseStatus(base::ReturnCode::kInvalidParameter, "empty db/name, or no tid", response);
        return;
    }

    auto tid = request->tid();
    {
        std::lock_guard<std::mutex> lock(mu_);

        if (databases_.find(db_name) == databases_.end()) {
            base::SetResponseStatus(base::ReturnCode::kDatabaseNotFound, "database not found", response);
            LOG(WARNING) << "database [" << db_name << "] not found";
            return;
        }
        auto& table_infos = db_table_info_[db_name];
        auto find_info = table_infos.find(table_name);
        if (find_info == table_infos.end() || find_info->second->tid() != request->tid()) {
            base::SetResponseStatus(base::ReturnCode::kTableIsNotExist, "table not exist", response);
            LOG(WARNING) << "table [" << table_name << "] not exist";
            return;
        }

        auto ori_table_info = find_info->second;

        // copy origin table info, do not modify origin table info until the zk update succeed
        auto new_info = std::make_shared<TableInfo>(*ori_table_info);
        if (!request->has_offline_table_info()) {
            // a tricky way to delete offline table info
            new_info->release_offline_table_info();
        } else {
            new_info->mutable_offline_table_info()->CopyFrom(request->offline_table_info());
        }
        if (IsClusterMode()) {
            // TODO(hw): DCHECK mode_?
            std::string info_str;
            new_info->SerializeToString(&info_str);

            // update to zk
            auto table_info_node = zk_path_.db_table_data_path_ + "/" + std::to_string(tid);
            if (zk_client_->IsExistNode(table_info_node) != 0) {
                base::SetResponseStatus(base::ReturnCode::kGetZkFailed,
                                        "tid " + std::to_string(tid) + " does not existed in zk", response);
                LOG(ERROR) << "table node does not existed in zk, but is existed in nameserver";
                return;
            }
            if (!zk_client_->SetNodeValue(table_info_node, info_str)) {
                base::SetResponseStatus(base::ReturnCode::kSetZkFailed, "set value failed", response);
                LOG(WARNING) << "set table info value failed. table " << table_name << ", node " << table_info_node;
                return;
            }
        }
        // update in this
        table_infos[table_name] = new_info;
        NotifyTableChanged(::openmldb::type::NotifyType::kTable);
    }
    LOG(INFO) << "[" << db_name << "." << table_name << "] update offline table info succeed";
}

std::vector<std::shared_ptr<TabletInfo>> NameServerImpl::GetAllHealthTablet() {
    std::vector<std::shared_ptr<TabletInfo>> tablets;
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& kv : tablets_) {
        if (!kv.second->Health()) {
            continue;
        }
        tablets.push_back(kv.second);
    }
    return tablets;
}

void NameServerImpl::CreateFunction(RpcController* controller, const CreateFunctionRequest* request,
                                    CreateFunctionResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_code(base::kRPCRunError);
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (external_fun_.find(request->fun().name()) != external_fun_.end()) {
            PDLOG(WARNING, "create function failed. function %s is exist", request->fun().name().c_str());
            response->set_msg("function is exist");
            return;
        }
    }
    auto tablets = GetAllHealthTablet();
    std::vector<std::shared_ptr<TabletInfo>> succ_tablets;
    std::string error_msgs;
    // try create on every tablet
    for (const auto& tablet : tablets) {
        std::string msg;
        if (!tablet->client_->CreateFunction(request->fun(), &msg)) {
            error_msgs.append("create function failed on " + tablet->client_->GetEndpoint() + ", reason: " + msg + ";");
        } else {
            succ_tablets.emplace_back(tablet);
        }
    }
    // rollback and return, it's ok if tablet rollback failed
    if (succ_tablets.size() < tablets.size()) {
        for (const auto& tablet : succ_tablets) {
            std::string msg;
            if (!tablet->client_->DropFunction(request->fun(), &msg)) {
                PDLOG(WARNING, "drop function failed. endpoint %s", tablet->client_->GetEndpoint().c_str());
            }
            PDLOG(INFO, "drop function on endpoint %s", tablet->client_->GetEndpoint().c_str());
        }
        SET_RESP_AND_WARN(response, base::ReturnCode::kCreateFunctionFailedOnTablet, error_msgs);
        return;
    }
    auto fun = std::make_shared<::openmldb::common::ExternalFun>(request->fun());
    if (IsClusterMode()) {
        std::string value;
        fun->SerializeToString(&value);
        std::string fun_node = zk_path_.external_function_path_ + "/" + fun->name();
        if (!zk_client_->CreateNode(fun_node, value)) {
            SET_RESP_AND_WARN(response, base::ReturnCode::kCreateZkFailed, "create function on zk failed: " + fun_node);
            return;
        }
    }
    PDLOG(INFO, "create function %s success", fun->name().c_str());
    base::SetResponseOK(response);
    std::lock_guard<std::mutex> lock(mu_);
    external_fun_.emplace(fun->name(), fun);
}

void NameServerImpl::DropFunction(RpcController* controller, const DropFunctionRequest* request,
                                  DropFunctionResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::common::ExternalFun> fun;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = external_fun_.find(request->name());
        if (iter != external_fun_.end()) {
            fun = iter->second;
        }
    }
    if (!fun) {
        if (request->if_exists()) {
            base::SetResponseOK(response);
        } else {
            SET_RESP_AND_WARN(response, base::ReturnCode::kError, "fun does not exist in nameserver meta");
        }
        return;
    }
    auto tablets = GetAllHealthTablet();
    for (const auto& tablet : tablets) {
        std::string msg;
        // if drop function failed on tablet, treat it as success(only log warning)
        if (!tablet->client_->DropFunction(*fun, &msg)) {
            LOG(WARNING) << "drop function failed on " << tablet->client_->GetEndpoint() << ", reason: " << msg;
        }
    }
    if (IsClusterMode()) {
        std::string fun_node = zk_path_.external_function_path_ + "/" + fun->name();
        if (!zk_client_->DeleteNode(fun_node)) {
            // if drop zk node failed, the whole drop function failed
            SET_RESP_AND_WARN(response, base::ReturnCode::kDelZkFailed, "delete function zk node failed:" + fun_node);
            return;
        }
        // func in taskmanager is deleted by client, not in here
    }
    base::SetResponseOK(response);
    LOG(INFO) << "drop function " << request->name() << " success";
    std::lock_guard<std::mutex> lock(mu_);
    external_fun_.erase(request->name());
}

void NameServerImpl::ShowFunction(RpcController* controller, const ShowFunctionRequest* request,
                                  ShowFunctionResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    if (request->has_name() && !request->name().empty()) {
        auto iter = external_fun_.find(request->name());
        if (iter != external_fun_.end()) {
            response->add_fun()->CopyFrom(*(iter->second));
        }
    } else {
        for (const auto& kv : external_fun_) {
            response->add_fun()->CopyFrom(*(kv.second));
        }
    }
    base::SetResponseOK(response);
}

base::Status NameServerImpl::InitGlobalVarTable() {
    std::map<std::string, std::string> default_value = {
        {"execute_mode", "offline"}, {"enable_trace", "false"}, {"sync_job", "false"}, {"job_timeout", "20000"}};
    // get table_info
    std::string db = INFORMATION_SCHEMA_DB;
    std::string table = GLOBAL_VARIABLES;
    std::shared_ptr<TableInfo> table_info;
    if (!GetTableInfo(table, db, &table_info)) {
        return {ReturnCode::kTableIsNotExist, "table does not exist"};
    }
    // encode row && dimensions
    std::vector<std::string> rows;
    std::vector<std::vector<std::pair<std::string, uint32_t>>> rows_dimensions;
    for (auto iter = default_value.begin(); iter != default_value.end(); iter++) {
        std::string row;
        std::vector<std::string> vec;
        vec.push_back(iter->first);
        vec.push_back(iter->second);
        codec::RowCodec::EncodeRow(vec, table_info->column_desc(), 1, row);
        rows.push_back(row);
        std::vector<std::pair<std::string, uint32_t>> dimensions;
        // only one index in system table
        dimensions.push_back(std::make_pair(iter->first, 0));
        rows_dimensions.push_back(dimensions);
    }
    // insert value
    uint32_t tid = table_info->tid();
    uint32_t pid_num = table_info->table_partition_size();
    for (size_t i = 0; i < default_value.size(); i++) {
        std::string row = rows[i];
        std::vector<std::pair<std::string, uint32_t>> dimensions = rows_dimensions[i];
        uint32_t pid = 0;
        if (pid_num > 0) {
            pid = (uint32_t)(::openmldb::base::hash64(dimensions[0].first) % pid_num);
        }
        // system table only have one partition, so table_partition(0) can be used
        for (int meta_idx = 0; meta_idx < table_info->table_partition(0).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(0).partition_meta(meta_idx).is_leader() &&
                table_info->table_partition(0).partition_meta(meta_idx).is_alive()) {
                uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
                std::string endpoint = table_info->table_partition(0).partition_meta(meta_idx).endpoint();
                auto table_ptr = GetTablet(endpoint);
                if (!table_ptr->client_->Put(tid, pid, cur_ts, row, dimensions).OK()) {
                    return {ReturnCode::kPutFailed, "fail to make a put request to table"};
                }
                break;
            }
        }
    }
    return {};
}

std::shared_ptr<Task> NameServerImpl::CreateTaskInternal(const TaskMeta* task_meta) {
    auto task_type = task_meta->task_info->task_type();
    std::shared_ptr<TabletClient> client;
    std::string endpoint = task_meta->task_info->endpoint();
    if (!endpoint.empty()) {
        auto it = tablets_.find(endpoint);
        if (it == tablets_.end() || it->second->state_ != ::openmldb::type::EndpointState::kHealthy) {
            return {};
        }
        client = it->second->client_;
    }
    auto task_info = task_meta->task_info;
    auto task = std::make_shared<Task>(endpoint, task_info);
    switch (task_type) {
        case ::openmldb::api::TaskType::kMakeSnapshot: {
            auto meta = dynamic_cast<const MakeSnapshotTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::MakeSnapshot, client, meta->tid, meta->pid, meta->end_offset, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kPauseSnapshot: {
            auto meta = dynamic_cast<const PauseSnapshotTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::PauseSnapshot, client, meta->tid, meta->pid, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kRecoverSnapshot: {
            auto meta = dynamic_cast<const RecoverSnapshotTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::RecoverSnapshot, client, meta->tid, meta->pid, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kSendSnapshot: {
            auto meta = dynamic_cast<const SendSnapshotTaskMeta*>(task_meta);
            boost::function<bool()> fun = boost::bind(&TabletClient::SendSnapshot, client, meta->tid, meta->remote_tid,
                                                      meta->pid, meta->des_endpoint, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kLoadTable: {
            auto meta = dynamic_cast<const LoadTableTaskMeta*>(task_meta);
            ::openmldb::api::TableMeta table_meta;
            table_meta.set_name(meta->name);
            table_meta.set_tid(meta->tid);
            table_meta.set_pid(meta->pid);
            table_meta.set_seg_cnt(meta->seg_cnt);
            table_meta.set_storage_mode(meta->storage_mode);
            if (meta->is_leader) {
                table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
            } else {
                table_meta.set_mode(::openmldb::api::TableMode::kTableFollower);
            }
            boost::function<bool()> fun = boost::bind(&TabletClient::LoadTable, client, table_meta, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kAddReplica: {
            auto meta = dynamic_cast<const AddReplicaTaskMeta*>(task_meta);
            boost::function<bool()> fun;
            if (meta->is_remote) {
                if (meta->task_id != INVALID_PARENT_ID) {
                    task_info->set_task_id(meta->task_id);
                }
                fun = boost::bind(&TabletClient::AddReplica, client, meta->tid, meta->pid, meta->des_endpoint,
                                  meta->remote_tid, task_info);
            } else {
                fun =
                    boost::bind(&TabletClient::AddReplica, client, meta->tid, meta->pid, meta->des_endpoint, task_info);
            }
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kDelReplica: {
            auto meta = dynamic_cast<const DelReplicaTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::DelReplica, client, meta->tid, meta->pid, meta->des_endpoint, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kDropTable: {
            auto meta = dynamic_cast<const DropTableTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::DropTable, client, meta->tid, meta->pid, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kAddTableInfo: {
            auto meta = dynamic_cast<const AddTableInfoTaskMeta*>(task_meta);
            if (meta->is_remote) {
                task->fun_ = boost::bind(&NameServerImpl::AddTableInfo, this, meta->alias, meta->endpoint, meta->name,
                                         meta->db, meta->remote_tid, meta->pid, task_info);
            } else {
                task->fun_ = boost::bind(&NameServerImpl::AddTableInfo, this, meta->name, meta->db, meta->endpoint,
                                         meta->pid, task_info);
            }
            break;
        }
        case ::openmldb::api::TaskType::kDelTableInfo: {
            auto meta = dynamic_cast<const DelTableInfoTaskMeta*>(task_meta);
            if (meta->has_flag) {
                task->fun_ = boost::bind(&NameServerImpl::DelTableInfo, this, meta->name, meta->db, meta->endpoint,
                                         meta->pid, task_info, meta->flag);
            } else {
                task->fun_ = boost::bind(&NameServerImpl::DelTableInfo, this, meta->name, meta->db, meta->endpoint,
                                         meta->pid, task_info);
            }
            break;
        }
        case ::openmldb::api::TaskType::kUpdateTableInfo: {
            auto meta = dynamic_cast<const UpdateTableInfoTaskMeta*>(task_meta);
            task->fun_ = boost::bind(&NameServerImpl::UpdateTableInfo, this, meta->src_endpoint, meta->name, meta->db,
                                     meta->pid, meta->des_endpoint, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kSendIndexRequest: {
            auto meta = dynamic_cast<const SendIndexRequestTaskMeta*>(task_meta);
            boost::function<bool()> fun = boost::bind(&TabletClient::SendIndexData, client, meta->tid, meta->pid,
                                                      meta->pid_endpoint_map, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kSendIndexData: {
            auto meta = dynamic_cast<const SendIndexDataTaskMeta*>(task_meta);
            for (const auto& kv : meta->pid_endpoint_map) {
                auto sub_task =
                    CreateTask<SendIndexRequestTaskMeta>(meta->task_info->op_id(), meta->task_info->op_type(),
                                                         kv.second, meta->tid, kv.first, meta->pid_endpoint_map);
                task->sub_task_.push_back(sub_task);
                PDLOG(INFO, "add subtask kSendIndexData. op_id[%lu] tid[%u] pid[%u] endpoint[%s]",
                      meta->task_info->op_id(), meta->tid, kv.first, kv.second.c_str());
            }
            task->fun_ = boost::bind(&NameServerImpl::RunSubTask, this, task);
            break;
        }
        case ::openmldb::api::TaskType::kLoadIndexRequest: {
            auto meta = dynamic_cast<const LoadIndexRequestTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::LoadIndexData, client, meta->tid, meta->pid, meta->partition_num, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kLoadIndexData: {
            auto meta = dynamic_cast<const LoadIndexDataTaskMeta*>(task_meta);
            for (const auto& kv : meta->pid_endpoint_map) {
                auto sub_task =
                    CreateTask<LoadIndexRequestTaskMeta>(meta->task_info->op_id(), meta->task_info->op_type(),
                                                         kv.second, meta->tid, kv.first, meta->pid_endpoint_map.size());
                task->sub_task_.push_back(sub_task);
                PDLOG(INFO, "add subtask kLoadIndexData. op_id[%lu] tid[%u] pid[%u] endpoint[%s]",
                      meta->task_info->op_id(), meta->tid, kv.first, kv.second.c_str());
            }
            task->fun_ = boost::bind(&NameServerImpl::RunSubTask, this, task);
            break;
        }
        case ::openmldb::api::TaskType::kExtractIndexRequest: {
            auto meta = dynamic_cast<const ExtractIndexRequestTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::ExtractIndexData, client, meta->tid, meta->pid, meta->partition_num,
                            meta->column_key, meta->offset, true, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kExtractIndexData: {
            auto meta = dynamic_cast<const ExtractIndexDataTaskMeta*>(task_meta);
            for (const auto& kv : meta->pid_endpoint_map) {
                auto iter = meta->pid_offset_map.find(kv.first);
                auto sub_task = CreateTask<ExtractIndexRequestTaskMeta>(
                    meta->task_info->op_id(), meta->task_info->op_type(), kv.second, meta->tid, kv.first,
                    meta->partition_num, meta->column_key, iter->second);
                task->sub_task_.push_back(sub_task);
                PDLOG(INFO, "add subtask kExtractIndexData. op_id[%lu] tid[%u] pid[%u] endpoint[%s]",
                      meta->task_info->op_id(), meta->tid, kv.first, kv.second.c_str());
            }
            task->fun_ = boost::bind(&NameServerImpl::RunSubTask, this, task);
            break;
        }
        case ::openmldb::api::TaskType::kAddIndexToTabletRequest: {
            auto meta = dynamic_cast<const AddIndexToTabletRequestTaskMeta*>(task_meta);
            boost::function<bool()> fun =
                boost::bind(&TabletClient::AddMultiIndex, client, meta->tid, meta->pid, meta->column_key, task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kAddIndexToTablet: {
            auto meta = dynamic_cast<const AddIndexToTabletTaskMeta*>(task_meta);
            for (const auto& part : meta->table_info.table_partition()) {
                for (const auto& part_meta : part.partition_meta()) {
                    const std::string& ep = part_meta.endpoint();
                    auto sub_task = CreateTask<AddIndexToTabletRequestTaskMeta>(
                        meta->task_info->op_id(), meta->task_info->op_type(), ep, meta->table_info.tid(), part.pid(),
                        meta->column_key);
                    task->sub_task_.push_back(sub_task);
                    PDLOG(INFO, "add subtask AddIndexToTablet. op_id[%lu] tid[%u] pid[%u] endpoint[%s]",
                          meta->task_info->op_id(), meta->table_info.tid(), part.pid(), ep.c_str());
                }
            }
            task->fun_ = boost::bind(&NameServerImpl::RunSubTask, this, task);
            break;
        }
        case ::openmldb::api::TaskType::kAddIndexToTableInfo: {
            auto meta = dynamic_cast<const AddIndexToTableInfoTaskMeta*>(task_meta);
            task->fun_ = boost::bind(&NameServerImpl::AddIndexToTableInfo, this, meta->name, meta->db, meta->column_key,
                                     task_info);
            break;
        }
        case ::openmldb::api::TaskType::kCheckBinlogSyncProgress: {
            auto meta = dynamic_cast<const CheckBinlogSyncProgressTaskMeta*>(task_meta);
            task->fun_ = boost::bind(&NameServerImpl::CheckBinlogSyncProgress, this, meta->name, meta->db, meta->pid,
                                     meta->follower, meta->offset_delta, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kChangeLeader: {
            task->fun_ = boost::bind(&NameServerImpl::ChangeLeader, this, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kSelectLeader: {
            auto meta = dynamic_cast<const SelectLeaderTaskMeta*>(task_meta);
            task->fun_ = boost::bind(&NameServerImpl::SelectLeader, this, meta->name, meta->db, meta->tid, meta->pid,
                                     meta->follower_endpoint, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kUpdateLeaderInfo: {
            task->fun_ = boost::bind(&NameServerImpl::UpdateLeaderInfo, this, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kRecoverTable: {
            auto meta = dynamic_cast<const RecoverTableTaskMeta*>(task_meta);
            task->fun_ = boost::bind(&NameServerImpl::RecoverEndpointTable, this, meta->name, meta->db, meta->pid,
                                     meta->endpoint, meta->offset_delta, meta->concurrency, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kUpdatePartitionStatus: {
            auto meta = dynamic_cast<const UpdatePartitionStatusTaskMeta*>(task_meta);
            task->fun_ = boost::bind(&NameServerImpl::UpdatePartitionStatus, this, meta->name, meta->db, meta->endpoint,
                                     meta->pid, meta->is_leader, meta->is_alive, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kCreateTableRemote: {
            auto meta = dynamic_cast<const CreateTableRemoteTaskMeta*>(task_meta);
            auto cluster = GetHealthCluster(meta->alias);
            if (!cluster) {
                PDLOG(WARNING, "replica[%s] not available op_index[%lu]", meta->alias.c_str(),
                      meta->task_info->op_id());
                return {};
            }
            std::string cluster_endpoint =
                std::atomic_load_explicit(&cluster->client_, std::memory_order_relaxed)->GetEndpoint();
            task->task_info_->set_endpoint(cluster_endpoint);
            boost::function<bool()> fun =
                boost::bind(&NameServerImpl::CreateTableRemote, this, *task_info, meta->table_info, cluster);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kDropTableRemote: {
            auto meta = dynamic_cast<const DropTableRemoteTaskMeta*>(task_meta);
            auto cluster = GetHealthCluster(meta->alias);
            if (!cluster) {
                PDLOG(WARNING, "replica[%s] not available op_index[%lu]", meta->alias.c_str(),
                      meta->task_info->op_id());
                return {};
            }
            std::string cluster_endpoint =
                std::atomic_load_explicit(&cluster->client_, std::memory_order_relaxed)->GetEndpoint();
            task->task_info_->set_endpoint(cluster_endpoint);
            boost::function<bool()> fun =
                boost::bind(&NameServerImpl::DropTableRemote, this, *task_info, meta->name, meta->db, cluster);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kAddReplicaNSRemote: {
            auto meta = dynamic_cast<const AddReplicaNSRemoteTaskMeta*>(task_meta);
            auto cluster = GetHealthCluster(meta->alias);
            if (!cluster) {
                PDLOG(WARNING, "replica[%s] not available op_index[%lu]", meta->alias.c_str(),
                      meta->task_info->op_id());
                return {};
            }
            std::string cluster_endpoint =
                std::atomic_load_explicit(&cluster->client_, std::memory_order_relaxed)->GetEndpoint();
            task->task_info_->set_endpoint(cluster_endpoint);
            boost::function<bool()> fun = boost::bind(
                &NsClient::AddReplicaNS, std::atomic_load_explicit(&cluster->client_, std::memory_order_relaxed),
                meta->name, meta->endpoint_vec, meta->pid, zone_info_, *task_info);
            task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kAddTableIndex: {
            auto meta = dynamic_cast<const AddTableIndexTaskMeta*>(task_meta);
            auto status = FillAddIndexTask(meta->task_info->op_id(), meta->task_info->op_type(), meta->name, meta->db,
                                           meta->column_key, &task->seq_task_);
            if (!status.OK()) {
                PDLOG(WARNING, "FillAddIndexTask failed. op_id %lu msg %s", meta->task_info->op_id(),
                      status.GetMsg().c_str());
                return {};
            }
            task->fun_ = boost::bind(&NameServerImpl::RunSeqTask, this, task);
            break;
        }
        case ::openmldb::api::TaskType::kAddMultiTableIndex: {
            auto meta = dynamic_cast<const AddMultiTableIndexTaskMeta*>(task_meta);
            for (const auto& cur_table_index : meta->table_index) {
                auto sub_task = CreateTask<AddTableIndexTaskMeta>(
                    meta->task_info->op_id(), meta->task_info->op_type(), cur_table_index.name(), cur_table_index.db(),
                    schema::IndexUtil::Convert2Vector(cur_table_index.column_key()));
                if (!sub_task) {
                    return {};
                }
                task->sub_task_.push_back(sub_task);
                PDLOG(INFO, "add subtask kAddTableIndex. op_id[%lu] table name %s db %s", meta->task_info->op_id(),
                      cur_table_index.name().c_str(), cur_table_index.db().c_str());
            }
            task->fun_ = boost::bind(&NameServerImpl::RunSubTask, this, task);
            break;
        }
        case ::openmldb::api::TaskType::kCreateProcedure: {
            auto meta = dynamic_cast<const CreateProcedureTaskMeta*>(task_meta);
            api::CreateProcedureRequest request;
            request.mutable_sp_info()->CopyFrom(meta->sp_info);
            boost::function<base::Status()> wrap_fun =
                boost::bind(&NameServerImpl::CreateProcedureInternal, this, request);
            task->fun_ = boost::bind(&NameServerImpl::WrapNormalTaskFun, this, wrap_fun, task_info);
            break;
        }
        case ::openmldb::api::TaskType::kDumpIndexData:     // deprecated
        case ::openmldb::api::TaskType::kUpdateTableAlive:  // deprecated
        case ::openmldb::api::TaskType::kTableSyncTask:     // deprecated
            break;
    }
    return task;
}

std::shared_ptr<api::ProcedureInfo> NameServerImpl::GetProcedure(const std::string& db, const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = db_sp_info_map_.find(db);
    if (iter != db_sp_info_map_.end()) {
        auto sp_iter = iter->second.find(name);
        if (sp_iter != iter->second.end()) {
            return sp_iter->second;
        }
    }
    return {};
}

bool NameServerImpl::IsExistDataBase(const std::string& db) {
    std::lock_guard<std::mutex> lock(mu_);
    return databases_.find(db) != databases_.end();
}

void NameServerImpl::DeploySQL(RpcController* controller, const DeploySQLRequest* request, DeploySQLResponse* response,
                               Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(::openmldb::base::ReturnCode::kNameserverIsNotLeader);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    const auto& sp_info = request->sp_info();
    const auto& db = sp_info.db_name();
    const auto& deploy_name = sp_info.sp_name();
    if (!IsExistDataBase(db)) {
        base::SetResponseStatus(ReturnCode::kDatabaseNotFound, "database not found", response);
        PDLOG(WARNING, "database[%s] not found", db.c_str());
        return;
    }
    if (auto procedure = GetProcedure(db, deploy_name);
        procedure && procedure->type() == ::openmldb::type::ProcedureType::kReqDeployment) {
        base::SetResponseStatus(ReturnCode::kProcedureAlreadyExists, "deployment already exists", response);
        PDLOG(WARNING, "deployment[%s] already exists in db[%s]", deploy_name.c_str(), db.c_str());
        return;
    }
    for (const auto& index : request->index()) {
        std::shared_ptr<TableInfo> table_info;
        std::string cur_db = index.has_db() && !index.db().empty() ? index.db() : db;
        const auto& table_name = index.name();
        if (!GetTableInfo(table_name, cur_db, &table_info)) {
            base::SetResponseStatus(ReturnCode::kTableIsNotExist, "table does not exist!", response);
            PDLOG(WARNING, "table %s.%s does not exit", cur_db.c_str(), table_name.c_str());
            return;
        }
        for (const auto& column_key : index.column_key()) {
            if (schema::IndexUtil::IsExist(column_key, table_info->column_key())) {
                base::SetResponseStatus(ReturnCode::kIndexAlreadyExists, "index already exist!", response);
                PDLOG(WARNING, "index already exist in table %s", table_name.c_str());
                return;
            }
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (IsExistActiveOp(db, "", api::OPType::kDeployOP)) {
        LOG(WARNING) << "create DeployOP failed. there is already a task running in db " << db;
        base::SetResponseStatus(ReturnCode::kOPAlreadyExists, "there is already a task running", response);
        return;
    }
    uint64_t op_id = 0;
    auto status = CreateDeployOP(*request, &op_id);
    if (!status.OK()) {
        PDLOG(WARNING, "%s", status.GetMsg().c_str());
    }
    response->set_op_id(op_id);
    SetResponseStatus(status, response);
}

base::Status NameServerImpl::CreateDeployOP(const DeploySQLRequest& request, uint64_t* op_id) {
    std::shared_ptr<OPData> op_data;
    const auto& sp_info = request.sp_info();
    const auto& deploy_name = sp_info.sp_name();
    std::string value;
    auto op_type = api::OPType::kDeployOP;
    if (CreateOPData(op_type, value, op_data, sp_info.main_table(), sp_info.db_name(), 0) < 0) {
        return {-1, absl::StrCat("create DeployOP data error. deploy name ", deploy_name)};
    }
    auto task = CreateTask<AddMultiTableIndexTaskMeta>(op_data->GetOpId(), op_type, request.index());
    if (!task) {
        return {-1, absl::StrCat("Create kAddMultiTableIndex task failed. deploy name ", deploy_name)};
    }
    op_data->task_list_.push_back(task);
    task = CreateTask<CreateProcedureTaskMeta>(op_data->GetOpId(), op_type, sp_info);
    if (!task) {
        return {-1, absl::StrCat("Create CreateProcedureTaskMeta task failed. deploy name ", deploy_name)};
    }
    op_data->task_list_.push_back(task);
    if (AddOPData(op_data) < 0) {
        return {-1, absl::StrCat("add op data failed. deploy name ", sp_info.sp_name())};
    }
    PDLOG(INFO, "create DeployOP success. op id %lu deploy name %s", op_data->GetOpId(), deploy_name.c_str());
    *op_id = op_data->GetOpId();
    return {};
}

bool NameServerImpl::IsExistActiveOp(const std::string& db, const std::string& name, api::OPType op_type) {
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        for (const auto& op_data : op_list) {
            if (op_data->op_info_.op_type() != op_type) {
                continue;
            }
            if (!db.empty() && op_data->op_info_.db() != db) {
                continue;
            }
            if (!name.empty() && op_data->op_info_.name() != name) {
                continue;
            }
            if (op_data->op_info_.task_status() == api::TaskStatus::kInited ||
                op_data->op_info_.task_status() == api::TaskStatus::kDoing) {
                return true;
            }
        }
    }
    return false;
}

bool NameServerImpl::IsExistActiveOp(const std::string& db, const std::string& name) {
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        for (const auto& op_data : op_list) {
            if (!db.empty() && op_data->op_info_.db() != db) {
                continue;
            }
            if (!name.empty() && op_data->op_info_.name() != name) {
                continue;
            }
            if (op_data->op_info_.task_status() == api::TaskStatus::kInited ||
                op_data->op_info_.task_status() == api::TaskStatus::kDoing) {
                return true;
            }
        }
    }
    return false;
}

}  // namespace nameserver
}  // namespace openmldb
