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

#include "client/ns_client.h"

#include <utility>

#include "base/strings.h"

DECLARE_int32(request_timeout_ms);
namespace openmldb {
namespace client {

NsClient::NsClient(const std::string& endpoint, const std::string& real_endpoint)
    : Client(endpoint, real_endpoint), client_(real_endpoint.empty() ? endpoint : real_endpoint) {}

int NsClient::Init() { return client_.Init(); }

const std::string& NsClient::GetDb() { return db_; }

void NsClient::ClearDb() { db_.clear(); }

bool NsClient::Use(const std::string& db, std::string& msg) {
    ::openmldb::nameserver::UseDatabaseRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db(db);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::UseDatabase, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        db_ = db;
        return true;
    }
    return false;
}

bool NsClient::CreateDatabase(const std::string& db, std::string& msg, bool if_not_exists) {
    if (db.empty()) {
        return false;
    }
    ::openmldb::nameserver::CreateDatabaseRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db(db);
    request.set_if_not_exists(if_not_exists);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateDatabase, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    return ok && response.code() == 0;
}

bool NsClient::ShowDatabase(std::vector<std::string>* dbs, std::string& msg) {
    ::openmldb::nameserver::GeneralRequest request;
    ::openmldb::nameserver::ShowDatabaseResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowDatabase, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    for (auto db : response.db()) {
        dbs->push_back(db);
    }
    msg = response.msg();
    return ok && response.code() == 0;
}

bool NsClient::DropDatabase(const std::string& db, std::string& msg) {
    ::openmldb::nameserver::DropDatabaseRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db(db);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropDatabase, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    return ok && response.code() == 0;
}

bool NsClient::ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg) {
    ::openmldb::nameserver::ShowTabletRequest request;
    ::openmldb::nameserver::ShowTabletResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTablet, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::openmldb::nameserver::TabletStatus& status = response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.state = status.state();
            info.age = status.age();
            info.real_endpoint = status.real_endpoint();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowSdkEndpoint(std::vector<TabletInfo>& tablets, std::string& msg) {
    ::openmldb::nameserver::ShowSdkEndpointRequest request;
    ::openmldb::nameserver::ShowSdkEndpointResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowSdkEndpoint, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::openmldb::nameserver::TabletStatus& status = response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.real_endpoint = status.real_endpoint();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowTable(const std::string& name, const std::string& db, bool show_all,
                         std::vector<::openmldb::nameserver::TableInfo>& tables, std::string& msg) {
    ::openmldb::nameserver::ShowTableRequest request;
    if (!name.empty()) {
        request.set_name(name);
    }
    request.set_db(db);
    request.set_show_all(show_all);
    ::openmldb::nameserver::ShowTableResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.table_info_size(); i++) {
            ::openmldb::nameserver::TableInfo table_info;
            table_info.CopyFrom(response.table_info(i));
            tables.push_back(table_info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowTable(const std::string& name, std::vector<::openmldb::nameserver::TableInfo>& tables,
                         std::string& msg) {
    return ShowTable(name, GetDb(), false, tables, msg);
}

base::Status NsClient::ShowDBTable(const std::string& db_name, std::vector<::openmldb::nameserver::TableInfo>* tables) {
    if (tables == nullptr) {
        return base::Status(base::ReturnCode::kError, "null ptr");
    }
    base::Status status(base::ReturnCode::kError, "");
    if (ShowTable("", db_name, false, *tables, status.msg)) {
        return {};
    }
    return status;
}

bool NsClient::ShowAllTable(std::vector<::openmldb::nameserver::TableInfo>& tables, std::string& msg) {
    return ShowTable("", "", true, tables, msg);
}

bool NsClient::MakeSnapshot(const std::string& name, uint32_t pid, uint64_t end_offset, std::string& msg) {
    return MakeSnapshot(name, GetDb(), pid, end_offset, msg);
}

bool NsClient::MakeSnapshot(const std::string& name, const std::string& db, uint32_t pid, uint64_t end_offset,
                            std::string& msg) {
    ::openmldb::nameserver::MakeSnapshotNSRequest request;
    request.set_name(name);
    request.set_pid(pid);
    request.set_offset(end_offset);
    request.set_db(db);
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::MakeSnapshotNS, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowOPStatus(::openmldb::nameserver::ShowOPStatusResponse& response, const std::string& name,
                            uint32_t pid, std::string& msg) {
    ::openmldb::nameserver::ShowOPStatusRequest request;
    if (const std::string& db = GetDb(); !db.empty()) {
        request.set_db(db);
    }
    if (!name.empty()) {
        request.set_name(name);
    }
    if (pid != INVALID_PID) {
        request.set_pid(pid);
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowOPStatus, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CancelOP(uint64_t op_id, std::string& msg) {
    ::openmldb::nameserver::CancelOPRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_op_id(op_id);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CancelOP, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddTableField(const std::string& table_name, const ::openmldb::common::ColumnDesc& column_desc,
                             std::string& msg) {
    ::openmldb::nameserver::AddTableFieldRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(table_name);
    request.set_db(GetDb());
    ::openmldb::common::ColumnDesc* column_desc_ptr = request.mutable_column_desc();
    column_desc_ptr->CopyFrom(column_desc);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddTableField, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

base::Status NsClient::CreateProcedure(const ::openmldb::api::ProcedureInfo& sp_info, uint64_t request_timeout) {
    ::openmldb::api::CreateProcedureRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    ::openmldb::api::ProcedureInfo* sp_info_ptr = request.mutable_sp_info();
    sp_info_ptr->CopyFrom(sp_info);
    request.set_timeout_ms(request_timeout);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateProcedure, &request, &response,
                                  request_timeout, 1);
    if (!ok || response.code() != 0) {
        return base::Status(base::ReturnCode::kError, response.msg());
    }
    return {};
}

bool NsClient::CreateTable(const ::openmldb::nameserver::TableInfo& table_info, const bool create_if_not_exist,
                           std::string& msg) {
    ::openmldb::nameserver::CreateTableRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_create_if_not_exist(create_if_not_exist);
    ::openmldb::nameserver::TableInfo* table_info_r = request.mutable_table_info();
    table_info_r->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DropTable(const std::string& name, std::string& msg) { return DropTable(GetDb(), name, msg); }

bool NsClient::DropTable(const std::string& db, const std::string& name, std::string& msg) {
    ::openmldb::nameserver::DropTableRequest request;
    request.set_name(name);
    request.set_db(db);
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SyncTable(const std::string& name, const std::string& cluster_alias, uint32_t pid, std::string& msg) {
    ::openmldb::nameserver::SyncTableRequest request;
    request.set_name(name);
    request.set_cluster_alias(cluster_alias);
    if (pid != INVALID_PID) {
        request.set_pid(pid);
    }
    request.set_db(GetDb());
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::SyncTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SetSdkEndpoint(const std::string& server_name, const std::string& sdk_endpoint, std::string* msg) {
    ::openmldb::nameserver::SetSdkEndpointRequest request;
    request.set_server_name(server_name);
    request.set_sdk_endpoint(sdk_endpoint);
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::SetSdkEndpoint, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplica(const std::string& name, const std::set<uint32_t>& pid_set, const std::string& endpoint,
                          std::string& msg) {
    if (pid_set.empty()) {
        return false;
    }
    ::openmldb::nameserver::AddReplicaNSRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(*(pid_set.begin()));
    request.set_endpoint(endpoint);
    request.set_db(GetDb());
    if (pid_set.size() > 1) {
        for (auto pid : pid_set) {
            request.add_pid_group(pid);
        }
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddReplicaNS, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaNS(const std::string& name, const std::vector<std::string>& endpoint_vec, uint32_t pid,
                            const ::openmldb::nameserver::ZoneInfo& zone_info,
                            const ::openmldb::api::TaskInfo& task_info) {
    if (endpoint_vec.empty()) {
        return false;
    }
    ::openmldb::nameserver::AddReplicaNSRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    for (auto& endpoint : endpoint_vec) {
        request.add_endpoint_group(endpoint);
    }
    request.set_pid(pid);
    request.set_endpoint(endpoint_vec.front());
    ::openmldb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::openmldb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddReplicaNSFromRemote, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DelReplica(const std::string& name, const std::set<uint32_t>& pid_set, const std::string& endpoint,
                          std::string& msg) {
    if (pid_set.empty()) {
        return false;
    }
    ::openmldb::nameserver::DelReplicaNSRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(*(pid_set.begin()));
    request.set_endpoint(endpoint);
    request.set_db(GetDb());
    if (pid_set.size() > 1) {
        for (auto pid : pid_set) {
            request.add_pid_group(pid);
        }
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DelReplicaNS, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfSet(const std::string& key, const std::string& value, std::string& msg) {
    ::openmldb::nameserver::ConfSetRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    ::openmldb::nameserver::Pair* conf = request.mutable_conf();
    conf->set_key(key);
    conf->set_value(value);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ConfSet, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfGet(const std::string& key, std::map<std::string, std::string>& conf_map, std::string& msg) {
    conf_map.clear();
    ::openmldb::nameserver::ConfGetRequest request;
    ::openmldb::nameserver::ConfGetResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ConfGet, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
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
    ::openmldb::nameserver::ChangeLeaderRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    if (!candidate_leader.empty()) {
        request.set_candidate_leader(candidate_leader);
    }
    request.set_db(GetDb());
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ChangeLeader, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::OfflineEndpoint(const std::string& endpoint, uint32_t concurrency, std::string& msg) {
    ::openmldb::nameserver::OfflineEndpointRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    if (concurrency > 0) {
        request.set_concurrency(concurrency);
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::OfflineEndpoint, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::Migrate(const std::string& src_endpoint, const std::string& name, const std::set<uint32_t>& pid_set,
                       const std::string& des_endpoint, std::string& msg) {
    ::openmldb::nameserver::MigrateRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_src_endpoint(src_endpoint);
    request.set_name(name);
    request.set_des_endpoint(des_endpoint);
    request.set_db(GetDb());
    for (auto pid : pid_set) {
        request.add_pid(pid);
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::Migrate, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::RecoverEndpoint(const std::string& endpoint, bool need_restore, uint32_t concurrency, std::string& msg) {
    ::openmldb::nameserver::RecoverEndpointRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    if (concurrency > 0) {
        request.set_concurrency(concurrency);
    }
    request.set_need_restore(need_restore);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::RecoverEndpoint, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::RecoverTable(const std::string& name, uint32_t pid, const std::string& endpoint, std::string& msg) {
    ::openmldb::nameserver::RecoverTableRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    request.set_db(GetDb());
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::RecoverTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConnectZK(std::string& msg) {
    ::openmldb::nameserver::ConnectZKRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ConnectZK, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DisConnectZK(std::string& msg) {
    ::openmldb::nameserver::DisConnectZKRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DisConnectZK, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SetTablePartition(const std::string& name, const ::openmldb::nameserver::TablePartition& table_partition,
                                 std::string& msg) {
    ::openmldb::nameserver::SetTablePartitionRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_db(GetDb());
    ::openmldb::nameserver::TablePartition* cur_table_partition = request.mutable_table_partition();
    cur_table_partition->CopyFrom(table_partition);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::SetTablePartition, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::GetTablePartition(const std::string& name, uint32_t pid,
                                 ::openmldb::nameserver::TablePartition& table_partition, std::string& msg) {
    ::openmldb::nameserver::GetTablePartitionRequest request;
    ::openmldb::nameserver::GetTablePartitionResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_db(GetDb());
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::GetTablePartition, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        table_partition.CopyFrom(response.table_partition());
        return true;
    }
    return false;
}

bool NsClient::UpdateTableAliveStatus(const std::string& endpoint, std::string& name, uint32_t pid, bool is_alive,
                                      std::string& msg) {
    ::openmldb::nameserver::UpdateTableAliveRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    request.set_name(name);
    request.set_is_alive(is_alive);
    request.set_db(GetDb());
    if (pid < UINT32_MAX) {
        request.set_pid(pid);
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::UpdateTableAliveStatus, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::UpdateTTL(const std::string& name, const ::openmldb::type::TTLType& type, uint64_t abs_ttl,
                         uint64_t lat_ttl, const std::string& index_name, std::string& msg) {
    ::openmldb::nameserver::UpdateTTLRequest request;
    ::openmldb::nameserver::UpdateTTLResponse response;
    request.set_name(name);
    request.set_db(GetDb());
    ::openmldb::common::TTLSt* ttl_desc = request.mutable_ttl_desc();
    ttl_desc->set_ttl_type(type);
    ttl_desc->set_abs_ttl(abs_ttl);
    ttl_desc->set_lat_ttl(lat_ttl);
    if (!index_name.empty()) {
        request.set_index_name(index_name);
    }
    request.set_db(GetDb());
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::UpdateTTL, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DeleteOPTask(const std::vector<uint64_t>& op_id_vec) {
    ::openmldb::api::DeleteTaskRequest request;
    ::openmldb::api::GeneralResponse response;
    for (auto op_id : op_id_vec) {
        request.add_op_id(op_id);
    }
    bool ret = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DeleteOPTask, &request, &response,
                                   FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool NsClient::GetTaskStatus(::openmldb::api::TaskStatusResponse& response) {
    ::openmldb::api::TaskStatusRequest request;
    bool ret = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::GetTaskStatus, &request, &response,
                                   FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool NsClient::LoadTable(const std::string& name, const std::string& endpoint, uint32_t pid,
                         const ::openmldb::nameserver::ZoneInfo& zone_info,
                         const ::openmldb::api::TaskInfo& task_info) {
    return LoadTable(name, GetDb(), endpoint, pid, zone_info, task_info);
}

bool NsClient::LoadTable(const std::string& name, const std::string& db, const std::string& endpoint, uint32_t pid,
                         const ::openmldb::nameserver::ZoneInfo& zone_info,
                         const ::openmldb::api::TaskInfo& task_info) {
    ::openmldb::nameserver::LoadTableRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_endpoint(endpoint);
    request.set_pid(pid);
    request.set_db(db);
    ::openmldb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::openmldb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::LoadTable, &request, &response,
                                  FLAGS_request_timeout_ms, 3);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateRemoteTableInfo(const ::openmldb::nameserver::ZoneInfo& zone_info,
                                     ::openmldb::nameserver::TableInfo& table_info, std::string& msg) {
    ::openmldb::nameserver::CreateTableInfoRequest request;
    ::openmldb::nameserver::CreateTableInfoResponse response;
    ::openmldb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    ::openmldb::nameserver::TableInfo* table_info_p = request.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTableInfo, &request, &response,
                                  FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    table_info = response.table_info();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

base::Status NsClient::CreateDatabaseRemote(const std::string& db, const ::openmldb::nameserver::ZoneInfo& zone_info) {
    if (db.empty()) {
        return {base::ReturnCode::kError, "db is empty"};
    }
    ::openmldb::nameserver::CreateDatabaseRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db(db);
    request.set_if_not_exists(true);
    request.mutable_zone_info()->CopyFrom(zone_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateDatabase, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return {};
    }
    return {response.code(), response.msg()};
}

base::Status NsClient::DropDatabaseRemote(const std::string& db, const ::openmldb::nameserver::ZoneInfo& zone_info) {
    ::openmldb::nameserver::DropDatabaseRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db(db);
    request.mutable_zone_info()->CopyFrom(zone_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropDatabase, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return {};
    }
    return {response.code(), response.msg()};
}

bool NsClient::CreateRemoteTableInfoSimply(const ::openmldb::nameserver::ZoneInfo& zone_info,
                                           ::openmldb::nameserver::TableInfo& table_info, std::string& msg) {
    ::openmldb::nameserver::CreateTableInfoRequest request;
    ::openmldb::nameserver::CreateTableInfoResponse response;
    ::openmldb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    ::openmldb::nameserver::TableInfo* table_info_p = request.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTableInfoSimply, &request, &response,
                                  FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    table_info = response.table_info();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DropTableRemote(const ::openmldb::api::TaskInfo& task_info, const std::string& name,
                               const std::string& db, const ::openmldb::nameserver::ZoneInfo& zone_info,
                               std::string& msg) {
    ::openmldb::nameserver::DropTableRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    ::openmldb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::openmldb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    request.set_name(name);
    request.set_db(db);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropTable, &request, &response,
                                  FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateTableRemote(const ::openmldb::api::TaskInfo& task_info,
                                 const ::openmldb::nameserver::TableInfo& table_info,
                                 const ::openmldb::nameserver::ZoneInfo& zone_info, std::string& msg) {
    ::openmldb::nameserver::CreateTableRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    ::openmldb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::openmldb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    ::openmldb::nameserver::TableInfo* table_info_p;
    table_info_p = request.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                  FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaClusterByNs(const std::string& alias, const std::string& name, const uint64_t term,
                                     std::string& msg) {
    ::openmldb::nameserver::ReplicaClusterByNsRequest request;
    ::openmldb::nameserver::ZoneInfo* zone_info = request.mutable_zone_info();
    ::openmldb::nameserver::AddReplicaClusterByNsResponse response;
    zone_info->set_replica_alias(alias);
    zone_info->set_zone_name(name);
    zone_info->set_zone_term(term);
    zone_info->set_mode(::openmldb::nameserver::kFOLLOWER);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddReplicaClusterByNs, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && ((response.code() == 0) || (response.code() == 408))) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaCluster(const std::string& zk_ep, const std::string& zk_path, const std::string& alias,
                                 std::string& msg) {
    ::openmldb::nameserver::ClusterAddress request;
    ::openmldb::nameserver::GeneralResponse response;
    if (zk_ep.empty() || zk_path.empty() || alias.empty()) {
        msg = "zookeeper endpoints or zk_path or alias is null";
        return false;
    }
    request.set_alias(alias);
    request.set_zk_path(zk_path);
    request.set_zk_endpoints(zk_ep);

    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddReplicaCluster, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();

    if (ok && (response.code() == 0)) {
        return true;
    }
    return false;
}

bool NsClient::ShowReplicaCluster(std::vector<::openmldb::nameserver::ClusterAddAge>& clusterinfo, std::string& msg) {
    clusterinfo.clear();
    ::openmldb::nameserver::GeneralRequest request;
    ::openmldb::nameserver::ShowReplicaClusterResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowReplicaCluster, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && (response.code() == 0)) {
        for (int32_t i = 0; i < response.replicas_size(); i++) {
            const auto& status = response.replicas(i);
            clusterinfo.push_back(status);
        }
        return true;
    }

    return false;
}

bool NsClient::RemoveReplicaCluster(const std::string& alias, std::string& msg) {
    ::openmldb::nameserver::RemoveReplicaOfRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_alias(alias);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::RemoveReplicaCluster, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
                                        int& code, std::string& msg) {
    ::openmldb::nameserver::ReplicaClusterByNsRequest request;
    ::openmldb::nameserver::ZoneInfo* zone_info = request.mutable_zone_info();
    ::openmldb::nameserver::GeneralResponse response;
    zone_info->set_replica_alias(alias);
    zone_info->set_zone_term(term);
    zone_info->set_zone_name(zone_name);
    zone_info->set_mode(::openmldb::nameserver::kNORMAL);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::RemoveReplicaClusterByNs, &request,
                                  &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SwitchMode(const ::openmldb::nameserver::ServerMode& mode, std::string& msg) {
    ::openmldb::nameserver::SwitchModeRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_sm(mode);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::SwitchMode, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddIndex(const std::string& table_name, const ::openmldb::common::ColumnKey& column_key,
                  std::vector<openmldb::common::ColumnDesc>* cols,
                  std::string& msg) {
    return AddIndex("", table_name, column_key, cols, msg);
}

bool NsClient::AddIndex(const std::string& db_name,
                        const std::string& table_name,
                        const ::openmldb::common::ColumnKey& column_key,
                        std::vector<openmldb::common::ColumnDesc>* cols,
                        std::string& msg) {
    ::openmldb::nameserver::AddIndexRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    ::openmldb::common::ColumnKey* cur_column_key = request.mutable_column_key();
    request.set_name(table_name);
    cur_column_key->CopyFrom(column_key);
    if (!db_name.empty()) {
        request.set_db(db_name);
    } else {
        request.set_db(GetDb());
    }
    if (cols != nullptr) {
        for (const auto& col : *cols) {
            openmldb::common::ColumnDesc* new_col = request.add_cols();
            new_col->CopyFrom(col);
        }
    }
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddIndex, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

base::Status NsClient::AddMultiIndex(const std::string& db, const std::string& table_name,
        const std::vector<::openmldb::common::ColumnKey>& column_keys) {
    ::openmldb::nameserver::AddIndexRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    if (column_keys.empty()) {
        return {base::ReturnCode::kError, "no column key"};
    } else {
        for (const auto& column_key : column_keys) {
            request.add_column_keys()->CopyFrom(column_key);
        }
    }
    request.set_name(table_name);
    request.set_db(db);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::AddIndex, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return {};
    }
    return {base::ReturnCode::kError, response.msg()};
}

bool NsClient::DeleteIndex(const std::string& db, const std::string& table_name, const std::string& idx_name,
                           std::string& msg) {
    ::openmldb::nameserver::DeleteIndexRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_table_name(table_name);
    request.set_idx_name(idx_name);
    request.set_db_name(db);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DeleteIndex, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    int code = response.code();
    return ok && code == 0;
}

bool NsClient::DeleteIndex(const std::string& table_name, const std::string& idx_name, std::string& msg) {
    return DeleteIndex(GetDb(), table_name, idx_name, msg);
}

bool NsClient::ShowCatalogVersion(std::map<std::string, uint64_t>* version_map, std::string* msg) {
    if (version_map == nullptr || msg == nullptr) {
        return false;
    }
    version_map->clear();
    ::openmldb::nameserver::ShowCatalogRequest request;
    ::openmldb::nameserver::ShowCatalogResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowCatalog, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    int code = response.code();
    if (ok && code == 0) {
        for (const auto& catalog_info : response.catalog()) {
            version_map->emplace(catalog_info.endpoint(), catalog_info.version());
        }
        return true;
    }
    *msg = response.msg();
    return false;
}

bool NsClient::DropProcedure(const std::string& db_name, const std::string& sp_name, std::string& msg) {
    ::openmldb::api::DropProcedureRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db_name(db_name);
    request.set_sp_name(sp_name);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropProcedure, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowProcedure(const std::string& db_name, const std::string& sp_name,
                             std::vector<api::ProcedureInfo>* infos, std::string* msg) {
    api::ShowProcedureRequest request;
    api::ShowProcedureResponse response;
    request.set_db_name(db_name);
    request.set_sp_name(sp_name);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowProcedure, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    *msg = response.msg();
    if (ok && response.code() == 0) {
        infos->clear();
        for (auto& sp : response.sp_info()) {
            infos->emplace_back(sp);
        }
        return true;
    }
    return false;
}

base::Status NsClient::UpdateOfflineTableInfo(const nameserver::TableInfo& table_info) {
    nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&nameserver::NameServer_Stub::UpdateOfflineTableInfo, &table_info, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return {};
    }
    return base::Status(base::ReturnCode::kError, response.msg());
}

base::Status NsClient::CreateFunction(const ::openmldb::common::ExternalFun& fun) {
    nameserver::CreateFunctionRequest request;
    nameserver::CreateFunctionResponse response;
    request.mutable_fun()->CopyFrom(fun);
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateFunction, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return base::Status(base::ReturnCode::kError, response.msg());
    }
    return {};
}

base::Status NsClient::DropFunction(const std::string& name, bool if_exists) {
    nameserver::DropFunctionRequest request;
    request.set_name(name);
    request.set_if_exists(if_exists);
    nameserver::DropFunctionResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropFunction, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return base::Status(base::ReturnCode::kError, response.msg());
    }
    return {};
}

base::Status NsClient::ShowFunction(const std::string& name,
        std::vector<::openmldb::common::ExternalFun>* fun_vec) {
    if (fun_vec == nullptr) {
        return base::Status(base::ReturnCode::kError, "nullptr");
    }
    fun_vec->clear();
    nameserver::ShowFunctionRequest request;
    if (!name.empty()) {
        request.set_name(name);
    }
    nameserver::ShowFunctionResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowFunction, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return base::Status(base::ReturnCode::kError, response.msg());
    }
    for (int i = 0; i < response.fun_size(); i++) {
        fun_vec->emplace_back(response.fun(i));
    }
    return {};
}

base::Status NsClient::DeploySQL(const std::string& db, const std::string& sql, const std::string& deploy_name,
        const std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& new_index_map,
        uint64_t* job_id) {
    if (new_index_map.empty()) {
        return {base::ReturnCode::kError, "new_index_map is empty. no need to execute DeploySQL"};
    }
    nameserver::DeploySQLRequest request;
    request.set_db(db);
    request.set_deploy_name(deploy_name);
    request.set_sql(sql);
    for (const auto& kv : new_index_map) {
        auto index = request.add_index();
        index->set_name(kv.first);
        for (const auto& column_key : kv.second) {
            index->add_column_key()->CopyFrom(column_key);
        }
    }
    nameserver::DeploySQLResponse response;
    bool ok = client_.SendRequest(&::openmldb::nameserver::NameServer_Stub::DeploySQL, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return base::Status(base::ReturnCode::kError, response.msg());
    }
    *job_id = response.job_id();
    return {};
}

}  // namespace client
}  // namespace openmldb
