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

#include "client/tablet_client.h"

#include <algorithm>
#include <iostream>
#include <set>

#include "base/glog_wrapper.h"
#include "brpc/channel.h"
#include "codec/codec.h"
#include "codec/sql_rpc_row_codec.h"
#include "common/timer.h"
#include "sdk/sql_request_row.h"

DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(absolute_ttl_max);

namespace openmldb {
namespace client {

TabletClient::TabletClient(const std::string& endpoint, const std::string& real_endpoint)
    : Client(endpoint, real_endpoint), client_(real_endpoint.empty() ? endpoint : real_endpoint) {}

TabletClient::TabletClient(const std::string& endpoint, const std::string& real_endpoint, bool use_sleep_policy)
    : Client(endpoint, real_endpoint), client_(real_endpoint.empty() ? endpoint : real_endpoint, use_sleep_policy) {}

TabletClient::~TabletClient() {}

int TabletClient::Init() { return client_.Init(); }

bool TabletClient::Query(const std::string& db, const std::string& sql, const std::string& row, brpc::Controller* cntl,
                         openmldb::api::QueryResponse* response, const bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::openmldb::api::QueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(false);
    request.set_is_debug(is_debug);
    request.set_row_size(row.size());
    request.set_row_slices(1);
    auto& io_buf = cntl->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(row.data()), row.size(), &io_buf)) {
        LOG(WARNING) << "Encode row buffer failed";
        return false;
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Query, cntl, &request, response);
    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool TabletClient::Query(const std::string& db, const std::string& sql,
                         const std::vector<openmldb::type::DataType>& parameter_types,
                         const std::string& parameter_row,
                         brpc::Controller* cntl, ::openmldb::api::QueryResponse* response, const bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::openmldb::api::QueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(true);
    request.set_is_debug(is_debug);
    request.set_parameter_row_size(parameter_row.size());
    request.set_parameter_row_slices(1);
    for (auto& type : parameter_types) {
        request.add_parameter_types(type);
    }
    auto& io_buf = cntl->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(parameter_row.data()), parameter_row.size(), &io_buf)) {
        LOG(WARNING) << "Encode parameter buffer failed";
        return false;
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Query, cntl, &request, response);

    if (!ok || response->code() != 0) {
        LOG(WARNING) << "send rpc request failed";
        return false;
    }
    return true;
}

/**
 * Utility function to encode row batch data into rpc attachment buffer
 */
static bool EncodeRowBatch(std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch> row_batch,
                           ::openmldb::api::SQLBatchRequestQueryRequest* request, butil::IOBuf* io_buf) {
    auto common_slice = row_batch->GetCommonSlice();
    if (common_slice->empty()) {
        request->set_common_slices(0);
    } else {
        if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(common_slice->data()), common_slice->size(), io_buf)) {
            LOG(WARNING) << "encode common row buf failed";
            return false;
        }
        request->add_row_sizes(common_slice->size());
        request->set_common_slices(1);
    }
    for (int i = 0; i < row_batch->Size(); ++i) {
        auto non_common_slice = row_batch->GetNonCommonSlice(i);
        if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(non_common_slice->data()), non_common_slice->size(),
                                 io_buf)) {
            LOG(WARNING) << "encode common row buf failed";
            return false;
        }
        request->add_row_sizes(non_common_slice->size());
        request->set_non_common_slices(1);
    }
    return true;
}

bool TabletClient::SQLBatchRequestQuery(const std::string& db, const std::string& sql,
                                        std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch> row_batch,
                                        brpc::Controller* cntl, ::openmldb::api::SQLBatchRequestQueryResponse* response,
                                        const bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::openmldb::api::SQLBatchRequestQueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_debug(is_debug);

    const std::set<size_t>& indices_set = row_batch->common_column_indices();
    for (size_t idx : indices_set) {
        request.add_common_column_indices(idx);
    }
    auto& io_buf = cntl->request_attachment();
    if (!EncodeRowBatch(row_batch, &request, &io_buf)) {
        return false;
    }

    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::SQLBatchRequestQuery, cntl, &request, response);
    if (!ok || response->code() != ::openmldb::base::kOk) {
        LOG(WARNING) << "fail to query tablet" << response->msg();
        return false;
    }
    return true;
}

bool TabletClient::CreateTable(const std::string& name, uint32_t tid, uint32_t pid, uint64_t abs_ttl, uint64_t lat_ttl,
                               bool leader, const std::vector<std::string>& endpoints,
                               const ::openmldb::type::TTLType& type, uint32_t seg_cnt, uint64_t term,
                               const ::openmldb::type::CompressType compress_type,
                               ::openmldb::common::StorageMode storage_mode) {
    ::openmldb::api::CreateTableRequest request;
    if (type == ::openmldb::type::kLatestTime) {
        if (lat_ttl > FLAGS_latest_ttl_max) {
            return false;
        }
    } else if (type == ::openmldb::type::TTLType::kAbsoluteTime) {
        if (abs_ttl > FLAGS_absolute_ttl_max) {
            return false;
        }
    } else {
        if (abs_ttl > FLAGS_absolute_ttl_max || lat_ttl > FLAGS_latest_ttl_max) {
            return false;
        }
    }
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_compress_type(compress_type);
    table_meta->set_seg_cnt(seg_cnt);
    table_meta->set_storage_mode(storage_mode);
    if (leader) {
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        table_meta->set_term(term);
    } else {
        table_meta->set_mode(::openmldb::api::TableMode::kTableFollower);
    }
    for (size_t i = 0; i < endpoints.size(); i++) {
        table_meta->add_replicas(endpoints[i]);
    }
    ::openmldb::common::ColumnDesc* column_desc = table_meta->add_column_desc();
    column_desc->set_name("idx0");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnKey* index = table_meta->add_column_key();
    index->set_index_name("idx0");
    index->add_col_name("idx0");
    ::openmldb::common::TTLSt* ttl = index->mutable_ttl();
    ttl->set_abs_ttl(abs_ttl);
    ttl->set_lat_ttl(lat_ttl);
    ttl->set_ttl_type(type);
    // table_meta->set_ttl_type(type);
    ::openmldb::api::CreateTableResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::CreateTable, &request, &response,
                                  FLAGS_request_timeout_ms * 2, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::CreateTable(const ::openmldb::api::TableMeta& table_meta) {
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta_ptr = request.mutable_table_meta();
    table_meta_ptr->CopyFrom(table_meta);
    ::openmldb::api::CreateTableResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::CreateTable, &request, &response,
                                  FLAGS_request_timeout_ms * 2, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::UpdateTableMetaForAddField(uint32_t tid, const std::vector<openmldb::common::ColumnDesc>& cols,
                                              const openmldb::common::VersionPair& pair, std::string& msg) {
    ::openmldb::api::UpdateTableMetaForAddFieldRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    for (const auto& col : cols) {
        ::openmldb::common::ColumnDesc* column_desc_ptr = request.add_column_descs();
        column_desc_ptr->CopyFrom(col);
    }
    openmldb::common::VersionPair* new_pair = request.mutable_version_pair();
    new_pair->CopyFrom(pair);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::UpdateTableMetaForAddField, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    msg = response.msg();
    return false;
}

bool TabletClient::Put(uint32_t tid, uint32_t pid, uint64_t time, const std::string& value,
                       const std::vector<std::pair<std::string, uint32_t>>& dimensions) {
    ::openmldb::api::PutRequest request;
    request.set_time(time);
    request.set_value(value);
    request.set_tid(tid);
    request.set_pid(pid);
    for (size_t i = 0; i < dimensions.size(); i++) {
        ::openmldb::api::Dimension* d = request.add_dimensions();
        d->set_key(dimensions[i].first);
        d->set_idx(dimensions[i].second);
    }
    ::openmldb::api::PutResponse response;
    bool ok =
        client_.SendRequest(&::openmldb::api::TabletServer_Stub::Put, &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    LOG(WARNING) << "fail to send write request for " << response.msg() << " and error code " << response.code();
    return false;
}

bool TabletClient::Put(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time, const std::string& value) {
    ::openmldb::api::PutRequest request;
    auto dim = request.add_dimensions();
    dim->set_key(pk);
    dim->set_idx(0);
    request.set_time(time);
    request.set_value(value);
    request.set_tid(tid);
    request.set_pid(pid);
    ::openmldb::api::PutResponse response;

    bool ok =
        client_.SendRequest(&::openmldb::api::TabletServer_Stub::Put, &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    LOG(WARNING) << "fail to put for error " << response.msg();
    return false;
}

bool TabletClient::MakeSnapshot(uint32_t tid, uint32_t pid, uint64_t offset, std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    if (offset > 0) {
        request.set_offset(offset);
    }
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::MakeSnapshot, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::FollowOfNoOne(uint32_t tid, uint32_t pid, uint64_t term, uint64_t& offset) {
    ::openmldb::api::AppendEntriesRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_term(term);
    ::openmldb::api::AppendEntriesResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::AppendEntries, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        offset = response.log_offset();
        return true;
    }
    return false;
}

bool TabletClient::PauseSnapshot(uint32_t tid, uint32_t pid, std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::PauseSnapshot, &request, &response,
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::RecoverSnapshot(uint32_t tid, uint32_t pid, std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::RecoverSnapshot, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::SendSnapshot(uint32_t tid, uint32_t remote_tid, uint32_t pid, const std::string& endpoint,
                                std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::SendSnapshotRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    request.set_remote_tid(remote_tid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::SendSnapshot, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl, uint32_t seg_cnt) {
    return LoadTable(name, id, pid, ttl, false, seg_cnt);
}

bool TabletClient::LoadTable(const std::string& name, uint32_t tid, uint32_t pid, uint64_t ttl, bool leader,
                             uint32_t seg_cnt, std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name(name);
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_seg_cnt(seg_cnt);
    if (leader) {
        table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    } else {
        table_meta.set_mode(::openmldb::api::TableMode::kTableFollower);
    }
    return LoadTable(table_meta, task_info);
}

bool TabletClient::LoadTable(const ::openmldb::api::TableMeta& table_meta, std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::LoadTableRequest request;
    ::openmldb::api::TableMeta* cur_table_meta = request.mutable_table_meta();
    cur_table_meta->CopyFrom(table_meta);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::LoadTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::LoadTable(uint32_t tid, uint32_t pid, std::string* msg) {
    ::openmldb::api::LoadTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::LoadTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader, uint64_t term) {
    std::vector<std::string> endpoints;
    return ChangeRole(tid, pid, leader, endpoints, term);
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader, const std::vector<std::string>& endpoints,
                              uint64_t term, const std::vector<::openmldb::common::EndpointAndTid>* endpoint_tid) {
    ::openmldb::api::ChangeRoleRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (leader) {
        request.set_mode(::openmldb::api::TableMode::kTableLeader);
        request.set_term(term);
        if ((endpoint_tid != nullptr) && (!endpoint_tid->empty())) {
            for (auto& endpoint : *endpoint_tid) {
                request.add_endpoint_tid()->CopyFrom(endpoint);
            }
        }
    } else {
        request.set_mode(::openmldb::api::TableMode::kTableFollower);
    }
    for (auto iter = endpoints.begin(); iter != endpoints.end(); iter++) {
        request.add_replicas(*iter);
    }
    ::openmldb::api::ChangeRoleResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::ChangeRole, &request, &response,
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::GetTaskStatus(::openmldb::api::TaskStatusResponse& response) {
    ::openmldb::api::TaskStatusRequest request;
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTaskStatus, &request, &response,
                                   FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::UpdateTTL(uint32_t tid, uint32_t pid, const ::openmldb::type::TTLType& type, uint64_t abs_ttl,
                             uint64_t lat_ttl, const std::string& index_name) {
    ::openmldb::api::UpdateTTLRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::openmldb::common::TTLSt* ttl_desc = request.mutable_ttl();
    ttl_desc->set_ttl_type(type);
    ttl_desc->set_abs_ttl(abs_ttl);
    ttl_desc->set_lat_ttl(lat_ttl);
    if (!index_name.empty()) {
        request.set_index_name(index_name);
    }
    ::openmldb::api::UpdateTTLResponse response;
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::UpdateTTL, &request, &response,
                                   FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ret && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Refresh(uint32_t tid) {
    ::openmldb::api::RefreshRequest request;
    request.set_tid(tid);
    ::openmldb::api::GeneralResponse response;
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Refresh, &request, &response,
                                   FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DeleteOPTask(const std::vector<uint64_t>& op_id_vec) {
    ::openmldb::api::DeleteTaskRequest request;
    ::openmldb::api::GeneralResponse response;
    for (auto op_id : op_id_vec) {
        request.add_op_id(op_id);
    }
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DeleteOPTask, &request, &response,
                                   FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetTermPair(uint32_t tid, uint32_t pid, ::openmldb::common::StorageMode storage_mode, uint64_t& term,
                               uint64_t& offset, bool& has_table, bool& is_leader) {
    ::openmldb::api::GetTermPairRequest request;
    ::openmldb::api::GetTermPairResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTermPair, &request, &response,
                                   FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    has_table = response.has_table();
    term = response.term();
    offset = response.offset();
    if (has_table) {
        is_leader = response.is_leader();
    }
    return true;
}

bool TabletClient::GetManifest(uint32_t tid, uint32_t pid, ::openmldb::common::StorageMode storage_mode,
                               ::openmldb::api::Manifest& manifest) {
    ::openmldb::api::GetManifestRequest request;
    ::openmldb::api::GetManifestResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetManifest, &request, &response,
                                   FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    manifest.CopyFrom(response.manifest());
    return true;
}

bool TabletClient::GetTableStatus(::openmldb::api::GetTableStatusResponse& response) {
    ::openmldb::api::GetTableStatusRequest request;
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTableStatus, &request, &response,
                                   FLAGS_request_timeout_ms, 1);
    if (ret) {
        return true;
    }
    return false;
}

bool TabletClient::GetTableStatus(uint32_t tid, uint32_t pid, ::openmldb::api::TableStatus& table_status) {
    return GetTableStatus(tid, pid, false, table_status);
}

bool TabletClient::GetTableStatus(uint32_t tid, uint32_t pid, bool need_schema,
                                  ::openmldb::api::TableStatus& table_status) {
    ::openmldb::api::GetTableStatusRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_need_schema(need_schema);
    ::openmldb::api::GetTableStatusResponse response;
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTableStatus, &request, &response,
                                   FLAGS_request_timeout_ms, 1);
    if (!ret) {
        return false;
    }
    if (response.all_table_status_size() > 0) {
        table_status = response.all_table_status(0);
        return true;
    }
    return false;
}

std::shared_ptr<openmldb::base::ScanKvIterator> TabletClient::Scan(uint32_t tid, uint32_t pid,
        const std::string& pk, const std::string& idx_name,
        uint64_t stime, uint64_t etime, uint32_t limit, uint32_t skip_record_num, std::string& msg) {
    ::openmldb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    request.set_limit(limit);
    request.set_skip_record_num(skip_record_num);
    auto response = std::make_shared<openmldb::api::ScanResponse>();
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Scan, &request, response.get(),
                FLAGS_request_timeout_ms, 1);
    if (response->has_msg()) {
        msg = response->msg();
    }
    if (!ok || response->code() != 0) {
        return {};
    }
    return std::make_shared<::openmldb::base::ScanKvIterator>(pk, response);
}

std::shared_ptr<openmldb::base::ScanKvIterator> TabletClient::Scan(uint32_t tid, uint32_t pid,
        const std::string& pk, const std::string& idx_name,
        uint64_t stime, uint64_t etime, uint32_t limit, std::string& msg) {
    return Scan(tid, pid, pk, idx_name, stime, etime, limit, 0, msg);
}

bool TabletClient::GetTableSchema(uint32_t tid, uint32_t pid, ::openmldb::api::TableMeta& table_meta) {
    ::openmldb::api::GetTableSchemaRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::openmldb::api::GetTableSchemaResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTableSchema, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        table_meta.CopyFrom(response.table_meta());
        return true;
    }
    return false;
}

bool TabletClient::DropTable(uint32_t id, uint32_t pid, std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::DropTableRequest request;
    request.set_tid(id);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::openmldb::api::DropTableResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DropTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
                              std::shared_ptr<TaskInfo> task_info) {
    return AddReplica(tid, pid, endpoint, INVALID_REMOTE_TID, task_info);
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint, uint32_t remote_tid,
                              std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::ReplicaRequest request;
    ::openmldb::api::AddReplicaResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (remote_tid != INVALID_REMOTE_TID) {
        request.set_remote_tid(remote_tid);
    }
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::AddReplica, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
                              std::shared_ptr<TaskInfo> task_info) {
    if (task_info) {
        // fix the bug FEX-439
        ::openmldb::api::GetTableFollowerRequest get_follower_request;
        ::openmldb::api::GetTableFollowerResponse get_follower_response;
        get_follower_request.set_tid(tid);
        get_follower_request.set_pid(pid);
        bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTableFollower, &get_follower_request,
                                      &get_follower_response, FLAGS_request_timeout_ms, 1);
        if (ok) {
            if (get_follower_response.code() < 0 && get_follower_response.msg() == "has no follower") {
                task_info->set_status(::openmldb::api::TaskStatus::kDone);
                PDLOG(INFO,
                      "update task status from[kDoing] to[kDone]. op_id[%lu], "
                      "task_type[%s]",
                      task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
                return true;
            }
            if (get_follower_response.code() == 0) {
                bool has_replica = false;
                for (int idx = 0; idx < get_follower_response.follower_info_size(); idx++) {
                    if (get_follower_response.follower_info(idx).endpoint() == endpoint) {
                        has_replica = true;
                    }
                }
                if (!has_replica) {
                    task_info->set_status(::openmldb::api::TaskStatus::kDone);
                    PDLOG(INFO,
                          "update task status from[kDoing] to[kDone]. "
                          "op_id[%lu], task_type[%s]",
                          task_info->op_id(), ::openmldb::api::TaskType_Name(task_info->task_type()).c_str());
                    return true;
                }
            }
        }
    }
    ::openmldb::api::ReplicaRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DelReplica, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::SetExpire(uint32_t tid, uint32_t pid, bool is_expire) {
    ::openmldb::api::SetExpireRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_is_expire(is_expire);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::SetExpire, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetTableFollower(uint32_t tid, uint32_t pid, uint64_t& offset,
                                    std::map<std::string, uint64_t>& info_map, std::string& msg) {
    ::openmldb::api::GetTableFollowerRequest request;
    ::openmldb::api::GetTableFollowerResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetTableFollower, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code() != 0) {
        return false;
    }
    for (int idx = 0; idx < response.follower_info_size(); idx++) {
        info_map.insert(std::make_pair(response.follower_info(idx).endpoint(), response.follower_info(idx).offset()));
    }
    offset = response.offset();
    return true;
}

bool TabletClient::Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time, std::string& value,
                       uint64_t& ts, std::string& msg) {
    ::openmldb::api::GetRequest request;
    ::openmldb::api::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_ts(time);
    bool ok =
        client_.SendRequest(&::openmldb::api::TabletServer_Stub::Get, &request, &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code() != 0) {
        return false;
    }
    ts = response.ts();
    value.assign(response.value());
    return true;
}

bool TabletClient::Count(uint32_t tid, uint32_t pid, const std::string& pk, const std::string& idx_name,
                         bool filter_expired_data, uint64_t& value, std::string& msg) {
    ::openmldb::api::CountRequest request;
    ::openmldb::api::CountResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_filter_expired_data(filter_expired_data);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Count, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code() != 0) {
        return false;
    }
    value = response.count();
    return true;
}

bool TabletClient::Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time, const std::string& idx_name,
                       std::string& value, uint64_t& ts, std::string& msg) {
    ::openmldb::api::GetRequest request;
    ::openmldb::api::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_ts(time);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    bool ok =
        client_.SendRequest(&::openmldb::api::TabletServer_Stub::Get, &request, &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    value.swap(*response.mutable_value());
    if (!ok || response.code() != 0) {
        return false;
    }
    ts = response.ts();
    return true;
}

bool TabletClient::Delete(uint32_t tid, uint32_t pid, const std::string& pk, const std::string& idx_name,
                          std::string& msg) {
    ::openmldb::api::DeleteRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Delete, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || (response.code() != 0 && response.code() != ::openmldb::base::ReturnCode::kDeleteFailed)) {
        return false;
    }
    return true;
}

base::Status TabletClient::Delete(uint32_t tid, uint32_t pid, const std::map<uint32_t, std::string>& index_val,
        const std::string& ts_name, const std::optional<uint64_t> start_ts, const std::optional<uint64_t>& end_ts) {
    ::openmldb::api::DeleteRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    for (const auto& kv : index_val) {
        auto dimension = request.add_dimensions();
        dimension->set_idx(kv.first);
        dimension->set_key(kv.second);
    }
    if (start_ts.has_value()) {
        request.set_ts(start_ts.value());
    }
    if (end_ts.has_value()) {
        request.set_end_ts(end_ts.value());
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Delete, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return {base::ReturnCode::kError, response.msg()};
    }
    return {};
}

bool TabletClient::ConnectZK() {
    ::openmldb::api::ConnectZKRequest request;
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::ConnectZK, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DisConnectZK() {
    ::openmldb::api::DisConnectZKRequest request;
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DisConnectZK, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DeleteBinlog(uint32_t tid, uint32_t pid, openmldb::common::StorageMode storage_mode) {
    ::openmldb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    ::openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DeleteBinlog, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

std::shared_ptr<openmldb::base::TraverseKvIterator> TabletClient::Traverse(uint32_t tid, uint32_t pid,
        const std::string& idx_name, const std::string& pk, uint64_t ts, uint32_t limit, bool skip_current_pk,
        uint32_t ts_pos, uint32_t& count) {
    ::openmldb::api::TraverseRequest request;
    auto response = std::make_shared<openmldb::api::TraverseResponse>();
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_limit(limit);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    if (!pk.empty()) {
        request.set_pk(pk);
        request.set_ts(ts);
        request.set_ts_pos(ts_pos);
    }
    request.set_skip_current_pk(skip_current_pk);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Traverse, &request, response.get(),
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response->code() != 0) {
        return {};
    }
    count = response->count();
    return std::make_shared<openmldb::base::TraverseKvIterator>(response);
}

bool TabletClient::SetMode(bool mode) {
    ::openmldb::api::SetModeRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_follower(mode);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::SetMode, &request, &response,
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetAllSnapshotOffset(std::map<uint32_t, std::map<uint32_t, uint64_t>>& tid_pid_offset) {
    ::openmldb::api::EmptyRequest request;
    ::openmldb::api::TableSnapshotOffsetResponse response;
    bool ok = client_.SendRequest(&openmldb::api::TabletServer_Stub::GetAllSnapshotOffset, &request, &response,
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok) {
        return false;
    }
    if (response.tables_size() < 1) {
        return true;
    }

    for (auto table : response.tables()) {
        uint32_t tid = table.tid();
        std::map<uint32_t, uint64_t> pid_offset;
        for (auto part : table.parts()) {
            pid_offset.insert(std::make_pair(part.pid(), part.offset()));
        }
        tid_pid_offset.insert(std::make_pair(tid, pid_offset));
    }
    return true;
}

bool TabletClient::DeleteIndex(uint32_t tid, uint32_t pid, const std::string& idx_name, std::string* msg) {
    ::openmldb::api::DeleteIndexRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_idx_name(idx_name);
    bool ok = client_.SendRequest(&openmldb::api::TabletServer_Stub::DeleteIndex, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
        *msg = response.msg();
    }
    return true;
}

bool TabletClient::AddIndex(uint32_t tid, uint32_t pid, const ::openmldb::common::ColumnKey& column_key,
                            std::shared_ptr<TaskInfo> task_info) {
    return AddMultiIndex(tid, pid, {column_key}, task_info);
}

bool TabletClient::AddMultiIndex(uint32_t tid, uint32_t pid,
        const std::vector<::openmldb::common::ColumnKey>& column_keys,
        std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::AddIndexRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    if (column_keys.empty()) {
        if (task_info) {
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        }
        return false;
    } else if (column_keys.size() == 1) {
        request.mutable_column_key()->CopyFrom(column_keys[0]);
    } else {
        for (const auto& column_key : column_keys) {
            request.add_column_keys()->CopyFrom(column_key);
        }
    }
    bool ok = client_.SendRequest(&openmldb::api::TabletServer_Stub::AddIndex, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        if (task_info) {
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        }
        return false;
    }
    if (task_info) {
        task_info->set_status(::openmldb::api::TaskStatus::kDone);
    }
    return true;
}

bool TabletClient::SendIndexData(uint32_t tid, uint32_t pid, const std::map<uint32_t, std::string>& pid_endpoint_map,
                                 std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::SendIndexDataRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    for (const auto& kv : pid_endpoint_map) {
        auto pair = request.add_pairs();
        pair->set_pid(kv.first);
        pair->set_endpoint(kv.second);
    }
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&openmldb::api::TabletServer_Stub::SendIndexData, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::LoadIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num,
                                 std::shared_ptr<TaskInfo> task_info) {
    ::openmldb::api::LoadIndexDataRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_partition_num(partition_num);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&openmldb::api::TabletServer_Stub::LoadIndexData, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::ExtractIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num,
                                    const std::vector<::openmldb::common::ColumnKey>& column_key,
                                    uint64_t offset, bool dump_data,
                                    std::shared_ptr<TaskInfo> task_info) {
    if (column_key.empty()) {
        if (task_info) {
            task_info->set_status(::openmldb::api::TaskStatus::kFailed);
        }
        return false;
    }
    ::openmldb::api::ExtractIndexDataRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_partition_num(partition_num);
    request.set_offset(offset);
    request.set_dump_data(dump_data);
    for (const auto& cur_column_key : column_key) {
        request.add_column_key()->CopyFrom(cur_column_key);
    }
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&openmldb::api::TabletServer_Stub::ExtractIndexData, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::CancelOP(const uint64_t op_id) {
    ::openmldb::api::CancelOPRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_op_id(op_id);
    bool ret = client_.SendRequest(&::openmldb::api::TabletServer_Stub::CancelOP, &request, &response,
                                   FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetCatalog(uint64_t* version) {
    if (version == nullptr) {
        return false;
    }
    ::openmldb::api::GetCatalogRequest request;
    ::openmldb::api::GetCatalogResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetCatalog, &request, &response,
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    *version = response.catalog().version();
    return true;
}

bool TabletClient::UpdateRealEndpointMap(const std::map<std::string, std::string>& map) {
    ::openmldb::api::UpdateRealEndpointMapRequest request;
    ::openmldb::api::GeneralResponse response;
    for (std::map<std::string, std::string>::const_iterator it = map.cbegin(); it != map.cend(); ++it) {
        ::openmldb::api::RealEndpointPair* pair = request.add_real_endpoint_map();
        pair->set_name(it->first);
        pair->set_real_endpoint(it->second);
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::UpdateRealEndpointMap, &request, &response,
                                  FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

base::Status TabletClient::CreateProcedure(const openmldb::api::CreateProcedureRequest& sp_request) {
    openmldb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::CreateProcedure, &sp_request, &response,
                                  sp_request.timeout_ms(), FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return {base::ReturnCode::kError, response.msg()};
    }
    return {};
}

bool TabletClient::AsyncScan(const ::openmldb::api::ScanRequest& request,
                             openmldb::RpcCallback<openmldb::api::ScanResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    return client_.SendRequest(&::openmldb::api::TabletServer_Stub::Scan, callback->GetController().get(), &request,
                               callback->GetResponse().get(), callback);
}

bool TabletClient::Scan(const ::openmldb::api::ScanRequest& request, brpc::Controller* cntl,
                        ::openmldb::api::ScanResponse* response) {
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Scan, cntl, &request, response);
    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to scan table with tid " << request.tid();
        return false;
    }
    return true;
}

bool TabletClient::CallProcedure(const std::string& db, const std::string& sp_name, const base::Slice& row,
                                 brpc::Controller* cntl, openmldb::api::QueryResponse* response, bool is_debug,
                                 uint64_t timeout_ms) {
    if (cntl == NULL || response == NULL) return false;
    ::openmldb::api::QueryRequest request;
    request.set_sp_name(sp_name);
    request.set_db(db);
    request.set_is_debug(is_debug);
    request.set_is_batch(false);
    request.set_is_procedure(true);
    request.set_row_size(row.size());
    request.set_row_slices(1);
    cntl->set_timeout_ms(timeout_ms);
    auto& io_buf = cntl->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(row.data()), row.size(), &io_buf)) {
        LOG(WARNING) << "encode row buf failed";
        return false;
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::Query, cntl, &request, response);
    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool TabletClient::SubQuery(const ::openmldb::api::QueryRequest& request,
                            openmldb::RpcCallback<openmldb::api::QueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    return client_.SendRequest(&::openmldb::api::TabletServer_Stub::SubQuery, callback->GetController().get(), &request,
                               callback->GetResponse().get(), callback);
}
bool TabletClient::SubBatchRequestQuery(const ::openmldb::api::SQLBatchRequestQueryRequest& request,
                                        openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    return client_.SendRequest(&::openmldb::api::TabletServer_Stub::SQLBatchRequestQuery,
                               callback->GetController().get(), &request, callback->GetResponse().get(), callback);
}

bool TabletClient::CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
                                                std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch> row_batch,
                                                brpc::Controller* cntl,
                                                openmldb::api::SQLBatchRequestQueryResponse* response, bool is_debug,
                                                uint64_t timeout_ms) {
    if (cntl == NULL || response == NULL) {
        return false;
    }
    ::openmldb::api::SQLBatchRequestQueryRequest request;
    request.set_sp_name(sp_name);
    request.set_is_procedure(true);
    request.set_db(db);
    request.set_is_debug(is_debug);
    cntl->set_timeout_ms(timeout_ms);

    auto& io_buf = cntl->request_attachment();
    if (!EncodeRowBatch(row_batch, &request, &io_buf)) {
        return false;
    }

    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::SQLBatchRequestQuery, cntl, &request, response);
    if (!ok || response->code() != ::openmldb::base::kOk) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool static ParseBatchRequestMeta(const base::Slice& meta, const base::Slice& data,
        ::openmldb::api::SQLBatchRequestQueryRequest* request) {
    uint64_t total_len = 0;
    const int32_t* buf = reinterpret_cast<const int32_t*>(meta.data());
    int32_t cnt = meta.size() / sizeof(int32_t);
    for (int32_t idx = 0; idx < cnt; idx++) {
        // the first field is for common_slice
        if (idx == 0) {
            if (buf[idx] == 0) {
                request->set_common_slices(0);
            } else {
                request->set_common_slices(1);
                request->add_row_sizes(buf[idx]);
            }
        } else {
            request->add_row_sizes(buf[idx]);
        }
        total_len += buf[idx];
    }
    if (total_len != data.size()) {
        return false;
    }
    return true;
}

base::Status TabletClient::CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
        const base::Slice& meta, const base::Slice& data,
        bool is_debug, uint64_t timeout_ms,
        brpc::Controller* cntl, openmldb::api::SQLBatchRequestQueryResponse* response) {
    ::openmldb::api::SQLBatchRequestQueryRequest request;
    request.set_sp_name(sp_name);
    request.set_is_procedure(true);
    request.set_db(db);
    request.set_is_debug(is_debug);
    request.set_common_slices(0);
    request.set_non_common_slices(1);
    cntl->set_timeout_ms(timeout_ms);
    if (!ParseBatchRequestMeta(meta, data, &request)) {
        return {base::ReturnCode::kError, "parse meta data failed"};
    }
    auto& io_buf = cntl->request_attachment();
    if (io_buf.append(data.data(), data.size()) != 0) {
        return {base::ReturnCode::kError, "append to iobuf error"};
    }
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::SQLBatchRequestQuery, cntl, &request, response);
    if (!ok || response->code() != ::openmldb::base::kOk) {
        LOG(WARNING) << "fail to query tablet";
        return {base::ReturnCode::kError, "fail to query tablet. " + response->msg()};
    }
    return {};
}

base::Status TabletClient::CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
        const base::Slice& meta, const base::Slice& data,
        bool is_debug, uint64_t timeout_ms,
        openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback) {
    if (callback == nullptr) {
        return {base::ReturnCode::kError, "callback is null"};
    }
    ::openmldb::api::SQLBatchRequestQueryRequest request;
    request.set_sp_name(sp_name);
    request.set_is_procedure(true);
    request.set_db(db);
    request.set_is_debug(is_debug);
    request.set_common_slices(0);
    request.set_non_common_slices(1);
    if (!ParseBatchRequestMeta(meta, data, &request)) {
        return {base::ReturnCode::kError, "parse meta data failed"};
    }
    auto& io_buf = callback->GetController()->request_attachment();
    if (io_buf.append(data.data(), data.size()) != 0) {
        return {base::ReturnCode::kError, "append to iobuf error"};
    }
    callback->GetController()->set_timeout_ms(timeout_ms);
    if (!client_.SendRequest(&::openmldb::api::TabletServer_Stub::SQLBatchRequestQuery,
                               callback->GetController().get(), &request, callback->GetResponse().get(), callback)) {
        return {base::ReturnCode::kError, "stub is null"};
    }
    return {};
}

bool TabletClient::DropProcedure(const std::string& db_name, const std::string& sp_name) {
    ::openmldb::api::DropProcedureRequest request;
    ::openmldb::api::GeneralResponse response;
    request.set_db_name(db_name);
    request.set_sp_name(sp_name);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DropProcedure, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::CallProcedure(const std::string& db, const std::string& sp_name, const base::Slice& row,
                                 uint64_t timeout_ms, bool is_debug,
                                 openmldb::RpcCallback<openmldb::api::QueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    ::openmldb::api::QueryRequest request;
    request.set_db(db);
    request.set_sp_name(sp_name);
    request.set_is_debug(is_debug);
    request.set_is_batch(false);
    request.set_is_procedure(true);
    request.set_row_size(row.size());
    request.set_row_slices(1);
    auto& io_buf = callback->GetController()->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(row.data()), row.size(), &io_buf)) {
        LOG(WARNING) << "Encode row buf failed";
        return false;
    }
    callback->GetController()->set_timeout_ms(timeout_ms);
    return client_.SendRequest(&::openmldb::api::TabletServer_Stub::Query, callback->GetController().get(), &request,
                               callback->GetResponse().get(), callback);
}

bool TabletClient::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch> row_batch,
    bool is_debug, uint64_t timeout_ms, openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    ::openmldb::api::SQLBatchRequestQueryRequest request;
    request.set_sp_name(sp_name);
    request.set_is_procedure(true);
    request.set_db(db);
    request.set_is_debug(is_debug);

    auto& io_buf = callback->GetController()->request_attachment();
    if (!EncodeRowBatch(row_batch, &request, &io_buf)) {
        return false;
    }

    callback->GetController()->set_timeout_ms(timeout_ms);
    return client_.SendRequest(&::openmldb::api::TabletServer_Stub::SQLBatchRequestQuery,
                               callback->GetController().get(), &request, callback->GetResponse().get(), callback);
}

bool TabletClient::CreateFunction(const ::openmldb::common::ExternalFun& fun, std::string* msg) {
    if (msg == nullptr) {
        return false;
    }
    ::openmldb::api::CreateFunctionRequest request;
    ::openmldb::api::CreateFunctionResponse response;
    request.mutable_fun()->CopyFrom(fun);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::CreateFunction, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        *msg = response.msg();
        return false;
    }
    return true;
}

bool TabletClient::DropFunction(const ::openmldb::common::ExternalFun& fun, std::string* msg) {
    if (msg == nullptr) {
        return false;
    }
    ::openmldb::api::DropFunctionRequest request;
    ::openmldb::api::DropFunctionResponse response;
    request.mutable_fun()->CopyFrom(fun);
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::DropFunction, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        *msg = response.msg();
        return false;
    }
    return true;
}

bool TabletClient::CreateAggregator(const ::openmldb::api::TableMeta& base_table_meta,
                          uint32_t aggr_tid, uint32_t aggr_pid, uint32_t index_pos,
                          const ::openmldb::base::LongWindowInfo& window_info) {
    ::openmldb::api::CreateAggregatorRequest request;
    ::openmldb::api::TableMeta* base_meta_ptr = request.mutable_base_table_meta();
    base_meta_ptr->CopyFrom(base_table_meta);
    request.set_aggr_table_tid(aggr_tid);
    request.set_aggr_table_pid(aggr_pid);
    request.set_index_pos(index_pos);
    request.set_aggr_func(window_info.aggr_func_);
    request.set_aggr_col(window_info.aggr_col_);
    request.set_order_by_col(window_info.order_col_);
    request.set_bucket_size(window_info.bucket_size_);
    if (!window_info.filter_col_.empty()) {
        request.set_filter_col(window_info.filter_col_);
    }
    ::openmldb::api::CreateAggregatorResponse response;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::CreateAggregator, &request, &response,
                                  FLAGS_request_timeout_ms * 2, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::GetAndFlushDeployStats(::openmldb::api::DeployStatsResponse* res) {
    ::openmldb::api::GAFDeployStatsRequest req;
    bool ok = client_.SendRequest(&::openmldb::api::TabletServer_Stub::GetAndFlushDeployStats, &req, res,
                               FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    return ok && res->code() == 0;
}

}  // namespace client
}  // namespace openmldb
