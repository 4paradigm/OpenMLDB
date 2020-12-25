//
// tablet_client.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize
// Date 2017-04-02
//

#include "client/tablet_client.h"

#include <algorithm>
#include <iostream>
#include <set>
#include "base/glog_wapper.h"  // NOLINT
#include "brpc/channel.h"
#include "codec/codec.h"
#include "codec/sql_rpc_row_codec.h"
#include "sdk/sql_request_row.h"
#include "timer.h"  // NOLINT

DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(absolute_ttl_max);
DECLARE_bool(enable_show_tp);

namespace rtidb {
namespace client {

TabletClient::TabletClient(const std::string& endpoint,
        const std::string& real_endpoint)
    : endpoint_(endpoint), real_endpoint_(endpoint), client_(endpoint) {
        if (!real_endpoint.empty()) {
            real_endpoint_ = real_endpoint;
            client_ = ::rtidb::RpcClient<
                ::rtidb::api::TabletServer_Stub>(real_endpoint);
        }
    }

TabletClient::TabletClient(const std::string& endpoint,
        const std::string& real_endpoint, bool use_sleep_policy)
    : endpoint_(endpoint), real_endpoint_(endpoint), client_(endpoint, use_sleep_policy) {
        if (!real_endpoint.empty()) {
            real_endpoint_ = real_endpoint;
            client_ = ::rtidb::RpcClient<::rtidb::api::TabletServer_Stub>(
                    real_endpoint, use_sleep_policy);
        }
    }

TabletClient::~TabletClient() {}

int TabletClient::Init() { return client_.Init(); }

std::string TabletClient::GetEndpoint() { return endpoint_; }

const std::string& TabletClient::GetRealEndpoint() const { return real_endpoint_; }

bool TabletClient::CreateTable(
    const std::string& name, uint32_t tid, uint32_t pid, uint64_t abs_ttl,
    uint64_t lat_ttl, uint32_t seg_cnt,
    const std::vector<::rtidb::codec::ColumnDesc>& columns,
    const ::rtidb::api::TTLType& type, bool leader,
    const std::vector<std::string>& endpoints, uint64_t term,
    const ::rtidb::api::CompressType compress_type) {
    std::string schema;
    ::rtidb::codec::SchemaCodec codec;
    bool codec_ok = codec.Encode(columns, schema);
    if (!codec_ok) {
        return false;
    }
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    for (uint32_t i = 0; i < columns.size(); i++) {
        if (columns[i].add_ts_idx) {
            table_meta->add_dimensions(columns[i].name);
        }
    }
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    if (type == ::rtidb::api::kLatestTime) {
        if (lat_ttl > FLAGS_latest_ttl_max) {
            return false;
        }
    } else if (type == ::rtidb::api::TTLType::kAbsoluteTime) {
        if (abs_ttl > FLAGS_absolute_ttl_max) {
            return false;
        }
    } else {
        if (lat_ttl > FLAGS_latest_ttl_max ||
            abs_ttl > FLAGS_absolute_ttl_max) {
            return false;
        }
    }
    table_meta->set_seg_cnt(seg_cnt);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    table_meta->set_schema(schema);
    table_meta->set_ttl_type(type);
    ::rtidb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
    ttl_desc->set_ttl_type(type);
    ttl_desc->set_abs_ttl(abs_ttl);
    ttl_desc->set_lat_ttl(lat_ttl);
    if (type == ::rtidb::api::TTLType::kAbsoluteTime) {
        table_meta->set_ttl(abs_ttl);
    } else {
        table_meta->set_ttl(lat_ttl);
    }
    table_meta->set_compress_type(compress_type);
    if (leader) {
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        table_meta->set_term(term);
        for (size_t i = 0; i < endpoints.size(); i++) {
            table_meta->add_replicas(endpoints[i]);
        }
    } else {
        table_meta->set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    ::rtidb::api::CreateTableResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateTable,
                            &request, &response, FLAGS_request_timeout_ms * 2, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Query(const std::string& db, const std::string& sql,
                         const std::string& row, brpc::Controller* cntl,
                         rtidb::api::QueryResponse* response,
                         const bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::rtidb::api::QueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(false);
    request.set_is_debug(is_debug);
    request.set_row_size(row.size());
    request.set_row_slices(1);
    auto& io_buf = cntl->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(row.data()),
          row.size(), &io_buf)) {
        LOG(WARNING) << "Encode row buffer failed";
        return false;
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Query, cntl,
                                  &request, response);
    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool TabletClient::Query(const std::string& db, const std::string& sql,
                         brpc::Controller* cntl,
                         ::rtidb::api::QueryResponse* response,
                         const bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::rtidb::api::QueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(true);
    request.set_is_debug(is_debug);
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Query, cntl,
                                  &request, response);

    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

/**
 * Utility function to encode row batch data into rpc attachment buffer
 */
static bool EncodeRowBatch(std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch> row_batch,
                           ::rtidb::api::SQLBatchRequestQueryRequest* request,
                           butil::IOBuf* io_buf) {
    auto common_slice = row_batch->GetCommonSlice();
    if (common_slice->empty()) {
        request->set_common_slices(0);
    } else {
        if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(common_slice->data()),
                                 common_slice->size(), io_buf)) {
            LOG(WARNING) << "encode common row buf failed";
            return false;
        }
        request->add_row_sizes(common_slice->size());
        request->set_common_slices(1);
    }
    for (int i = 0; i < row_batch->Size(); ++i) {
        auto non_common_slice = row_batch->GetNonCommonSlice(i);
        if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(non_common_slice->data()),
                                 non_common_slice->size(), io_buf)) {
            LOG(WARNING) << "encode common row buf failed";
            return false;
        }
        request->add_row_sizes(non_common_slice->size());
        request->set_non_common_slices(1);
    }
    return true;
}

bool TabletClient::SQLBatchRequestQuery(const std::string& db, const std::string& sql,
                                        std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch> row_batch,
                                        brpc::Controller* cntl,
                                        ::rtidb::api::SQLBatchRequestQueryResponse* response,
                                        const bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::rtidb::api::SQLBatchRequestQueryRequest request;
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

    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SQLBatchRequestQuery,
                                  cntl, &request, response);
    if (!ok || response->code() != ::rtidb::base::kOk) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool TabletClient::CreateTable(const std::string& name, uint32_t tid,
                               uint32_t pid, uint64_t abs_ttl, uint64_t lat_ttl,
                               bool leader,
                               const std::vector<std::string>& endpoints,
                               const ::rtidb::api::TTLType& type,
                               uint32_t seg_cnt, uint64_t term,
                               const ::rtidb::api::CompressType compress_type) {
    ::rtidb::api::CreateTableRequest request;
    if (type == ::rtidb::api::kLatestTime) {
        if (lat_ttl > FLAGS_latest_ttl_max) {
            return false;
        }
    } else if (type == ::rtidb::api::TTLType::kAbsoluteTime) {
        if (abs_ttl > FLAGS_absolute_ttl_max) {
            return false;
        }
    } else {
        if (abs_ttl > FLAGS_absolute_ttl_max ||
            lat_ttl > FLAGS_latest_ttl_max) {
            return false;
        }
    }
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    ::rtidb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
    ttl_desc->set_ttl_type(type);
    ttl_desc->set_abs_ttl(abs_ttl);
    ttl_desc->set_lat_ttl(lat_ttl);
    table_meta->set_compress_type(compress_type);
    table_meta->set_seg_cnt(seg_cnt);
    if (leader) {
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        table_meta->set_term(term);
    } else {
        table_meta->set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (size_t i = 0; i < endpoints.size(); i++) {
        table_meta->add_replicas(endpoints[i]);
    }
    // table_meta->set_ttl_type(type);
    ::rtidb::api::CreateTableResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateTable,
                            &request, &response, FLAGS_request_timeout_ms * 2, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::CreateTable(const ::rtidb::api::TableMeta& table_meta) {
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta_ptr = request.mutable_table_meta();
    table_meta_ptr->CopyFrom(table_meta);
    ::rtidb::api::CreateTableResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateTable,
                            &request, &response, FLAGS_request_timeout_ms * 2, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::UpdateTableMetaForAddField(
    uint32_t tid, const std::vector<rtidb::common::ColumnDesc>& cols,
    const rtidb::common::VersionPair& pair, const std::string& schema, std::string& msg) {
    ::rtidb::api::UpdateTableMetaForAddFieldRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    for (const auto& col : cols) {
        ::rtidb::common::ColumnDesc* column_desc_ptr = request.add_column_descs();
        column_desc_ptr->CopyFrom(col);
    }
    request.set_schema(schema);
    rtidb::common::VersionPair* new_pair = request.mutable_version_pair();
    new_pair->CopyFrom(pair);
    bool ok = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::UpdateTableMetaForAddField, &request,
        &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    msg = response.msg();
    return false;
}

bool TabletClient::Update(
    uint32_t tid, uint32_t pid,
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns>&
        cd_columns,
    const Schema& new_value_schema, const std::string& value, uint32_t* count,
    std::string* msg) {
    ::rtidb::api::UpdateRequest request;
    ::rtidb::api::UpdateResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns>*
        cd_columns_ptr = request.mutable_condition_columns();
    cd_columns_ptr->CopyFrom(cd_columns);
    ::rtidb::api::Columns* val = request.mutable_value_columns();
    for (int i = 0; i < new_value_schema.size(); i++) {
        val->add_name(new_value_schema.Get(i).name());
    }
    val->set_allocated_value(const_cast<std::string*>(&value));
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Update, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    val->release_value();
    if (ok && response.code() == 0) {
        *count = response.count();
        return true;
    }
    *msg = response.msg();
    return false;
}

bool TabletClient::Put(uint32_t tid, uint32_t pid, const std::string& value,
                       const ::rtidb::api::WriteOption& wo,
                       int64_t* auto_gen_pk, std::vector<int64_t>* blob_keys,
                       std::string* msg) {
    ::rtidb::api::PutRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_allocated_value(const_cast<std::string*>(&value));
    ::rtidb::api::WriteOption* wo_ptr = request.mutable_wo();
    wo_ptr->CopyFrom(wo);
    ::rtidb::api::PutResponse response;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    request.release_value();
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        *auto_gen_pk = response.auto_gen_pk();
        if (blob_keys != nullptr) {
            for (int64_t key : response.blob_keys()) {
                blob_keys->push_back(key);
            }
        }
        return true;
    }
    return false;
}

bool TabletClient::Put(
    uint32_t tid, uint32_t pid, uint64_t time, const std::string& value,
    const std::vector<std::pair<std::string, uint32_t>>& dimensions) {
    return Put(tid, pid, time, value, dimensions, 0);
}

bool TabletClient::Put(
    uint32_t tid, uint32_t pid, uint64_t time, const std::string& value,
    const std::vector<std::pair<std::string, uint32_t>>& dimensions,
    uint32_t format_version) {
    ::rtidb::api::PutRequest request;
    request.set_time(time);
    request.set_value(value);
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_format_version(format_version);
    for (size_t i = 0; i < dimensions.size(); i++) {
        ::rtidb::api::Dimension* d = request.add_dimensions();
        d->set_key(dimensions[i].first);
        d->set_idx(dimensions[i].second);
    }
    ::rtidb::api::PutResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    LOG(WARNING) << "fail to send write request for " << response.msg() << " and error code " << response.code();
    return false;
}
bool TabletClient::Put(
    uint32_t tid, uint32_t pid,
    const std::vector<std::pair<std::string, uint32_t>>& dimensions,
    const std::vector<uint64_t>& ts_dimensions, const std::string& value,
    uint32_t format_version) {
    ::rtidb::api::PutRequest request;
    request.set_value(value);
    request.set_tid(tid);
    request.set_pid(pid);
    for (size_t i = 0; i < dimensions.size(); i++) {
        ::rtidb::api::Dimension* d = request.add_dimensions();
        d->set_key(dimensions[i].first);
        d->set_idx(dimensions[i].second);
    }
    for (size_t i = 0; i < ts_dimensions.size(); i++) {
        ::rtidb::api::TSDimension* d = request.add_ts_dimensions();
        d->set_ts(ts_dimensions[i]);
        d->set_idx(i);
    }
    request.set_format_version(format_version);
    ::rtidb::api::PutResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    LOG(WARNING) << "put row to table " << tid << " failed with error "
               << response.msg() << " and error code " << response.code();
    return false;
}

bool TabletClient::Put(
    uint32_t tid, uint32_t pid,
    const std::vector<std::pair<std::string, uint32_t>>& dimensions,
    const std::vector<uint64_t>& ts_dimensions, const std::string& value) {
    return Put(tid, pid, dimensions, ts_dimensions, value, 0);
}

bool TabletClient::Put(uint32_t tid, uint32_t pid, const char* pk,
                       uint64_t time, const char* value, uint32_t size,
                       uint32_t format_version) {
    ::rtidb::api::PutRequest request;
    request.set_pk(pk);
    request.set_time(time);
    request.set_value(value, size);
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_format_version(format_version);
    ::rtidb::api::PutResponse response;

    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    LOG(WARNING) << "fail to put for error " << response.msg();
    return false;
}

bool TabletClient::Put(uint32_t tid, uint32_t pid, const std::string& pk,
                       uint64_t time, const std::string& value,
                       uint32_t format_version) {
    return Put(tid, pid, pk.c_str(), time, value.c_str(), value.size(),
               format_version);
}

bool TabletClient::MakeSnapshot(uint32_t tid, uint32_t pid, uint64_t offset,
                                std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    if (offset > 0) {
        request.set_offset(offset);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::MakeSnapshot,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::FollowOfNoOne(uint32_t tid, uint32_t pid, uint64_t term,
                                 uint64_t& offset) {
    ::rtidb::api::AppendEntriesRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_term(term);
    ::rtidb::api::AppendEntriesResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::AppendEntries,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        offset = response.log_offset();
        return true;
    }
    return false;
}

bool TabletClient::PauseSnapshot(uint32_t tid, uint32_t pid,
                                 std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::PauseSnapshot, &request, &response,
        FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::RecoverSnapshot(uint32_t tid, uint32_t pid,
                                   std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::RecoverSnapshot,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::SendSnapshot(uint32_t tid, uint32_t remote_tid, uint32_t pid,
                                const std::string& endpoint,
                                std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::SendSnapshotRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    request.set_remote_tid(remote_tid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::SendSnapshot,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::LoadTable(const std::string& name, uint32_t id, uint32_t pid,
                             uint64_t ttl, uint32_t seg_cnt) {
    return LoadTable(name, id, pid, ttl, false, seg_cnt,
                     ::rtidb::common::StorageMode::kMemory);
}

bool TabletClient::LoadTable(const std::string& name, uint32_t tid,
                             uint32_t pid, uint64_t ttl, bool leader,
                             uint32_t seg_cnt,
                             ::rtidb::common::StorageMode storage_mode,
                             std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_name(name);
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_ttl(ttl);
    table_meta.set_seg_cnt(seg_cnt);
    table_meta.set_storage_mode(storage_mode);
    if (leader) {
        table_meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    } else {
        table_meta.set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    return LoadTable(table_meta, task_info);
}

bool TabletClient::LoadTable(const ::rtidb::api::TableMeta& table_meta,
                             std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::LoadTableRequest request;
    ::rtidb::api::TableMeta* cur_table_meta = request.mutable_table_meta();
    cur_table_meta->CopyFrom(table_meta);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::LoadTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::LoadTable(uint32_t tid, uint32_t pid,
                             ::rtidb::common::StorageMode storage_mode,
                             std::string* msg) {
    ::rtidb::api::LoadTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_table_type(::rtidb::type::kRelational);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_storage_mode(storage_mode);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::LoadTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader,
                              uint64_t term) {
    std::vector<std::string> endpoints;
    return ChangeRole(tid, pid, leader, endpoints, term);
}

bool TabletClient::ChangeRole(
    uint32_t tid, uint32_t pid, bool leader,
    const std::vector<std::string>& endpoints, uint64_t term,
    const std::vector<::rtidb::common::EndpointAndTid>* endpoint_tid) {
    ::rtidb::api::ChangeRoleRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (leader) {
        request.set_mode(::rtidb::api::TableMode::kTableLeader);
        request.set_term(term);
        if ((endpoint_tid != nullptr) && (!endpoint_tid->empty())) {
            for (auto& endpoint : *endpoint_tid) {
                request.add_endpoint_tid()->CopyFrom(endpoint);
            }
        }
    } else {
        request.set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (auto iter = endpoints.begin(); iter != endpoints.end(); iter++) {
        request.add_replicas(*iter);
    }
    ::rtidb::api::ChangeRoleResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::ChangeRole,
                                  &request, &response, FLAGS_request_timeout_ms,
                                  FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::SetMaxConcurrency(const std::string& key,
                                     int32_t max_concurrency) {
    ::rtidb::api::SetConcurrencyRequest request;
    request.set_key(key);
    request.set_max_concurrency(max_concurrency);
    ::rtidb::api::SetConcurrencyResponse response;
    bool ret =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetConcurrency,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        std::cout << response.msg() << std::endl;
        return false;
    }
    return true;
}

bool TabletClient::GetTaskStatus(::rtidb::api::TaskStatusResponse& response) {
    ::rtidb::api::TaskStatusRequest request;
    bool ret =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTaskStatus,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::UpdateTTL(uint32_t tid, uint32_t pid,
                             const ::rtidb::api::TTLType& type,
                             uint64_t abs_ttl, uint64_t lat_ttl,
                             const std::string& ts_name) {
    ::rtidb::api::UpdateTTLRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_type(type);
    if (type == ::rtidb::api::TTLType::kLatestTime) {
        request.set_value(lat_ttl);
    } else {
        request.set_value(abs_ttl);
    }
    ::rtidb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
    ttl_desc->set_ttl_type(type);
    ttl_desc->set_abs_ttl(abs_ttl);
    ttl_desc->set_lat_ttl(lat_ttl);
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    ::rtidb::api::UpdateTTLResponse response;
    bool ret = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::UpdateTTL, &request, &response,
        FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ret && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::DeleteOPTask(const std::vector<uint64_t>& op_id_vec) {
    ::rtidb::api::DeleteTaskRequest request;
    ::rtidb::api::GeneralResponse response;
    for (auto op_id : op_id_vec) {
        request.add_op_id(op_id);
    }
    bool ret = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::DeleteOPTask, &request, &response,
        FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetTermPair(uint32_t tid, uint32_t pid,
                               ::rtidb::common::StorageMode storage_mode,
                               uint64_t& term, uint64_t& offset,
                               bool& has_table, bool& is_leader) {
    ::rtidb::api::GetTermPairRequest request;
    ::rtidb::api::GetTermPairResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    bool ret = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::GetTermPair, &request, &response,
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

bool TabletClient::GetManifest(uint32_t tid, uint32_t pid,
                               ::rtidb::common::StorageMode storage_mode,
                               ::rtidb::api::Manifest& manifest) {
    ::rtidb::api::GetManifestRequest request;
    ::rtidb::api::GetManifestResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    bool ret = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::GetManifest, &request, &response,
        FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    manifest.CopyFrom(response.manifest());
    return true;
}

bool TabletClient::GetTableStatus(
    ::rtidb::api::GetTableStatusResponse& response) {
    ::rtidb::api::GetTableStatusRequest request;
    bool ret =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableStatus,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ret) {
        return true;
    }
    return false;
}

bool TabletClient::GetTableStatus(uint32_t tid, uint32_t pid,
                                  ::rtidb::api::TableStatus& table_status) {
    return GetTableStatus(tid, pid, false, table_status);
}

bool TabletClient::GetTableStatus(uint32_t tid, uint32_t pid, bool need_schema,
                                  ::rtidb::api::TableStatus& table_status) {
    ::rtidb::api::GetTableStatusRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_need_schema(need_schema);
    ::rtidb::api::GetTableStatusResponse response;
    bool ret =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableStatus,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ret) {
        return false;
    }
    if (response.all_table_status_size() > 0) {
        table_status = response.all_table_status(0);
        return true;
    }
    return false;
}

::rtidb::base::KvIterator* TabletClient::Scan(
    uint32_t tid, uint32_t pid, const std::string& pk, uint64_t stime,
    uint64_t etime, const std::string& idx_name, const std::string& ts_name,
    uint32_t limit, uint32_t atleast, std::string& msg) {
    if (limit != 0 && atleast > limit) {
        msg = "atleast should be no greater than limit";
        return NULL;
    }
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_atleast(atleast);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    request.set_limit(limit);
    ::rtidb::api::ScanResponse* response = new ::rtidb::api::ScanResponse();
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan, &request,
                            response, FLAGS_request_timeout_ms, 1);
    response->mutable_metric()->set_rptime(
        ::baidu::common::timer::get_micros());
    if (response->has_msg()) {
        msg = response->msg();
    }
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    return kv_it;
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid, uint32_t pid,
                                              const std::string& pk,
                                              uint64_t stime, uint64_t etime,
                                              const std::string& idx_name,
                                              uint32_t limit, uint32_t atleast,
                                              std::string& msg) {
    return Scan(tid, pid, pk, stime, etime, idx_name, "", limit, atleast, msg);
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid, uint32_t pid,
                                              const std::string& pk,
                                              uint64_t stime, uint64_t etime,
                                              uint32_t limit, uint32_t atleast,
                                              std::string& msg) {
    if (limit != 0 && atleast > limit) {
        msg = "atleast should be no greater than limit";
        return NULL;
    }
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_limit(limit);
    request.set_atleast(atleast);
    request.mutable_metric()->set_sqtime(::baidu::common::timer::get_micros());
    ::rtidb::api::ScanResponse* response = new ::rtidb::api::ScanResponse();
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan, &request,
                            response, FLAGS_request_timeout_ms, 1);
    response->mutable_metric()->set_rptime(
        ::baidu::common::timer::get_micros());
    if (response->has_msg()) {
        msg = response->msg();
    }
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    return kv_it;
}

bool TabletClient::GetTableSchema(uint32_t tid, uint32_t pid,
                                  ::rtidb::api::TableMeta& table_meta) {
    ::rtidb::api::GetTableSchemaRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::GetTableSchemaResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableSchema,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        table_meta.CopyFrom(response.table_meta());
        if (response.has_schema() && response.schema().size() == 0) {
            table_meta.set_schema(response.schema());
        }
        return true;
    }
    return false;
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid, uint32_t pid,
                                              const char* pk, uint64_t stime,
                                              uint64_t etime, std::string& msg,
                                              bool showm) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.mutable_metric()->set_sqtime(::baidu::common::timer::get_micros());
    ::rtidb::api::ScanResponse* response = new ::rtidb::api::ScanResponse();
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan, &request,
                            response, FLAGS_request_timeout_ms, 1);
    response->mutable_metric()->set_rptime(
        ::baidu::common::timer::get_micros());
    if (response->has_msg()) {
        msg = response->msg();
    }
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    if (showm) {
        while (kv_it->Valid()) {
            kv_it->Next();
            kv_it->GetValue().ToString();
        }
    }
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    if (showm) {
        uint64_t rpc_send_time =
            response->metric().rqtime() - response->metric().sqtime();
        uint64_t mutex_time =
            response->metric().sctime() - response->metric().rqtime();
        uint64_t seek_time =
            response->metric().sitime() - response->metric().sctime();
        uint64_t it_time =
            response->metric().setime() - response->metric().sitime();
        uint64_t encode_time =
            response->metric().sptime() - response->metric().setime();
        uint64_t receive_time =
            response->metric().rptime() - response->metric().sptime();
        uint64_t decode_time =
            ::baidu::common::timer::get_micros() - response->metric().rptime();
        std::cout << "Metric: rpc_send=" << rpc_send_time << " "
                  << "db_lock=" << mutex_time << " "
                  << "seek_time=" << seek_time << " "
                  << "iterator_time=" << it_time << " "
                  << "encode=" << encode_time << " "
                  << "receive_time=" << receive_time << " "
                  << "decode_time=" << decode_time << std::endl;
    }
    return kv_it;
}

bool TabletClient::DropTable(uint32_t id, uint32_t pid,
                             std::shared_ptr<TaskInfo> task_info) {
    return DropTable(id, pid, ::rtidb::type::kTimeSeries, task_info);
}

bool TabletClient::DropTable(uint32_t id, uint32_t pid, TableType table_type,
                             std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::DropTableRequest request;
    request.set_tid(id);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    request.set_table_type(table_type);
    ::rtidb::api::DropTableResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::DropTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid,
                              const std::string& endpoint,
                              std::shared_ptr<TaskInfo> task_info) {
    return AddReplica(tid, pid, endpoint, INVALID_REMOTE_TID, task_info);
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid,
                              const std::string& endpoint, uint32_t remote_tid,
                              std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::AddReplicaResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (remote_tid != INVALID_REMOTE_TID) {
        request.set_remote_tid(remote_tid);
    }
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::AddReplica,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DelReplica(uint32_t tid, uint32_t pid,
                              const std::string& endpoint,
                              std::shared_ptr<TaskInfo> task_info) {
    if (task_info) {
        // fix the bug FEX-439
        ::rtidb::api::GetTableFollowerRequest get_follower_request;
        ::rtidb::api::GetTableFollowerResponse get_follower_response;
        get_follower_request.set_tid(tid);
        get_follower_request.set_pid(pid);
        bool ok = client_.SendRequest(
            &::rtidb::api::TabletServer_Stub::GetTableFollower,
            &get_follower_request, &get_follower_response,
            FLAGS_request_timeout_ms, 1);
        if (ok) {
            if (get_follower_response.code() < 0 &&
                get_follower_response.msg() == "has no follower") {
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(INFO,
                      "update task status from[kDoing] to[kDone]. op_id[%lu], "
                      "task_type[%s]",
                      task_info->op_id(),
                      ::rtidb::api::TaskType_Name(task_info->task_type())
                          .c_str());
                return true;
            }
            if (get_follower_response.code() == 0) {
                bool has_replica = false;
                for (int idx = 0;
                     idx < get_follower_response.follower_info_size(); idx++) {
                    if (get_follower_response.follower_info(idx).endpoint() ==
                        endpoint) {
                        has_replica = true;
                    }
                }
                if (!has_replica) {
                    task_info->set_status(::rtidb::api::TaskStatus::kDone);
                    PDLOG(INFO,
                          "update task status from[kDoing] to[kDone]. "
                          "op_id[%lu], task_type[%s]",
                          task_info->op_id(),
                          ::rtidb::api::TaskType_Name(task_info->task_type())
                              .c_str());
                    return true;
                }
            }
        }
    }
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::DelReplica,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::SetExpire(uint32_t tid, uint32_t pid, bool is_expire) {
    ::rtidb::api::SetExpireRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_is_expire(is_expire);
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetExpire,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::SetTTLClock(uint32_t tid, uint32_t pid, uint64_t timestamp) {
    ::rtidb::api::SetTTLClockRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_timestamp(timestamp);
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetTTLClock,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetTableFollower(uint32_t tid, uint32_t pid,
                                    uint64_t& offset,
                                    std::map<std::string, uint64_t>& info_map,
                                    std::string& msg) {
    ::rtidb::api::GetTableFollowerRequest request;
    ::rtidb::api::GetTableFollowerResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableFollower,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code() != 0) {
        return false;
    }
    for (int idx = 0; idx < response.follower_info_size(); idx++) {
        info_map.insert(std::make_pair(response.follower_info(idx).endpoint(),
                                       response.follower_info(idx).offset()));
    }
    offset = response.offset();
    return true;
}

void TabletClient::ShowTp() {
    if (!FLAGS_enable_show_tp) {
        return;
    }
    std::sort(percentile_.begin(), percentile_.end());
    uint32_t size = percentile_.size();
    std::cout << "Percentile:99=" << percentile_[(uint32_t)(size * 0.99)]
              << " ,95=" << percentile_[(uint32_t)(size * 0.95)]
              << " ,90=" << percentile_[(uint32_t)(size * 0.90)]
              << " ,50=" << percentile_[(uint32_t)(size * 0.5)] << std::endl;
    percentile_.clear();
}

bool TabletClient::Get(uint32_t tid, uint32_t pid, const std::string& pk,
                       uint64_t time, std::string& value, uint64_t& ts,
                       std::string& msg) {
    ::rtidb::api::GetRequest request;
    ::rtidb::api::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_ts(time);
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Get, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
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

bool TabletClient::Count(uint32_t tid, uint32_t pid, const std::string& pk,
                         const std::string& idx_name, bool filter_expired_data,
                         uint64_t& value, std::string& msg) {
    return Count(tid, pid, pk, idx_name, "", filter_expired_data, value, msg);
}

bool TabletClient::Count(uint32_t tid, uint32_t pid, const std::string& pk,
                         const std::string& idx_name,
                         const std::string& ts_name, bool filter_expired_data,
                         uint64_t& value, std::string& msg) {
    ::rtidb::api::CountRequest request;
    ::rtidb::api::CountResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_filter_expired_data(filter_expired_data);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Count, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code() != 0) {
        return false;
    }
    value = response.count();
    return true;
}

bool TabletClient::Get(uint32_t tid, uint32_t pid, const std::string& pk,
                       uint64_t time, const std::string& idx_name,
                       std::string& value, uint64_t& ts, std::string& msg) {
    return Get(tid, pid, pk, time, idx_name, "", value, ts, msg);
}

bool TabletClient::Get(uint32_t tid, uint32_t pid, const std::string& pk,
                       uint64_t time, const std::string& idx_name,
                       const std::string& ts_name, std::string& value,
                       uint64_t& ts, std::string& msg) {
    ::rtidb::api::GetRequest request;
    ::rtidb::api::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_ts(time);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Get, &request,
                            &response, FLAGS_request_timeout_ms, 1);
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

bool TabletClient::Delete(uint32_t tid, uint32_t pid, const std::string& pk,
                          const std::string& idx_name, std::string& msg) {
    ::rtidb::api::DeleteRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Delete, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::Delete(uint32_t tid, uint32_t pid,
                          const Cond_Column& cd_columns, uint32_t* count,
                          std::string* msg) {
    return Delete(tid, pid, cd_columns, count, msg, nullptr);
}

bool TabletClient::ConnectZK() {
    ::rtidb::api::ConnectZKRequest request;
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::ConnectZK,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DisConnectZK() {
    ::rtidb::api::DisConnectZKRequest request;
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::DisConnectZK,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DeleteBinlog(uint32_t tid, uint32_t pid,
                                ::rtidb::common::StorageMode storage_mode) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    ::rtidb::api::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::DeleteBinlog,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

::rtidb::base::KvIterator* TabletClient::Traverse(uint32_t tid, uint32_t pid,
                                                  const std::string& idx_name,
                                                  const std::string& pk,
                                                  uint64_t ts, uint32_t limit,
                                                  uint32_t& count) {
    ::rtidb::api::TraverseRequest request;
    ::rtidb::api::TraverseResponse* response =
        new ::rtidb::api::TraverseResponse();
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_limit(limit);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    if (!pk.empty()) {
        request.set_pk(pk);
        request.set_ts(ts);
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Traverse,
                                  &request, response, FLAGS_request_timeout_ms,
                                  FLAGS_request_max_retry);
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    count = response->count();
    return kv_it;
}
bool TabletClient::Traverse(uint32_t tid, uint32_t pid,
                            const ::rtidb::api::ReadOption& ro, uint32_t limit,
                            std::string* pk, uint64_t* snapshot_id,
                            std::string* data, uint32_t* count, bool* is_finish,
                            std::string* msg) {
    rtidb::api::TraverseRequest request;
    rtidb::api::TraverseResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_limit(limit);
    if (*snapshot_id > 0) {
        request.set_snapshot_id(*snapshot_id);
    }
    request.mutable_read_option()->CopyFrom(ro);
    if (!pk->empty()) {
        request.set_pk(*pk);
    }
    bool ok = client_.SendRequest(&rtidb::api::TabletServer_Stub::Traverse,
                                  &request, &response, FLAGS_request_timeout_ms,
                                  FLAGS_request_max_retry);
    data->swap(*response.mutable_pairs());
    msg->swap(*response.mutable_msg());
    pk->swap(*response.mutable_pk());
    if (!ok || response.code() != 0) {
        return false;
    }
    *count = response.count();
    *is_finish = response.is_finish();
    *snapshot_id = response.snapshot_id();
    return true;
}

bool TabletClient::SetMode(bool mode) {
    ::rtidb::api::SetModeRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_follower(mode);
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetMode,
                                  &request, &response, FLAGS_request_timeout_ms,
                                  FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::BatchQuery(
    uint32_t tid, uint32_t pid,
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::ReadOption>& ros,
    std::string* data, uint32_t* count, std::string* msg) {
    rtidb::api::BatchQueryRequest request;
    rtidb::api::BatchQueryResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    ::google::protobuf::RepeatedPtrField<::rtidb::api::ReadOption>* ros_ptr =
        request.mutable_read_option();
    ros_ptr->CopyFrom(ros);
    bool ok = client_.SendRequest(&rtidb::api::TabletServer_Stub::BatchQuery,
                                  &request, &response, FLAGS_request_timeout_ms,
                                  FLAGS_request_max_retry);
    data->swap(*response.mutable_pairs());
    msg->swap(*response.mutable_msg());
    if (!ok || response.code() != 0) {
        return false;
    }
    *count = response.count();
    return true;
}

bool TabletClient::GetAllSnapshotOffset(
    std::map<uint32_t, std::map<uint32_t, uint64_t>>& tid_pid_offset) {
    ::rtidb::api::EmptyRequest request;
    ::rtidb::api::TableSnapshotOffsetResponse response;
    bool ok = client_.SendRequest(
        &rtidb::api::TabletServer_Stub::GetAllSnapshotOffset, &request,
        &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
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

bool TabletClient::DeleteIndex(uint32_t tid, uint32_t pid,
                               const std::string& idx_name, std::string* msg) {
    ::rtidb::api::DeleteIndexRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_idx_name(idx_name);
    bool ok =
        client_.SendRequest(&rtidb::api::TabletServer_Stub::DeleteIndex,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
        *msg = response.msg();
    }
    return true;
}

bool TabletClient::AddIndex(uint32_t tid, uint32_t pid,
                            const ::rtidb::common::ColumnKey& column_key,
                            std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::AddIndexRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::common::ColumnKey* cur_column_key = request.mutable_column_key();
    cur_column_key->CopyFrom(column_key);
    bool ok =
        client_.SendRequest(&rtidb::api::TabletServer_Stub::AddIndex, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return false;
    }
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    return true;
}

bool TabletClient::DumpIndexData(uint32_t tid, uint32_t pid,
                                 uint32_t partition_num,
                                 const ::rtidb::common::ColumnKey& column_key,
                                 uint32_t idx,
                                 std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::DumpIndexDataRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_partition_num(partition_num);
    request.set_idx(idx);
    ::rtidb::common::ColumnKey* cur_column_key = request.mutable_column_key();
    cur_column_key->CopyFrom(column_key);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok =
        client_.SendRequest(&rtidb::api::TabletServer_Stub::DumpIndexData,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::SendIndexData(
    uint32_t tid, uint32_t pid,
    const std::map<uint32_t, std::string>& pid_endpoint_map,
    std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::SendIndexDataRequest request;
    ::rtidb::api::GeneralResponse response;
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
    bool ok =
        client_.SendRequest(&rtidb::api::TabletServer_Stub::SendIndexData,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::LoadIndexData(uint32_t tid, uint32_t pid,
                                 uint32_t partition_num,
                                 std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::LoadIndexDataRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_partition_num(partition_num);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok =
        client_.SendRequest(&rtidb::api::TabletServer_Stub::LoadIndexData,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::ExtractIndexData(
    uint32_t tid, uint32_t pid, uint32_t partition_num,
    const ::rtidb::common::ColumnKey& column_key, uint32_t idx,
    std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::ExtractIndexDataRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_partition_num(partition_num);
    request.set_idx(idx);
    ::rtidb::common::ColumnKey* cur_column_key = request.mutable_column_key();
    cur_column_key->CopyFrom(column_key);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok =
        client_.SendRequest(&rtidb::api::TabletServer_Stub::ExtractIndexData,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::CancelOP(const uint64_t op_id) {
    ::rtidb::api::CancelOPRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_op_id(op_id);
    bool ret = client_.SendRequest(
        &::rtidb::api::TabletServer_Stub::CancelOP, &request, &response,
        FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}
bool TabletClient::Delete(uint32_t tid, uint32_t pid,
                          const Cond_Column& cd_columns, uint32_t* count,
                          std::string* msg, std::vector<int64_t>* additions) {
    ::rtidb::api::DeleteRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.mutable_condition_columns()->CopyFrom(cd_columns);
    if (additions != nullptr) {
        request.set_receive_blobs(true);
    }
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::Delete, &request,
                            &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        *count = response.count();
        if (additions != nullptr) {
            for (auto& add : response.additional_ids()) {
                additions->push_back(add);
            }
        }
        return true;
    }
    *msg = response.msg();
    return false;
}

bool TabletClient::GetCatalog(uint64_t* version) {
    if (version == nullptr) {
        return false;
    }
    ::rtidb::api::GetCatalogRequest request;
    ::rtidb::api::GetCatalogResponse response;
    bool ok = client_.SendRequest(
            &::rtidb::api::TabletServer_Stub::GetCatalog,
            &request, &response, FLAGS_request_timeout_ms,
            FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    *version = response.catalog().version();
    return true;
}

bool TabletClient::UpdateRealEndpointMap(
        const std::map<std::string, std::string>& map) {
    ::rtidb::api::UpdateRealEndpointMapRequest request;
    ::rtidb::api::GeneralResponse response;
    for (std::map<std::string, std::string>::const_iterator it = map.cbegin();
            it != map.cend(); ++it) {
        ::rtidb::api::RealEndpointPair* pair =
            request.add_real_endpoint_map();
        pair->set_name(it->first);
        pair->set_real_endpoint(it->second);
    }
    bool ok = client_.SendRequest(
            &::rtidb::api::TabletServer_Stub::UpdateRealEndpointMap,
            &request, &response, FLAGS_request_timeout_ms,
            FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::CreateProcedure(const rtidb::api::CreateProcedureRequest& sp_request, std::string& msg) {
    rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateProcedure,
            &sp_request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    msg = response.msg();
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::Scan(const ::rtidb::api::ScanRequest& request,
        brpc::Controller* cntl,
        ::rtidb::api::ScanResponse* response) {
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan, cntl,
                                  &request, response);
    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to scan table with tid " << request->tid();
        return false;
    }
    return true;
}

bool TabletClient::CallProcedure(const std::string& db, const std::string& sp_name,
                         const std::string& row, brpc::Controller* cntl,
                         rtidb::api::QueryResponse* response,
                         bool is_debug) {
    if (cntl == NULL || response == NULL) return false;
    ::rtidb::api::QueryRequest request;
    request.set_sp_name(sp_name);
    request.set_db(db);
    request.set_is_debug(is_debug);
    request.set_is_batch(false);
    request.set_is_procedure(true);
    request.set_row_size(row.size());
    request.set_row_slices(1);
    auto& io_buf = cntl->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(row.data()),
                             row.size(), &io_buf)) {
        LOG(WARNING) << "encode row buf failed";
        return false;
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Query, cntl,
                                  &request, response);
    if (!ok || response->code() != 0) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool TabletClient::SubQuery(const ::rtidb::api::QueryRequest& request,
        rtidb::RpcCallback<rtidb::api::QueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    return client_.SendRequest(&::rtidb::api::TabletServer_Stub::SubQuery,
            callback->GetController().get(), &request,
            callback->GetResponse().get(), callback);
}

bool TabletClient::CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
        std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch> row_batch,
        brpc::Controller* cntl,
        rtidb::api::SQLBatchRequestQueryResponse* response,
        bool is_debug) {
    if (cntl == NULL || response == NULL) {
        return false;
    }
    ::rtidb::api::SQLBatchRequestQueryRequest request;
    request.set_sp_name(sp_name);
    request.set_is_procedure(true);
    request.set_db(db);
    request.set_is_debug(is_debug);

    auto& io_buf = cntl->request_attachment();
    if (!EncodeRowBatch(row_batch, &request, &io_buf)) {
        return false;
    }

    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SQLBatchRequestQuery,
                                  cntl, &request, response);
    if (!ok || response->code() != ::rtidb::base::kOk) {
        LOG(WARNING) << "fail to query tablet";
        return false;
    }
    return true;
}

bool TabletClient::DropProcedure(const std::string& db_name, const std::string& sp_name) {
    ::rtidb::api::DropProcedureRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_db_name(db_name);
    request.set_sp_name(sp_name);
    bool ok =
        client_.SendRequest(&::rtidb::api::TabletServer_Stub::DropProcedure,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::CallProcedure(const std::string& db, const std::string& sp_name,
        const std::string& row, int64_t timeout_ms, bool is_debug,
        rtidb::RpcCallback<rtidb::api::QueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    ::rtidb::api::QueryRequest request;
    request.set_db(db);
    request.set_sp_name(sp_name);
    request.set_is_debug(is_debug);
    request.set_is_batch(false);
    request.set_is_procedure(true);
    request.set_row_size(row.size());
    request.set_row_slices(1);
    auto& io_buf = callback->GetController()->request_attachment();
    if (!codec::EncodeRpcRow(reinterpret_cast<const int8_t*>(row.data()),
                             row.size(), &io_buf)) {
        LOG(WARNING) << "Encode row buf failed";
        return false;
    }
    callback->GetController()->set_timeout_ms(timeout_ms);
    return client_.SendRequest(&::rtidb::api::TabletServer_Stub::Query,
            callback->GetController().get(), &request, callback->GetResponse().get(), callback);
}

bool TabletClient::CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name,
        std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch> row_batch,
        bool is_debug, int64_t timeout_ms,
        rtidb::RpcCallback<rtidb::api::SQLBatchRequestQueryResponse>* callback) {
    if (callback == nullptr) {
        return false;
    }
    ::rtidb::api::SQLBatchRequestQueryRequest request;
    request.set_sp_name(sp_name);
    request.set_is_procedure(true);
    request.set_db(db);
    request.set_is_debug(is_debug);

    auto& io_buf = callback->GetController()->request_attachment();
    if (!EncodeRowBatch(row_batch, &request, &io_buf)) {
        return false;
    }

    callback->GetController()->set_timeout_ms(timeout_ms);
    return client_.SendRequest(&::rtidb::api::TabletServer_Stub::SQLBatchRequestQuery,
            callback->GetController().get(), &request, callback->GetResponse().get(), callback);
}


}  // namespace client
}  // namespace rtidb
