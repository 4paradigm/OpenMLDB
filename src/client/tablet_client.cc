//
// tablet_client.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-02
//

#include "client/tablet_client.h"
#include "base/codec.h"
#include "timer.h"
#include <iostream>
#include "logging.h"

DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(absolute_ttl_max);
DECLARE_bool(enable_show_tp);

namespace rtidb {
namespace client {

TabletClient::TabletClient(const std::string& endpoint):endpoint_(endpoint), client_(endpoint) {
}

TabletClient::TabletClient(const std::string& endpoint, bool use_sleep_policy):endpoint_(endpoint), 
        client_(endpoint, use_sleep_policy) {
}

TabletClient::~TabletClient() {
}

int TabletClient::Init() {
    return client_.Init();
}

std::string TabletClient::GetEndpoint() {
    return endpoint_;
}

bool TabletClient::CreateTable(const std::string& name, 
                     uint32_t tid, uint32_t pid,
                     uint64_t ttl, uint32_t seg_cnt,
                     const std::vector<::rtidb::base::ColumnDesc>& columns,
                     const ::rtidb::api::TTLType& type,
                     bool leader, const std::vector<std::string>& endpoints,
                     uint64_t term, const ::rtidb::api::CompressType compress_type) {
    std::string schema;
    ::rtidb::base::SchemaCodec codec;
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
        if (ttl > FLAGS_latest_ttl_max) {
            return false;
        }    
    } else {
        if (ttl > FLAGS_absolute_ttl_max) {
            return false;
        }
    }
    table_meta->set_ttl(ttl);
    table_meta->set_seg_cnt(seg_cnt);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    table_meta->set_schema(schema);
    table_meta->set_ttl_type(type);
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateTable,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::CreateTable(const std::string& name,
                     uint32_t tid, uint32_t pid, uint64_t ttl,
                     bool leader, 
                     const std::vector<std::string>& endpoints,
                     const ::rtidb::api::TTLType& type,
                     uint32_t seg_cnt, uint64_t term, const ::rtidb::api::CompressType compress_type) {
    ::rtidb::api::CreateTableRequest request;
    if (type == ::rtidb::api::kLatestTime) {
        if (ttl > FLAGS_latest_ttl_max) {
            return false;
        }    
    } else {
        if (ttl > FLAGS_absolute_ttl_max) {
            return false;
        }
    }
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_ttl(ttl);
    table_meta->set_compress_type(compress_type);
    table_meta->set_seg_cnt(seg_cnt);
    if (leader) {
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        table_meta->set_term(term);
    }else {
        table_meta->set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (size_t i = 0; i < endpoints.size(); i++) {
        table_meta->add_replicas(endpoints[i]);
    }
    table_meta->set_ttl_type(type);
    ::rtidb::api::CreateTableResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateTable,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::CreateTable,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::UpdateTableMetaForAddField(uint32_t tid, 
        const ::rtidb::common::ColumnDesc& column_desc, 
        const std::string& schema, 
        std::string& msg) {
    ::rtidb::api::UpdateTableMetaForAddFieldRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    ::rtidb::common::ColumnDesc* column_desc_ptr = request.mutable_column_desc();
    column_desc_ptr->CopyFrom(column_desc);
    request.set_schema(schema);
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::UpdateTableMetaForAddField,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    msg = response.msg();
    return false;
}

bool TabletClient::Put(uint32_t tid,
             uint32_t pid,
             uint64_t time,
             const std::string& value,
             const std::vector<std::pair<std::string, uint32_t> >& dimensions) {
    ::rtidb::api::PutRequest request;
    request.set_time(time);
    request.set_value(value);
    request.set_tid(tid);
    request.set_pid(pid);
    for (size_t i = 0; i < dimensions.size(); i++) {
        ::rtidb::api::Dimension* d = request.add_dimensions();
        d->set_key(dimensions[i].first);
        d->set_idx(dimensions[i].second);
    }
    ::rtidb::api::PutResponse response;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Put(uint32_t tid,
             uint32_t pid,
             const std::vector<std::pair<std::string, uint32_t> >& dimensions,
             const std::vector<uint64_t>& ts_dimensions,
             const std::string& value) {
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
    ::rtidb::api::PutResponse response;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Put(uint32_t tid,
                       uint32_t pid,
                       const char* pk,
                       uint64_t time,
                       const char* value,
                       uint32_t size) {
    ::rtidb::api::PutRequest request;
    request.set_pk(pk);
    request.set_time(time);
    request.set_value(value, size);
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::PutResponse response;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Put,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Put(uint32_t tid,
                       uint32_t pid,
                       const std::string& pk,
                       uint64_t time, 
                       const std::string& value) {
    return Put(tid, pid, pk.c_str(), time, value.c_str(), value.size());
}

bool TabletClient::MakeSnapshot(uint32_t tid, uint32_t pid,
        std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::MakeSnapshot,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::FollowOfNoOne(uint32_t tid, uint32_t pid, uint64_t term, uint64_t& offset) {
    ::rtidb::api::AppendEntriesRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_term(term);
    ::rtidb::api::AppendEntriesResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::AppendEntries,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::PauseSnapshot,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::RecoverSnapshot,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::SendSnapshot(uint32_t tid, uint32_t pid, const std::string& endpoint,
        std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::SendSnapshotRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SendSnapshot,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}        

bool TabletClient::LoadTable(const std::string& name, uint32_t id,
        uint32_t pid, uint64_t ttl, uint32_t seg_cnt) {
    std::vector<std::string> endpoints;
    return LoadTable(name, id, pid, ttl, false, endpoints, seg_cnt);
}

bool TabletClient::LoadTable(const std::string& name,
                               uint32_t tid, uint32_t pid, uint64_t ttl,
                               bool leader, const std::vector<std::string>& endpoints,
                               uint32_t seg_cnt, std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_name(name);
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_ttl(ttl);
    table_meta.set_seg_cnt(seg_cnt);
    if (leader) {
        table_meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    } else {
        table_meta.set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (size_t i = 0; i < endpoints.size(); i++) {
        table_meta.add_replicas(endpoints[i]);
    }
    return LoadTable(table_meta, task_info);
}

bool TabletClient::LoadTable(const ::rtidb::api::TableMeta& table_meta, std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::LoadTableRequest request;
    ::rtidb::api::TableMeta* cur_table_meta = request.mutable_table_meta();
    cur_table_meta->CopyFrom(table_meta);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::LoadTable,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader, uint64_t term) {
    std::vector<std::string> endpoints;
    return ChangeRole(tid, pid, leader, endpoints, term);
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader,
        const std::vector<std::string>& endpoints,
        uint64_t term, const std::vector<::rtidb::common::EndpointAndTid>* endpoint_tid) {
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
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::SetMaxConcurrency(const std::string& key,  int32_t max_concurrency) {
    ::rtidb::api::SetConcurrencyRequest request;
    request.set_key(key);
    request.set_max_concurrency(max_concurrency);
    ::rtidb::api::SetConcurrencyResponse response;
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetConcurrency,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        std::cout << response.msg() << std::endl;
        return false;
    }
    return true;
}

bool TabletClient::GetTaskStatus(::rtidb::api::TaskStatusResponse& response) {
    ::rtidb::api::TaskStatusRequest request;
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTaskStatus,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::UpdateTTL(uint32_t tid, uint32_t pid,
                             const ::rtidb::api::TTLType& type,
                             uint64_t ttl, const std::string& ts_name) {
    ::rtidb::api::UpdateTTLRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_type(type);
    request.set_value(ttl);
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    ::rtidb::api::UpdateTTLResponse response;
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::UpdateTTL,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
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
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DeleteOPTask,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::GetTermPair(uint32_t tid, uint32_t pid, ::rtidb::common::StorageMode storage_mode,
            uint64_t& term, uint64_t& offset, bool& has_table, bool& is_leader) {
    ::rtidb::api::GetTermPairRequest request;
    ::rtidb::api::GetTermPairResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTermPair,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
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

bool TabletClient::GetManifest(uint32_t tid, uint32_t pid, ::rtidb::common::StorageMode storage_mode,
            ::rtidb::api::Manifest& manifest) {
    ::rtidb::api::GetManifestRequest request;
    ::rtidb::api::GetManifestResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetManifest,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ret || response.code() != 0) {
        return false;
    }
    manifest.CopyFrom(response.manifest());
    return true;
}

bool TabletClient::GetTableStatus(::rtidb::api::GetTableStatusResponse& response) {
    ::rtidb::api::GetTableStatusRequest request;
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableStatus,
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
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableStatus,
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

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
                                 uint32_t pid,
                                 const std::string& pk,
                                 uint64_t stime,
                                 uint64_t etime,
                                 const std::string& idx_name,
                                 const std::string& ts_name,
                                 uint32_t limit,
                                 std::string& msg) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    request.set_limit(limit);
    ::rtidb::api::ScanResponse* response  = new ::rtidb::api::ScanResponse();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan,
            &request, response, FLAGS_request_timeout_ms, 1);
    response->mutable_metric()->set_rptime(::baidu::common::timer::get_micros());
    if (response->has_msg()) {
        msg = response->msg();
    }
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    return kv_it;
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
                                 uint32_t pid,
                                 const std::string& pk,
                                 uint64_t stime,
                                 uint64_t etime,
                                 const std::string& idx_name,
                                 uint32_t limit,
                                 std::string& msg) {
    return Scan(tid, pid, pk, stime, etime, idx_name, "", limit, msg);
}


::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
             uint32_t pid,
             const std::string& pk,
             uint64_t stime,
             uint64_t etime,
             uint32_t limit,
             std::string& msg) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_limit(limit);
    request.mutable_metric()->set_sqtime(::baidu::common::timer::get_micros());
    ::rtidb::api::ScanResponse* response  = new ::rtidb::api::ScanResponse();
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan,
            &request, response, FLAGS_request_timeout_ms, 1);
    response->mutable_metric()->set_rptime(::baidu::common::timer::get_micros());
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableSchema,
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

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
             uint32_t pid,
             const char* pk,
             uint64_t stime,
             uint64_t etime,
             std::string& msg,
             bool showm) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.mutable_metric()->set_sqtime(::baidu::common::timer::get_micros());
    ::rtidb::api::ScanResponse* response  = new ::rtidb::api::ScanResponse();
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan,
            &request, response, FLAGS_request_timeout_ms, 1);
    response->mutable_metric()->set_rptime(::baidu::common::timer::get_micros());
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
        uint64_t rpc_send_time = response->metric().rqtime() - response->metric().sqtime();
        uint64_t mutex_time = response->metric().sctime() - response->metric().rqtime();
        uint64_t seek_time = response->metric().sitime() - response->metric().sctime();
        uint64_t it_time = response->metric().setime() - response->metric().sitime();
        uint64_t encode_time = response->metric().sptime() - response->metric().setime();
        uint64_t receive_time = response->metric().rptime() - response->metric().sptime();
        uint64_t decode_time = ::baidu::common::timer::get_micros() - response->metric().rptime();
        std::cout << "Metric: rpc_send="<< rpc_send_time << " "
                  << "db_lock="<< mutex_time << " "
                  << "seek_time="<< seek_time << " "
                  << "iterator_time=" << it_time << " "
                  << "encode="<<encode_time << " "
                  << "receive_time="<<receive_time << " "
                  << "decode_time=" << decode_time << std::endl;
    }
    return kv_it;
}



bool TabletClient::DropTable(uint32_t id, uint32_t pid, std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::DropTableRequest request;
    request.set_tid(id);
    request.set_pid(pid);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::DropTableResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DropTable,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
        std::shared_ptr<TaskInfo> task_info) {
    return AddReplica(tid, pid, endpoint, INVALID_TID, task_info);
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
        uint32_t remote_tid, std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::AddReplicaResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if(remote_tid != INVALID_TID) {
        request.set_remote_tid(remote_tid);
    }
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::AddReplica,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
            std::shared_ptr<TaskInfo> task_info) {
    if (task_info) {        
        // fix the bug FEX-439
        ::rtidb::api::GetTableFollowerRequest get_follower_request;
        ::rtidb::api::GetTableFollowerResponse get_follower_response;
        get_follower_request.set_tid(tid);
        get_follower_request.set_pid(pid);
        bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableFollower,
                &get_follower_request, &get_follower_response, FLAGS_request_timeout_ms, 1);
        if (ok) {
            if (get_follower_response.code() < 0 && get_follower_response.msg() == "has no follower") {
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                                task_info->op_id(), ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
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
                    task_info->set_status(::rtidb::api::TaskStatus::kDone);
                    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                                    task_info->op_id(), ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DelReplica,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code()  != 0) {
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetExpire,
                                  &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code()  != 0) {
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetTTLClock,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;

}

bool TabletClient::GetTableFollower(uint32_t tid, uint32_t pid, uint64_t& offset, 
            std::map<std::string, uint64_t>& info_map, std::string& msg) {
    ::rtidb::api::GetTableFollowerRequest request;
    ::rtidb::api::GetTableFollowerResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableFollower,
            &request, &response, FLAGS_request_timeout_ms, 1);
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

void TabletClient::ShowTp() {
    if (!FLAGS_enable_show_tp) {
        return;
    }
    std::sort(percentile_.begin(), percentile_.end());
    uint32_t size = percentile_.size();
    std::cout << "Percentile:99=" << percentile_[(uint32_t)(size * 0.99)] 
              << " ,95=" << percentile_[(uint32_t)(size * 0.95)]
              << " ,90=" << percentile_[(uint32_t)(size * 0.90)]
              << " ,50=" << percentile_[(uint32_t)(size * 0.5)]
              << std::endl;
    percentile_.clear();
}

bool TabletClient::Get(uint32_t tid, 
             uint32_t pid,
             const std::string& pk,
             uint64_t time,
             std::string& value,
             uint64_t& ts,
             std::string& msg) {
    ::rtidb::api::GetRequest request;
    ::rtidb::api::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_ts(time);
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Get,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (FLAGS_enable_show_tp) {
        consumed = ::baidu::common::timer::get_micros() - consumed;
        percentile_.push_back(consumed);
    }
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code()  != 0) {
        return false;
    }
    ts = response.ts();
    value.assign(response.value());
    return true;
}

bool TabletClient::Count(uint32_t tid, uint32_t pid, const std::string& pk,
            const std::string& idx_name, bool filter_expired_data,
            uint64_t& value, std::string& msg) {
    return Count(tid, pid, pk, idx_name ,"", filter_expired_data, value, msg);
}

bool TabletClient::Count(uint32_t tid, uint32_t pid, const std::string& pk,
            const std::string& idx_name, const std::string& ts_name, bool filter_expired_data,
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Count,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code()  != 0) {
        return false;
    }
    value = response.count();
    return true;
}

bool TabletClient::Get(uint32_t tid, 
             uint32_t pid,
             const std::string& pk,
             uint64_t time,
             const std::string& idx_name,
             std::string& value,
             uint64_t& ts,
             std::string& msg) {
    return Get(tid, pid, pk, time, idx_name, "", value, ts, msg);
}

bool TabletClient::Get(uint32_t tid, 
             uint32_t pid,
             const std::string& pk,
             uint64_t time,
             const std::string& idx_name,
             const std::string& ts_name,
             std::string& value,
             uint64_t& ts,
             std::string& msg) {
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
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Get,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code()  != 0) {
        return false;
    }
    ts = response.ts();
    value.assign(response.value());
    return true;
}

bool TabletClient::Delete(uint32_t tid, uint32_t pid,
             const std::string& pk, const std::string& idx_name,
             std::string& msg) {
    ::rtidb::api::DeleteRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    if (!idx_name.empty()) {
        request.set_idx_name(idx_name);
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Delete,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (response.has_msg()) {
        msg = response.msg();
    }
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::ConnectZK() {
    ::rtidb::api::ConnectZKRequest request;
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::ConnectZK,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DisConnectZK() {
    ::rtidb::api::DisConnectZKRequest request;
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DisConnectZK,
            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DeleteBinlog(uint32_t tid, uint32_t pid, ::rtidb::common::StorageMode storage_mode) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_storage_mode(storage_mode);
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DeleteBinlog,
            &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

::rtidb::base::KvIterator* TabletClient::Traverse(uint32_t tid, uint32_t pid, const std::string& idx_name,
            const std::string& pk, uint64_t ts, uint32_t limit, uint32_t& count) {
    ::rtidb::api::TraverseRequest request;
    ::rtidb::api::TraverseResponse* response = new ::rtidb::api::TraverseResponse();
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
            &request, response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response->code()  != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    count = response->count();
    return kv_it;
}

bool TabletClient::SetMode(const bool mode) {
    ::rtidb::api::SetModeRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_follower(mode);
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::SetMode,
        &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

}
}
