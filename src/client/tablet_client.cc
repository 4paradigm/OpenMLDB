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

namespace rtidb {
namespace client {

const uint32_t KEEP_LATEST_MAX_NUM = 1000;

TabletClient::TabletClient(const std::string& endpoint):endpoint_(endpoint), client_(endpoint){
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
                     uint64_t term) {
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
    if (type == ::rtidb::api::kLatestTime && ttl > KEEP_LATEST_MAX_NUM) {
        return false;
    }
    table_meta->set_ttl(ttl);
    table_meta->set_seg_cnt(seg_cnt);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    table_meta->set_schema(schema);
    table_meta->set_ttl_type(type);
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
            &request, &response, 12, 1);
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
                     uint32_t seg_cnt, uint64_t term) {
    ::rtidb::api::CreateTableRequest request;
    if (type == ::rtidb::api::kLatestTime && ttl > KEEP_LATEST_MAX_NUM) {
        return false;
    }
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_ttl(ttl);
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
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
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
            &request, &response, 12, 1);
    percentile_.push_back(consumed);
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
            &request, &response, 12, 1);
    consumed = ::baidu::common::timer::get_micros() - consumed;
    percentile_.push_back(consumed);
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
            &request, &response, 12, 1);
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
            &request, &response, 12, 1);
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
            &request, &response, 12, 1);
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
            &request, &response, 12, 1);
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
            &request, &response, 12, 1);
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
    ::rtidb::api::LoadTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_ttl(ttl);
    table_meta->set_seg_cnt(seg_cnt);
    if (leader) {
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    }else {
        table_meta->set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (size_t i = 0; i < endpoints.size(); i++) {
        table_meta->add_replicas(endpoints[i]);
    }
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::LoadTable,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader) {
    std::vector<std::string> endpoints;
    return ChangeRole(tid, pid, leader, endpoints);
}

bool TabletClient::ChangeRole(uint32_t tid, uint32_t pid, bool leader,
        const std::vector<std::string>& endpoints, uint64_t term) {
    ::rtidb::api::ChangeRoleRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (leader) {
        request.set_mode(::rtidb::api::TableMode::kTableLeader);
        request.set_term(term);
    } else {
        request.set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (auto iter = endpoints.begin(); iter != endpoints.end(); iter++) {
        request.add_replicas(*iter);
    }
    ::rtidb::api::ChangeRoleResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::ChangeRole,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}


bool TabletClient::GetTaskStatus(::rtidb::api::TaskStatusResponse& response) {
    ::rtidb::api::TaskStatusRequest request;
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTaskStatus,
            &request, &response, 12, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DeleteOPTask(const std::vector<uint64_t>& op_id_vec) {
    ::rtidb::api::DeleteTaskRequest request;
    ::rtidb::api::GeneralResponse response;
    for (auto op_id : op_id_vec) {
        request.add_op_id(op_id);
    }
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DeleteOPTask,
            &request, &response, 12, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

int TabletClient::GetTableStatus(::rtidb::api::GetTableStatusResponse& response) {
    ::rtidb::api::GetTableStatusRequest request;
    bool ret = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableStatus,
            &request, &response, 12, 1);
    if (ret) {
        return 0;
    }
    return -1;
}

int TabletClient::GetTableStatus(uint32_t tid, uint32_t pid, 
            ::rtidb::api::TableStatus& table_status) {
    ::rtidb::api::GetTableStatusResponse response;
    if (GetTableStatus(response) < 0) {
        return -1;
    }
    for (int idx = 0; idx < response.all_table_status_size(); idx++) {
        if (response.all_table_status(idx).tid() == tid &&
                response.all_table_status(idx).pid() == pid) {
            table_status = response.all_table_status(idx);
            return 0;    
        }
    }
    return -1;
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
                                 uint32_t pid,
                                 const std::string& pk,
                                 uint64_t stime,
                                 uint64_t etime,
                                 const std::string& idx_name) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_idx_name(idx_name);
    ::rtidb::api::ScanResponse* response  = new ::rtidb::api::ScanResponse();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan,
            &request, response, 12, 1);
    response->mutable_metric()->set_rptime(::baidu::common::timer::get_micros());
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
             uint64_t etime) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    request.set_pid(pid);
    request.mutable_metric()->set_sqtime(::baidu::common::timer::get_micros());
    ::rtidb::api::ScanResponse* response  = new ::rtidb::api::ScanResponse();
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Scan,
            &request, response, 12, 1);
    response->mutable_metric()->set_rptime(::baidu::common::timer::get_micros());
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    return kv_it;
}

bool TabletClient::GetTableSchema(uint32_t tid, uint32_t pid,
        std::string& schema) {
    ::rtidb::api::GetTableSchemaRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::GetTableSchemaResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::GetTableSchema,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        schema.assign(response.schema());
        return true;
    }
    return false;
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
             uint32_t pid,
             const char* pk,
             uint64_t stime,
             uint64_t etime,
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
            &request, response, 12, 1);
    response->mutable_metric()->set_rptime(::baidu::common::timer::get_micros());
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
    consumed = ::baidu::common::timer::get_micros() - consumed;
    percentile_.push_back(consumed);
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



bool TabletClient::DropTable(uint32_t id, uint32_t pid) {
    ::rtidb::api::DropTableRequest request;
    request.set_tid(id);
    request.set_pid(pid);
    ::rtidb::api::DropTableResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DropTable,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
            std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::AddReplicaResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::AddReplica,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
            std::shared_ptr<TaskInfo> task_info) {
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    if (task_info) {
        request.mutable_task_info()->CopyFrom(*task_info);
    }
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DelReplica,
            &request, &response, 12, 1);
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
                                  &request, &response, 12, 1);
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
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;

}

void TabletClient::ShowTp() {
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
             uint64_t& ts) {
    ::rtidb::api::GetRequest request;
    ::rtidb::api::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(pk);
    request.set_ts(time);
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::Get,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    ts = response.ts();
    value.assign(response.value());
    return true;
}

bool TabletClient::ConnectZK() {
    ::rtidb::api::ConnectZKRequest request;
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::ConnectZK,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DisConnectZK() {
    ::rtidb::api::DisConnectZKRequest request;
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::api::TabletServer_Stub::DisConnectZK,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

}
}
