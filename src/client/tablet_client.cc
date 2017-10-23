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

TabletClient::TabletClient(const std::string& endpoint):endpoint_(endpoint),
    client_(), tablet_(NULL){
    client_.GetStub(endpoint_, &tablet_);
}

TabletClient::~TabletClient() {
    delete tablet_;
}

bool TabletClient::CreateTable(const std::string& name, uint32_t id,
        uint32_t pid, uint64_t ttl, uint32_t seg_cnt) {
    std::vector<std::string> endpoints;
    return CreateTable(name, id, pid, ttl, true, endpoints, seg_cnt);
}

bool TabletClient::CreateTable(const std::string& name,
                               uint32_t tid, uint32_t pid, uint64_t ttl,
                               bool leader, const std::vector<std::string>& endpoints,
                               uint32_t seg_cnt) {
    ::rtidb::api::CreateTableRequest request;
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
    ::rtidb::api::CreateTableResponse response;
    bool ok = client_.SendRequest(tablet_,
            &::rtidb::api::TabletServer_Stub::CreateTable,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Put(uint32_t tid,
                       uint32_t pid,
                       const char* pk,
                       uint64_t time,
                       const char* value) {
    ::rtidb::api::PutRequest request;
    request.set_pk(pk);
    request.set_time(time);
    request.set_value(value);
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::PutResponse response;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::Put,
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
    return Put(tid, pid, pk.c_str(), time, value.c_str());
}

bool TabletClient::MakeSnapshot(uint32_t tid, uint32_t pid) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::MakeSnapshot,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::PauseSnapshot(uint32_t tid, uint32_t pid) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::PauseSnapshot,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::RecoverSnapshot(uint32_t tid, uint32_t pid) {
    ::rtidb::api::GeneralRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::RecoverSnapshot,
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
                               uint32_t seg_cnt) {
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
    ::rtidb::api::GeneralResponse response;
    bool ok = client_.SendRequest(tablet_,
            &::rtidb::api::TabletServer_Stub::LoadTable,
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
        const std::vector<std::string>& endpoints) {
    ::rtidb::api::ChangeRoleRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    if (leader) {
        request.set_mode(::rtidb::api::TableMode::kTableLeader);
    } else {
        request.set_mode(::rtidb::api::TableMode::kTableFollower);
    }
    for (auto iter = endpoints.begin(); iter != endpoints.end(); iter++) {
        request.add_replicas(*iter);
    }
    ::rtidb::api::ChangeRoleResponse response;
    bool ok = client_.SendRequest(tablet_,
            &::rtidb::api::TabletServer_Stub::ChangeRole,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

::rtidb::base::KvIterator* TabletClient::Scan(uint32_t tid,
                         uint32_t pid,
                         const std::string& pk,
                         uint64_t stime,
                         uint64_t etime,
                         bool showm) {
    return Scan(tid, pid, pk.c_str(), stime, etime, showm);
}

::rtidb::base::KvIterator* TabletClient::BatchGet(uint32_t tid, uint32_t pid,
        const std::vector<std::string>& keys) {

    uint64_t consumed = ::baidu::common::timer::get_micros();
    ::rtidb::api::BatchGetRequest request;
    request.set_pid(pid);
    request.set_tid(tid);
    for (size_t i = 0; i < keys.size(); i++) {
        request.add_keys(keys[i]);
    }
    ::rtidb::api::BatchGetResponse* response = new ::rtidb::api::BatchGetResponse();
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::BatchGet,
            &request, response, 12, 1);
    consumed = ::baidu::common::timer::get_micros() - consumed;
    percentile_.push_back(consumed);
    if (!ok || response->code() != 0) {
        return NULL;
    }
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(response);
    return kv_it;
}

int TabletClient::GetTableStatus(::rtidb::api::GetTableStatusResponse& response) {
    ::rtidb::api::GetTableStatusRequest request;
    bool ret = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::GetTableStatus,
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
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::Scan,
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
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::DropTable,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint) {
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::AddReplicaResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::AddReplica,
            &request, &response, 12, 1);
    if (!ok || response.code()  != 0) {
        return false;
    }
    return true;
}

bool TabletClient::DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint) {
    ::rtidb::api::ReplicaRequest request;
    ::rtidb::api::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::DelReplica,
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

}
}



