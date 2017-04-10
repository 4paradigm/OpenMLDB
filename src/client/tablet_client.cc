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
        uint32_t pid, uint32_t ttl) {
    ::rtidb::api::CreateTableRequest request;
    request.set_name(name);
    request.set_tid(id);
    request.set_pid(pid);
    request.set_ttl(ttl);
    ::rtidb::api::CreateTableResponse response;
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::CreateTable,
            &request, &response, 12, 1);
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
    ::rtidb::api::PutRequest request;
    request.set_pk(pk);
    request.set_time(time);
    request.set_value(value);
    request.set_tid(tid);
    ::rtidb::api::PutResponse response;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::Put,
            &request, &response, 12, 1);
    consumed = ::baidu::common::timer::get_micros() - consumed;
    percentile_.push_back(consumed/1000);
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
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
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
    percentile_.push_back(consumed/1000);
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



