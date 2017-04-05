//
// tablet_client.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-02
//

#include "client/tablet_client.h"
#include "base/codec.h"

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
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::Put,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool TabletClient::Scan(uint32_t tid,
                         uint32_t pid,
                         const std::string& pk,
                         uint64_t stime,
                         uint64_t etime,
                         std::vector<std::pair<uint64_t, std::string*> >& pairs) {
    ::rtidb::api::ScanRequest request;
    request.set_pk(pk);
    request.set_st(stime);
    request.set_et(etime);
    request.set_tid(tid);
    ::rtidb::api::ScanResponse response;
    bool ok = client_.SendRequest(tablet_, &::rtidb::api::TabletServer_Stub::Scan,
            &request, &response, 12, 1);
    if (!ok || response.code() != 0) {
        return false;
    }
    ::rtidb::base::Decode(response.mutable_pairs(), pairs);
    return true;
}


}
}



