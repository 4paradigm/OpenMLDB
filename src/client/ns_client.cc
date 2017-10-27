//
// ns_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//

#include "client/ns_client.h"

namespace rtidb {
namespace client {

NsClient::NsClient(const std::string& endpoint):endpoint_(endpoint),client_(),
    ns_(NULL){}

NsClient::~NsClient() {
    delete ns_;
}

bool NsClient::Init() {
    client_.GetStub(endpoint_, &ns_);
    return true;
}

bool NsClient::ShowTablet(std::vector<TabletInfo>& tablets) {
    ::rtidb::nameserver::ShowTabletRequest request;
    ::rtidb::nameserver::ShowTabletResponse response;
    bool ok = client_.SendRequest(ns_, &::rtidb::nameserver::NameServer_Stub::ShowTablet,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::rtidb::nameserver::TabletStatus status = response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.state = status.state();
            info.age = status.age();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::MakeSnapshot(const std::string& name, uint32_t pid) {
    ::rtidb::nameserver::MakeSnapshotNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(ns_, &::rtidb::nameserver::NameServer_Stub::MakeSnapshotNS,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

}
}



