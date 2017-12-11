//
// ns_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//

#include "client/ns_client.h"
#include "base/strings.h"

namespace rtidb {
namespace client {

NsClient::NsClient(const std::string& endpoint):endpoint_(endpoint),client_(endpoint) {}

int NsClient::Init() {
    return client_.Init();
}

bool NsClient::ShowTablet(std::vector<TabletInfo>& tablets) {
    ::rtidb::nameserver::ShowTabletRequest request;
    ::rtidb::nameserver::ShowTabletResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTablet,
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
    request.set_name(name);
    request.set_pid(pid);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::MakeSnapshotNS,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response) {
    ::rtidb::nameserver::ShowOPStatusRequest request;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowOPStatus,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateTable(const ::rtidb::nameserver::TableInfo& table_info) {
    ::rtidb::nameserver::CreateTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::TableInfo* table_info_r = request.mutable_table_info();
    table_info_r->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CreateTable,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplica(const std::string& name, uint32_t pid, const std::string& endpoint) {
    ::rtidb::nameserver::AddReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddReplicaNS,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

}
}



