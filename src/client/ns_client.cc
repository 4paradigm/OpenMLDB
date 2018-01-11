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

bool NsClient::ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg) {
    ::rtidb::nameserver::ShowTabletRequest request;
    ::rtidb::nameserver::ShowTabletResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTablet,
            &request, &response, 12, 1);
    msg = response.msg();
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

bool NsClient::ShowTable(const std::string& name, 
            std::vector<::rtidb::nameserver::TableInfo>& tables, std::string& msg) {
    ::rtidb::nameserver::ShowTableRequest request;
    if (!name.empty()) {
        request.set_name(name);
    }
    ::rtidb::nameserver::ShowTableResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTable,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.table_info_size(); i++) {
            ::rtidb::nameserver::TableInfo table_info;
            table_info.CopyFrom(response.table_info(i));
            tables.push_back(table_info);
        }
        return true;
    }
    return false;
}

bool NsClient::MakeSnapshot(const std::string& name, uint32_t pid, std::string& msg) {
    ::rtidb::nameserver::MakeSnapshotNSRequest request;
    request.set_name(name);
    request.set_pid(pid);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::MakeSnapshotNS,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response, std::string& msg) {
    ::rtidb::nameserver::ShowOPStatusRequest request;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowOPStatus,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateTable(const ::rtidb::nameserver::TableInfo& table_info, std::string& msg) {
    ::rtidb::nameserver::CreateTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::TableInfo* table_info_r = request.mutable_table_info();
    table_info_r->CopyFrom(table_info);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CreateTable,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DropTable(const std::string& name, std::string& msg) {
    ::rtidb::nameserver::DropTableRequest request;
    request.set_name(name);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DropTable,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplica(const std::string& name, uint32_t pid, 
            const std::string& endpoint, std::string& msg) {
    ::rtidb::nameserver::AddReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddReplicaNS,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DelReplica(const std::string& name, uint32_t pid, 
            const std::string& endpoint, std::string& msg) {
    ::rtidb::nameserver::DelReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::DelReplicaData* data = request.mutable_data();
    data->set_name(name);
    data->set_pid(pid);
    data->set_endpoint(endpoint);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DelReplicaNS,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfSet(const std::string& key, const std::string& value, std::string& msg) {
    ::rtidb::nameserver::ConfSetRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::Pair* conf = request.mutable_conf();
    conf->set_key(key);
    conf->set_value(value);
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConfSet,
            &request, &response, 12, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfGet(const std::string& key, std::map<std::string, std::string>& conf_map, 
            std::string& msg) {
    ::rtidb::nameserver::ConfGetRequest request;
    ::rtidb::nameserver::ConfGetResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConfGet,
            &request, &response, 12, 1);
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

}
}



