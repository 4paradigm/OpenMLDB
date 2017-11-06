//
// ns_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//

#include "client/ns_client.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "base/strings.h"

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
    request.set_name(name);
    request.set_pid(pid);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(ns_, &::rtidb::nameserver::NameServer_Stub::MakeSnapshotNS,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response) {
    ::rtidb::nameserver::ShowOPStatusRequest request;
    bool ok = client_.SendRequest(ns_, &::rtidb::nameserver::NameServer_Stub::ShowOPStatus,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateTable(const ::rtidb::client::TableInfo& table_info) {
    ::rtidb::nameserver::CreateTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::TableInfo* ns_table_info = request.mutable_table_info();
    ns_table_info->set_name(table_info.name());
    ns_table_info->set_ttl(table_info.ttl());
    ns_table_info->set_seg_cnt(table_info.seg_cnt());
    for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
        std::string pid_group = table_info.table_partition(idx).pid_group();
        if (::rtidb::base::IsNumber(pid_group)) {
            ::rtidb::nameserver::TablePartition* table_partition = ns_table_info->add_table_partition();
            table_partition->set_endpoint(table_info.table_partition(idx).endpoint());
            table_partition->set_pid(boost::lexical_cast<uint32_t>(pid_group));
            table_partition->set_is_leader(table_info.table_partition(idx).is_leader());
        } else {
            std::vector<std::string> vec;
            boost::split(vec, pid_group, boost::is_any_of("-"));
            if (vec.size() != 2 || !::rtidb::base::IsNumber(vec[0]) || !::rtidb::base::IsNumber(vec[1])) {
                return false;
            }
            uint32_t start_index = boost::lexical_cast<uint32_t>(vec[0]);
            uint32_t end_index = boost::lexical_cast<uint32_t>(vec[1]);
            for (uint32_t pid = start_index; pid <= end_index; pid++) {
                ::rtidb::nameserver::TablePartition* table_partition = ns_table_info->add_table_partition();
                table_partition->set_endpoint(table_info.table_partition(idx).endpoint());
                table_partition->set_pid(pid);
                table_partition->set_is_leader(table_info.table_partition(idx).is_leader());
            }

        }
    }
    bool ok = client_.SendRequest(ns_, &::rtidb::nameserver::NameServer_Stub::CreateTable,
            &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

}
}



