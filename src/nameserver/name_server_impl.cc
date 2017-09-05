//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "name_server_impl.h"

namespace rtidb {
namespace nameserver {

using ::baidu::common::MutexLock;

NameServerImpl::NameServerImpl() {

}

NameServerImpl::~NameServerImpl() {

}

int NameServerImpl::Init() {
    return 0;
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {
    return true;        
}        

std::string NameServerImpl::GetMaster() {
    // to do. get master point form zk

    std::string endpoint("");
    return endpoint;
}

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    MutexLock lock(&mu_);
    if (table_info_.find(request->name()) != table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is already exisit!");
        LOG(WARNING, "table[%s] is already exisit!", request->name().c_str());
        return;
    }

    std::vector<TableInfo> table_info_vec;
    std::map<uint32_t, std::vector<std::string> > endpoint_vec;
    TableInfo table_info;
    table_info.name_ = request->name();
    table_info.tid_ = table_index_;
    table_info.is_leader_ = false;
    table_info.ttl_ = request->ttl();
    table_info.seg_cnt_ = request->seg_cnt();
    for (int idx = 0; idx < request->table_meta_size(); idx++) {
        const ::rtidb::nameserver::TableMeta& table_meta = request->table_meta(idx);
        if (table_meta.is_leader()) {
            continue;
        }
        auto iter = tablet_client_.find(table_meta.endpoint());
        if (iter == tablet_client_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_meta.endpoint().c_str());
            continue;
        }
        if (!iter->second->CreateTable(
                request->name(), table_index_, table_meta.pid(), request->ttl(), false, std::vector<std::string>())) {
            LOG(WARNING, "create table[%s] failed! tid[%s] pid[%s] endpoint[%s]", 
                        request->name().c_str(), table_index_, table_meta.pid(), table_meta.endpoint().c_str());
            // TODO: drop table when create failed
            break;
        }
        table_info.endpoint_ = table_meta.endpoint();
        table_info.pid_ = table_meta.pid();
        table_info_vec.push_back(table_info);
        if (endpoint_vec.find(table_meta.pid()) == endpoint_vec.end()) {
            endpoint_vec.insert(std::make_pair(table_meta.pid(), std::vector<std::string>()));
        }
        endpoint_vec[table_meta.pid()].push_back(table_meta.endpoint());
    }

    // create master table
    for (int idx = 0; idx < request->table_meta_size(); idx++) {
        const ::rtidb::nameserver::TableMeta& table_meta = request->table_meta(idx);
        if (!table_meta.is_leader()) {
            continue;
        }
        auto iter = tablet_client_.find(table_meta.endpoint());
        if (iter == tablet_client_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_meta.endpoint().c_str());
            continue;
        }
        std::vector<std::string> endpoint;
        if (endpoint_vec.find(table_meta.pid()) != endpoint_vec.end()) {
            endpoint_vec[table_meta.pid()].swap(endpoint);
        }
        if (!iter->second->CreateTable(
                request->name(), table_index_, table_meta.pid(), request->ttl(), true, endpoint)) {
            LOG(WARNING, "create table[%s] failed! tid[%s] pid[%s] endpoint[%s]", 
                        request->name().c_str(), table_index_, table_meta.pid(), table_meta.endpoint().c_str());
            // TODO: drop table when create failed
            break;
        }
        table_info.endpoint_ = table_meta.endpoint();
        table_info.pid_ = table_meta.pid();
        table_info.is_leader_ = true;
        table_info_vec.push_back(table_info);
    }

    table_index_++;
    // TODO: update zk table_info

    response->set_code(0);
    response->set_msg("ok");
}

}
}
