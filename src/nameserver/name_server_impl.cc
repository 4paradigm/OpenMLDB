//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "name_server_impl.h"
#include <gflags/gflags.h>
#include <boost/lexical_cast.hpp>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace nameserver {

using ::baidu::common::MutexLock;

NameServerImpl::NameServerImpl():thread_pool_(1)  {
    zk_client_ = NULL;
    zk_table_path_ = FLAGS_zk_root_path + "/table";
    zk_data_path_ = FLAGS_zk_root_path + "/table/data";
    zk_table_index_node_ = zk_data_path_ + "/table_index";
}

NameServerImpl::~NameServerImpl() {
    delete zk_client_;
}

bool NameServerImpl::Init() {
    if (!FLAGS_zk_cluster.empty()) {
        zk_client_ = new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
                FLAGS_endpoint, FLAGS_zk_root_path);
        bool ok = zk_client_->Init();
        if (!ok) {
            LOG(WARNING, "fail to init zookeeper with cluster %s", FLAGS_zk_cluster.c_str());
            return false;
        }
        ok = zk_client_->Register();
        if (!ok) {
            LOG(WARNING, "fail to register nameserver with endpoint %s", FLAGS_endpoint.c_str());
            return false;
        }
        LOG(INFO, "nameserver with endpoint %s register to zk cluster %s ok", FLAGS_endpoint.c_str(), FLAGS_zk_cluster.c_str());
        thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
        std::string value;
        if (!zk_client_->GetNodeValue(zk_table_index_node_, value)) {
            char buffer[30];
            snprintf(buffer, 30, "%d", 1);
            if (!zk_client_->CreateNode(zk_table_index_node_, std::string(buffer))) {
                LOG(WARNING, "create table index node failed!");
                return false;
            }
        }

    }else {
        LOG(INFO, "zk cluster disabled");
    }
    return true;
}

void NameServerImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        bool ok = zk_client_->Reconnect();
        if (ok) {
            zk_client_->Register();
        }
    }
    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {
    return true;        
}        

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    MutexLock lock(&mu_);
    const ::rtidb::nameserver::TableMeta& table_meta = request->table_meta();
    if (table_info_.find(table_meta.name()) != table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is already exisit!");
        LOG(WARNING, "table[%s] is already exisit!", table_meta.name().c_str());
        return;
    }

    uint32_t table_index;
    std::string index_value;
    if (!zk_client_->GetNodeValue(zk_table_index_node_, index_value)) {
        response->set_code(-1);
        response->set_msg("get table index node failed");
        LOG(WARNING, "get table index node failed!");
        return;
    }
    try {
        table_index = boost::lexical_cast<uint32_t>(index_value);
    } catch (boost::bad_lexical_cast& e) {
        response->set_code(-1);
        response->set_msg("get table index node failed");
        LOG(WARNING, "get table index node failed!");
        return;
    }
    char buff[30];
    snprintf(buff, 30, "%u", table_index + 1);
    if (!zk_client_->SetNodeValue(zk_table_index_node_, std::string(buff))) {
        response->set_code(-1);
        response->set_msg("set table index node failed");
        LOG(WARNING, "set table index node failed! table_index[%s]", buff);
        return;
    }
    std::map<uint32_t, std::vector<std::string> > endpoint_vec;
    for (int idx = 0; idx < table_meta.table_partition_size(); idx++) {
        const ::rtidb::nameserver::TablePartition& table_partition = table_meta.table_partition(idx);
        if (table_partition.is_leader()) {
            continue;
        }
        auto iter = tablet_client_.find(table_partition.endpoint());
        if (iter == tablet_client_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_partition.endpoint().c_str());
            continue;
        }
        if (!iter->second->CreateTable(
                table_meta.name(), table_index, table_partition.pid(), table_meta.ttl(), false, std::vector<std::string>())) {
            LOG(WARNING, "create table[%s] failed! tid[%s] pid[%s] endpoint[%s]", 
                        table_meta.name().c_str(), table_index, table_partition.pid(), table_partition.endpoint().c_str());
            // TODO: drop table when create failed
            break;
        }
        if (endpoint_vec.find(table_partition.pid()) == endpoint_vec.end()) {
            endpoint_vec.insert(std::make_pair(table_partition.pid(), std::vector<std::string>()));
        }
        endpoint_vec[table_partition.pid()].push_back(table_partition.endpoint());
    }

    // create master table
    for (int idx = 0; idx < table_meta.table_partition_size(); idx++) {
        const ::rtidb::nameserver::TablePartition& table_partition = table_meta.table_partition(idx);
        if (!table_partition.is_leader()) {
            continue;
        }
        auto iter = tablet_client_.find(table_partition.endpoint());
        if (iter == tablet_client_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_partition.endpoint().c_str());
            continue;
        }
        std::vector<std::string> endpoint;
        if (endpoint_vec.find(table_partition.pid()) != endpoint_vec.end()) {
            endpoint_vec[table_partition.pid()].swap(endpoint);
        }
        if (!iter->second->CreateTable(
                table_meta.name(), table_index, table_partition.pid(), table_meta.ttl(), true, endpoint)) {
            LOG(WARNING, "create table[%s] failed! tid[%s] pid[%s] endpoint[%s]", 
                        table_meta.name().c_str(), table_index, table_partition.pid(), table_partition.endpoint().c_str());
            // TODO: drop table when create failed
            break;
        }
    }

    table_info_.insert(std::make_pair(table_meta.name(), table_meta));
    table_info_[table_meta.name()].set_tid(table_index);

    std::string table_value;
    table_info_[table_meta.name()].SerializeToString(&table_value);
    if (!zk_client_->CreateNode(zk_table_path_ + "/" + table_meta.name(), table_value)) {
        LOG(WARNING, "create table node[%s/%s] failed!", zk_table_path_.c_str(), table_meta.name().c_str());
        response->set_code(-1);
        response->set_msg("create table node failed");
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
}

}
}
