//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "nameserver/name_server_impl.h"

#include <gflags/gflags.h>
#include <boost/lexical_cast.hpp>
#include "gflags/gflags.h"

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace nameserver {

using ::baidu::common::MutexLock;

NameServerImpl::NameServerImpl():mu_(), tablet_client_(),
    table_info_(), zk_client_(NULL), dist_lock_(NULL), thread_pool_(1),
    cv_(&mu_) {
    zk_table_path_ = FLAGS_zk_root_path + "/table";
    zk_data_path_ = FLAGS_zk_root_path + "/table/data";
    zk_table_index_node_ = zk_data_path_ + "/table_index";
    zk_op_path_ = zk_data_path_ + "/op_task";
    running_.store(false, std::memory_order_release);
}

NameServerImpl::~NameServerImpl() {
    delete zk_client_;
}

// become name server leader
bool NameServerImpl::Recover() {
    std::string value;
    if (!zk_client_->GetNodeValue(zk_table_index_node_, value)) {
        char buffer[30];
        snprintf(buffer, 30, "%d", 1);
        if (!zk_client_->CreateNode(zk_table_index_node_, std::string(buffer))) {
            LOG(WARNING, "create table index node failed!");
            return false;
        }
    }

    LOG(INFO, "recover table id counter %s", value.c_str());
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        LOG(WARNING, "get endpoints node failed!");
        return false;
    }
    MutexLock lock(&mu_);
    for (auto iter = endpoints.begin(); iter != endpoints.end(); ++iter) {
        LOG(DEBUG, "create endpoint[%s] client", iter->c_str());
        tablet_client_.insert(std::make_pair(*iter, std::make_shared<::rtidb::client::TabletClient>(*iter)));
    }
    //TODO recover tablet status
    return true;
}

bool NameServerImpl::Init() {
    if (FLAGS_zk_cluster.empty()) {
        LOG(WARNING, "zk cluster disabled");
        return false;
    }
    zk_client_ = new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
            FLAGS_endpoint, FLAGS_zk_root_path);
    if (!zk_client_->Init()) {
        LOG(WARNING, "fail to init zookeeper with cluster %s", FLAGS_zk_cluster.c_str());
        return false;
    }
    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
    dist_lock_ = new DistLock(FLAGS_zk_root_path + "/leader", zk_client_, 
            boost::bind(&NameServerImpl::OnLocked, this), boost::bind(&NameServerImpl::OnLostLock, this),
            FLAGS_endpoint);
    dist_lock_->Lock();
    return true;
}

void NameServerImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        zk_client_->Reconnect();
    }
    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {
    return true;        
}

int NameServerImpl::GetTabletClient(std::vector<std::shared_ptr<::rtidb::client::TabletClient> >& client_vec) {
    MutexLock lock(&mu_);
    for (auto iter = tablet_client_.begin(); iter != tablet_client_.end(); ++iter) {
        client_vec.push_back(iter->second);
    }
    return 0;
}

int NameServerImpl::UpdateTaskStatus() {
    std::vector<std::shared_ptr<::rtidb::client::TabletClient> > client_vec;
    GetTabletClient(client_vec);
    for (auto iter = client_vec.begin(); iter != client_vec.end(); ++iter) {
        ::rtidb::api::TaskStatusResponse response;
        if ((*iter)->GetTaskStatus(response)) {
            MutexLock lock(&mu_);
            for (int idx = 0; idx < response.task_size(); idx++) {
                auto it = task_map_.find(response.task(idx).op_id());
                if (it == task_map_.end()) {
                    LOG(WARNING, "cannot find op_id[%lu] in task_map", response.task(idx).op_id());
                    continue;
                }
                if (it->second.task_list.empty()) {
                    continue;
                }
                // update task status
                Task* task = it->second.task_list.front().get();
                if (task->task_type_ == response.task(idx).task_type()) {
                    LOG(DEBUG, "update task status from[%d] to[%d]. op_id[%lu], task_type[%d]", 
                                task->status_, response.task(idx).status(), 
                                response.task(idx).op_id(), task->task_type_);
                    task->status_ = response.task(idx).status();
                }
            }
        }
    }
    return 0;
}

int NameServerImpl::UpdateZKStatus(const std::vector<uint64_t>& run_task_vec) {
    if (run_task_vec.empty()) {
        return 0;
    }
    for (auto iter = run_task_vec.begin(); iter != run_task_vec.end(); ++iter) {
        std::string node = zk_op_path_ + "/" + boost::lexical_cast<std::string>(*iter);
        std::string value;
        if (zk_client_->SetNodeValue(node, value)) {
            LOG(DEBUG, "set zk status value success. node[%s] value[%s]",
                        node.c_str(), value.c_str());
            continue;
        }
        LOG(WARNING, "set zk status value failed! node[%s] value[%s]", 
                      node.c_str(), value.c_str()); 
    }
    return 0;
}

int NameServerImpl::DeleteTask(const std::vector<uint64_t>& done_task_vec) {
    if (done_task_vec.empty()) {
        return 0;
    }
    std::vector<std::shared_ptr<::rtidb::client::TabletClient> > client_vec;
    GetTabletClient(client_vec);
    uint32_t suc_count = 0;
    for (auto iter = client_vec.begin(); iter != client_vec.end(); ++iter) {
        if ((*iter)->DeleteOPTask(done_task_vec)) {
            LOG(DEBUG, "endpoint[%s] delete op success", (*iter)->GetEndpoint().c_str()); 
            suc_count++;
        }
    }
    if (suc_count == client_vec.size()) {
        {
            MutexLock lock(&mu_);
            for (auto iter = done_task_vec.begin(); iter != done_task_vec.end(); ++iter) {
                auto it = task_map_.find(*iter);
                if (it != task_map_.end()) {
                    task_map_.erase(it);
                    LOG(DEBUG, "delete op[%lu]", *iter); 
                }
            }
        }
        for (auto iter = done_task_vec.begin(); iter != done_task_vec.end(); ++iter) {
            std::string node = zk_op_path_ + "/" + boost::lexical_cast<std::string>(*iter);
            zk_client_->DeleteNode(node);
        }
    }
    return 0;
}

void NameServerImpl::ProcessTask() {
    while (1) {
        mu_.Lock();
        while (task_map_.empty()) {
            cv_.Wait();
        }
        mu_.Unlock();
        if (running_.load(std::memory_order_acquire)) {
            break;
        }
        UpdateTaskStatus();
        
        std::vector<uint64_t> done_task_vec;
        std::vector<uint64_t> run_task_vec;
        mu_.Lock();
        for (auto iter = task_map_.begin(); iter != task_map_.end(); iter++) {
            if (iter->second.task_list.empty()) {
                continue;
            }
            if (iter->second.task_list.front()->status_ == ::rtidb::api::kDone) {
                iter->second.task_list.pop_front();
            } else if (iter->second.task_list.front()->status_ == ::rtidb::api::kFailed) {
                // TODO. tackle failed case here;
            }
            if (iter->second.task_list.empty()) {
                LOG(DEBUG, "operation has finished! op_id[%lu]", iter->first);
                done_task_vec.push_back(iter->first);
                continue;
            }
            task_thread_pool_.AddTask(iter->second.task_list.front()->fun);
            run_task_vec.push_back(iter->first);

        }
        mu_.Unlock();
        DeleteTask(done_task_vec);
        UpdateZKStatus(run_task_vec);
    }
}

int NameServerImpl::CreateTable(const ::rtidb::nameserver::TableMeta& table_meta, uint32_t tid,
            bool is_leader, std::map<uint32_t, std::vector<std::string> >& endpoint_vec) {
    for (int idx = 0; idx < table_meta.table_partition_size(); idx++) {
        const ::rtidb::nameserver::TablePartition& table_partition = table_meta.table_partition(idx);
        if (table_partition.is_leader() != is_leader) {
            continue;
        }
        auto iter = tablet_client_.find(table_partition.endpoint());
        if (iter == tablet_client_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_partition.endpoint().c_str());
            continue;
        }
        std::vector<std::string> endpoint;
        if (is_leader && endpoint_vec.find(table_partition.pid()) != endpoint_vec.end()) {
            endpoint_vec[table_partition.pid()].swap(endpoint);
        }
        if (!iter->second->CreateTable(
                table_meta.name(), tid, table_partition.pid(), table_meta.ttl(), is_leader, endpoint)) {
            LOG(WARNING, "create table[%s] failed! tid[%u] pid[%u] endpoint[%s]", 
                        table_meta.name().c_str(), tid, table_partition.pid(), table_partition.endpoint().c_str());
            // TODO: drop table when create failed
            break;
        }
        LOG(DEBUG, "create table[%s] tid[%u] pid[%u] endpoint[%s] success", 
                    table_meta.name().c_str(), tid, table_partition.pid(), table_partition.endpoint().c_str());
        if (!is_leader) {            
            if (endpoint_vec.find(table_partition.pid()) == endpoint_vec.end()) {
                endpoint_vec.insert(std::make_pair(table_partition.pid(), std::vector<std::string>()));
            }
            endpoint_vec[table_partition.pid()].push_back(table_partition.endpoint());
        }
    }
    return 0;
}

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is offline");
        LOG(WARNING, "cur nameserver is offline");
        done->Run();
        return;
    }
    MutexLock lock(&mu_);
    const ::rtidb::nameserver::TableMeta& table_meta = request->table_meta();
    if (table_info_.find(table_meta.name()) != table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is already exisit!");
        LOG(WARNING, "table[%s] is already exisit!", table_meta.name().c_str());
        done->Run();
        return;
    }

    uint32_t table_index = 0;
    std::string index_value;
    if (!zk_client_->GetNodeValue(zk_table_index_node_, index_value)) {
        response->set_code(-1);
        response->set_msg("get table index node failed");
        LOG(WARNING, "get table index node failed!");
        done->Run();
        return;
    }
    try {
        table_index = boost::lexical_cast<uint32_t>(index_value);
    } catch (boost::bad_lexical_cast& e) {
        response->set_code(-1);
        response->set_msg("get table index node failed");
        LOG(WARNING, "get table index node failed!");
        done->Run();
        return;
    }
    char buff[30];
    snprintf(buff, 30, "%u", table_index + 1);
    if (!zk_client_->SetNodeValue(zk_table_index_node_, std::string(buff))) {
        response->set_code(-1);
        response->set_msg("set table index node failed");
        LOG(WARNING, "set table index node failed! table_index[%s]", buff);
        done->Run();
        return;
    }
    std::map<uint32_t, std::vector<std::string> > endpoint_vec;
    CreateTable(table_meta, table_index, false, endpoint_vec);
    // create master table
    CreateTable(table_meta, table_index, true, endpoint_vec);

    table_info_.insert(std::make_pair(table_meta.name(), table_meta));
    table_info_[table_meta.name()].set_tid(table_index);

    std::string table_value;
    table_info_[table_meta.name()].SerializeToString(&table_value);
    if (!zk_client_->CreateNode(zk_table_path_ + "/" + table_meta.name(), table_value)) {
        LOG(WARNING, "create table node[%s/%s] failed! value[%s]", zk_table_path_.c_str(), table_meta.name().c_str(), table_value.c_str());
        response->set_code(-1);
        response->set_msg("create table node failed");
        done->Run();
        return;
    }
    LOG(DEBUG, "create table node[%s/%s] success! value[%s]", zk_table_path_.c_str(), table_meta.name().c_str(), table_value.c_str());
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void NameServerImpl::OnLocked() {
    LOG(INFO, "become the leader name server");
    bool ok = Recover();
    if (!ok) {
        //TODO fail to recover discard the lock
    }
    running_.store(true, std::memory_order_release);
}

void NameServerImpl::OnLostLock() {
    LOG(INFO, "become the stand by name sever");
    running_.store(false, std::memory_order_release);
}

}
}
