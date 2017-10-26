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
#include "timer.h"

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(get_task_status_interval);
DECLARE_int32(name_server_task_pool_size);

namespace rtidb {
namespace nameserver {

using ::baidu::common::MutexLock;

NameServerImpl::NameServerImpl():mu_(), tablets_(),
    table_info_(), zk_client_(NULL), dist_lock_(NULL), thread_pool_(1), 
    task_thread_pool_(FLAGS_name_server_task_pool_size), cv_(&mu_) {
    zk_table_path_ = FLAGS_zk_root_path + "/table";
    zk_data_path_ = FLAGS_zk_root_path + "/table/data";
    zk_table_index_node_ = zk_data_path_ + "/table_index";
    zk_op_index_node_ = zk_data_path_ + "/op_index";
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
        if (!zk_client_->CreateNode(zk_table_index_node_, "1")) {
            LOG(WARNING, "create table index node failed!");
            return false;
        }
        table_index_ = 1;
        LOG(INFO, "init table_index[%u]", table_index_);
    } else {
        table_index_ = boost::lexical_cast<uint64_t>(value);
        LOG(INFO, "recover table_index[%u]", table_index_);
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_op_index_node_, value)) {
        if (!zk_client_->CreateNode(zk_op_index_node_, "1")) {
            LOG(WARNING, "create op index node failed!");
            return false;
        }
        op_index_ = 1;
        LOG(INFO, "init op_index[%u]", op_index_);
    } else {
        op_index_ = boost::lexical_cast<uint64_t>(value);
        LOG(INFO, "recover op_index[%u]", op_index_);
    }

    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        LOG(WARNING, "get endpoints node failed!");
        return false;
    }

    MutexLock lock(&mu_);
    UpdateTablets(endpoints);
    zk_client_->WatchNodes(boost::bind(&NameServerImpl::UpdateTabletsLocked, this, _1));
    zk_client_->WatchNodes();
    return true;
}

void NameServerImpl::UpdateTabletsLocked(const std::vector<std::string>& endpoints) {
    MutexLock lock(&mu_);
    UpdateTablets(endpoints);
}


void NameServerImpl::UpdateTablets(const std::vector<std::string>& endpoints) {
    mu_.AssertHeld();
    // check exist and newly add tablets
    std::set<std::string> alive;
    std::vector<std::string>::const_iterator it = endpoints.begin();
    for (; it != endpoints.end(); ++it) {
        alive.insert(*it);
        Tablets::iterator tit = tablets_.find(*it);
        // register a new tablet
        if (tit == tablets_.end()) {
            std::shared_ptr<TabletInfo> tablet = std::make_shared<TabletInfo>();
            tablet->state_ = ::rtidb::api::TabletState::kTabletHealthy;
            tablet->client_ = std::make_shared<::rtidb::client::TabletClient>(*it);
            tablet->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            tablets_.insert(std::make_pair(*it, tablet));
        } else {
            //TODO wangtaize notify if state changes
            ::rtidb::api::TabletState old = tit->second->state_;
            if (old != ::rtidb::api::TabletState::kTabletHealthy) {
                tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            }
            tit->second->state_ = ::rtidb::api::TabletState::kTabletHealthy;
        }
        LOG(INFO, "healthy tablet with endpoint %s", it->c_str());
    }
    // handle offline tablet
    Tablets::iterator tit = tablets_.begin();
    for (; tit !=  tablets_.end(); ++tit) {
        if (alive.find(tit->first) == alive.end()) {
            // tablet offline
            LOG(INFO, "offline tablet with endpoint %s", tit->first.c_str());
            tit->second->state_ = ::rtidb::api::TabletState::kTabletOffline;
        }
    }
}

void NameServerImpl::ShowTablet(RpcController* controller,
            const ShowTabletRequest* request,
            ShowTabletResponse* response,
            Closure* done) {
    MutexLock lock(&mu_);
    Tablets::iterator it = tablets_.begin();
    for (; it !=  tablets_.end(); ++it) {
        TabletStatus* status = response->add_tablets();
        status->set_endpoint(it->first);
        status->set_state(::rtidb::api::TabletState_Name(it->second->state_));
        status->set_age(::baidu::common::timer::get_micros() / 1000 - it->second->ctime_);
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
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

int NameServerImpl::UpdateTaskStatus() {
    if (!running_.load(std::memory_order_acquire)) {
        LOG(DEBUG, "cur name_server is not running. return");
        return 0;
    }
    std::vector<std::shared_ptr<TabletClient>> vec;
    {
        MutexLock lock(&mu_);
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                LOG(DEBUG, "tablet[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            vec.push_back(iter->second->client_);
        }    
    }

    for (auto iter = vec.begin(); iter != vec.end(); ++iter) {
        ::rtidb::api::TaskStatusResponse response;
        // get task status from tablet
        if ((*iter)->GetTaskStatus(response)) {
            MutexLock lock(&mu_);
            for (int idx = 0; idx < response.task_size(); idx++) {
                auto it = task_map_.find(response.task(idx).op_id());
                if (it == task_map_.end()) {
                    LOG(WARNING, "cannot find op_id[%lu] in task_map", response.task(idx).op_id());
                    continue;
                }
                if (it->second->task_list_.empty()) {
                    continue;
                }
                // update task status
                std::shared_ptr<Task> task = it->second->task_list_.front();
                if (task->task_info_->task_type() == response.task(idx).task_type()) {
                    LOG(DEBUG, "update task status from[%s] to[%s]. op_id[%lu], task_type[%s]", 
                                ::rtidb::api::TaskStatus_Name(task->task_info_->status()).c_str(), 
                                ::rtidb::api::TaskStatus_Name(response.task(idx).status()).c_str(), 
                                response.task(idx).op_id(), 
                                ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
                    task->task_info_->set_status(response.task(idx).status());
                }
            }
        }
    }
    if (running_.load(std::memory_order_acquire)) {
        task_thread_pool_.DelayTask(FLAGS_get_task_status_interval, boost::bind(&NameServerImpl::UpdateTaskStatus, this));
    }
    return 0;
}

int NameServerImpl::UpdateZKTaskStatus(const std::vector<uint64_t>& run_task_vec) {
    if (run_task_vec.empty()) {
        return 0;
    }
    for (auto iter = run_task_vec.begin(); iter != run_task_vec.end(); ++iter) {
        std::shared_ptr<OPData> op_data;
        {
            MutexLock lock(&mu_);
            auto pos = task_map_.find(*iter);
            if (pos == task_map_.end()) {
                LOG(WARNING, "cannot find op[%lu] in task_map", *iter);
                continue;
            }
            op_data = pos->second;
        }
        uint32_t cur_task_index = op_data->op_info_.task_index();
        op_data->op_info_.set_task_index(cur_task_index + 1);
        std::string value;
        op_data->op_info_.SerializeToString(&value);
        std::string node = zk_op_path_ + "/" + boost::lexical_cast<std::string>(*iter);
        if (zk_client_->SetNodeValue(node, value)) {
            LOG(DEBUG, "set zk status value success. node[%s] value[%s]",
                        node.c_str(), value.c_str());
            continue;
        }
        LOG(WARNING, "set zk status value failed! node[%s] op_id[%lu] op_type[%s] task_index[%u]", 
                      node.c_str(), *iter, 
                      ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                      op_data->op_info_.task_index()); 
    }
    return 0;
}

int NameServerImpl::DeleteTask() {
    std::vector<uint64_t> done_task_vec;
    std::vector<std::shared_ptr<TabletClient>> client_vec;
    {
        MutexLock lock(&mu_);
        for (auto iter = task_map_.begin(); iter != task_map_.end(); iter++) {
            if (iter->second->task_list_.empty()) {
                done_task_vec.push_back(iter->first);
            }
        }
        if (done_task_vec.empty()) {
            LOG(DEBUG, "has not done task. return");
            return 0;
        }
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                LOG(DEBUG, "tablet[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            client_vec.push_back(iter->second->client_);
        }
    }
    bool has_failed = false;
    for (auto iter = client_vec.begin(); iter != client_vec.end(); ++iter) {
        if (!(*iter)->DeleteOPTask(done_task_vec)) {
            LOG(WARNING, "tablet[%s] delete op failed", (*iter)->GetEndpoint().c_str()); 
            has_failed = true;
            continue;
        }
        LOG(DEBUG, "tablet[%s] delete op success", (*iter)->GetEndpoint().c_str()); 
    }
    if (!has_failed) {
        for (auto iter = done_task_vec.begin(); iter != done_task_vec.end(); ++iter) {
            std::string node = zk_op_path_ + "/" + boost::lexical_cast<std::string>(*iter);
            if (zk_client_->DeleteNode(node)) {
                MutexLock lock(&mu_);
                task_map_.erase(*iter);
                LOG(DEBUG, "delete op[%lu]", *iter); 
            } else {
                LOG(WARNING, "zk delete op_node failed. opid[%lu]", *iter); 
            }
        }
    }
    return 0;
}

void NameServerImpl::ProcessTask() {
    while (running_.load(std::memory_order_relaxed)) {
        std::vector<uint64_t> run_task_vec;
        {
            MutexLock lock(&mu_);
            while (task_map_.empty()) {
                cv_.Wait();
            }
            if (running_.load(std::memory_order_relaxed)) {
                break;
            }
            
            for (auto iter = task_map_.begin(); iter != task_map_.end(); iter++) {
                if (iter->second->task_list_.empty()) {
                    continue;
                }
                std::shared_ptr<Task> task = iter->second->task_list_.front();
                if (task->task_info_->status() == ::rtidb::api::kDone) {
                    iter->second->task_list_.pop_front();
                } else if (task->task_info_->status() == ::rtidb::api::kFailed) {
                    // TODO.denglong tackle failed case here
                }
                if (iter->second->task_list_.empty()) {
                    LOG(DEBUG, "operation has finished! op_id[%lu]", iter->first);
                    continue;
                }
                task_thread_pool_.AddTask(task->fun_);
                run_task_vec.push_back(iter->first);

            }
        }
        UpdateZKTaskStatus(run_task_vec);
        DeleteTask();
    }
}

void NameServerImpl::MakeSnapshotNS(RpcController* controller,
        const MakeSnapshotNSRequest* request,
        GeneralResponse* response,
        Closure* done) {
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        LOG(WARNING, "cur nameserver is not leader");
        done->Run();
        return;
    }
    MutexLock lock(&mu_);
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("get table info failed");
        LOG(WARNING, "get table info failed! name[%s]", request->name().c_str());
        done->Run();
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableMeta> table_meta = iter->second;
    uint32_t tid = table_meta->tid();
    uint32_t pid = request->pid();
    std::string endpoint;
    for (int idx = 0; idx < table_meta->table_partition_size(); idx++) {
        if (table_meta->table_partition(idx).pid() == pid) {
            endpoint = table_meta->table_partition(idx).endpoint();
            break;
        }
    }
    if (endpoint.empty()) {
        response->set_code(-1);
        response->set_msg("partition not exisit");
        LOG(WARNING, "partition[%u] not exisit", pid);
        done->Run();
        return;
    }
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("tablet is not online");
        LOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        done->Run();
        return;
    }

    if (!zk_client_->SetNodeValue(zk_op_index_node_, boost::lexical_cast<std::string>(op_index_ + 1))) {
        response->set_code(-1);
        response->set_msg("set op index node failed");
        LOG(WARNING, "set op index node failed! op_index[%s]", op_index_);
        done->Run();
        return;
    }
    op_index_++;

    std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(::rtidb::api::OPType::kMakeSnapshotOP);
    op_data->op_info_.set_task_index(0);
    std::string value;
    request->SerializeToString(&value);
    op_data->op_info_.set_data(value);

    value.clear();
    op_data->op_info_.SerializeToString(&value);
    std::string node = zk_op_path_ + "/" + boost::lexical_cast<std::string>(op_index_);
    if (!zk_client_->CreateNode(node, value)) {
        response->set_code(-1);
        response->set_msg("create op node failed");
        LOG(WARNING, "create op node[%s] failed", node.c_str());
        done->Run();
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();

    std::shared_ptr<::rtidb::api::TaskInfo> task_info = std::make_shared<::rtidb::api::TaskInfo>();
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, task_info);
    task->task_info_->set_op_id(op_index_);
    task->task_info_->set_op_type(::rtidb::api::OPType::kMakeSnapshotOP);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kMakeSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kDoing);
    task->fun_ = boost::bind(&TabletClient::MakeSnapshotNS, it->second->client_, tid, pid, task->task_info_);
    op_data->task_list_.push_back(task);
    task_map_.insert(std::make_pair(op_index_, op_data));
    cv_.Signal();
}

int NameServerImpl::ConstructCreateTableTask(std::shared_ptr<::rtidb::nameserver::TableMeta> table_meta,
            bool is_leader,
            std::list<std::shared_ptr<Task>>& task_list, 
            std::map<uint32_t, std::vector<std::string>>& endpoint_map) {
    mu_.AssertHeld();        
    for (int idx = 0; idx < table_meta->table_partition_size(); idx++) {
        const ::rtidb::nameserver::TablePartition& table_partition = table_meta->table_partition(idx);
        if (table_partition.is_leader() != is_leader) {
            continue;
        }
        auto iter = tablets_.find(table_partition.endpoint());
        // check tablet if exist
        if (iter == tablets_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_partition.endpoint().c_str());
            continue;
        }
        // check tablet healthy
        if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            LOG(WARNING, "endpoint [%s] is offline", table_partition.endpoint().c_str());
            continue;
        }
        std::vector<std::string> endpoint;
        if (is_leader) {
            if (endpoint_map.find(table_partition.pid()) != endpoint_map.end()) {
                endpoint_map[table_partition.pid()].swap(endpoint);
            }
        } else {
            if (endpoint_map.find(table_partition.pid()) == endpoint_map.end()) {
                endpoint_map.insert(std::make_pair(table_partition.pid(), std::vector<std::string>()));
            }
            endpoint_map[table_partition.pid()].push_back(table_partition.endpoint());
        }
        std::shared_ptr<::rtidb::api::TaskInfo> task_info = std::make_shared<::rtidb::api::TaskInfo>();
        std::shared_ptr<Task> task = std::make_shared<Task>(table_partition.endpoint(), task_info);
        task->task_info_->set_op_id(op_index_);
        task->task_info_->set_op_type(::rtidb::api::OPType::kCreateTableOP);
        task->task_info_->set_task_type(::rtidb::api::TaskType::kCreateTable);
        task->task_info_->set_status(::rtidb::api::TaskStatus::kDoing);
        task->fun_ = boost::bind(&TabletClient::CreateTable, iter->second->client_, table_meta->name(), 
                                table_index_, table_partition.pid(),
                                table_meta->ttl(), is_leader, endpoint, table_meta->seg_cnt());
        task_list.push_back(task);
        LOG(DEBUG, "add create table task. tid[%u] pid[%u] endpoint[%s] success", 
                    table_index_, table_partition.pid(), table_partition.endpoint().c_str());
    }
    return 0;
}

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        LOG(WARNING, "cur nameserver is not leader");
        done->Run();
        return;
    }
    MutexLock lock(&mu_);
    std::shared_ptr<::rtidb::nameserver::TableMeta> table_meta(request->table_meta().New());
    table_meta->CopyFrom(request->table_meta());
    if (table_info_.find(table_meta->name()) != table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is already exisit!");
        LOG(WARNING, "table[%s] is already exisit!", table_meta->name().c_str());
        done->Run();
        return;
    }
    if (!zk_client_->SetNodeValue(zk_table_index_node_, 
                boost::lexical_cast<std::string>(table_index_ + 1))) {
        response->set_code(-1);
        response->set_msg("set table index node failed");
        LOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
        done->Run();
        return;
    }
    table_index_++;

    table_meta->set_tid(table_index_);
    std::string table_value;
    table_meta->SerializeToString(&table_value);
    if (!zk_client_->CreateNode(zk_table_path_ + "/" + table_meta->name(), table_value)) {
        LOG(WARNING, "create table node[%s/%s] failed! value[%s]", zk_table_path_.c_str(), table_meta->name().c_str(), table_value.c_str());
        response->set_code(-1);
        response->set_msg("create table node failed");
        done->Run();
        return;
    }
    LOG(DEBUG, "create table node[%s/%s] success! value[%s]", zk_table_path_.c_str(), table_meta->name().c_str(), table_value.c_str());

    if (!zk_client_->SetNodeValue(zk_op_index_node_, boost::lexical_cast<std::string>(op_index_ + 1))) {
        response->set_code(-1);
        response->set_msg("set op index node failed");
        LOG(WARNING, "set op index node failed! op_index[%s]", op_index_);
        done->Run();
        return;
    }
    op_index_++;

    std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(::rtidb::api::OPType::kCreateTableOP);
    op_data->op_info_.set_task_index(0);
    std::string value;
    request->SerializeToString(&value);
    op_data->op_info_.set_data(value);

    value.clear();
    op_data->op_info_.SerializeToString(&value);
    std::string node = zk_op_path_ + "/" + boost::lexical_cast<std::string>(op_index_);
    if (!zk_client_->CreateNode(node, value)) {
        response->set_code(-1);
        response->set_msg("create op node failed");
        LOG(WARNING, "create op node[%s] failed", node.c_str());
        done->Run();
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
    table_info_.insert(std::make_pair(table_meta->name(), table_meta));

    std::map<uint32_t, std::vector<std::string>> endpoint_map;
    ConstructCreateTableTask(table_meta, false, op_data->task_list_, endpoint_map);
    ConstructCreateTableTask(table_meta, true, op_data->task_list_, endpoint_map);
    task_map_.insert(std::make_pair(op_index_, op_data));
    cv_.Signal();
}

void NameServerImpl::OnLocked() {
    LOG(INFO, "become the leader name server");
    bool ok = Recover();
    if (!ok) {
        //TODO fail to recover discard the lock
    }
    running_.store(true, std::memory_order_release);
    task_thread_pool_.DelayTask(FLAGS_get_task_status_interval, boost::bind(&NameServerImpl::UpdateTaskStatus, this));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::ProcessTask, this));
}

void NameServerImpl::OnLostLock() {
    LOG(INFO, "become the stand by name sever");
    running_.store(false, std::memory_order_release);
}

}
}
