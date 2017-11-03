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
#include "base/strings.h"

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(get_task_status_interval);
DECLARE_int32(name_server_task_pool_size);
DECLARE_int32(name_server_task_wait_time);

namespace rtidb {
namespace nameserver {

using ::baidu::common::MutexLock;

NameServerImpl::NameServerImpl():mu_(), tablets_(),
    table_info_(), zk_client_(NULL), dist_lock_(NULL), thread_pool_(1), 
    task_thread_pool_(FLAGS_name_server_task_pool_size), cv_(&mu_) {
    std::string zk_table_path = FLAGS_zk_root_path + "/table";
    zk_table_index_node_ = zk_table_path + "/table_index";
    zk_table_data_path_ = zk_table_path + "/table_data";
    std::string zk_op_path = FLAGS_zk_root_path + "/op";
    zk_op_index_node_ = zk_op_path + "/op_index";
    zk_op_data_path_ = zk_op_path + "/op_data";
    running_.store(false, std::memory_order_release);
}

NameServerImpl::~NameServerImpl() {
    running_.store(false, std::memory_order_release);
    thread_pool_.Stop(true);
    task_thread_pool_.Stop(true);
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

    if (!RecoverTableInfo()) {
        LOG(WARNING, "recover table info failed!");
        return false;
    }

    if (!RecoverOPTask()) {
        LOG(WARNING, "recover task failed!");
        return false;
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

bool NameServerImpl::RecoverTableInfo() {
    table_info_.clear();
    std::vector<std::string> table_vec;
    if (!zk_client_->GetChildren(zk_table_data_path_, table_vec)) {
        if (zk_client_->IsExistNode(zk_table_data_path_) > 0) {
            LOG(WARNING, "table data node is not exist");
            return true;
        }
        LOG(WARNING, "get table name failed!");
        return false;
    }
    LOG(INFO, "need to recover table num[%d]", table_vec.size());
    for (const auto& table_name : table_vec) {
        std::string table_name_node = zk_table_data_path_ + "/" + table_name;
        std::string value;
        if (!zk_client_->GetNodeValue(table_name_node, value)) {
            LOG(WARNING, "get table info failed! table node[%s]", table_name_node.c_str());
            return false;
        }
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = 
                    std::make_shared<::rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            LOG(WARNING, "parse table info failed! value[%s]", value.c_str());
            return false;
        }
        table_info_.insert(std::make_pair(table_name, table_info));
        LOG(INFO, "recover table[%s] success", table_name.c_str());
    }
    return true;
}

bool NameServerImpl::RecoverOPTask() {
    task_map_.clear();
    std::vector<std::string> op_vec;
    if (!zk_client_->GetChildren(zk_op_data_path_, op_vec)) {
        if (zk_client_->IsExistNode(zk_op_data_path_) > 0) {
            LOG(WARNING, "op data node is not exist");
            return true;
        }
        LOG(WARNING, "get op failed!");
        return false;
    }
    LOG(INFO, "need to recover op num[%d]", op_vec.size());
    for (const auto& op_id : op_vec) {
        std::string op_node = zk_op_data_path_ + "/" + op_id;
        std::string value;
        if (!zk_client_->GetNodeValue(op_node, value)) {
            LOG(WARNING, "get table info failed! table node[%s]", op_node.c_str());
            return false;
        }
        std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
        if (!op_data->op_info_.ParseFromString(value)) {
            LOG(WARNING, "parse op info failed! value[%s]", value.c_str());
            return false;
        }

        switch (op_data->op_info_.op_type()) {
            case ::rtidb::api::OPType::kMakeSnapshotOP:
                if (!RecoverMakeSnapshot(op_data)) {
                    LOG(WARNING, "recover op[%s] failed", 
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
                    return false;
                }
                break;
            // add other op recover code here    
            default:
                LOG(WARNING, "unsupport recover op[%s]!", 
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
                return false;
        }

        uint64_t cur_op_id = boost::lexical_cast<uint64_t>(op_id);;
        task_map_.insert(std::make_pair(cur_op_id, op_data));
        LOG(INFO, "recover op[%s] success. op_id[%lu]", 
                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(), cur_op_id);
    }

    return true;
}

bool NameServerImpl::RecoverMakeSnapshot(std::shared_ptr<OPData> op_data) {
    if (!op_data->op_info_.IsInitialized()) {
        LOG(WARNING, "op_info is not init!");
        return false;
    }
    if (op_data->op_info_.op_type() != ::rtidb::api::OPType::kMakeSnapshotOP) {
        LOG(WARNING, "op_type[%s] is not match", 
                    ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
        return false;
    }
    MakeSnapshotNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        LOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return false;
    }
    auto iter = table_info_.find(request.name());
    if (iter == table_info_.end()) {
        LOG(WARNING, "get table info failed! name[%s]", request.name().c_str());
        return false;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    uint32_t pid = request.pid();
    std::string endpoint;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() == pid) {
            endpoint = table_info->table_partition(idx).endpoint();
            break;
        }
    }
    if (endpoint.empty()) {
        LOG(WARNING, "partition[%u] not exisit. table name is [%s]", pid, request.name().c_str());
        return false;
    }
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        LOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return false;
    }
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_data->op_info_.op_id());
    task->task_info_->set_op_type(::rtidb::api::OPType::kMakeSnapshotOP);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kMakeSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::MakeSnapshotNS, it->second->client_, tid, pid, task->task_info_);
    op_data->task_list_.push_back(task);

    SkipDoneTask(op_data->op_info_.task_index(), op_data->task_list_);
    return true;
}

void NameServerImpl::SkipDoneTask(uint32_t task_index, std::list<std::shared_ptr<Task>>& task_list) {
    for (uint32_t idx = 0; idx < task_index; idx++) {
        std::shared_ptr<Task> task = task_list.front();
        LOG(INFO, "task has done, remove from task_list. op_id[%lu] op_type[%s] task_type[%s]",
                        task->task_info_->op_id(),
                        ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                        ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
        task_list.pop_front();
    }
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
                if (task->task_info_->task_type() == response.task(idx).task_type() && 
                        task->task_info_->status() != response.task(idx).status()) {
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

int NameServerImpl::UpdateZKTaskStatus() {
    std::vector<uint64_t> done_vec;
    {
        MutexLock lock(&mu_);
        for (const auto& kv : task_map_) {
            if (kv.second->task_list_.empty()) {
                continue;
            }
            std::shared_ptr<Task> task = kv.second->task_list_.front();
            if (task->task_info_->status() == ::rtidb::api::kDone) {
                done_vec.push_back(kv.first);
            }
        }
    }
    for (auto op_id : done_vec) {
        std::shared_ptr<OPData> op_data;
        {
            MutexLock lock(&mu_);
            auto pos = task_map_.find(op_id);
            if (pos == task_map_.end()) {
                LOG(WARNING, "cannot find op[%lu] in task_map", op_id);
                continue;
            }
            op_data = pos->second;
        }
        uint32_t cur_task_index = op_data->op_info_.task_index();
        op_data->op_info_.set_task_index(cur_task_index + 1);
        std::string value;
        op_data->op_info_.SerializeToString(&value);
        std::string node = zk_op_data_path_ + "/" + boost::lexical_cast<std::string>(op_id);
        if (zk_client_->SetNodeValue(node, value)) {
            LOG(DEBUG, "set zk status value success. node[%s] value[%s]",
                        node.c_str(), value.c_str());
            op_data->task_list_.pop_front();
            continue;
        }
        // revert task index
        op_data->op_info_.set_task_index(cur_task_index);
        LOG(WARNING, "set zk status value failed! node[%s] op_id[%lu] op_type[%s] task_index[%u]", 
                      node.c_str(), op_id, 
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
            if (iter->second->task_list_.empty() && 
                    iter->second->task_status_ == ::rtidb::api::kDoing) {
                done_task_vec.push_back(iter->first);
            }
        }
        if (done_task_vec.empty()) {
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
        for (auto op_id : done_task_vec) {
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_id);
            if (zk_client_->DeleteNode(node)) {
                LOG(DEBUG, "delete zk op node[%s] success.", node.c_str()); 
                MutexLock lock(&mu_);
                auto pos = task_map_.find(op_id);
                if (pos != task_map_.end()) {
                    pos->second->end_time_ = time(0);
                    pos->second->task_status_ = ::rtidb::api::kDone;
                }
            } else {
                LOG(WARNING, "delete zk op_node failed. opid[%lu] node[%s]", op_id, node.c_str()); 
            }
        }
    }
    return 0;
}

void NameServerImpl::ProcessTask() {
    while (running_.load(std::memory_order_acquire)) {
        {
            MutexLock lock(&mu_);
            while (task_map_.empty()) {
                cv_.TimeWait(FLAGS_name_server_task_wait_time);
                if (!running_.load(std::memory_order_acquire)) {
                    return;
                }
            }
            
            for (auto iter = task_map_.begin(); iter != task_map_.end(); iter++) {
                if (iter->second->task_list_.empty()) {
                    continue;
                }
                std::shared_ptr<Task> task = iter->second->task_list_.front();
                if (task->task_info_->status() == ::rtidb::api::kFailed) {
                    // TODO.denglong tackle failed case here
                } else if (task->task_info_->status() == ::rtidb::api::kInited) {
                    LOG(DEBUG, "run task. opid[%lu] op_type[%s] task_type[%s]", iter->first, 
                                ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(), 
                                ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str()); 
                    task_thread_pool_.AddTask(task->fun_);
                    task->task_info_->set_status(::rtidb::api::kDoing);;
                }
            }
        }
        UpdateZKTaskStatus();
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
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    uint32_t pid = request->pid();
    std::string endpoint;
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() == pid) {
            endpoint = table_info->table_partition(idx).endpoint();
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
    op_data->start_time_ = time(0);
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(::rtidb::api::OPType::kMakeSnapshotOP);
    op_data->op_info_.set_task_index(0);
    std::string value;
    request->SerializeToString(&value);
    op_data->op_info_.set_data(value);
    op_data->task_status_ = ::rtidb::api::kDoing;

    value.clear();
    op_data->op_info_.SerializeToString(&value);
    std::string node = zk_op_data_path_ + "/" + boost::lexical_cast<std::string>(op_index_);
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
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index_);
    task->task_info_->set_op_type(::rtidb::api::OPType::kMakeSnapshotOP);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kMakeSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::MakeSnapshotNS, it->second->client_, tid, pid, task->task_info_);
    op_data->task_list_.push_back(task);
    task_map_.insert(std::make_pair(op_index_, op_data));
    LOG(DEBUG, "add makesnapshot op ok. op_id[%lu]", op_index_);
    cv_.Signal();
}

int NameServerImpl::CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader,
            std::map<uint32_t, std::vector<std::string>>& endpoint_map) {
    mu_.AssertHeld();        
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        const ::rtidb::nameserver::TablePartition& table_partition = table_info->table_partition(idx);
        if (table_partition.is_leader() != is_leader) {
            continue;
        }
        auto iter = tablets_.find(table_partition.endpoint());
        // check tablet if exist
        if (iter == tablets_.end()) {
            LOG(WARNING, "endpoint[%s] can not find client", table_partition.endpoint().c_str());
            return -1;
        }
        // check tablet healthy
        if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            LOG(WARNING, "endpoint [%s] is offline", table_partition.endpoint().c_str());
            return -1;
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
        if (!iter->second->client_->CreateTable(table_info->name(), table_index_, table_partition.pid(), 
                                table_info->ttl(), is_leader, endpoint, table_info->seg_cnt())) {

            LOG(WARNING, "create table failed. tid[%u] pid[%u] endpoint[%s]", 
                    table_index_, table_partition.pid(), table_partition.endpoint().c_str());
            return -1;

        }
        LOG(INFO, "create table success. tid[%u] pid[%u] endpoint[%s]", 
                    table_index_, table_partition.pid(), table_partition.endpoint().c_str());
    }
    return 0;
}

void NameServerImpl::ShowOPStatus(RpcController* controller,
		const ShowOPStatusRequest* request,
		ShowOPStatusResponse* response,
		Closure* done) {
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        LOG(WARNING, "cur nameserver is not leader");
        done->Run();
        return;
    }
    MutexLock lock(&mu_);
	for (const auto& kv : task_map_) {
		OPStatus* op_status = response->add_op_status();
		op_status->set_op_id(kv.first);
		op_status->set_op_type(::rtidb::api::OPType_Name(kv.second->op_info_.op_type()));
		if (kv.second->task_list_.empty()) {
			op_status->set_status("Done");
			op_status->set_task_type("-");
		} else { 
			std::shared_ptr<Task> task = kv.second->task_list_.front();
			op_status->set_task_type(::rtidb::api::TaskType_Name(task->task_info_->task_type()));
			if (task->task_info_->status() == ::rtidb::api::kFailed) {
				op_status->set_status("Failed");
			} else {
				op_status->set_status("Doing");
			}
		}
		op_status->set_start_time(kv.second->start_time_);
		op_status->set_end_time(kv.second->end_time_);
	}
	response->set_code(0);
	response->set_msg("ok");
	done->Run();
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
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info(request->table_info().New());
    table_info->CopyFrom(request->table_info());
    if (table_info_.find(table_info->name()) != table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is already exisit!");
        LOG(WARNING, "table[%s] is already exisit!", table_info->name().c_str());
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
    table_info->set_tid(table_index_);
    std::map<uint32_t, std::vector<std::string>> endpoint_map;
    if (CreateTableOnTablet(table_info, false, endpoint_map) < 0 || 
            CreateTableOnTablet(table_info, true, endpoint_map) < 0) {
        response->set_code(-1);
        response->set_msg("create table failed");
        LOG(WARNING, "create table failed. tid[%u]", table_index_);
        done->Run();
        return;
    }

    std::string table_value;
    table_info->SerializeToString(&table_value);
    if (!zk_client_->CreateNode(zk_table_data_path_ + "/" + table_info->name(), table_value)) {
        LOG(WARNING, "create table node[%s/%s] failed! value[%s]", zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str());
        response->set_code(-1);
        response->set_msg("create table node failed");
        done->Run();
        return;
    }

    LOG(DEBUG, "create table node[%s/%s] success! value[%s]", zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str());
    table_info_.insert(std::make_pair(table_info->name(), table_info));
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
    task_thread_pool_.DelayTask(FLAGS_get_task_status_interval, boost::bind(&NameServerImpl::UpdateTaskStatus, this));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::ProcessTask, this));
}

void NameServerImpl::OnLostLock() {
    LOG(INFO, "become the stand by name sever");
    running_.store(false, std::memory_order_release);
}

}
}
