//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "nameserver/name_server_impl.h"

#include <gflags/gflags.h>
#include "gflags/gflags.h"
#include "timer.h"
#include <strings.h>
#include "base/strings.h"
#include <chrono>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(get_task_status_interval);
DECLARE_int32(name_server_task_pool_size);
DECLARE_int32(name_server_task_wait_time);
DECLARE_int32(tablet_startup_wait_time);
DECLARE_bool(auto_failover);
DECLARE_bool(auto_recover_table);

namespace rtidb {
namespace nameserver {

NameServerImpl::NameServerImpl():mu_(), tablets_(),
    table_info_(), zk_client_(NULL), dist_lock_(NULL), thread_pool_(1), 
    task_thread_pool_(FLAGS_name_server_task_pool_size), cv_() {
    std::string zk_table_path = FLAGS_zk_root_path + "/table";
    zk_table_index_node_ = zk_table_path + "/table_index";
    zk_table_data_path_ = zk_table_path + "/table_data";
    zk_term_node_ = zk_table_path + "/term";
    std::string zk_op_path = FLAGS_zk_root_path + "/op";
    zk_op_index_node_ = zk_op_path + "/op_index";
    zk_op_data_path_ = zk_op_path + "/op_data";
    std::string zk_config_path = FLAGS_zk_root_path + "/config";
    zk_auto_failover_node_ = zk_config_path + "/auto_failover";
    zk_auto_recover_table_node_ = zk_config_path + "/auto_recover_table";
    running_.store(false, std::memory_order_release);
    auto_failover_.store(FLAGS_auto_failover, std::memory_order_release);
    auto_recover_table_.store(FLAGS_auto_recover_table, std::memory_order_release);
}

NameServerImpl::~NameServerImpl() {
    running_.store(false, std::memory_order_release);
    thread_pool_.Stop(true);
    task_thread_pool_.Stop(true);
    delete zk_client_;
}

// become name server leader
bool NameServerImpl::Recover() {
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        PDLOG(WARNING, "get endpoints node failed!");
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    UpdateTablets(endpoints);

    std::string value;
    if (!zk_client_->GetNodeValue(zk_table_index_node_, value)) {
        if (!zk_client_->CreateNode(zk_table_index_node_, "1")) {
            PDLOG(WARNING, "create table index node failed!");
            return false;
        }
        table_index_ = 1;
        PDLOG(INFO, "init table_index[%u]", table_index_);
    } else {
        table_index_ = std::stoull(value);
        PDLOG(INFO, "recover table_index[%u]", table_index_);
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_term_node_, value)) {
        if (!zk_client_->CreateNode(zk_term_node_, "1")) {
            PDLOG(WARNING, "create term node failed!");
            return false;
        }
        term_ = 1;
        PDLOG(INFO, "init term[%lu]", term_);
    } else {
        term_ = std::stoull(value);
        PDLOG(INFO, "recover term[%u]", term_);
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_op_index_node_, value)) {
        if (!zk_client_->CreateNode(zk_op_index_node_, "1")) {
            PDLOG(WARNING, "create op index node failed!");
            return false;
        }
        op_index_ = 1;
        PDLOG(INFO, "init op_index[%u]", op_index_);
    } else {
        op_index_ = std::stoull(value);
        PDLOG(INFO, "recover op_index[%u]", op_index_);
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_auto_failover_node_, value)) {
        auto_failover_.load(std::memory_order_acquire) ? value = "1" : value = "0";
        if (!zk_client_->CreateNode(zk_auto_failover_node_, value)) {
            PDLOG(WARNING, "create auto failover node failed!");
            return false;
        }
        PDLOG(INFO, "set zk_auto_failover_node %s", value.c_str());
    } else {
        value == "1" ? auto_failover_.store(true, std::memory_order_release) :
                       auto_failover_.store(false, std::memory_order_release);
        PDLOG(INFO, "get zk_auto_failover_node %s", value.c_str());
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_auto_recover_table_node_, value)) {
        auto_recover_table_.load(std::memory_order_acquire) ? value = "1" : value = "0";
        if (!zk_client_->CreateNode(zk_auto_recover_table_node_, value)) {
            PDLOG(WARNING, "create auto recover table node failed!");
            return false;
        }
        PDLOG(INFO, "set zk_auto_recover_table_node %s", value.c_str());
    } else {
        value == "1" ? auto_recover_table_.store(true, std::memory_order_release) :
                       auto_recover_table_.store(false, std::memory_order_release);
        PDLOG(INFO, "get zk_auto_recover_table_node %s", value.c_str());

    }

    if (!RecoverTableInfo()) {
        PDLOG(WARNING, "recover table info failed!");
        return false;
    }

    if (!RecoverOPTask()) {
        PDLOG(WARNING, "recover task failed!");
        return false;
    }
    return true;
}

bool NameServerImpl::RecoverTableInfo() {
    table_info_.clear();
    std::vector<std::string> table_vec;
    if (!zk_client_->GetChildren(zk_table_data_path_, table_vec)) {
        if (zk_client_->IsExistNode(zk_table_data_path_) > 0) {
            PDLOG(WARNING, "table data node is not exist");
            return true;
        }
        PDLOG(WARNING, "get table name failed!");
        return false;
    }
    PDLOG(INFO, "need to recover table num[%d]", table_vec.size());
    for (const auto& table_name : table_vec) {
        std::string table_name_node = zk_table_data_path_ + "/" + table_name;
        std::string value;
        if (!zk_client_->GetNodeValue(table_name_node, value)) {
            PDLOG(WARNING, "get table info failed! table node[%s]", table_name_node.c_str());
            return false;
        }
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = 
                    std::make_shared<::rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            PDLOG(WARNING, "parse table info failed! value[%s]", value.c_str());
            return false;
        }
        table_info_.insert(std::make_pair(table_name, table_info));
        PDLOG(INFO, "recover table[%s] success", table_name.c_str());
    }
    return true;
}

bool NameServerImpl::RecoverOPTask() {
    task_map_.clear();
    std::vector<std::string> op_vec;
    if (!zk_client_->GetChildren(zk_op_data_path_, op_vec)) {
        if (zk_client_->IsExistNode(zk_op_data_path_) > 0) {
            PDLOG(WARNING, "op data node is not exist");
            return true;
        }
        PDLOG(WARNING, "get op failed!");
        return false;
    }
    PDLOG(INFO, "need to recover op num[%d]", op_vec.size());
    for (const auto& op_id : op_vec) {
        std::string op_node = zk_op_data_path_ + "/" + op_id;
        std::string value;
        if (!zk_client_->GetNodeValue(op_node, value)) {
            PDLOG(WARNING, "get table info failed! table node[%s]", op_node.c_str());
            return false;
        }
        std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
        op_data->start_time_ = ::baidu::common::timer::now_time();
        op_data->task_status_ = ::rtidb::api::kDoing;
        if (!op_data->op_info_.ParseFromString(value)) {
            PDLOG(WARNING, "parse op info failed! value[%s]", value.c_str());
            return false;
        }

        switch (op_data->op_info_.op_type()) {
            case ::rtidb::api::OPType::kMakeSnapshotOP:
                if (!RecoverMakeSnapshot(op_data)) {
                    PDLOG(WARNING, "recover op[%s] failed", 
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
                    return false;
                }
                break;
            case ::rtidb::api::OPType::kAddReplicaOP:
                if (!RecoverAddReplica(op_data)) {
                    PDLOG(WARNING, "recover op[%s] failed", 
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
                    return false;
                }
                break;
            default:
                PDLOG(WARNING, "unsupport recover op[%s]!", 
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
                return false;
        }

        uint64_t cur_op_id = std::stoull(op_id);;
        task_map_.insert(std::make_pair(cur_op_id, op_data));
        PDLOG(INFO, "recover op[%s] success. op_id[%lu]", 
                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(), cur_op_id);
    }

    return true;
}

bool NameServerImpl::RecoverMakeSnapshot(std::shared_ptr<OPData> op_data) {
    if (!op_data->op_info_.IsInitialized()) {
        PDLOG(WARNING, "op_info is not init!");
        return false;
    }
    if (op_data->op_info_.op_type() != ::rtidb::api::OPType::kMakeSnapshotOP) {
        PDLOG(WARNING, "op_type[%s] is not match", 
                    ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
        return false;
    }
    MakeSnapshotNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return false;
    }
    auto iter = table_info_.find(request.name());
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "get table info failed! name[%s]", request.name().c_str());
        return false;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    uint32_t pid = request.pid();
    std::string endpoint;
    if (GetLeader(table_info, pid, endpoint) < 0 || endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return false;
    }
    std::shared_ptr<Task> task = CreateMakeSnapshotTask(endpoint, op_data->op_info_.op_id(), 
                ::rtidb::api::OPType::kMakeSnapshotOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create makesnapshot task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);

    SkipDoneTask(op_data->op_info_.task_index(), op_data->task_list_);
    return true;
}

void NameServerImpl::SkipDoneTask(uint32_t task_index, std::list<std::shared_ptr<Task>>& task_list) {
    for (uint32_t idx = 0; idx < task_index; idx++) {
        std::shared_ptr<Task> task = task_list.front();
        PDLOG(INFO, "task has done, remove from task_list. op_id[%lu] op_type[%s] task_type[%s]",
                        task->task_info_->op_id(),
                        ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                        ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
        task_list.pop_front();
    }
}

void NameServerImpl::UpdateTabletsLocked(const std::vector<std::string>& endpoints) {
    std::lock_guard<std::mutex> lock(mu_);
    UpdateTablets(endpoints);
}


void NameServerImpl::UpdateTablets(const std::vector<std::string>& endpoints) {
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
            if (tablet->client_->Init() != 0) {
                PDLOG(WARNING, "tablet client init error. endpoint %s", it->c_str());
                continue;
            }
            tablet->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            tablets_.insert(std::make_pair(*it, tablet));
            PDLOG(INFO, "add tablet client. endpoint %s", it->c_str());
        } else {
            //TODO wangtaize notify if state changes
            ::rtidb::api::TabletState old = tit->second->state_;
            tit->second->state_ = ::rtidb::api::TabletState::kTabletHealthy;
            if (old != ::rtidb::api::TabletState::kTabletHealthy) {
                if (tit->second->client_->Reconnect() < 0) {
                    PDLOG(WARNING, "tablet client reconnect error. endpoint %s", it->c_str());
                    continue;
                }
                tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
                PDLOG(INFO, "tablet is online. endpoint %s", tit->first.c_str());
                if (auto_recover_table_.load(std::memory_order_acquire)) {
                    // wait until the tablet serivce start ok
                    thread_pool_.DelayTask(FLAGS_tablet_startup_wait_time, 
                            boost::bind(&NameServerImpl::OnTabletOnline, this, tit->first));
                }
            }
        }
        PDLOG(INFO, "healthy tablet with endpoint %s", it->c_str());
    }
    // handle offline tablet
    Tablets::iterator tit = tablets_.begin();
    for (; tit !=  tablets_.end(); ++tit) {
        if (alive.find(tit->first) == alive.end() 
                && tit->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
            // tablet offline
            PDLOG(INFO, "offline tablet with endpoint %s", tit->first.c_str());
            tit->second->state_ = ::rtidb::api::TabletState::kTabletOffline;
            if (auto_failover_.load(std::memory_order_acquire)) {
                thread_pool_.AddTask(boost::bind(&NameServerImpl::OnTabletOffline, this, tit->first));
            }
        }
    }
}

void NameServerImpl::OnTabletOffline(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        std::set<uint32_t> leader_pid;
        std::set<uint32_t> follower_pid;
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                // tackle the alive partition only
                if (kv.second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint &&
                        kv.second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    if (kv.second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        leader_pid.insert(kv.second->table_partition(idx).pid());
                    } else {
                        follower_pid.insert(kv.second->table_partition(idx).pid());
                    }
                }
            }
        }
        for (auto pid : leader_pid) {
            // change leader
            PDLOG(INFO, "table %s pid %u change leader", kv.first.c_str(), pid);
            CreateChangeLeaderOP(kv.first, pid);
        }
        // delete replica
        DelReplicaData del_replica_data;
        del_replica_data.set_name(kv.first);
        del_replica_data.set_endpoint(endpoint);
        for (auto pid : follower_pid) {
            del_replica_data.set_pid(pid);
            CreateDelReplicaOP(del_replica_data, ::rtidb::api::OPType::kOfflineReplicaOP);
        }
    }
}

void NameServerImpl::OnTabletOnline(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid =  kv.second->table_partition(idx).pid();
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (kv.second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    thread_pool_.AddTask(boost::bind(&NameServerImpl::RecoverTable, this, kv.first, pid, endpoint));
                    PDLOG(INFO, "recover table %s pid %u endpoint %s", kv.first.c_str(), pid, endpoint.c_str());
                }
            }
        }
    }
}

void NameServerImpl::ShowTablet(RpcController* controller,
            const ShowTabletRequest* request,
            ShowTabletResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    Tablets::iterator it = tablets_.begin();
    for (; it !=  tablets_.end(); ++it) {
        TabletStatus* status = response->add_tablets();
        status->set_endpoint(it->first);
        status->set_state(::rtidb::api::TabletState_Name(it->second->state_));
        status->set_age(::baidu::common::timer::get_micros() / 1000 - it->second->ctime_);
    }
    response->set_code(0);
    response->set_msg("ok");
}

bool NameServerImpl::Init() {
    if (FLAGS_zk_cluster.empty()) {
        PDLOG(WARNING, "zk cluster disabled");
        return false;
    }
    zk_client_ = new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
            FLAGS_endpoint, FLAGS_zk_root_path);
    if (!zk_client_->Init()) {
        PDLOG(WARNING, "fail to init zookeeper with cluster %s", FLAGS_zk_cluster.c_str());
        return false;
    }
    std::string value;
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        zk_client_->CreateNode(FLAGS_zk_root_path + "/nodes", "");
    }
    zk_client_->WatchNodes(boost::bind(&NameServerImpl::UpdateTabletsLocked, this, _1));
    zk_client_->WatchNodes();

    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
    dist_lock_ = new DistLock(FLAGS_zk_root_path + "/leader", zk_client_, 
            boost::bind(&NameServerImpl::OnLocked, this), boost::bind(&NameServerImpl::OnLostLock, this),
            FLAGS_endpoint);
    dist_lock_->Lock();
    return true;
}

void NameServerImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        OnLostLock();
        PDLOG(WARNING, "reconnect zk");
        if (zk_client_->Reconnect()) {
            PDLOG(INFO, "reconnect zk ok");
        }
    }
    thread_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&NameServerImpl::CheckZkClient, this));
}

int NameServerImpl::UpdateTaskStatus() {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(DEBUG, "cur name_server is not running. return");
        return 0;
    }
    std::vector<std::shared_ptr<TabletClient>> vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                PDLOG(DEBUG, "tablet[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            vec.push_back(iter->second->client_);
        }    
    }
    for (auto iter = vec.begin(); iter != vec.end(); ++iter) {
        ::rtidb::api::TaskStatusResponse response;
        // get task status from tablet
        if ((*iter)->GetTaskStatus(response)) {
            std::lock_guard<std::mutex> lock(mu_);
            for (int idx = 0; idx < response.task_size(); idx++) {
                auto it = task_map_.find(response.task(idx).op_id());
                if (it == task_map_.end()) {
                    PDLOG(WARNING, "cannot find op_id[%lu] in task_map", response.task(idx).op_id());
                    continue;
                }
                if (it->second->task_list_.empty()) {
                    continue;
                }
                // update task status
                std::shared_ptr<Task> task = it->second->task_list_.front();
                if (task->task_info_->task_type() == response.task(idx).task_type() && 
                        task->task_info_->status() != response.task(idx).status()) {
                    PDLOG(DEBUG, "update task status from[%s] to[%s]. op_id[%lu], task_type[%s]", 
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
        std::lock_guard<std::mutex> lock(mu_);
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
            std::lock_guard<std::mutex> lock(mu_);
            auto pos = task_map_.find(op_id);
            if (pos == task_map_.end()) {
                PDLOG(WARNING, "cannot find op[%lu] in task_map", op_id);
                continue;
            }
            op_data = pos->second;
        }
        uint32_t cur_task_index = op_data->op_info_.task_index();
        op_data->op_info_.set_task_index(cur_task_index + 1);
        std::string value;
        op_data->op_info_.SerializeToString(&value);
        std::string node = zk_op_data_path_ + "/" + std::to_string(op_id);
        if (zk_client_->SetNodeValue(node, value)) {
            PDLOG(DEBUG, "set zk status value success. node[%s] value[%s]",
                        node.c_str(), value.c_str());
            op_data->task_list_.pop_front();
            continue;
        }
        // revert task index
        op_data->op_info_.set_task_index(cur_task_index);
        PDLOG(WARNING, "set zk status value failed! node[%s] op_id[%lu] op_type[%s] task_index[%u]", 
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
        std::lock_guard<std::mutex> lock(mu_);
        for (auto iter = task_map_.begin(); iter != task_map_.end(); iter++) {
            if ((iter->second->task_list_.empty() && 
                    iter->second->task_status_ == ::rtidb::api::kDoing) ||
                    (!iter->second->task_list_.empty() && 
                    iter->second->task_status_ == ::rtidb::api::kFailed)) {
                done_task_vec.push_back(iter->first);
            }
        }
        if (done_task_vec.empty()) {
            return 0;
        }
        for (auto iter = tablets_.begin(); iter != tablets_.end(); ++iter) {
            if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                PDLOG(DEBUG, "tablet[%s] is not Healthy", iter->first.c_str());
                continue;
            }
            client_vec.push_back(iter->second->client_);
        }
    }
    bool has_failed = false;
    for (auto iter = client_vec.begin(); iter != client_vec.end(); ++iter) {
        if (!(*iter)->DeleteOPTask(done_task_vec)) {
            PDLOG(WARNING, "tablet[%s] delete op failed", (*iter)->GetEndpoint().c_str()); 
            has_failed = true;
            continue;
        }
        PDLOG(DEBUG, "tablet[%s] delete op success", (*iter)->GetEndpoint().c_str()); 
    }
    if (!has_failed) {
        for (auto op_id : done_task_vec) {
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_id);
            if (zk_client_->DeleteNode(node)) {
                PDLOG(INFO, "delete zk op node[%s] success.", node.c_str()); 
                std::lock_guard<std::mutex> lock(mu_);
                auto pos = task_map_.find(op_id);
                if (pos != task_map_.end()) {
                    pos->second->end_time_ = ::baidu::common::timer::now_time();
                    if (pos->second->task_status_ == ::rtidb::api::kDoing) {
                        pos->second->task_status_ = ::rtidb::api::kDone;
                    }
                    pos->second->task_list_.clear();
                }
            } else {
                PDLOG(WARNING, "delete zk op_node failed. opid[%lu] node[%s]", op_id, node.c_str()); 
            }
        }
    }
    return 0;
}

void NameServerImpl::ProcessTask() {
    while (running_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            while (task_map_.empty()) {
                cv_.wait_for(lock, std::chrono::milliseconds(FLAGS_name_server_task_wait_time));
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
                    PDLOG(WARNING, "task[%s] run failed, terminate op[%s]. op_id[%lu]",
                                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                                    ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                                    iter->first);
                    iter->second->task_status_ = ::rtidb::api::kFailed;
                } else if (task->task_info_->status() == ::rtidb::api::kInited) {
                    PDLOG(DEBUG, "run task. opid[%lu] op_type[%s] task_type[%s]", iter->first, 
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

void NameServerImpl::ConnectZK(RpcController* controller,
        const ConnectZKRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
	if (zk_client_->Reconnect()) {
        response->set_code(0);
        response->set_msg("ok");
        PDLOG(INFO, "connect zk ok");
        return;
    }
    response->set_code(-1);
    response->set_msg("reconnect failed");
}        

void NameServerImpl::DisConnectZK(RpcController* controller,
        const DisConnectZKRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    zk_client_->CloseZK();
	OnLostLock();
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "disconnect zk ok");
}

void NameServerImpl::MakeSnapshotNS(RpcController* controller,
        const MakeSnapshotNSRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("get table info failed");
        PDLOG(WARNING, "get table info failed! name[%s]", request->name().c_str());
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    uint32_t pid = request->pid();
    std::string endpoint;
    if (GetLeader(table_info, pid, endpoint) < 0 || endpoint.empty()) {
        response->set_code(-1);
        response->set_msg("get leader failed");
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request->name().c_str(), pid);
        return;
    }
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("leader is not online");
        PDLOG(WARNING, "leader[%s] is not online", endpoint.c_str());
        return;
    }

    std::shared_ptr<OPData> op_data;
    std::string value;
    request->SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kMakeSnapshotOP, value, op_data) < 0) {
        response->set_code(-1);
        response->set_msg("create makesnapshot op date error");
        PDLOG(WARNING, "create makesnapshot op data error. tid %u pid %u", tid, pid);
        return;
    }

    std::shared_ptr<Task> task = CreateMakeSnapshotTask(endpoint, op_index_, 
                ::rtidb::api::OPType::kMakeSnapshotOP, tid, pid);
    if (!task) {
        response->set_code(-1);
        response->set_msg("create makesnapshot task failed");
        PDLOG(WARNING, "create makesnapshot task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);
    if (AddOPData(op_data) < 0) {
        response->set_code(-1);
        response->set_msg("add op data failed");
        PDLOG(WARNING, "add op data failed. tid[%u] pid[%u]", tid, pid);
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "add makesnapshot op ok. op_id[%lu] tid[%u] pid[%u]", 
                op_index_, tid, pid);
}

int NameServerImpl::CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader, const std::vector<::rtidb::base::ColumnDesc>& columns,
            std::map<uint32_t, std::vector<std::string>>& endpoint_map) {
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader() != is_leader) {
                continue;
            }
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            std::shared_ptr<TabletInfo> tablet_ptr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto iter = tablets_.find(endpoint);
                // check tablet if exist
                if (iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    return -1;
                }
                tablet_ptr = iter->second;
                // check tablet healthy
                if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    return -1;
                }
            }
            std::vector<std::string> endpoint_vec;
            uint64_t term = 0;
            if (is_leader) {
                if (endpoint_map.find(pid) != endpoint_map.end()) {
                    endpoint_map[pid].swap(endpoint_vec);
                }
                std::lock_guard<std::mutex> lock(mu_);
                if (!zk_client_->SetNodeValue(zk_term_node_, std::to_string(term_ + 1))) {
                    PDLOG(WARNING, "update leader id  node failed. table name %s pid %u", 
                                    table_info->name().c_str(), pid);
                    return -1;
                }
                term_++;
                term = term_;
                ::rtidb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
                ::rtidb::nameserver::TermPair* term_pair = table_partition->add_term_offset();
                term_pair->set_term(term);
                term_pair->set_offset(0);
            } else {
                if (endpoint_map.find(pid) == endpoint_map.end()) {
                    endpoint_map.insert(std::make_pair(pid, std::vector<std::string>()));
                }
                endpoint_map[pid].push_back(endpoint);
            }
            ::rtidb::api::TTLType ttl_type = ::rtidb::api::TTLType::kAbsoluteTime;
            if (table_info->ttl_type() == "kLatestTime") {
                ttl_type = ::rtidb::api::TTLType::kLatestTime;
            }
            if (!tablet_ptr->client_->CreateTable(table_info->name(), table_index_, pid, 
                                    table_info->ttl(), table_info->seg_cnt(), columns, ttl_type,
                                    is_leader, endpoint_vec, term)) {

                PDLOG(WARNING, "create table failed. tid[%u] pid[%u] endpoint[%s]", 
                        table_index_, pid, endpoint.c_str());
                return -1;

            }
            PDLOG(INFO, "create table success. tid[%u] pid[%u] endpoint[%s] idx[%d]", 
                        table_index_, pid, endpoint.c_str(), idx);
        }
    }
    return 0;
}

void NameServerImpl::ConfSet(RpcController* controller,
            const ConfSetRequest* request,
            GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string key = request->conf().key();
    std::string value = request->conf().value();
    if (key.empty() || value.empty()) {
        response->set_code(-1);
        response->set_msg("key or value is empty");
        PDLOG(WARNING, "key[%s] value[%s]", key.c_str(), value.c_str());
        return;
    }
    if (key == "auto_failover") {
        if (strcasecmp(value.c_str(), "true") == 0) {
            auto_failover_.store(true, std::memory_order_release);
        } else if (strcasecmp(value.c_str(), "false") == 0) {
            auto_failover_.store(false, std::memory_order_release);
        } else {
            response->set_code(-1);
            response->set_msg("invalid value");
            PDLOG(WARNING, "invalid value[%s]", value.c_str());
            return;
        }
    } else if (key == "auto_recover_table") {
        if (strcasecmp(value.c_str(), "true") == 0) {
            auto_recover_table_.store(true, std::memory_order_release);
        } else if (strcasecmp(value.c_str(), "false") == 0) {
            auto_recover_table_.store(false, std::memory_order_release);
        } else {
            response->set_code(-1);
            response->set_msg("invalid value");
            PDLOG(WARNING, "invalid value[%s]", value.c_str());
            return;
        }

    } else {
        response->set_code(-1);
        response->set_msg("unsupport set this key");
        PDLOG(WARNING, "unsupport set key %s", key.c_str());
        return;
    }
    PDLOG(INFO, "config set ok. key %s value %s", key.c_str(), value.c_str());
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::ConfGet(RpcController* controller,
            const ConfGetRequest* request,
            ConfGetResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    ::rtidb::nameserver::Pair* conf = response->add_conf();
    conf->set_key("auto_failover");
    auto_failover_.load(std::memory_order_acquire) ? conf->set_value("true") : conf->set_value("false");

    conf = response->add_conf();
    conf->set_key("auto_recover_table");
    auto_recover_table_.load(std::memory_order_acquire) ? conf->set_value("true") : conf->set_value("false");

    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::ChangeLeader(RpcController* controller,
            const ChangeLeaderRequest* request,
            GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (CreateChangeLeaderOP(request->name(), request->pid()) < 0) {
        response->set_code(-1);
        response->set_msg("change leader failed");
        PDLOG(WARNING, "change leader failed. name %s pid %u", 
                        request->name().c_str(), request->pid());
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::OfflineEndpoint(RpcController* controller,
            const OfflineEndpointRequest* request,
            GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string endpoint = request->endpoint();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(endpoint);
        if (iter == tablets_.end()) {
            response->set_code(-1);
            response->set_msg("endpoint is not exist");
            PDLOG(WARNING, "endpoint %s is not exist", endpoint.c_str());
            return;
        } else if (iter->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
            response->set_code(-1);
            response->set_msg("endpoint is healthy");
            PDLOG(WARNING, "endpoint %s is healthy", endpoint.c_str());
            return;
        }
    }
    OnTabletOffline(endpoint);
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::RecoverEndpoint(RpcController* controller,
            const RecoverEndpointRequest* request,
            GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string endpoint = request->endpoint();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(endpoint);
        if (iter == tablets_.end()) {
            response->set_code(-1);
            response->set_msg("endpoint is not exist");
            PDLOG(WARNING, "endpoint %s is not exist", endpoint.c_str());
            return;
        } else if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            response->set_code(-1);
            response->set_msg("endpoint is not healthy");
            PDLOG(WARNING, "endpoint %s is not healthy", endpoint.c_str());
            return;
        }
    }
    OnTabletOnline(endpoint);
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::ShowOPStatus(RpcController* controller,
        const ShowOPStatusRequest* request,
        ShowOPStatusResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
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
}

void NameServerImpl::ShowTable(RpcController* controller,
            const ShowTableRequest* request,
            ShowTableResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        if (request->has_name() && request->name() != kv.first) {
            continue;
        }
        ::rtidb::nameserver::TableInfo* table_info = response->add_table_info();
        table_info->CopyFrom(*(kv.second));
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::DropTable(RpcController* controller, 
        const DropTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is not exist!");
        PDLOG(WARNING, "table[%s] is not exist!", request->name().c_str());
        return;
    }
    int code = 0;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            do {
                std::string endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (!iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    PDLOG(WARNING, "table %s is not alive. pid %u endpoint %s", 
                                    request->name().c_str(), iter->second->table_partition(idx).pid(), endpoint.c_str());
                    continue;
                }
                auto tablets_iter = tablets_.find(endpoint);
                // check tablet if exist
                if (tablets_iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    break;
                }
                // check tablet healthy
                if (tablets_iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    continue;
                }
                if (!tablets_iter->second->client_->DropTable(iter->second->tid(),
                                        iter->second->table_partition(idx).pid())) {
                    PDLOG(WARNING, "drop table failed. tid[%u] pid[%u] endpoint[%s]", 
                                    iter->second->tid(), iter->second->table_partition(idx).pid(),
                                    endpoint.c_str());
                    code = -1; // if drop table failed, return error                
                    break;
                }
                PDLOG(INFO, "drop table. tid[%u] pid[%u] endpoint[%s]", 
                                iter->second->tid(), iter->second->table_partition(idx).pid(),
                                endpoint.c_str());
            } while (0);
        }
    }
    if (!zk_client_->DeleteNode(zk_table_data_path_ + "/" + request->name())) {
        PDLOG(WARNING, "delete table node[%s/%s] failed! value[%s]", 
                        zk_table_data_path_.c_str(), request->name().c_str());
        code = -1;
    } else {
        PDLOG(INFO, "delete table node[%s/%s]", zk_table_data_path_.c_str(), request->name().c_str());
    }
    table_info_.erase(request->name());
    response->set_code(code);
    code == 0 ?  response->set_msg("ok") : response->set_msg("drop table error");
}

int NameServerImpl::ConvertColumnDesc(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
                    std::vector<::rtidb::base::ColumnDesc>& columns) {
    for (int idx = 0; idx < table_info->column_desc_size(); idx++) {
		::rtidb::base::ColType type;
		std::string raw_type = table_info->column_desc(idx).type();
		if (raw_type == "int32") {
			type = ::rtidb::base::ColType::kInt32;
		} else if (raw_type == "int64") {
			type = ::rtidb::base::ColType::kInt64;
		} else if (raw_type == "uint32") {
			type = ::rtidb::base::ColType::kUInt32;
		} else if (raw_type == "uint64") {
			type = ::rtidb::base::ColType::kUInt64;
		} else if (raw_type == "float") {
			type = ::rtidb::base::ColType::kFloat;
		} else if (raw_type == "double") {
			type = ::rtidb::base::ColType::kDouble;
		} else if (raw_type == "string") {
			type = ::rtidb::base::ColType::kString;
		} else {
        	PDLOG(WARNING, "invalid type %s", table_info->column_desc(idx).type().c_str());
			return -1;
		}
		::rtidb::base::ColumnDesc column_desc;
		column_desc.type = type;
		column_desc.name = table_info->column_desc(idx).name();
		column_desc.add_ts_idx = table_info->column_desc(idx).add_ts_idx();
        columns.push_back(column_desc);
    }
    return 0;
}

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info(request->table_info().New());
    table_info->CopyFrom(request->table_info());
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_info_.find(table_info->name()) != table_info_.end()) {
            response->set_code(-1);
            response->set_msg("table is already exist!");
            PDLOG(WARNING, "table[%s] is already exist!", table_info->name().c_str());
            return;
        }
        if (!zk_client_->SetNodeValue(zk_table_index_node_, std::to_string(table_index_ + 1))) {
            response->set_code(-1);
            response->set_msg("set table index node failed");
            PDLOG(WARNING, "set table index node failed! table_index[%u]", table_index_ + 1);
            return;
        }
        table_index_++;
        table_info->set_tid(table_index_);
    }
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (ConvertColumnDesc(table_info, columns) < 0) {
        response->set_code(-1);
        response->set_msg("convert column desc failed");
        PDLOG(WARNING, "convert table column desc failed. tid[%u]", table_index_);
        return;
    }
    std::map<uint32_t, std::vector<std::string>> endpoint_map;
    if (CreateTableOnTablet(table_info, false, columns, endpoint_map) < 0 ||
            CreateTableOnTablet(table_info, true, columns, endpoint_map) < 0) {
        response->set_code(-1);
        response->set_msg("create table failed");
        PDLOG(WARNING, "create table failed. tid[%u]", table_index_);
        return;
    }

    std::string table_value;
    table_info->SerializeToString(&table_value);
    if (!zk_client_->CreateNode(zk_table_data_path_ + "/" + table_info->name(), table_value)) {
        PDLOG(WARNING, "create table node[%s/%s] failed! value[%s]", zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str());
        response->set_code(-1);
        response->set_msg("create table node failed");
        return;
    }
    PDLOG(DEBUG, "create table node[%s/%s] success! value[%s]", zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str());
    {
        std::lock_guard<std::mutex> lock(mu_);
        table_info_.insert(std::make_pair(table_info->name(), table_info));
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::AddReplicaNS(RpcController* controller,
       const AddReplicaNSRequest* request,
       GeneralResponse* response,
       Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("tablet is not online");
        PDLOG(WARNING, "tablet[%s] is not online", request->endpoint().c_str());
        return;
    }
    auto pos = table_info_.find(request->name());
    if (pos == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is not  exist!");
        PDLOG(WARNING, "table[%s] is not exist!", request->name().c_str());
        return;
    }
    uint32_t tid = pos->second->tid();
    uint32_t pid = request->pid();;
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        response->set_code(-1);
        response->set_msg("get leader failed");
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request->name().c_str(), pid);
        return;
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    request->SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kAddReplicaOP, value, op_data) < 0) {
        PDLOG(WARNING, "create AddReplicaOP data error. table %s pid %u",
                        request->name().c_str(), pid);
        return;
    }

    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        response->set_code(-1);
        response->set_msg("create pausesnapshot task failed");
        PDLOG(WARNING, "create pausesnapshot task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request->endpoint());
    if (!task) {
        response->set_code(-1);
        response->set_msg("create sendsnapshot task failed");
        PDLOG(WARNING, "create sendsnapshot task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(request->endpoint(), op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, request->name(), 
                tid, pid, ttl, seg_cnt);
    if (!task) {
        response->set_code(-1);
        response->set_msg("create loadtable task failed");
        PDLOG(WARNING, "create loadtable task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request->endpoint());
    if (!task) {
        response->set_code(-1);
        response->set_msg("create addreplica task failed");
        PDLOG(WARNING, "create addreplica task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        response->set_code(-1);
        response->set_msg("create recoversnapshot task failed");
        PDLOG(WARNING, "create recoversnapshot task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddTableInfoTask(request->name(), pid, request->endpoint(),
                op_index_, ::rtidb::api::OPType::kAddReplicaOP);
    if (!task) {
        response->set_code(-1);
        response->set_msg("create addtableinfo task failed");
        PDLOG(WARNING, "create addtableinfo task failed. tid %u pid %u", tid, pid);
        return;
    }
    op_data->task_list_.push_back(task);

    if (AddOPData(op_data) < 0) {
        response->set_code(-1);
        response->set_msg("add op data failed");
        PDLOG(WARNING, "add op data failed. tid[%u] pid[%u]", tid, pid);
        return;
    }
    PDLOG(INFO, "add addreplica op ok. op_id[%lu] tid[%u] pid[%u]", 
                op_index_, tid, pid);
    response->set_code(0);
    response->set_msg("ok");
}

bool NameServerImpl::RecoverAddReplica(std::shared_ptr<OPData> op_data) {
    if (!op_data->op_info_.IsInitialized()) {
        PDLOG(WARNING, "op_info is not init!");
        return false;
    }
    if (op_data->op_info_.op_type() != ::rtidb::api::OPType::kAddReplicaOP) {
        PDLOG(WARNING, "op_type[%s] is not match", 
                    ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
        return false;
    }
    AddReplicaNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return false;
    }
    auto it = tablets_.find(request.endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", request.endpoint().c_str());
        return false;
    }
    auto pos = table_info_.find(request.name());
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", request.name().c_str());
        return false;
    }
    uint32_t tid = pos->second->tid();
    uint32_t pid = request.pid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return false;
    }
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(request.endpoint(), op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, request.name(), 
                tid, pid, ttl, seg_cnt);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddTableInfoTask(request.name(), pid, request.endpoint(),
                op_index_, ::rtidb::api::OPType::kAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid %u pid %u", tid, pid);
        return false;
    }
    op_data->task_list_.push_back(task);

    SkipDoneTask(op_data->op_info_.task_index(), op_data->task_list_);
    return true;
}

void NameServerImpl::DelReplicaNS(RpcController* controller,
       const DelReplicaNSRequest* request,
       GeneralResponse* response,
       Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (table_info_.find(request->data().name()) == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is not  exist!");
        PDLOG(WARNING, "table[%s] is not exist!", request->data().name().c_str());
        return;
    }
    if (CreateDelReplicaOP(request->data(), ::rtidb::api::OPType::kDelReplicaOP) < 0) {
        response->set_code(-1);
        response->set_msg("create op failed");
    } else {
        response->set_code(0);
        response->set_msg("ok");
    }
}

int NameServerImpl::CreateOPData(::rtidb::api::OPType op_type, const std::string& value, 
        std::shared_ptr<OPData>& op_data) {
    if (!zk_client_->SetNodeValue(zk_op_index_node_, std::to_string(op_index_ + 1))) {
        PDLOG(WARNING, "set op index node failed! op_index[%lu]", op_index_);
        return -1;
    }
    op_index_++;
    op_data = std::make_shared<OPData>();
    op_data->start_time_ = ::baidu::common::timer::now_time();
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(op_type);
    op_data->op_info_.set_task_index(0);
    op_data->op_info_.set_data(value);
    op_data->task_status_ = ::rtidb::api::kDoing;
    return 0;
}

int NameServerImpl::AddOPData(const std::shared_ptr<OPData>& op_data) {
    std::string value;
    op_data->op_info_.SerializeToString(&value);
    std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
    if (!zk_client_->CreateNode(node, value)) {
        PDLOG(WARNING, "create op node[%s] failed. op_index[%lu] op_type[%s]", 
                        node.c_str(), op_data->op_info_.op_id(),
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()));
        return -1;
    }
    task_map_.insert(std::make_pair(op_data->op_info_.op_id(), op_data));
    cv_.notify_one();
    return 0;
}

int NameServerImpl::CreateDelReplicaOP(const DelReplicaData& del_replica_data, ::rtidb::api::OPType op_type) {
    if (op_type != ::rtidb::api::OPType::kDelReplicaOP && 
            op_type != ::rtidb::api::OPType::kOfflineReplicaOP) {
        PDLOG(WARNING, "optype is %s", ::rtidb::api::OPType_Name(op_type).c_str());
        return -1;
    }
    std::string leader_endpoint;
    uint32_t tid;
    auto iter = table_info_.find(del_replica_data.name());
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map", del_replica_data.name().c_str());
        return -1;
    }
    tid = iter->second->tid();
    if (GetLeader(iter->second, del_replica_data.pid(), leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", 
                        del_replica_data.name().c_str(), del_replica_data.pid());
        return -1;
    }
    if (leader_endpoint == del_replica_data.endpoint()) {
        PDLOG(WARNING, "endpoint is leader. table %s pid %u",
                        del_replica_data.name().c_str(), del_replica_data.pid());
        return -1;
    }
    std::string value;
    del_replica_data.SerializeToString(&value);
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(op_type, value, op_data) < 0) {
        PDLOG(WARNING, "create op data error. table %s pid %u",
                        del_replica_data.name().c_str(), del_replica_data.pid());
        return -1;
    }

    std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_index_, 
                op_type, tid, del_replica_data.pid(), del_replica_data.endpoint());
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table %s pid %u endpoint %s", 
                        del_replica_data.name().c_str(), del_replica_data.pid(), 
                        del_replica_data.endpoint().c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    if (op_type == ::rtidb::api::OPType::kDelReplicaOP) {
        task = CreateDelTableInfoTask(del_replica_data.name(),
                    del_replica_data.pid(), del_replica_data.endpoint(),
                    op_index_, op_type);
        if (!task) {
            PDLOG(WARNING, "create deltableinfo task failed. table %s pid %u endpoint %s", 
                            del_replica_data.name().c_str(), del_replica_data.pid(),
                            del_replica_data.endpoint().c_str());
            return -1;
        }
    } else {
        task = CreateUpdatePartitionStatusTask(del_replica_data.name(),
                    del_replica_data.pid(), del_replica_data.endpoint(), false, false,
                    op_index_, op_type);
        if (!task) {
            PDLOG(WARNING, "create update table alive status task failed. table %s pid %u endpoint %s", 
                            del_replica_data.name().c_str(), del_replica_data.pid(),
                            del_replica_data.endpoint().c_str());
            return -1;
        }
    }
    op_data->task_list_.push_back(task);
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name %s pid %u endpoint %s", 
                        del_replica_data.name().c_str(), del_replica_data.pid(), 
                        del_replica_data.endpoint().c_str());
        return -1;
    }
    PDLOG(INFO, "add delreplica op. op_id[%lu] table %s pid %u endpoint %s", 
                op_index_, del_replica_data.name().c_str(), del_replica_data.pid(),
                del_replica_data.endpoint().c_str());
    return 0;
}

int NameServerImpl::CreateChangeLeaderOP(const std::string& name, uint32_t pid) {
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    std::vector<std::string> follower_endpoint;
    bool has_alive_leader = false;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                std::string endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) { 
                    auto tablets_iter = tablets_.find(endpoint);
                    if (tablets_iter == tablets_.end()) {
                        PDLOG(WARNING, "endpoint %s is not exist. table %s pid %u",
                                        endpoint.c_str(), name.c_str(), pid);
                        return -1;
                    } else if (tablets_iter->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                        PDLOG(WARNING, "endpoint %s is healthy, cannot change leader. table %s pid %u",
                                        endpoint.c_str(), name.c_str(), pid);
                        return -1;
                    }
                    has_alive_leader = true;
                } else {
                    auto tablets_iter = tablets_.find(endpoint);
                    if (tablets_iter != tablets_.end() && 
                            tablets_iter->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                        follower_endpoint.push_back(endpoint);
                    } else {
                        PDLOG(WARNING, "endpoint %s is offline. table %s pid %u", 
                                        endpoint.c_str(), name.c_str(), pid);
                    }
                }
            }
        }
        break;
    }
    if (!has_alive_leader) {
        PDLOG(WARNING, "has not alive leader, cannot change leader. table %s pid %u",
                        name.c_str(), pid);
        return -1;
    }
    if (follower_endpoint.empty()) {
        PDLOG(INFO, "table not found follower. name %s pid %u", name.c_str(), pid);
        return 0;
    }
    std::shared_ptr<OPData> op_data;
    std::string value = name + "\t" + std::to_string(pid);
    if (CreateOPData(::rtidb::api::OPType::kSelectLeaderOP, value, op_data) < 0) {
        PDLOG(WARNING, "create SelectLeaderOP data error. table %s pid %u",
                        name.c_str(), pid);
        return -1;
    }

    std::shared_ptr<Task> task = CreateChangeLeaderTask(
                op_index_, ::rtidb::api::OPType::kSelectLeaderOP, 
                name, tid, pid, follower_endpoint);
    if (!task) {
        PDLOG(WARNING, "create changeleader task failed. table %s pid %u", 
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);

    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name %s pid %u", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add changeleader op. op_id[%lu] table %s pid %u", 
                op_index_, name.c_str(), pid);
    return 0;
}

void NameServerImpl::OnLocked() {
    PDLOG(INFO, "become the leader name server");
    bool ok = Recover();
    if (!ok) {
        PDLOG(WARNING, "recover failed");
        //TODO fail to recover discard the lock
    }
    running_.store(true, std::memory_order_release);
    task_thread_pool_.DelayTask(FLAGS_get_task_status_interval, boost::bind(&NameServerImpl::UpdateTaskStatus, this));
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::ProcessTask, this));
}

void NameServerImpl::OnLostLock() {
    PDLOG(INFO, "become the stand by name sever");
    running_.store(false, std::memory_order_release);
}

void NameServerImpl::RecoverTable(const std::string& name, uint32_t pid, const std::string& endpoint) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    uint32_t tid = 0;
    std::shared_ptr<TabletInfo> leader_tablet_ptr;
    std::shared_ptr<TabletInfo> tablet_ptr;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
            return;
        }
        tid = iter->second->tid();
        for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
            if (iter->second->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (iter->second->table_partition(idx).partition_meta(meta_idx).is_leader() &&
                        iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    std::string leader_endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();    
                    auto tablet_iter = tablets_.find(leader_endpoint);
                    if (tablet_iter == tablets_.end()) {
                        PDLOG(WARNING, "can not find the leader endpoint[%s]'s client", leader_endpoint.c_str());
                        return;
                    }
                    leader_tablet_ptr = tablet_iter->second;
                    if (leader_tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                        PDLOG(WARNING, "leader endpoint [%s] is offline", leader_endpoint.c_str());
                        return;
                    }
                }
                if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    auto tablet_iter = tablets_.find(endpoint);
                    if (tablet_iter == tablets_.end()) {
                        PDLOG(WARNING, "can not find the endpoint[%s]'s client", endpoint.c_str());
                        return;
                    }
                    tablet_ptr = tablet_iter->second;
                    if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                        PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                        return;
                    }
                }
            }
            break;
        }
    }
	bool has_table = false;
	uint64_t term = 0;
	uint64_t offset = 0;
	if (!tablet_ptr->client_->GetTermPair(tid, pid, has_table, term, offset)) {
		PDLOG(WARNING, "GetTermPair failed. name[%s] tid[%u] pid[%u] endpoint[%s]", 
						name.c_str(), tid, pid, endpoint.c_str());
		return;
	}
	int ret_code = MatchTermOffset(name, pid, has_table, term, offset);
	if (ret_code < 0) {
		PDLOG(WARNING, "term and offset match error. name[%s] tid[%u] pid[%u] endpoint[%s]", 
						name.c_str(), tid, pid, endpoint.c_str());
		return;
	}
	if (has_table) {
		if (ret_code == 0) {
			int code = 0;
			::rtidb::api::Manifest manifest;
			if (!leader_tablet_ptr->client_->GetManifest(tid, pid, code, manifest) || code < 0) {
				PDLOG(WARNING, "get manifest failed. name[%s] tid[%u] pid[%u]", 
						name.c_str(), tid, pid);
				return;
			}
        	std::lock_guard<std::mutex> lock(mu_);
			if (code == 0 && offset > manifest.offset()) {
				CreateReAddReplicaSimplifyOP(name, pid, endpoint);
			} else {
				CreateReAddReplicaWithDropOP(name, pid, endpoint);
			}
		} else {
        	std::lock_guard<std::mutex> lock(mu_);
			CreateReAddReplicaWithDropOP(name, pid, endpoint);
		}
	} else {
        std::lock_guard<std::mutex> lock(mu_);
		if (ret_code == 0) {
			CreateReAddReplicaNoSendOP(name, pid, endpoint);
		} else {	
			CreateReAddReplicaOP(name, pid, endpoint);
		}
	}
}

int NameServerImpl::CreateReAddReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    AddReplicaData data;
    data.set_name(name);
    data.set_pid(pid);
    data.set_endpoint(endpoint);
    data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaOP, value, op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP data error. table %s pid %u endpoint %s",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaOP, name, 
                tid, pid, ttl, seg_cnt);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index_, ::rtidb::api::OPType::kReAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica op ok. op_id %lu name %s pid %u endpoint %s", 
                op_index_, name.c_str(), pid, endpoint.c_str());
	return 0;
}


int NameServerImpl::CreateReAddReplicaWithDropOP(const std::string& name, uint32_t pid, 
            const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    AddReplicaData data;
    data.set_name(name);
    data.set_pid(pid);
    data.set_endpoint(endpoint);
    data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaWithDropOP, value, op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaWithDropOP data error. table %s pid %u endpoint %s",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(endpoint, op_index_, ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    task = CreateSendSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, name, 
                tid, pid, ttl, seg_cnt);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index_, ::rtidb::api::OPType::kReAddReplicaWithDropOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);

    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica with drop op ok. op_id %lu name %s pid %u endpoint %s", 
                op_index_, name.c_str(), pid, endpoint.c_str());
	return 0;
}

int NameServerImpl::CreateReAddReplicaNoSendOP(const std::string& name, uint32_t pid, 
            const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    AddReplicaData data;
    data.set_name(name);
    data.set_pid(pid);
    data.set_endpoint(endpoint);
    data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaNoSendOP, value, op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaNoSendOP data error. table %s pid %u endpoint %s",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, name, 
                tid, pid, ttl, seg_cnt);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index_, ::rtidb::api::OPType::kReAddReplicaNoSendOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);

    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica no send op ok. op_id %lu name %s pid %u endpoint %s", 
                op_index_, name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::GetLeader(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info, uint32_t pid, std::string& leader_endpoint) {
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        if (table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader() &&
                    table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                leader_endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                return 0;
            }
        }
        break;
    }
    return -1;
}

int NameServerImpl::CreateReAddReplicaSimplifyOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    std::string value;
    AddReplicaData data;
    data.set_name(name);
    data.set_pid(pid);
    data.set_endpoint(endpoint);
    data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaSimplifyOP, value, op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaSimplifyOP data error. table %s pid %u endpoint %s",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    std::shared_ptr<Task> task = CreateAddReplicaTask(leader_endpoint, op_index_, 
                ::rtidb::api::OPType::kReAddReplicaSimplifyOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid %u pid %u", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index_, ::rtidb::api::OPType::kReAddReplicaSimplifyOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);

    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name %s pid %u endpoint %s", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica simplify op ok. op_id %lu name %s pid %u endpoint %s", 
                op_index_, name.c_str(), pid, endpoint.c_str());
	return 0;

}

int NameServerImpl::MatchTermOffset(const std::string& name, uint32_t pid, bool has_table, uint64_t term, uint64_t offset) {
    if (!has_table && offset == 0) {
        return 1;
    }
	std::map<uint64_t, uint64_t> term_map;
	{
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
            return -1;
        }
        for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
            if (iter->second->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int term_idx = 0; term_idx < iter->second->table_partition(idx).term_offset_size(); term_idx++) {
				term_map.insert(std::make_pair(iter->second->table_partition(idx).term_offset(term_idx).term(),
							iter->second->table_partition(idx).term_offset(term_idx).offset()));
			}
			break;
		}
	}
	auto iter = term_map.find(term);
	if (iter == term_map.end()) {
        PDLOG(WARNING, "not found term %lu in table_info. name %s pid %u", 
						term, name.c_str(), pid);
		return -1;
	} else if (iter->second > offset) {
        PDLOG(WARNING, "offset match error. name %s pid %u term %lu term start offset %lu cur offset %lu", 
						name.c_str(), pid, term, iter->second, offset);
		return -1;
	}
	iter++;
	if (iter == term_map.end()) {
        PDLOG(WARNING, "cur term %lu is the last one. name %s pid %u", 
						term, name.c_str(), pid);
		return -1;
	}
	if (iter->second <= offset) {
        PDLOG(INFO, "term %lu offset not matched. name %s pid %u offset %lu", 
						term, name.c_str(), pid, offset);
		return 1;
	}
	PDLOG(INFO, "term %lu offset has matched. name %s pid %u offset %lu", 
					term, name.c_str(), pid, offset);
	return 0;
}

std::shared_ptr<Task> NameServerImpl::CreateMakeSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kMakeSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::MakeSnapshot, it->second->client_, tid, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreatePauseSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kPauseSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::PauseSnapshot, it->second->client_, tid, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateRecoverSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kRecoverSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::RecoverSnapshot, it->second->client_, tid, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateSendSnapshotTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
                    const std::string& des_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kSendSnapshot);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::SendSnapshot, it->second->client_, tid, pid, 
                des_endpoint, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateLoadTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, const std::string& name, 
                    uint32_t tid, uint32_t pid, uint64_t ttl, uint32_t seg_cnt) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kLoadTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::LoadTable, it->second->client_, name, tid, pid, 
                ttl, false, std::vector<std::string>(),
                seg_cnt, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddReplicaTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
                    const std::string& des_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddReplica);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::AddReplica, it->second->client_, tid, pid, 
                des_endpoint, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateAddTableInfoTask(const std::string& name,  uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kAddTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::AddTableInfo, this, name, endpoint, pid, task->task_info_);
    return task;
}

void NameServerImpl::AddTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() == pid) {
            ::rtidb::nameserver::TablePartition* table_partition = iter->second->mutable_table_partition(idx);
            ::rtidb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
            partition_meta->set_endpoint(endpoint);
            partition_meta->set_is_leader(false);
            break;
        }
    }
    std::string table_value;
    iter->second->SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                        zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
        return;         
    }
    PDLOG(INFO, "update table node[%s/%s]. value is [%s]", 
                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
}

std::shared_ptr<Task> NameServerImpl::CreateDelReplicaTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid,
                    const std::string& follower_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDelReplica);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::DelReplica, it->second->client_, tid, pid, 
                follower_endpoint, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateDropTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, uint32_t tid, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDropTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&TabletClient::DropTable, it->second->client_, tid, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateDelTableInfoTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kDelTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::DelTableInfo, this, name, endpoint, pid, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdatePartitionStatusTask(const std::string& name, uint32_t pid,
                    const std::string& endpoint, bool is_leader, bool is_alive, uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdatePartitionStatus);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdatePartitionStatus, this, name, endpoint, pid, is_leader, is_alive, task->task_info_);
    return task;
}

void NameServerImpl::DelTableInfo(const std::string& name, const std::string& endpoint, uint32_t pid,
                std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::rtidb::nameserver::TablePartition* table_partition = 
                            iter->second->mutable_table_partition(idx);
                ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta = 
                            table_partition->mutable_partition_meta();
                if (meta_idx != partition_meta->size() - 1) {
                    partition_meta->SwapElements(idx, partition_meta->size() - 1);
                }
                PDLOG(INFO, "remove pid %u in table %s. endpoint is %s", 
                            pid, name.c_str(), endpoint.c_str());
                partition_meta->RemoveLast();
                std::string table_value;
                iter->second->SerializeToString(&table_value);
                if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
                    PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                                    zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
                    task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
                    return;         
                }
                PDLOG(INFO, "update table node[%s/%s]. value is [%s]", 
                                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                break;
            }
        }
        break;
    }
}

void NameServerImpl::UpdatePartitionStatus(const std::string& name, const std::string& endpoint, uint32_t pid,
                bool is_leader, bool is_alive, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::rtidb::nameserver::TablePartition* table_partition =
                        iter->second->mutable_table_partition(idx);
                ::rtidb::nameserver::PartitionMeta* partition_meta = 
                        table_partition->mutable_partition_meta(meta_idx);        
                partition_meta->set_is_leader(is_leader);
                partition_meta->set_is_alive(is_alive);
                std::string table_value;
                iter->second->SerializeToString(&table_value);
                if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
                    PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                                    zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
                    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                    return;         
                }
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(INFO, "update table node[%s/%s]. value is [%s]", 
                                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
                return;
            }
        }
        break;
    }
    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
    PDLOG(WARNING, "name %s endpoint %s pid %u is not exist",
                    name.c_str(), endpoint.c_str(), pid);
}

std::shared_ptr<Task> NameServerImpl::CreateChangeLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t tid, uint32_t pid,
                    std::vector<std::string>& follower_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kChangeLeader);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::ChangeLeader, this, name, tid, pid, follower_endpoint, task->task_info_);
    PDLOG(INFO, "create change leader task success. name %s tid %u pid %u", name.c_str(), tid, pid);
    return task;
}

void NameServerImpl::ChangeLeader(const std::string& name, uint32_t tid, uint32_t pid, 
            std::vector<std::string>& follower_endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_term_node_, std::to_string(term_ + 1))) {
            PDLOG(WARNING, "update leader id  node failed. table name %s pid %u", name.c_str(), pid);
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
        term_++;
        cur_term = term_;
    }
    // select the max offset endpoint as leader
    uint64_t max_offset = 0;    
    std::string leader_endpoint;
    for (const auto& endpoint : follower_endpoint) {
        std::shared_ptr<TabletInfo> tablet_ptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = tablets_.find(endpoint);
            if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {

                PDLOG(WARNING, "endpoint %s is offline. table %s pid %u", 
                                endpoint.c_str(), name.c_str(), pid);
                task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
                return;
            }
            tablet_ptr = it->second;
        }
        uint64_t offset = 0;
        if (!tablet_ptr->client_->FollowOfNoOne(tid, pid, cur_term, offset)) {

            PDLOG(WARNING, "followOfNoOne failed. tid[%u] pid[%u] endpoint[%s]", 
                            tid, pid, endpoint.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
        if (offset >= max_offset) {
            leader_endpoint = endpoint;
            max_offset = offset;
        }
    }
    PDLOG(INFO, "new leader is %s. name %s tid %u pid %u offset %lu", 
                leader_endpoint.c_str(), name.c_str(), tid, pid, max_offset);

    std::shared_ptr<TabletInfo> tablet_ptr;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(leader_endpoint);           
        if (iter == tablets_.end() || iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            PDLOG(WARNING, "endpoint %s is offline", leader_endpoint.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
        follower_endpoint.erase(std::find(follower_endpoint.begin(), follower_endpoint.end(), leader_endpoint));
        if (!zk_client_->SetNodeValue(zk_term_node_, std::to_string(term_ + 1))) {
            PDLOG(WARNING, "update leader id  node failed. table name %s pid %u", name.c_str(), pid);
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
        tablet_ptr = iter->second;
        term_++;
        cur_term = term_;
    }
    if (!tablet_ptr->client_->ChangeRole(tid, pid, true, follower_endpoint, cur_term)) {
        PDLOG(WARNING, "change leader failed. name %s tid %u pid %u endpoint %s", 
                        name.c_str(), tid, pid, leader_endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    auto table_iter = table_info_.find(name);
    if (table_iter == table_info_.end()) {
        PDLOG(WARNING, "not found table %s in table_info map", name.c_str());
        return;
    }
    int old_leader_index = 0;
    int new_leader_index = 0;
    for (int idx = 0; idx < table_iter->second->table_partition_size(); idx++) {
        if (table_iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                old_leader_index = meta_idx;
            } else if (table_iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == leader_endpoint) {
                new_leader_index = meta_idx;
            }
        }
        ::rtidb::nameserver::TablePartition* table_partition = 
                table_iter->second->mutable_table_partition(idx);
        ::rtidb::nameserver::PartitionMeta* old_leader_meta = 
                table_partition->mutable_partition_meta(old_leader_index);
        old_leader_meta->set_is_alive(false);
        ::rtidb::nameserver::PartitionMeta* new_leader_meta = 
                table_partition->mutable_partition_meta(new_leader_index);
        new_leader_meta->set_is_leader(true);
        ::rtidb::nameserver::TermPair* term_offset = table_partition->add_term_offset();
        term_offset->set_term(cur_term);
        term_offset->set_offset(max_offset + 1);
        std::string table_value;
        table_iter->second->SerializeToString(&table_value);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return; 
        }
        PDLOG(INFO, "change leader success. name %s pid %u new leader %s", 
                    name.c_str(), pid, leader_endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kDone);
        break;
    }
}

}
}
