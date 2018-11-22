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
#include <boost/algorithm/string.hpp>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(get_task_status_interval);
DECLARE_int32(name_server_task_pool_size);
DECLARE_int32(name_server_task_wait_time);
DECLARE_int32(max_op_num);
DECLARE_uint32(partition_num);
DECLARE_uint32(replica_num);
DECLARE_bool(auto_failover);
DECLARE_bool(auto_recover_table);
DECLARE_uint32(tablet_heartbeat_timeout);
DECLARE_uint32(tablet_offline_check_interval);
DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(name_server_task_max_concurrency);

namespace rtidb {
namespace nameserver {

NameServerImpl::NameServerImpl():mu_(), tablets_(),
    table_info_(), zk_client_(NULL), dist_lock_(NULL), thread_pool_(1), 
    task_thread_pool_(FLAGS_name_server_task_pool_size), cv_(),
    rand_(0xdeadbeef), session_term_(0) {
    std::string zk_table_path = FLAGS_zk_root_path + "/table";
    zk_table_index_node_ = zk_table_path + "/table_index";
    zk_table_data_path_ = zk_table_path + "/table_data";
    zk_term_node_ = zk_table_path + "/term";
    std::string zk_op_path = FLAGS_zk_root_path + "/op";
    zk_op_index_node_ = zk_op_path + "/op_index";
    zk_op_data_path_ = zk_op_path + "/op_data";
    zk_offline_endpoint_lock_node_ = FLAGS_zk_root_path + "/offline_endpoint_lock";
    std::string zk_config_path = FLAGS_zk_root_path + "/config";
    zk_auto_failover_node_ = zk_config_path + "/auto_failover";
    zk_auto_recover_table_node_ = zk_config_path + "/auto_recover_table";
    zk_table_changed_notify_node_ = zk_table_path + "/notify";
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
    if (!zk_client_->GetNodeValue(zk_table_changed_notify_node_, value)) {
        if (!zk_client_->CreateNode(zk_table_changed_notify_node_, "1")) {
            PDLOG(WARNING, "create zk table changed notify node failed");
            return false;
        }
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_auto_failover_node_, value)) {
        auto_failover_.load(std::memory_order_acquire) ? value = "true" : value = "false";
        if (!zk_client_->CreateNode(zk_auto_failover_node_, value)) {
            PDLOG(WARNING, "create auto failover node failed!");
            return false;
        }
        PDLOG(INFO, "set zk_auto_failover_node[%s]", value.c_str());
    } else {
        value == "true" ? auto_failover_.store(true, std::memory_order_release) :
                       auto_failover_.store(false, std::memory_order_release);
        PDLOG(INFO, "get zk_auto_failover_node[%s]", value.c_str());
    }
    value.clear();
    if (!zk_client_->GetNodeValue(zk_auto_recover_table_node_, value)) {
        auto_recover_table_.load(std::memory_order_acquire) ? value = "true" : value = "false";
        if (!zk_client_->CreateNode(zk_auto_recover_table_node_, value)) {
            PDLOG(WARNING, "create auto recover table node failed!");
            return false;
        }
        PDLOG(INFO, "set zk_auto_recover_table_node[%s]", value.c_str());
    } else {
        value == "true" ? auto_recover_table_.store(true, std::memory_order_release) :
                       auto_recover_table_.store(false, std::memory_order_release);
        PDLOG(INFO, "get zk_auto_recover_table_node[%s]", value.c_str());

    }

    if (!RecoverTableInfo()) {
        PDLOG(WARNING, "recover table info failed!");
        return false;
    }

    if (!RecoverOPTask()) {
        PDLOG(WARNING, "recover task failed!");
        return false;
    }
    RecoverOfflineTablet();
    return true;
}

void NameServerImpl::RecoverOfflineTablet() {
    offline_endpoint_map_.clear();
    for(const auto& tablet : tablets_) {
        if (tablet.second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            offline_endpoint_map_.insert(std::make_pair(tablet.first, tablet.second->ctime_));
            thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval, boost::bind(&NameServerImpl::OnTabletOffline, this, tablet.first, false));
            PDLOG(INFO, "recover offlinetablet. endpoint %s", tablet.first.c_str());
        }
    }
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
            PDLOG(WARNING, "get table info failed! name[%s] table node[%s]", table_name.c_str(), table_name_node.c_str());
            continue;
        }
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = 
                    std::make_shared<::rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            PDLOG(WARNING, "parse table info failed! name[%s] value[%s] value size[%d]", table_name.c_str(), value.c_str(), value.length());
            continue;
        }
        table_info_.insert(std::make_pair(table_name, table_info));
        PDLOG(INFO, "recover table[%s] success", table_name.c_str());
    }
    return true;
}

bool NameServerImpl::RecoverOPTask() {
    for (auto& op_list : task_vec_) {
        op_list.clear();
    }
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
            continue;
        }
        std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
        if (!op_data->op_info_.ParseFromString(value)) {
            PDLOG(WARNING, "parse op info failed! value[%s]", value.c_str());
            continue;
        }
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kDone) {
            PDLOG(DEBUG, "op status is kDone. op_id[%lu]", op_data->op_info_.op_id());
            continue;
        }
        switch (op_data->op_info_.op_type()) {
            case ::rtidb::api::OPType::kMakeSnapshotOP:
                if (CreateMakeSnapshotOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kAddReplicaOP:
                if (CreateAddReplicaOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kChangeLeaderOP:
                if (CreateChangeLeaderOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kMigrateOP:
                if (CreateMigrateTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kRecoverTableOP:
                if (CreateRecoverTableOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kOfflineReplicaOP:
                if (CreateOfflineReplicaTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kDelReplicaOP:
                if (CreateDelReplicaOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaOP:
                if (CreateReAddReplicaTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaNoSendOP:
                if (CreateReAddReplicaNoSendTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaWithDropOP:
                if (CreateReAddReplicaWithDropTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReAddReplicaSimplifyOP:
                if (CreateReAddReplicaSimplifyTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kUpdateTableAliveOP:
                if (CreateUpdateTableAliveOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kReLoadTableOP:
                if (CreateReLoadTableTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            case ::rtidb::api::OPType::kUpdatePartitionStatusOP:
                if (CreateUpdatePartitionStatusOPTask(op_data) < 0) {
                    PDLOG(WARNING, "recover op[%s] failed. op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                    continue;
                }
                break;
            default:
                PDLOG(WARNING, "unsupport recover op[%s]! op_id[%lu]", 
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_data->op_info_.op_id());
                continue;
        }
        if (!SkipDoneTask(op_data)) {
            PDLOG(WARNING, "SkipDoneTask task failed. op_id[%lu] task_index[%u]", 
                            op_data->op_info_.op_id(), op_data->op_info_.task_index());
            continue;
        }
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kFailed) {
            done_op_list_.push_back(op_data);
        } else {
            uint32_t idx = op_data->op_info_.pid() % task_vec_.size();
            if (op_data->op_info_.has_vec_idx() && op_data->op_info_.vec_idx() < task_vec_.size()) {
                idx = op_data->op_info_.vec_idx();
            }
            task_vec_[idx].push_back(op_data);
        }
        PDLOG(INFO, "recover op[%s] success. op_id[%lu]", 
                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(), op_data->op_info_.op_id());
    }
    return true;
}

int NameServerImpl::CreateMakeSnapshotOPTask(std::shared_ptr<OPData> op_data) {
    MakeSnapshotNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto iter = table_info_.find(request.name());
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "get table info failed! name[%s]", request.name().c_str());
        return -1;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    uint32_t pid = request.pid();
    std::string endpoint;
    if (GetLeader(table_info, pid, endpoint) < 0 || endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return -1;
    }
    std::shared_ptr<Task> task = CreateMakeSnapshotTask(endpoint, op_data->op_info_.op_id(), 
                ::rtidb::api::OPType::kMakeSnapshotOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create makesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create makesnapshot op task ok. tid[%u] pid[%u]", tid, pid);
    return 0;
}

bool NameServerImpl::SkipDoneTask(std::shared_ptr<OPData> op_data) {
    uint64_t op_id = op_data->op_info_.op_id();
    std::string op_type = ::rtidb::api::OPType_Name(op_data->op_info_.op_type());
    uint32_t task_index = op_data->op_info_.task_index();
    if (op_data->task_list_.empty()) {
        PDLOG(WARNING, "skip task failed, task_list is empty. op_id[%lu] op_type[%s]", 
                        op_id, op_type.c_str());
        return false;
    }
    if (task_index > op_data->task_list_.size() - 1) {
        PDLOG(WARNING, "skip task failed. op_id[%lu] op_type[%s] task_index[%u]", 
                        op_id, op_type.c_str(), task_index);
        return false;
    }
    for (uint32_t idx = 0; idx < task_index; idx++) {
        op_data->task_list_.pop_front();
    }
    if (!op_data->task_list_.empty()) {
        std::shared_ptr<Task> task = op_data->task_list_.front();
        PDLOG(INFO, "cur task[%s]. op_id[%lu] op_type[%s]",
                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                    op_id, op_type.c_str());
        if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kFailed) {
            task->task_info_->set_status(::rtidb::api::TaskStatus::kFailed);
            return true;
        }
        switch (task->task_info_->task_type()) {
            case ::rtidb::api::TaskType::kSelectLeader:
            case ::rtidb::api::TaskType::kUpdateLeaderInfo:
            case ::rtidb::api::TaskType::kUpdatePartitionStatus:
            case ::rtidb::api::TaskType::kUpdateTableAlive:
            case ::rtidb::api::TaskType::kUpdateTableInfo:
            case ::rtidb::api::TaskType::kRecoverTable:
            case ::rtidb::api::TaskType::kAddTableInfo:
                // execute the task again
                task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
                break;
            default:
                task->task_info_->set_status(::rtidb::api::TaskStatus::kDoing);
        }
    }
    return true;
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
            tablet->client_ = std::make_shared<::rtidb::client::TabletClient>(*it, true);
            if (tablet->client_->Init() != 0) {
                PDLOG(WARNING, "tablet client init error. endpoint[%s]", it->c_str());
                continue;
            }
            tablet->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            tablets_.insert(std::make_pair(*it, tablet));
            PDLOG(INFO, "add tablet client. endpoint[%s]", it->c_str());
        } else {
            if (tit->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                tit->second->state_ = ::rtidb::api::TabletState::kTabletHealthy;
                tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
                PDLOG(INFO, "tablet is online. endpoint[%s]", tit->first.c_str());
                thread_pool_.AddTask(boost::bind(&NameServerImpl::OnTabletOnline, this, tit->first));
            }
        }
        PDLOG(INFO, "healthy tablet with endpoint[%s]", it->c_str());
    }
    // handle offline tablet
    Tablets::iterator tit = tablets_.begin();
    for (; tit !=  tablets_.end(); ++tit) {
        if (alive.find(tit->first) == alive.end() 
                && tit->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
            // tablet offline
            PDLOG(INFO, "offline tablet with endpoint[%s]", tit->first.c_str());
            tit->second->state_ = ::rtidb::api::TabletState::kTabletOffline;
            tit->second->ctime_ = ::baidu::common::timer::get_micros() / 1000;
            if (offline_endpoint_map_.find(tit->first) == offline_endpoint_map_.end()) {
                offline_endpoint_map_.insert(std::make_pair(tit->first, tit->second->ctime_));
                thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval, boost::bind(&NameServerImpl::OnTabletOffline, this, tit->first, false));
            } else {
                offline_endpoint_map_[tit->first] = tit->second->ctime_;
            }
        }
    }
}

void NameServerImpl::OnTabletOffline(const std::string& endpoint, bool startup_flag) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto tit = tablets_.find(endpoint);
    if (tit == tablets_.end()) {
        PDLOG(WARNING, "cannot find endpoint %s in tablet map", endpoint.c_str());
        return;
    }
    auto iter = offline_endpoint_map_.find(endpoint);
    if (iter == offline_endpoint_map_.end()) {
        PDLOG(WARNING, "cannot find endpoint %s in offline endpoint map", endpoint.c_str());
        return;
    }
    if (!startup_flag && tit->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(INFO, "endpoint %s is healthy, need not offline endpoint", endpoint.c_str());
        // offline_endpoint_map_.erase(iter);
        return;
    }
    if (table_info_.empty()) {
        PDLOG(INFO, "endpoint %s has no table, need not offline endpoint", endpoint.c_str());
        offline_endpoint_map_.erase(iter);
        return;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    if (!startup_flag && cur_time < iter->second + FLAGS_tablet_heartbeat_timeout) {
        thread_pool_.DelayTask(FLAGS_tablet_offline_check_interval, boost::bind(&NameServerImpl::OnTabletOffline, this, endpoint, false));
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire) && 
            zk_client_->IsExistNode(zk_offline_endpoint_lock_node_ + "/" + endpoint) == 0) {
        PDLOG(WARNING, "offline endpoint lock node is exist! endpoint %s", endpoint.c_str());
        return;
    }
    if (auto_failover_.load(std::memory_order_acquire) && 
            !zk_client_->CreateNode(zk_offline_endpoint_lock_node_ + "/" + endpoint, "1")) {
        PDLOG(WARNING, "create offline endpoint lock node failed! endpoint %s", endpoint.c_str());
        return;
    }
    for (const auto& kv : table_info_) {
        if (!auto_failover_.load(std::memory_order_acquire)) {
            CreateUpdateTableAliveOP(kv.second->name(), endpoint, false);
            continue;
        }
        std::set<uint32_t> leader_pid;
        std::set<uint32_t> follower_pid;
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                // tackle the alive partition only
                if (kv.second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    if (kv.second->table_partition(idx).partition_meta_size() == 1) {
                        CreateUpdatePartitionStatusOP(kv.first, kv.second->table_partition(idx).pid(), 
                                endpoint, true, false);
                        break;
                    }
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
            PDLOG(INFO, "table[%s] pid[%u] change leader", kv.first.c_str(), pid);
            CreateChangeLeaderOP(kv.first, pid, "");
        }
        // delete replica
        for (auto pid : follower_pid) {
            CreateOfflineReplicaOP(kv.first, pid, endpoint);
        }
    }
    offline_endpoint_map_.erase(iter);
}

void NameServerImpl::OnTabletOnline(const std::string& endpoint) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string value;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->GetNodeValue(FLAGS_zk_root_path + "/nodes/" + endpoint, value)) {
            PDLOG(WARNING, "get tablet node value failed");
            return;
        }
    }
    if (boost::starts_with(value, "startup_")) {
        PDLOG(INFO, "endpoint %s is startup, exe tablet offline", endpoint.c_str());
        OnTabletOffline(endpoint, true);
    }
    if (auto_recover_table_.load(std::memory_order_acquire)) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (zk_client_->IsExistNode(zk_offline_endpoint_lock_node_ + "/" + endpoint) != 0) {
                PDLOG(WARNING, "offline endpoint lock node is not exist, need not recover. endpoint %s", endpoint.c_str());
                return;
            }
        }
        RecoverEndpoint(endpoint);
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (!zk_client_->DeleteNode(zk_offline_endpoint_lock_node_ + "/" + endpoint)) {
                PDLOG(WARNING, "offline endpoint lock node delete failed. endpoint[%s]", endpoint.c_str());
            }
        }
    }
}

void NameServerImpl::RecoverEndpoint(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : table_info_) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid =  kv.second->table_partition(idx).pid();
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (kv.second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    PDLOG(INFO, "recover table[%s] pid[%u] endpoint[%s]", kv.first.c_str(), pid, endpoint.c_str());
                    CreateRecoverTableOP(kv.first, pid, endpoint);
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
        PDLOG(WARNING, "fail to init zookeeper with cluster[%s]", FLAGS_zk_cluster.c_str());
        return false;
    }
    task_vec_.resize(FLAGS_name_server_task_max_concurrency);
    std::string value;
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        zk_client_->CreateNode(FLAGS_zk_root_path + "/nodes", "");
    } else {
        UpdateTablets(endpoints);
    }
    zk_client_->WatchNodes(boost::bind(&NameServerImpl::UpdateTabletsLocked, this, _1));
    zk_client_->WatchNodes();
    session_term_ = zk_client_->GetSessionTerm();

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
    if (session_term_ != zk_client_->GetSessionTerm()) {
        if (zk_client_->WatchNodes()) {
            session_term_ = zk_client_->GetSessionTerm();
            PDLOG(INFO, "watch node ok");
        } else {
            PDLOG(WARNING, "watch node falied");
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
                for (const auto& op_list : task_vec_) {
                    if (op_list.empty()) {
                        continue;
                    }
                    std::shared_ptr<OPData> op_data = op_list.front();
                    if (op_data->op_info_.op_id() != response.task(idx).op_id() || op_data->task_list_.empty()) {
                        continue;
                    }
                    // update task status
                    std::shared_ptr<Task> task = op_data->task_list_.front();
                    if (task->task_info_->status() == ::rtidb::api::kFailed) {
                        continue;
                    }
                    if (task->task_info_->task_type() == response.task(idx).task_type() && 
                            task->task_info_->status() != response.task(idx).status()) {
                        PDLOG(INFO, "update task status from[%s] to[%s]. op_id[%lu], task_type[%s]", 
                                    ::rtidb::api::TaskStatus_Name(task->task_info_->status()).c_str(), 
                                    ::rtidb::api::TaskStatus_Name(response.task(idx).status()).c_str(), 
                                    response.task(idx).op_id(), 
                                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str());
                        task->task_info_->set_status(response.task(idx).status());
                    }
                    break;
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
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        std::shared_ptr<OPData> op_data = op_list.front();
        if (op_data->task_list_.empty()) {
            continue;
        }
        std::shared_ptr<Task> task = op_data->task_list_.front();
        if (task->task_info_->status() == ::rtidb::api::kDone) {
            uint32_t cur_task_index = op_data->op_info_.task_index();
            op_data->op_info_.set_task_index(cur_task_index + 1);
            std::string value;
            op_data->op_info_.SerializeToString(&value);
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
            if (zk_client_->SetNodeValue(node, value)) {
                PDLOG(DEBUG, "set zk status value success. node[%s] value[%s]",
                            node.c_str(), value.c_str());
                op_data->task_list_.pop_front();
                continue;
            }
            // revert task index
            op_data->op_info_.set_task_index(cur_task_index);
            PDLOG(WARNING, "set zk status value failed! node[%s] op_id[%lu] op_type[%s] task_index[%u]", 
                          node.c_str(), op_data->op_info_.op_id(), 
                          ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                          op_data->op_info_.task_index()); 
        }
    }
    return 0;
}

int NameServerImpl::DeleteTask() {
    std::vector<uint64_t> done_task_vec;
    std::vector<std::shared_ptr<TabletClient>> client_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& op_list : task_vec_) {
            if (op_list.empty()) {
                continue;
            }
            std::shared_ptr<OPData> op_data = op_list.front();
            if (op_data->task_list_.empty()) {
                done_task_vec.push_back(op_data->op_info_.op_id());
            } else {
                std::shared_ptr<Task> task = op_data->task_list_.front();
                if (task->task_info_->status() == ::rtidb::api::kFailed) {
                    done_task_vec.push_back(op_data->op_info_.op_id());
                }
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
        std::lock_guard<std::mutex> lock(mu_);
        for (auto op_id : done_task_vec) {
            std::shared_ptr<OPData> op_data;
            uint32_t index = 0;
            for (uint32_t idx = 0; idx < task_vec_.size(); idx++) {
                if (task_vec_[idx].empty()) {
                    continue;
                }
                if (task_vec_[idx].front()->op_info_.op_id() == op_id) {
                    op_data = task_vec_[idx].front();
                    index = idx;
                    break;
                }
            }    
            if (!op_data) {
                PDLOG(WARNING, "has not found op[%lu] in running op", op_id); 
                continue;
            }
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_id);
            if (!op_data->task_list_.empty() && 
                    op_data->task_list_.front()->task_info_->status() == ::rtidb::api::kFailed) {
                op_data->op_info_.set_task_status(::rtidb::api::kFailed);
                op_data->op_info_.set_end_time(::baidu::common::timer::now_time());
                PDLOG(WARNING, "set op[%s] status failed. op_id[%lu]",
                                ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str(),
                                op_id);
                std::string value;
                op_data->op_info_.SerializeToString(&value);
                if (!zk_client_->SetNodeValue(node, value)) {
                    PDLOG(WARNING, "set zk status value failed. node[%s] value[%s]",
                                node.c_str(), value.c_str());
                }
                done_op_list_.push_back(op_data);
                task_vec_[index].pop_front();
                PDLOG(INFO, "delete op[%lu] in running op", op_id); 
            } else {
                if (zk_client_->DeleteNode(node)) {
                    PDLOG(INFO, "delete zk op node[%s] success.", node.c_str()); 
                    op_data->op_info_.set_end_time(::baidu::common::timer::now_time());
                    if (op_data->op_info_.task_status() == ::rtidb::api::kDoing) {
                        op_data->op_info_.set_task_status(::rtidb::api::kDone);
                        op_data->task_list_.clear();
                    }
                    done_op_list_.push_back(op_data);
                    task_vec_[index].pop_front();
                    PDLOG(INFO, "delete op[%lu] in running op", op_id); 
                } else {
                    PDLOG(WARNING, "delete zk op_node failed. opid[%lu] node[%s]", op_id, node.c_str()); 
                }
            }
        }
    }
    return 0;
}

void NameServerImpl::ProcessTask() {
    while (running_.load(std::memory_order_acquire)) {
        {
            bool has_task = false;
            std::unique_lock<std::mutex> lock(mu_);
            for (const auto& op_list : task_vec_) {
                if (!op_list.empty()) {
                    has_task = true;
                    break;
                }
            }
            if (!has_task) {
                cv_.wait_for(lock, std::chrono::milliseconds(FLAGS_name_server_task_wait_time));
                if (!running_.load(std::memory_order_acquire)) {
                    PDLOG(WARNING, "cur nameserver is not leader");
                    return;
                }
            }

            for (const auto& op_list : task_vec_) {
                if (op_list.empty()) {
                    continue;
                }
                std::shared_ptr<OPData> op_data = op_list.front();
                if (op_data->task_list_.empty() || 
                        op_data->op_info_.task_status() == ::rtidb::api::kFailed) {
                    continue;
                }
                op_data->op_info_.set_task_status(::rtidb::api::kDoing);
                std::shared_ptr<Task> task = op_data->task_list_.front();
                if (task->task_info_->status() == ::rtidb::api::kFailed) {
                    PDLOG(WARNING, "task[%s] run failed, terminate op[%s]. op_id[%lu]",
                                    ::rtidb::api::TaskType_Name(task->task_info_->task_type()).c_str(),
                                    ::rtidb::api::OPType_Name(task->task_info_->op_type()).c_str(),
                                    task->task_info_->op_id());
                } else if (task->task_info_->status() == ::rtidb::api::kInited) {
                    PDLOG(DEBUG, "run task. opid[%lu] op_type[%s] task_type[%s]", task->task_info_->op_id(), 
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
        if (session_term_ != zk_client_->GetSessionTerm()) {
            if (zk_client_->WatchNodes()) {
                session_term_ = zk_client_->GetSessionTerm();
                PDLOG(INFO, "watch node ok");
            }
        }
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

void NameServerImpl::GetTablePartition(RpcController* controller,
        const GetTablePartitionRequest* request,
        GetTablePartitionResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string name = request->name();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        response->set_code(-1);
        response->set_msg("table is not exist");
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        ::rtidb::nameserver::TablePartition* table_partition = response->mutable_table_partition();
        table_partition->CopyFrom(iter->second->table_partition(idx));
        break;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::SetTablePartition(RpcController* controller,
        const SetTablePartitionRequest* request,
        GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string name = request->name();
    uint32_t pid = request->table_partition().pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        response->set_code(-1);
        response->set_msg("table is not exist");
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> cur_table_info(iter->second->New());
    cur_table_info->CopyFrom(*(iter->second));
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (cur_table_info->table_partition(idx).pid() != pid) {
            continue;
        }
        ::rtidb::nameserver::TablePartition* table_partition =
                cur_table_info->mutable_table_partition(idx);
        table_partition->Clear();        
        table_partition->CopyFrom(request->table_partition());
        std::string table_value;
        cur_table_info->SerializeToString(&table_value);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            response->set_code(-1);
            response->set_msg("set zk failed");
            return;
        }
        NotifyTableChanged();
        iter->second = cur_table_info;
        break;
    }
    response->set_code(0);
    response->set_msg("ok");
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
    std::shared_ptr<OPData> op_data;
    std::string value;
    request->SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kMakeSnapshotOP, value, op_data,
                    request->name(), request->pid()) < 0) {
        response->set_code(-1);
        response->set_msg("create makesnapshot op date error");
        PDLOG(WARNING, "create makesnapshot op data error. name[%s] pid[%u]", 
                        request->name().c_str(), request->pid());
        return;
    }
    if (CreateMakeSnapshotOPTask(op_data) < 0) {
        response->set_code(-1);
        response->set_msg("create makesnapshot op task failed");
        PDLOG(WARNING, "create makesnapshot op task failed. name[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        return;
    }
    if (AddOPData(op_data) < 0) {
        response->set_code(-1);
        response->set_msg("add op data failed");
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "add makesnapshot op ok. op_id[%lu] name[%s] pid[%u]", 
                 op_data->op_info_.op_id(), request->name().c_str(), request->pid());
}

int NameServerImpl::SetPartitionInfo(TableInfo& table_info) {
    uint32_t partition_num = FLAGS_partition_num;
    if (table_info.has_partition_num() && table_info.partition_num() > 0) {
        partition_num = table_info.partition_num();
    }
    std::vector<std::string> endpoint_vec;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : tablets_) {
            if (kv.second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                endpoint_vec.push_back(kv.first);
            }
        }
    }
    uint32_t replica_num = std::min(FLAGS_replica_num, (uint32_t)endpoint_vec.size());
    if (table_info.has_replica_num() && table_info.replica_num() > 0) {
        replica_num = table_info.replica_num();
    }
    if (endpoint_vec.size() < replica_num) {
        PDLOG(WARNING, "healthy endpoint num[%u] is less than replica_num[%u]",
                        endpoint_vec.size(), replica_num);
        return -1;
    }
    for (uint32_t pid = 0; pid < partition_num; pid++) {
        TablePartition* table_partition = table_info.add_table_partition();
        table_partition->set_pid(pid);
        PartitionMeta* partition_meta = table_partition->add_partition_meta();
        uint32_t endpoint_pos = rand_.Next() % endpoint_vec.size();
        partition_meta->set_endpoint(endpoint_vec[endpoint_pos]);
        partition_meta->set_is_leader(true);
        for (uint32_t idx = 1; idx < replica_num; idx++) {
            PartitionMeta* partition_meta = table_partition->add_partition_meta();
            partition_meta->set_endpoint(endpoint_vec[(endpoint_pos + idx) % endpoint_vec.size()]);
            partition_meta->set_is_leader(false);
        }
    }
    PDLOG(INFO, "set table partition ok. name[%s] partition_num[%u] replica_num[%u]", 
                 table_info.name().c_str(), partition_num, replica_num);
    return 0;
}

int NameServerImpl::CreateTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
            bool is_leader, const std::vector<::rtidb::base::ColumnDesc>& columns,
            std::map<uint32_t, std::vector<std::string>>& endpoint_map, uint64_t term) {
    ::rtidb::api::TTLType ttl_type = ::rtidb::api::TTLType::kAbsoluteTime;
    if (table_info->ttl_type() == "kLatestTime") {
        ttl_type = ::rtidb::api::TTLType::kLatestTime;
    }
    ::rtidb::api::CompressType compress_type = ::rtidb::api::CompressType::kNoCompress;
    if (table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        compress_type = ::rtidb::api::CompressType::kSnappy;
    }
    ::rtidb::api::TableMeta table_meta;
    for (uint32_t i = 0; i < columns.size(); i++) {
        if (columns[i].add_ts_idx) {
            table_meta.add_dimensions(columns[i].name);
        }
    }
    std::string schema;
    ::rtidb::base::SchemaCodec codec;
    bool codec_ok = codec.Encode(columns, schema);
    if (!codec_ok) {
        return false;
    }
    table_meta.set_name(table_info->name());
    table_meta.set_tid(table_index_);
    table_meta.set_ttl(table_info->ttl());
    table_meta.set_seg_cnt(table_info->seg_cnt());
    table_meta.set_schema(schema);
    table_meta.set_ttl_type(ttl_type);
    table_meta.set_compress_type(compress_type);
    if (table_info->has_key_entry_max_height()) {
        table_meta.set_key_entry_max_height(table_info->key_entry_max_height());
    }
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        table_meta.set_pid(pid);
        table_meta.clear_replicas();
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
            if (is_leader) {
                ::rtidb::nameserver::TablePartition* table_partition = table_info->mutable_table_partition(idx);
                ::rtidb::nameserver::TermPair* term_pair = table_partition->add_term_offset();
                term_pair->set_term(term);
                term_pair->set_offset(0);
                table_meta.set_mode(::rtidb::api::TableMode::kTableLeader);
                table_meta.set_term(term);
                for (const auto& endpoint : endpoint_map[pid]) {
                    table_meta.add_replicas(endpoint);
                }
            } else {
                if (endpoint_map.find(pid) == endpoint_map.end()) {
                    endpoint_map.insert(std::make_pair(pid, std::vector<std::string>()));
                }
                endpoint_map[pid].push_back(endpoint);
                table_meta.set_mode(::rtidb::api::TableMode::kTableFollower);
            }
            if (!tablet_ptr->client_->CreateTable(table_meta)) {
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

int NameServerImpl::DropTableOnTablet(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info) {
    uint32_t tid = table_info->tid();
    for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
        uint32_t pid = table_info->table_partition(idx).pid();
        for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
            std::shared_ptr<TabletInfo> tablet_ptr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto iter = tablets_.find(endpoint);
                // check tablet if exist
                if (iter == tablets_.end()) {
                    PDLOG(WARNING, "endpoint[%s] can not find client", endpoint.c_str());
                    continue;
                }
                tablet_ptr = iter->second;
                // check tablet healthy
                if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                    PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                    continue;
                }
            }
            if (!tablet_ptr->client_->DropTable(tid, pid)) {
                PDLOG(WARNING, "drop table failed. tid[%u] pid[%u] endpoint[%s]", 
                                tid, pid, endpoint.c_str());
            }
            PDLOG(INFO, "drop table success. tid[%u] pid[%u] endpoint[%s]", 
                        tid, pid, endpoint.c_str());
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
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value != "true" && value != "false") {
        response->set_code(-1);
        response->set_msg("invalid value");
        PDLOG(WARNING, "invalid value[%s]", request->conf().value().c_str());
        return;
    }
    if (key == "auto_failover") {
        if (!zk_client_->SetNodeValue(zk_auto_failover_node_, value)) {
            PDLOG(WARNING, "set auto_failover_node failed!");
            response->set_code(-1);
            response->set_msg("set auto_failover_node failed");
            return;
        }
        if (value == "true") {
            auto_failover_.store(true, std::memory_order_release);
        } else {
            auto_failover_.store(false, std::memory_order_release);
        }
    } else if (key == "auto_recover_table") {
        if (!zk_client_->SetNodeValue(zk_auto_recover_table_node_, value)) {
            PDLOG(WARNING, "set auto_recover_table_node failed!");
            response->set_code(-1);
            response->set_msg("set auto_recover_table_node failed");
            return;
        }
        if (value == "true") {
            auto_recover_table_.store(true, std::memory_order_release);
        } else {
            auto_recover_table_.store(false, std::memory_order_release);
        }
    } else {
        response->set_code(-1);
        response->set_msg("unsupport set this key");
        PDLOG(WARNING, "unsupport set key[%s]", key.c_str());
        return;
    }
    PDLOG(INFO, "config set ok. key[%s] value[%s]", key.c_str(), value.c_str());
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
    std::string name = request->name();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        response->set_code(-1);
        response->set_msg("table is not exist");
        return;
    }
    if (pid > (uint32_t)iter->second->table_partition_size() - 1) {
        PDLOG(WARNING, "pid[%u] is not exist, table[%s]", pid, name.c_str());
        response->set_code(-1);
        response->set_msg("pid is not exist");
        return;
    }
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        if (iter->second->table_partition(idx).partition_meta_size() == 1) {
            PDLOG(WARNING, "table[%s] pid[%u] has no followers, cannot change leader", 
                        name.c_str(), iter->second->table_partition(idx).pid());
            response->set_code(-1);
            response->set_msg("leader has no followers");
            return;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                if (!iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) { 
                    follower_endpoint.push_back(iter->second->table_partition(idx).partition_meta(meta_idx).endpoint());
                } else if (!request->has_candidate_leader()) {
                    PDLOG(WARNING, "leader is alive, cannot change leader. table[%s] pid[%u]",
                                    name.c_str(), pid);
                    response->set_code(-1);
                    response->set_msg("leader is alive");
                    return;
                }
            }
        }
        break;
    }
    if (follower_endpoint.empty()) {
        response->set_code(-1);
        response->set_msg("no alive follower");
        PDLOG(WARNING, "no alive follower. table[%s] pid[%u]", name.c_str(), pid);
        return;
    }
    std::string candidate_leader;
    if (request->has_candidate_leader() && request->candidate_leader() != "auto") {
        candidate_leader = request->candidate_leader();
    }
    if (CreateChangeLeaderOP(name, pid, candidate_leader) < 0) {
        response->set_code(-1);
        response->set_msg("change leader failed");
        PDLOG(WARNING, "change leader failed. name[%s] pid[%u]", name.c_str(), pid);
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
    uint32_t concurrency = FLAGS_name_server_task_concurrency;
    if (request->has_concurrency()) {
        if (request->concurrency() > FLAGS_name_server_task_max_concurrency) {
            response->set_code(-1);
            response->set_msg("concurrency is greater than the max value " + std::to_string(FLAGS_name_server_task_max_concurrency));
            PDLOG(WARNING, "concurrency is greater than the max value %u", FLAGS_name_server_task_max_concurrency);
            return;
        } else {
            concurrency = request->concurrency();
        }
    }
    std::string endpoint = request->endpoint();
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = tablets_.find(endpoint);
    if (iter == tablets_.end()) {
        response->set_code(-1);
        response->set_msg("endpoint is not exist");
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        return;
    }
    for (const auto& kv : table_info_) {
        for (int idx = 0; idx < kv.second->table_partition_size(); idx++) {
            uint32_t pid = kv.second->table_partition(idx).pid();
            if (kv.second->table_partition(idx).partition_meta_size() == 1 && 
                    kv.second->table_partition(idx).partition_meta(0).endpoint() == endpoint) {
                PDLOG(INFO, "table[%s] pid[%u] has no followers", kv.first.c_str(), pid);
                if (kv.second->table_partition(idx).partition_meta(0).is_alive()) {
                    CreateUpdatePartitionStatusOP(kv.first, pid, endpoint, true, false, concurrency);
                } else {
                    PDLOG(INFO, "table[%s] pid[%u] is_alive status is no, need not offline", 
                                kv.first.c_str(), pid);
                }
                continue;
            }
            std::string alive_leader;
            int endpoint_index = -1;
            for (int meta_idx = 0; meta_idx < kv.second->table_partition(idx).partition_meta_size(); meta_idx++) {
                const ::rtidb::nameserver::PartitionMeta& partition_meta = 
                    kv.second->table_partition(idx).partition_meta(meta_idx);  
                if (partition_meta.is_leader() && partition_meta.is_alive()) {
                    alive_leader = partition_meta.endpoint();
                }
                if (partition_meta.endpoint() == endpoint) {
                    endpoint_index = meta_idx;
                }
            }
            if (endpoint_index < 0) {
                continue;
            }
            const ::rtidb::nameserver::PartitionMeta& partition_meta = 
                    kv.second->table_partition(idx).partition_meta(endpoint_index);  
            if (partition_meta.is_leader()) {
                if (alive_leader.empty() || alive_leader == endpoint) {
                    PDLOG(INFO, "table[%s] pid[%u] change leader", kv.first.c_str(), pid);
                    CreateChangeLeaderOP(kv.first, pid, "", concurrency);
                } else {
                    PDLOG(INFO, "table[%s] pid[%u] need not change leader", kv.first.c_str(), pid);
                }
            } else {
                CreateOfflineReplicaOP(kv.first, pid, endpoint, concurrency);
            }
        }
    }
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
            PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
            return;
        } else if (iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            response->set_code(-1);
            response->set_msg("endpoint is not healthy");
            PDLOG(WARNING, "endpoint[%s] is not healthy", endpoint.c_str());
            return;
        }
    }
    RecoverEndpoint(endpoint);
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::RecoverTable(RpcController* controller,
            const RecoverTableRequest* request,
            GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::string name = request->name();
    std::string endpoint = request->endpoint();
    uint32_t pid = request->pid();
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        response->set_code(-1);
        response->set_msg("endpoint is not exist");
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        return;
    } else if (it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("endpoint is not healthy");
        PDLOG(WARNING, "endpoint[%s] is not healthy", endpoint.c_str());
        return;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        response->set_code(-1);
        response->set_msg("table is not exist");
        return;
    }
    bool has_found = false;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    PDLOG(WARNING, "status is alive, need not recover. name[%s] pid[%u] endpoint[%s]", 
                                    name.c_str(), pid, endpoint.c_str());
                    response->set_code(-1);
                    response->set_msg("table is alive, need not recover");
                    return;
                }
                has_found = true;
            }
        }
        break;
    }
    if (!has_found) {
        PDLOG(WARNING, "not found table[%s] pid[%u] in endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        response->set_code(-1);
        response->set_msg("has not found table partition in this endpoint");
        return;
    }
    CreateRecoverTableOP(name, pid, endpoint);
    PDLOG(INFO, "recover table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
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
    std::map<uint64_t, std::shared_ptr<OPData>> op_map;
    std::lock_guard<std::mutex> lock(mu_);
    DeleteDoneOP();
    for (const auto& op_data : done_op_list_) {
        if (request->has_name() && op_data->op_info_.name() != request->name()) {
            continue;
        }
        if (request->has_pid() && op_data->op_info_.pid() != request->pid()) {
            continue;
        }
        op_map.insert(std::make_pair(op_data->op_info_.op_id(), op_data));
    }
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        for (const auto& op_data : op_list) {
            if (request->has_name() && op_data->op_info_.name() != request->name()) {
                continue;
            }
            if (request->has_pid() && op_data->op_info_.pid() != request->pid()) {
                continue;
            }
            op_map.insert(std::make_pair(op_data->op_info_.op_id(), op_data));
        }
    }
    for (const auto& kv : op_map) {
        OPStatus* op_status = response->add_op_status();
        op_status->set_op_id(kv.second->op_info_.op_id());
        op_status->set_op_type(::rtidb::api::OPType_Name(kv.second->op_info_.op_type()));
        op_status->set_name(kv.second->op_info_.name());
        op_status->set_pid(kv.second->op_info_.pid());
        op_status->set_status(::rtidb::api::TaskStatus_Name(kv.second->op_info_.task_status()));
        if (kv.second->task_list_.empty() || kv.second->op_info_.task_status() == ::rtidb::api::kInited) {
            op_status->set_task_type("-");
        } else {
            std::shared_ptr<Task> task = kv.second->task_list_.front();
            op_status->set_task_type(::rtidb::api::TaskType_Name(task->task_info_->task_type()));
        }
        op_status->set_start_time(kv.second->op_info_.start_time());
        op_status->set_end_time(kv.second->op_info_.end_time());
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
                    PDLOG(WARNING, "table[%s] is not alive. pid[%u] endpoint[%s]", 
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
        PDLOG(WARNING, "delete table node[%s/%s] failed!", 
                        zk_table_data_path_.c_str(), request->name().c_str());
        code = -1;
    } else {
        PDLOG(INFO, "delete table node[%s/%s]", zk_table_data_path_.c_str(), request->name().c_str());
        table_info_.erase(request->name());
    }
    response->set_code(code);
    code == 0 ?  response->set_msg("ok") : response->set_msg("drop table error");
    NotifyTableChanged();
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
    if ((table_info->ttl_type() == "kAbsoluteTime" && table_info->ttl() > FLAGS_absolute_ttl_max) 
            || (table_info->ttl_type() == "kLatestTime" && table_info->ttl() > FLAGS_latest_ttl_max)) {
        response->set_code(-1);
        uint32_t max_ttl = table_info->ttl_type() == "kAbsoluteTime" ? FLAGS_absolute_ttl_max : FLAGS_latest_ttl_max;
        response->set_msg("ttl is greater than conf value. max ttl is " + std::to_string(max_ttl));
        PDLOG(WARNING, "ttl is greater than conf value. ttl[%lu] ttl_type[%s] max ttl[%u]", 
                        table_info->ttl(), table_info->ttl_type().c_str(), max_ttl);
        return;
    }
    if (table_info->table_partition_size() > 0) {
        std::set<uint32_t> pid_set;
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            pid_set.insert(table_info->table_partition(idx).pid());
        }
        auto iter = pid_set.rbegin();
        if (*iter != (uint32_t)table_info->table_partition_size() - 1) {
            response->set_code(-1);
            response->set_msg("pid is not start with zero and consecutive");
            PDLOG(WARNING, "pid is not start with zero and consecutive");
            return;
        }
    } else {
        // 
        if (SetPartitionInfo(*table_info) < 0) {
            response->set_code(-1);
            response->set_msg("set partition info failed");
            PDLOG(WARNING, "set partition info failed");
            return;
        }
    }
    uint32_t tid = 0;
    uint64_t cur_term = 0;
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
        tid = table_index_;
        cur_term = term_;
    }
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (::rtidb::base::SchemaCodec::ConvertColumnDesc(*table_info, columns) < 0) {
        response->set_code(-1);
        response->set_msg("convert column desc failed");
        PDLOG(WARNING, "convert table column desc failed. name[%s] tid[%u]", 
                        table_info->name().c_str(), tid);
        return;
    }
    std::map<uint32_t, std::vector<std::string>> endpoint_map;
    do {
        if (CreateTableOnTablet(table_info, false, columns, endpoint_map, cur_term) < 0 ||
                CreateTableOnTablet(table_info, true, columns, endpoint_map, cur_term) < 0) {
            response->set_code(-1);
            response->set_msg("create table failed");
            PDLOG(WARNING, "create table failed. name[%s] tid[%u]", 
                            table_info->name().c_str(), tid);
            break;
        }
        std::string table_value;
        table_info->SerializeToString(&table_value);
        if (!zk_client_->CreateNode(zk_table_data_path_ + "/" + table_info->name(), table_value)) {
            PDLOG(WARNING, "create table node[%s/%s] failed! value[%s] value_size[%u]", 
                            zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str(), table_value.length());
            response->set_code(-1);
            response->set_msg("create table node failed");
            break;
        }
        PDLOG(INFO, "create table node[%s/%s] success! value[%s] value_size[%u]", 
                      zk_table_data_path_.c_str(), table_info->name().c_str(), table_value.c_str(), table_value.length());
        {
            std::lock_guard<std::mutex> lock(mu_);
            table_info_.insert(std::make_pair(table_info->name(), table_info));
            NotifyTableChanged();
        }
        response->set_code(0);
        response->set_msg("ok");
        return;
    } while (0);
    task_thread_pool_.AddTask(boost::bind(&NameServerImpl::DropTableOnTablet, this, table_info));
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
    std::shared_ptr<OPData> op_data;
    std::string value;
    request->SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kAddReplicaOP, value, op_data, 
                    request->name(), request->pid()) < 0) {
        PDLOG(WARNING, "create AddReplicaOP data failed. table[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        response->set_code(-1);
        response->set_msg("create AddReplicaOP data failed");
        return;
    }
    if (CreateAddReplicaOPTask(op_data) < 0) {
        PDLOG(WARNING, "create AddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]",
                        request->name().c_str(), request->pid(), request->endpoint().c_str());
        response->set_code(-1);
        response->set_msg("create AddReplicaOP task failed");
        return;
    }
    if (AddOPData(op_data) < 0) {
        response->set_code(-1);
        response->set_msg("add op data failed");
        PDLOG(WARNING, "add op data failed. table[%s] pid[%u]",
                        request->name().c_str(), request->pid());
        return;
    }
    PDLOG(INFO, "add addreplica op ok. op_id[%lu] table[%s] pid[%u]", 
                op_data->op_info_.op_id(), request->name().c_str(), request->pid());
    response->set_code(0);
    response->set_msg("ok");
}

int NameServerImpl::CreateAddReplicaOPTask(std::shared_ptr<OPData> op_data) {
    AddReplicaNSRequest request;
    if (!request.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse request failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    auto it = tablets_.find(request.endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", request.endpoint().c_str());
        return -1;
    }
    auto pos = table_info_.find(request.name());
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", request.name().c_str());
        return -1;
    }
    uint32_t tid = pos->second->tid();
    uint32_t pid = request.pid();
    uint64_t ttl =  pos->second->ttl();
    uint32_t seg_cnt =  pos->second->seg_cnt();
    std::string leader_endpoint;
    if (GetLeader(pos->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", request.name().c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(request.endpoint(), op_index, 
                ::rtidb::api::OPType::kAddReplicaOP, request.name(), 
                tid, pid, ttl, seg_cnt, false);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid, request.endpoint());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddTableInfoTask(request.name(), pid, request.endpoint(),
                op_index, ::rtidb::api::OPType::kAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create addtableinfo task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create AddReplicaOP task ok. tid[%u] pid[%u] endpoint[%s]", 
                    tid, pid, request.endpoint().c_str());
    return 0;
}

void NameServerImpl::Migrate(RpcController* controller,
       const MigrateRequest* request,
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
    auto pos = tablets_.find(request->src_endpoint());
    if (pos == tablets_.end() || pos->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("src_endpoint is not exist or not healthy");
        PDLOG(WARNING, "src_endpoint[%s] is not exist or not healthy", request->src_endpoint().c_str());
        return;
    }
    pos = tablets_.find(request->des_endpoint());
    if (pos == tablets_.end() || pos->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("des_endpoint is not exist or not healthy");
        PDLOG(WARNING, "des_endpoint[%s] is not exist or not healthy", request->des_endpoint().c_str());
        return;
    }
    auto iter = table_info_.find(request->name());
    if (iter == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is not exist");
        PDLOG(WARNING, "table[%s] is not exist", request->name().c_str());
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    char error_msg[1024];
    bool has_error = false;
    for (int i = 0; i < request->pid_size(); i++) {
        uint32_t pid = request->pid(i);
        std::string leader_endpoint;
        bool has_found_src_endpoint = false;
        bool has_found_des_endpoint = false;
        for (int idx = 0; idx < table_info->table_partition_size(); idx++) {
            if (table_info->table_partition(idx).pid() != pid) {
                continue;
            }
            for (int meta_idx = 0; meta_idx < table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
                if (table_info->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    std::string endpoint = table_info->table_partition(idx).partition_meta(meta_idx).endpoint();
                    if (table_info->table_partition(idx).partition_meta(meta_idx).is_leader()) {
                        leader_endpoint = endpoint;
                    } 
                    if (request->src_endpoint() == endpoint) {
                        has_found_src_endpoint = true;
                    } else if (request->des_endpoint() == endpoint) {
                        has_found_des_endpoint = true;
                    } 
                }
            }
            break;
        }
        if (leader_endpoint.empty()) {
            sprintf(error_msg, "leader endpoint is empty. name[%s] pid[%u]", 
                            request->name().c_str(), pid);
            has_error = true;
            break;
        }
        if (leader_endpoint == request->src_endpoint()) {
            sprintf(error_msg, "cannot migrate leader. name[%s] pid[%u]",
                            request->name().c_str(), pid);
            has_error = true;
            break;
        }
        auto it = tablets_.find(leader_endpoint);
        if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            sprintf(error_msg, "leader[%s] is offline. name[%s] pid[%u]", 
                                leader_endpoint.c_str(), request->name().c_str(), pid);
            has_error = true;
            break;
        }
        if (!has_found_src_endpoint) {
            sprintf(error_msg, "src_endpoint[%s] has not partition[%u]. name[%s]", 
                            request->src_endpoint().c_str(), pid, request->name().c_str());
            has_error = true;
            break;
        }
        if (has_found_des_endpoint) {
            sprintf(error_msg, "partition[%u] is already in des_endpoint[%s]. name[%s]", 
                                pid, request->des_endpoint().c_str(), request->name().c_str());
            has_error = true;
            break;
        }
    }
    if (has_error) {
        response->set_code(-1);
        response->set_msg(error_msg);
        PDLOG(WARNING, "%s", error_msg);
        return;
    }
    for (int i = 0; i < request->pid_size(); i++) {
        uint32_t pid = request->pid(i);
        CreateMigrateOP(request->src_endpoint(), request->name(), pid, request->des_endpoint());
    }    
    response->set_code(0);
    response->set_msg("ok");
}

int NameServerImpl::CreateMigrateOP(const std::string& src_endpoint, const std::string& name, 
            uint32_t pid, const std::string& des_endpoint) {
    std::shared_ptr<OPData> op_data;
    MigrateInfo migrate_info;
    migrate_info.set_src_endpoint(src_endpoint);
    migrate_info.set_des_endpoint(des_endpoint);
    std::string value;
    migrate_info.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kMigrateOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create migrate op data failed. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", 
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    if (CreateMigrateTask(op_data) < 0) {
        PDLOG(WARNING, "create migrate op task failed. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", 
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add migrate op data failed. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", 
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add migrate op ok. op_id[%lu] src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", 
                 op_data->op_info_.op_id(), src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateMigrateTask(std::shared_ptr<OPData> op_data) { 
    MigrateInfo migrate_info;
    if (!migrate_info.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse migrate_info failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string src_endpoint = migrate_info.src_endpoint();
    std::string des_endpoint = migrate_info.des_endpoint();
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "get table info failed! name[%s]", name.c_str());
        return -1;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info = iter->second;
    uint32_t tid = table_info->tid();
    std::string leader_endpoint;
    if (GetLeader(table_info, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    auto it = tablets_.find(leader_endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "leader[%s] is not online", leader_endpoint.c_str());
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u] endpoint[%s]", 
                        tid, pid, leader_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, pid, des_endpoint.c_str());
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", 
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(des_endpoint, op_index, ::rtidb::api::OPType::kMigrateOP, 
                 name, tid, pid, table_info->ttl(), table_info->seg_cnt(), false);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u] endpoint[%s]", 
                        tid, pid, des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, pid, des_endpoint.c_str());
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", 
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", 
                        tid, pid, leader_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDelReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, pid, src_endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. tid[%u] pid[%u] leader[%s] follower[%s]", 
                        tid, pid, leader_endpoint.c_str(), src_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(src_endpoint, op_index, 
                ::rtidb::api::OPType::kMigrateOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u] endpoint[%s]", 
                        tid, pid, src_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdateTableInfoTask(src_endpoint, name, pid, des_endpoint, 
                op_index, ::rtidb::api::OPType::kMigrateOP);
    if (!task) {
        PDLOG(WARNING, "create migrate table info task failed. tid[%u] pid[%u] endpoint[%s] des_endpoint[%s]", 
                        tid, pid, src_endpoint.c_str(), des_endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create migrate op task ok. src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", 
                 src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
    return 0;
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
    if (table_info_.find(request->name()) == table_info_.end()) {
        response->set_code(-1);
        response->set_msg("table is not  exist!");
        PDLOG(WARNING, "table[%s] is not exist!", request->name().c_str());
        return;
    }
    auto it = tablets_.find(request->endpoint());
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        response->set_code(-1);
        response->set_msg("tablet is not online");
        PDLOG(WARNING, "tablet[%s] is not online", request->endpoint().c_str());
        return;
    }
    if (CreateDelReplicaOP(request->name(), request->pid(), request->endpoint()) < 0) {
        response->set_code(-1);
        response->set_msg("create op failed");
    } else {
        response->set_code(0);
        response->set_msg("ok");
    }
}

int NameServerImpl::CreateOPData(::rtidb::api::OPType op_type, const std::string& value, 
        std::shared_ptr<OPData>& op_data, const std::string& name, uint32_t pid) {
    if (!zk_client_->SetNodeValue(zk_op_index_node_, std::to_string(op_index_ + 1))) {
        PDLOG(WARNING, "set op index node failed! op_index[%lu]", op_index_);
        return -1;
    }
    op_index_++;
    op_data = std::make_shared<OPData>();
    op_data->op_info_.set_start_time(::baidu::common::timer::now_time());
    op_data->op_info_.set_op_id(op_index_);
    op_data->op_info_.set_op_type(op_type);
    op_data->op_info_.set_task_index(0);
    op_data->op_info_.set_data(value);
    op_data->op_info_.set_task_status(::rtidb::api::kInited);
    op_data->op_info_.set_name(name);
    op_data->op_info_.set_pid(pid);
    return 0;
}

int NameServerImpl::AddOPData(const std::shared_ptr<OPData>& op_data, uint32_t concurrency) {
    uint32_t idx = op_data->op_info_.pid() % task_vec_.size();
    if (concurrency < task_vec_.size() && concurrency > 0) {
        idx = op_data->op_info_.pid() % concurrency;
    }
    op_data->op_info_.set_vec_idx(idx);
    std::string value;
    op_data->op_info_.SerializeToString(&value);
    std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
    if (!zk_client_->CreateNode(node, value)) {
        PDLOG(WARNING, "create op node[%s] failed. op_index[%lu] op_type[%s]", 
                        node.c_str(), op_data->op_info_.op_id(),
                        ::rtidb::api::OPType_Name(op_data->op_info_.op_type()).c_str());
        return -1;
    }
    task_vec_[idx].push_back(op_data);
    DeleteDoneOP();
    cv_.notify_one();
    return 0;
}

void NameServerImpl::DeleteDoneOP() {
    if (done_op_list_.empty()) {
        return;
    }
    while (done_op_list_.size() > (uint32_t)FLAGS_max_op_num) {
        std::shared_ptr<OPData> op_data = done_op_list_.front();
		if (op_data->op_info_.task_status() == ::rtidb::api::TaskStatus::kFailed) {
            std::string node = zk_op_data_path_ + "/" + std::to_string(op_data->op_info_.op_id());
            if (zk_client_->DeleteNode(node)) {
                PDLOG(INFO, "delete zk op node[%s] success.", node.c_str()); 
                op_data->task_list_.clear();
            } else {
                PDLOG(WARNING, "delete zk op_node failed. op_id[%lu] node[%s]", 
                                op_data->op_info_.op_id(), node.c_str()); 
                break;
            }
        }
        PDLOG(INFO, "done_op_list size[%u] is greater than the max_op_num[%u], delete op[%lu]", 
                    done_op_list_.size(), (uint32_t)FLAGS_max_op_num, op_data->op_info_.op_id()); 
        done_op_list_.pop_front();
    }
}

int NameServerImpl::CreateDelReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kDelReplicaOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create op data error. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (CreateDelReplicaOPTask(op_data) < 0) {
        PDLOG(WARNING, "create delreplica op task failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add delreplica op. op_id[%lu] table[%s] pid[%u] endpoint[%s]", 
                op_index_, name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateDelReplicaOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (leader_endpoint == endpoint) {
        PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kDelReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDelTableInfoTask(name, pid, endpoint, op_index, ::rtidb::api::OPType::kDelReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create deltableinfo task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(endpoint, op_index, ::rtidb::api::OPType::kDelReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u] endpoint[%s]", 
                        tid, pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create DelReplica op task ok. table[%s] pid[%u] endpoint[%s]", 
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}    

int NameServerImpl::CreateOfflineReplicaOP(const std::string& name, uint32_t pid, 
                const std::string& endpoint, uint32_t concurrency) {
    std::string value = endpoint;
    std::shared_ptr<OPData> op_data;
    if (CreateOPData(::rtidb::api::OPType::kOfflineReplicaOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create op data failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateOfflineReplicaTask(op_data) < 0) {
        PDLOG(WARNING, "create offline replica task failed. table[%s] pid[%u] endpoint[%s]", name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "add kOfflineReplicaOP. op_id[%lu] table[%s] pid[%u] endpoint[%s]", 
                op_index_, name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateOfflineReplicaTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::string leader_endpoint;
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
        PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    if (leader_endpoint == endpoint) {
        PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kOfflineReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, false,
                op_index, ::rtidb::api::OPType::kOfflineReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create OfflineReplica task ok. table[%s] pid[%u] endpoint[%s]", 
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateChangeLeaderOP(const std::string& name, uint32_t pid, 
            const std::string& candidate_leader, uint32_t concurrency) {
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    uint32_t tid = iter->second->tid();
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                std::string endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();
                if (!iter->second->table_partition(idx).partition_meta(meta_idx).is_leader()) { 
                    auto tablets_iter = tablets_.find(endpoint);
                    if (tablets_iter != tablets_.end() && 
                            tablets_iter->second->state_ == ::rtidb::api::TabletState::kTabletHealthy) {
                        follower_endpoint.push_back(endpoint);
                    } else {
                        PDLOG(WARNING, "endpoint[%s] is offline. table[%s] pid[%u]", 
                                        endpoint.c_str(), name.c_str(), pid);
                    }
                }
            }
        }
        break;
    }
    if (follower_endpoint.empty()) {
        PDLOG(INFO, "table not found follower. name[%s] pid[%u]", name.c_str(), pid);
        return 0;
    }
    if (!candidate_leader.empty() && std::find(follower_endpoint.begin(), follower_endpoint.end(), candidate_leader) == follower_endpoint.end()) {
        PDLOG(WARNING, "candidate_leader[%s] is not in followers. name[%s] pid[%u]", candidate_leader.c_str(), name.c_str(), pid);
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    ChangeLeaderData change_leader_data;
    change_leader_data.set_name(name);
    change_leader_data.set_tid(tid);
    change_leader_data.set_pid(pid);
    for (const auto& endpoint : follower_endpoint) {
        change_leader_data.add_follower(endpoint);
    }
    if (!candidate_leader.empty()) {
        change_leader_data.set_candidate_leader(candidate_leader);
    }
    std::string value;
    change_leader_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kChangeLeaderOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ChangeLeaderOP data error. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    if (CreateChangeLeaderOPTask(op_data) < 0) {
        PDLOG(WARNING, "create ChangeLeaderOP task failed. table[%s] pid[%u]",
                        name.c_str(), pid);
        return -1;
    }
    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u]", name.c_str(), pid);
        return -1;
    }
    PDLOG(INFO, "add changeleader op. op_id[%lu] table[%s] pid[%u]", 
                op_data->op_info_.op_id(), name.c_str(), pid);
    return 0;
}

int NameServerImpl::CreateChangeLeaderOPTask(std::shared_ptr<OPData> op_data) {
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]", 
                        op_data->op_info_.op_id(), op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = change_leader_data.name();
    uint32_t tid = change_leader_data.tid();
    uint32_t pid = change_leader_data.pid();
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < change_leader_data.follower_size(); idx++) {
        follower_endpoint.push_back(change_leader_data.follower(idx));
    }
    std::shared_ptr<Task> task = CreateSelectLeaderTask(
                op_data->op_info_.op_id(), ::rtidb::api::OPType::kChangeLeaderOP, 
                name, tid, pid, follower_endpoint);
    if (!task) {
        PDLOG(WARNING, "create selectleader task failed. table[%s] pid[%u]", 
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateChangeLeaderTask(op_data->op_info_.op_id(), 
                ::rtidb::api::OPType::kChangeLeaderOP, name, pid);
    if (!task) {
        PDLOG(WARNING, "create changeleader task failed. table[%s] pid[%u]", 
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdateLeaderInfoTask(op_data->op_info_.op_id(), 
                ::rtidb::api::OPType::kChangeLeaderOP, name, pid);
    if (!task) {
        PDLOG(WARNING, "create updateleaderinfo task failed. table[%s] pid[%u]", 
                        name.c_str(), pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ChangeLeader op task ok. name[%s] pid[%u]", name.c_str(), pid);
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

int NameServerImpl::CreateRecoverTableOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kRecoverTableOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create RecoverTableOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return -1;
    }
    do {
        std::string leader_endpoint;
        uint32_t tid = iter->second->tid();
        if (GetLeader(iter->second, pid, leader_endpoint) < 0 || leader_endpoint.empty()) {
            PDLOG(WARNING, "get leader failed. table[%s] pid[%u]", name.c_str(), pid);
            break;
        }
        if (leader_endpoint == endpoint) {
            PDLOG(WARNING, "endpoint is leader. table[%s] pid[%u]", name.c_str(), pid);
            break;
        }
        std::shared_ptr<Task> task = CreateDelReplicaTask(leader_endpoint, op_data->op_info_.op_id(), 
                    ::rtidb::api::OPType::kRecoverTableOP, tid, pid, endpoint);
        if (!task) {
            PDLOG(WARNING, "create delreplica task failed. table[%s] pid[%u] endpoint[%s]", 
                            name.c_str(), pid, endpoint.c_str());
            break;
        }
        op_data->task_list_.push_back(task);
    } while (0);
    std::shared_ptr<Task> task = CreateRecoverTableTask(op_data->op_info_.op_id(), 
            ::rtidb::api::OPType::kRecoverTableOP, name, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create RecoverTable task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create RecoverTable op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateRecoverTableOPTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
    std::shared_ptr<Task> task = CreateRecoverTableTask(op_data->op_info_.op_id(), 
            ::rtidb::api::OPType::kRecoverTableOP, name, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create RecoverTable task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create RecoverTable task ok. name[%s] pid[%u] endpoint[%s]",
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

std::shared_ptr<Task> NameServerImpl::CreateRecoverTableTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                const std::string& name, uint32_t pid, const std::string& endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kRecoverTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::RecoverEndpointTable, this, name, pid, endpoint, task->task_info_);
    return task;
}

void NameServerImpl::RecoverEndpointTable(const std::string& name, uint32_t pid, const std::string& endpoint,
            std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    uint32_t tid = 0;
    std::shared_ptr<TabletInfo> leader_tablet_ptr;
    std::shared_ptr<TabletInfo> tablet_ptr;
    bool has_follower = true;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
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
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                    leader_tablet_ptr = tablet_iter->second;
                    if (leader_tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                        PDLOG(WARNING, "leader endpoint [%s] is offline", leader_endpoint.c_str());
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                }
                if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                    if (iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                        PDLOG(INFO, "endpoint[%s] is alive, need not recover. name[%s] pid[%u]", 
                                        endpoint.c_str(), name.c_str(), pid);
                        task_info->set_status(::rtidb::api::TaskStatus::kDone);
                        return;
                    }
                    auto tablet_iter = tablets_.find(endpoint);
                    if (tablet_iter == tablets_.end()) {
                        PDLOG(WARNING, "can not find the endpoint[%s]'s client", endpoint.c_str());
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                    tablet_ptr = tablet_iter->second;
                    if (tablet_ptr->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
                        PDLOG(WARNING, "endpoint [%s] is offline", endpoint.c_str());
                        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
                        return;
                    }
                    if (iter->second->table_partition(idx).partition_meta_size() == 1) {
                        has_follower = false;           
                        break;
                    }
                }
            }
            break;
        }
    }
    if ((has_follower && !leader_tablet_ptr) || !tablet_ptr) {
        PDLOG(WARNING, "not has tablet. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                        name.c_str(), tid, pid, endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    bool has_table = false;
    bool is_leader = false;
    uint64_t term = 0;
    uint64_t offset = 0;
    if (!tablet_ptr->client_->GetTermPair(tid, pid, term, offset, has_table, is_leader)) {
        PDLOG(WARNING, "GetTermPair failed. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                        name.c_str(), tid, pid, endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    if (!has_follower) {
        std::lock_guard<std::mutex> lock(mu_);
        if (has_table) {
            CreateUpdatePartitionStatusOP(name, pid, endpoint, true, true);
        } else {
            CreateReLoadTableOP(name, pid, endpoint);
        }
        task_info->set_status(::rtidb::api::TaskStatus::kDone);
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                    task_info->op_id(), 
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
        return;
    }
    if (has_table && is_leader) {
        if (!tablet_ptr->client_->ChangeRole(tid, pid, false)) {
            PDLOG(WARNING, "change role failed. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                            name.c_str(), tid, pid, endpoint.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "change to follower. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                    name.c_str(), tid, pid, endpoint.c_str());
    }
    if (!has_table) {
        if (!tablet_ptr->client_->DeleteBinlog(tid, pid)) {
            PDLOG(WARNING, "delete binlog failed. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                            name.c_str(), tid, pid, endpoint.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        PDLOG(INFO, "delete binlog ok. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                            name.c_str(), tid, pid, endpoint.c_str());
    }
    int ret_code = MatchTermOffset(name, pid, has_table, term, offset);
    if (ret_code < 0) {
        PDLOG(WARNING, "match error. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                        name.c_str(), tid, pid, endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ::rtidb::api::Manifest manifest;
    if (!leader_tablet_ptr->client_->GetManifest(tid, pid, manifest)) {
        PDLOG(WARNING, "get manifest failed. name[%s] tid[%u] pid[%u]", 
                name.c_str(), tid, pid);
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    PDLOG(INFO, "offset[%lu] manifest offset[%lu]. name[%s] tid[%u] pid[%u]", 
                 offset,  manifest.offset(), name.c_str(), tid, pid);
    if (has_table) {
        if (ret_code == 0 && offset >= manifest.offset()) {
            CreateReAddReplicaSimplifyOP(name, pid, endpoint);
        } else {
            CreateReAddReplicaWithDropOP(name, pid, endpoint);
        }
    } else {
        if (ret_code == 0 && offset >= manifest.offset()) {
            CreateReAddReplicaNoSendOP(name, pid, endpoint);
        } else {
            CreateReAddReplicaOP(name, pid, endpoint);
        }
    }
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "recover table task run success. name[%s] tid[%u] pid[%u]", 
                name.c_str(), tid, pid);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

int NameServerImpl::CreateReAddReplicaOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (CreateReAddReplicaTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;

    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
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
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaOP, name, 
                tid, pid, ttl, seg_cnt, false);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index, ::rtidb::api::OPType::kReAddReplicaOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica op task ok. name[%s] pid[%u] endpoint[%s]", 
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaWithDropOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaWithDropOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ReAddReplicaWithDropOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReAddReplicaWithDropTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaWithDropOP task error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica with drop op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                 op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaWithDropTask(std::shared_ptr<OPData> op_data) { 
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
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
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateDropTableTask(endpoint, op_index, ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create droptable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateSendSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create sendsnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, name, 
                tid, pid, ttl, seg_cnt, false);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaWithDropOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index, ::rtidb::api::OPType::kReAddReplicaWithDropOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ReAddReplicaWithDrop task ok. name[%s] pid[%u] endpoint[%s]", 
                name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaNoSendOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        PDLOG(WARNING, "tablet[%s] is not online", endpoint.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaNoSendOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ReAddReplicaNoSendOP data failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (CreateReAddReplicaNoSendTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaNoSendOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica no send op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReAddReplicaNoSendTask(std::shared_ptr<OPData> op_data) { 
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
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
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreatePauseSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create pausesnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateLoadTableTask(endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, name, 
                tid, pid, ttl, seg_cnt, false);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateAddReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateRecoverSnapshotTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaNoSendOP, tid, pid);
    if (!task) {
        PDLOG(WARNING, "create recoversnapshot task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index, ::rtidb::api::OPType::kReAddReplicaNoSendOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica no send task ok. name[%s] pid[%u] endpoint[%s]", 
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateUpdateTableAliveOP(const std::string& name, 
                const std::string& endpoint, bool is_alive) {
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    EndpointStatusData endpoint_status_data;
    endpoint_status_data.set_endpoint(endpoint);
    endpoint_status_data.set_is_alive(is_alive);
    std::string value;
    endpoint_status_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kUpdateTableAliveOP, value, op_data, name, 0) < 0) {
        PDLOG(WARNING, "create UpdateTableAliveOP data error. table[%s] endpoint[%s]",
                        name.c_str(), endpoint.c_str());
        return -1;
    }
    if (CreateUpdateTableAliveOPTask(op_data) < 0) {
        PDLOG(WARNING, "create UpdateTableAliveOP task failed. table[%s] endpoint[%s]",
                        name.c_str(), endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add UpdateTableAliveOP data failed. name[%s] endpoint[%s]", 
                        name.c_str(), endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create UpdateTableAliveOP ok. op_id[%lu] name[%s] endpoint[%s] is_alive[%d]", 
                op_data->op_info_.op_id(), name.c_str(), endpoint.c_str(), is_alive);
    return 0;
}

int NameServerImpl::CreateUpdateTableAliveOPTask(std::shared_ptr<OPData> op_data) { 
    EndpointStatusData endpoint_status_data;
    if (!endpoint_status_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse endpoint_status_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    std::string endpoint = endpoint_status_data.endpoint();
    bool is_alive = endpoint_status_data.is_alive();
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        return -1;
    }
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist", name.c_str());
        return -1;
    }
    std::shared_ptr<Task> task = CreateUpdateTableAliveTask(name, endpoint, is_alive, 
                        op_data->op_info_.op_id(), ::rtidb::api::OPType::kUpdateTableAliveOP);
    if (!task) {
        PDLOG(WARNING, "create UpdateTableAliveTask failed. name[%s] endpoint[%s]", 
                        name.c_str(), endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create UpdateTableAliveOP task ok. name[%s] endpoint[%s] is_alive[%d]", 
                 name.c_str(), endpoint.c_str(), is_alive);
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
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReAddReplicaSimplifyOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ReAddReplicaSimplifyOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReAddReplicaSimplifyTask(op_data) < 0) {
        PDLOG(WARNING, "create ReAddReplicaSimplifyOP task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create readdreplica simplify op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;

}

int NameServerImpl::CreateReAddReplicaSimplifyTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
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
    uint64_t op_index = op_data->op_info_.op_id();
    std::shared_ptr<Task> task = CreateAddReplicaTask(leader_endpoint, op_index, 
                ::rtidb::api::OPType::kReAddReplicaSimplifyOP, tid, pid, endpoint);
    if (!task) {
        PDLOG(WARNING, "create addreplica task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, false, true, 
                op_index, ::rtidb::api::OPType::kReAddReplicaSimplifyOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create readdreplica simplify task ok. name[%s] pid[%u] endpoint[%s]", 
                 name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableOP(const std::string& name, uint32_t pid, const std::string& endpoint) {
    std::shared_ptr<OPData> op_data;
    std::string value = endpoint;
    if (CreateOPData(::rtidb::api::OPType::kReLoadTableOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create ReLoadTableOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateReLoadTableTask(op_data) < 0) {
        PDLOG(WARNING, "create ReLoadTable task failed. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (AddOPData(op_data) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create ReLoadTableOP op ok. op_id[%lu] name[%s] pid[%u] endpoint[%s]", 
                op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateReLoadTableTask(std::shared_ptr<OPData> op_data) {
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = op_data->op_info_.data();
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
    std::shared_ptr<Task> task = CreateLoadTableTask(endpoint, op_data->op_info_.op_id(), 
                ::rtidb::api::OPType::kReLoadTableOP, name, 
                tid, pid, ttl, seg_cnt, true);
    if (!task) {
        PDLOG(WARNING, "create loadtable task failed. tid[%u] pid[%u]", tid, pid);
        return -1;
    }
    op_data->task_list_.push_back(task);
    task = CreateUpdatePartitionStatusTask(name, pid, endpoint, true, true, 
                op_data->op_info_.op_id(), ::rtidb::api::OPType::kReLoadTableOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create ReLoadTable task ok. name[%s] pid[%u] endpoint[%s]", 
                name.c_str(), pid, endpoint.c_str());
    return 0;
}

int NameServerImpl::CreateUpdatePartitionStatusOP(const std::string& name, uint32_t pid, 
                const std::string& endpoint, bool is_leader, bool is_alive, uint32_t concurrency) {
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    std::shared_ptr<OPData> op_data;
    EndpointStatusData endpoint_status_data;
    endpoint_status_data.set_endpoint(endpoint);
    endpoint_status_data.set_is_leader(is_leader);
    endpoint_status_data.set_is_alive(is_alive);
    std::string value;
    endpoint_status_data.SerializeToString(&value);
    if (CreateOPData(::rtidb::api::OPType::kUpdatePartitionStatusOP, value, op_data, name, pid) < 0) {
        PDLOG(WARNING, "create UpdatePartitionStatusOP data error. table[%s] pid[%u] endpoint[%s]",
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    if (CreateUpdatePartitionStatusOPTask(op_data) < 0) {
        PDLOG(WARNING, "create UpdatePartitionStatusOP task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }

    if (AddOPData(op_data, concurrency) < 0) {
        PDLOG(WARNING, "add op data failed. name[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    PDLOG(INFO, "create UpdatePartitionStatusOP op ok."
                 "op_id[%lu] name[%s] pid[%u] endpoint[%s] is_leader[%d] is_alive[%d] concurrency[%u]", 
                 op_data->op_info_.op_id(), name.c_str(), pid, endpoint.c_str(), is_leader, is_alive, concurrency);
    return 0;
}

int NameServerImpl::CreateUpdatePartitionStatusOPTask(std::shared_ptr<OPData> op_data) { 
    EndpointStatusData endpoint_status_data;
    if (!endpoint_status_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse endpont_status_data failed. data[%s]", op_data->op_info_.data().c_str());
        return -1;
    }
    std::string name = op_data->op_info_.name();
    uint32_t pid = op_data->op_info_.pid();
    std::string endpoint = endpoint_status_data.endpoint();
    bool is_leader = endpoint_status_data.is_leader();
    bool is_alive = endpoint_status_data.is_alive();
    auto pos = table_info_.find(name);
    if (pos == table_info_.end()) {
        PDLOG(WARNING, "table[%s] is not exist!", name.c_str());
        return -1;
    }
    std::shared_ptr<Task> task = CreateUpdatePartitionStatusTask(name, pid, endpoint, is_leader, is_alive, 
                op_data->op_info_.op_id(), ::rtidb::api::OPType::kUpdatePartitionStatusOP);
    if (!task) {
        PDLOG(WARNING, "create update table alive status task failed. table[%s] pid[%u] endpoint[%s]", 
                        name.c_str(), pid, endpoint.c_str());
        return -1;
    }
    op_data->task_list_.push_back(task);
    PDLOG(INFO, "create UpdatePartitionStatusOP task ok."
                 "name[%s] pid[%u] endpoint[%s] is_leader[%d] is_alive[%d]", 
                 name.c_str(), pid, endpoint.c_str(), is_leader, is_alive);
    return 0;
}

int NameServerImpl::MatchTermOffset(const std::string& name, uint32_t pid, bool has_table, uint64_t term, uint64_t offset) {
    if (!has_table && offset == 0) {
        PDLOG(INFO, "has not table, offset is zero. name[%s] pid[%u]", name.c_str(), pid);
        return 1;
    }
    std::map<uint64_t, uint64_t> term_map;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = table_info_.find(name);
        if (iter == table_info_.end()) {
            PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
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
        PDLOG(WARNING, "not found term[%lu] in table_info. name[%s] pid[%u]", 
                        term, name.c_str(), pid);
        return 1;
    } else if (iter->second > offset) {
        if (term_map.rbegin()->second == offset + 1) {
            PDLOG(INFO, "term[%lu] offset[%lu] has matched. name[%s] pid[%u]", 
                            term, offset, name.c_str(), pid);
            return 0;
        }
        PDLOG(INFO, "offset is not matched. name[%s] pid[%u] term[%lu] term start offset[%lu] cur offset[%lu]", 
                        name.c_str(), pid, term, iter->second, offset);
        return 1;
    }
    iter++;
    if (iter == term_map.end()) {
        PDLOG(INFO, "cur term[%lu] is the last one. name[%s] pid[%u]", 
                        term, name.c_str(), pid);
        return 0;
    }
    if (iter->second <= offset) {
        PDLOG(INFO, "term[%lu] offset not matched. name[%s] pid[%u] offset[%lu]", 
                        term, name.c_str(), pid, offset);
        return 1;
    }
    PDLOG(INFO, "term[%lu] offset has matched. name[%s] pid[%u] offset[%lu]", 
                    term, name.c_str(), pid, offset);
    return 0;
}

void NameServerImpl::WrapTaskFun(const boost::function<bool ()>& fun, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!fun()) {
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        PDLOG(WARNING, "task[%s] run failed. op_id[%lu]", 
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str(), task_info->op_id());
    }
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::MakeSnapshot, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::PauseSnapshot, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::RecoverSnapshot, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::SendSnapshot, it->second->client_, tid, pid, 
                des_endpoint, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateLoadTableTask(const std::string& endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type, const std::string& name, 
                    uint32_t tid, uint32_t pid, uint64_t ttl, uint32_t seg_cnt, bool is_leader) {
    std::shared_ptr<Task> task = std::make_shared<Task>(endpoint, std::make_shared<::rtidb::api::TaskInfo>());
    auto it = tablets_.find(endpoint);
    if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
        return std::shared_ptr<Task>();
    }
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kLoadTable);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    boost::function<bool ()> fun = boost::bind(&TabletClient::LoadTable, it->second->client_, name, tid, pid, 
                ttl, is_leader, std::vector<std::string>(),
                seg_cnt, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::AddReplica, it->second->client_, tid, pid, 
                des_endpoint, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
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
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
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
    NotifyTableChanged();
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::DelReplica, it->second->client_, tid, pid, 
                follower_endpoint, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
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
    boost::function<bool ()> fun = boost::bind(&TabletClient::DropTable, it->second->client_, tid, pid, task->task_info_);
    task->fun_ = boost::bind(&NameServerImpl::WrapTaskFun, this, fun, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdateTableInfoTask(const std::string& src_endpoint,
                    const std::string& name, uint32_t pid, const std::string& des_endpoint,
                    uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdateTableInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdateTableInfo, this, src_endpoint, name, pid, 
            des_endpoint,task->task_info_);
    return task;
}

void NameServerImpl::UpdateTableInfo(const std::string& src_endpoint, const std::string& name, uint32_t pid,
                const std::string& des_endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
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
        int src_endpoint_index = -1;
        int des_endpoint_index = -1;
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            std::string endpoint = iter->second->table_partition(idx).partition_meta(meta_idx).endpoint();
            if (endpoint == src_endpoint) {
                src_endpoint_index = meta_idx;
            } else if (endpoint == des_endpoint) {
                des_endpoint_index = meta_idx;
            }
        }
        if (src_endpoint_index < 0) {
            PDLOG(WARNING, "has not found src_endpoint[%s]. name[%s] pid[%u]", 
                            src_endpoint.c_str(), name.c_str(), pid);
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        ::rtidb::nameserver::TablePartition* table_partition = 
                    iter->second->mutable_table_partition(idx);
        ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta_field = 
                    table_partition->mutable_partition_meta();
        if (des_endpoint_index < 0) {
            // use src_endpoint's meta when the meta of des_endpoint is not exist
            PDLOG(INFO, "des_endpoint meta is not exist, use src_endpoint's meta."
                        "src_endpoint[%s] name[%s] pid[%u] des_endpoint[%s]", 
                        src_endpoint.c_str(), name.c_str(), pid, des_endpoint.c_str());
            ::rtidb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(src_endpoint_index);
            partition_meta->set_endpoint(des_endpoint);
            partition_meta->set_is_alive(true);
            partition_meta->set_is_leader(false);
        } else {
            ::rtidb::nameserver::PartitionMeta* partition_meta = partition_meta_field->Mutable(des_endpoint_index);
            partition_meta->set_is_alive(true);
            partition_meta->set_is_leader(false);
            PDLOG(INFO, "remove partition[%u] in endpoint[%s]. name[%s]", 
                        pid, src_endpoint.c_str(), name.c_str());
            partition_meta_field->DeleteSubrange(src_endpoint_index, 1);
        }
        break;
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
    NotifyTableChanged();
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
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

std::shared_ptr<Task> NameServerImpl::CreateUpdatePartitionStatusTask(const std::string& name, 
                    uint32_t pid, const std::string& endpoint, bool is_leader, bool is_alive, 
                    uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdatePartitionStatus);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdatePartitionStatus, this, name, endpoint, 
                        pid, is_leader, is_alive, task->task_info_);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdateTableAliveTask(const std::string& name,
                    const std::string& endpoint, bool is_alive, uint64_t op_index, ::rtidb::api::OPType op_type) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdateTableAlive);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdateTableAlive, this, name, endpoint, is_alive, task->task_info_);
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
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return;
    }
    for (int idx = 0; idx < iter->second->table_partition_size(); idx++) {
        if (iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        bool has_found = false;
        for (int meta_idx = 0; meta_idx < iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
             if (iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::rtidb::nameserver::TablePartition* table_partition = 
                            iter->second->mutable_table_partition(idx);
                ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta >* partition_meta = 
                            table_partition->mutable_partition_meta();
                PDLOG(INFO, "remove pid[%u] in table[%s]. endpoint is[%s]", 
                            pid, name.c_str(), endpoint.c_str());
                partition_meta->DeleteSubrange(meta_idx, 1);
                has_found = true;
                break;
            }
        }
        if (!has_found) {
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            PDLOG(INFO, "not found endpoint[%s] in partition_meta. name [%s] pid[%u]",
                         endpoint.c_str(), name.c_str(), pid);
            return;
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
        NotifyTableChanged();
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]",
                    task_info->op_id(),
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
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
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
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
                NotifyTableChanged();
                task_info->set_status(::rtidb::api::TaskStatus::kDone);
                PDLOG(INFO, "update table node[%s/%s]. value is [%s]", 
                                zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
                PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                            task_info->op_id(), 
                            ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
                return;
            }
        }
        break;
    }
    task_info->set_status(::rtidb::api::TaskStatus::kFailed);
    PDLOG(WARNING, "name[%s] endpoint[%s] pid[%u] is not exist",
                    name.c_str(), endpoint.c_str(), pid);
}

void NameServerImpl::UpdateTableAliveStatus(RpcController* controller,
            const UpdateTableAliveRequest* request,
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
    std::string name = request->name();
    std::string endpoint = request->endpoint();
    if (tablets_.find(endpoint) == tablets_.end()) {
        PDLOG(WARNING, "endpoint[%s] is not exist", endpoint.c_str());
        response->set_code(-1);
        response->set_msg("endpoint is not exist");
        return;
    }
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        response->set_code(-1);
        response->set_msg("table is not exist");
        return;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> cur_table_info(iter->second->New());
    cur_table_info->CopyFrom(*(iter->second));
    bool has_update = false;
    for (int idx = 0; idx < cur_table_info->table_partition_size(); idx++) {
        if (request->has_pid() && cur_table_info->table_partition(idx).pid() != request->pid()) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < cur_table_info->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (cur_table_info->table_partition(idx).partition_meta(meta_idx).endpoint() == endpoint) {
                ::rtidb::nameserver::TablePartition* table_partition =
                        cur_table_info->mutable_table_partition(idx);
                ::rtidb::nameserver::PartitionMeta* partition_meta = 
                        table_partition->mutable_partition_meta(meta_idx);        
                partition_meta->set_is_alive(request->is_alive());
                std::string is_alive = request->is_alive() ? "true" : "false";
                PDLOG(INFO, "update status[%s]. name[%s] endpoint[%s] pid[%u]", 
                            is_alive.c_str(), name.c_str(), endpoint.c_str(), iter->second->table_partition(idx).pid());
                has_update = true;
                break;
            }
        }
    }
    if (has_update) {
        std::string table_value;
        cur_table_info->SerializeToString(&table_value);
        if (zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            NotifyTableChanged();
            iter->second = cur_table_info;
            PDLOG(INFO, "update alive status ok. name[%s] endpoint[%s]", name.c_str(), endpoint.c_str());
            response->set_code(0);
            response->set_msg("ok");
            return;         
        } else {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            response->set_msg("no pid has set");
        }
    } else {
        response->set_msg("no pid has update");
    }
    response->set_code(-1);
}

void NameServerImpl::UpdateTableAlive(const std::string& name, const std::string& endpoint,
                bool is_alive, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    if (!running_.load(std::memory_order_acquire)) {
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = table_info_.find(name);
    if (iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::TablePartition>* table_partition = 
                iter->second->mutable_table_partition();
    bool has_update = false;            
    for (int idx = 0; idx < table_partition->size(); idx++) {
        ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::PartitionMeta>* partition_meta = 
                table_partition->Mutable(idx)->mutable_partition_meta();;
        for (int meta_idx = 0; meta_idx < partition_meta->size(); meta_idx++) {
            ::rtidb::nameserver::PartitionMeta* cur_partition_meta = partition_meta->Mutable(meta_idx);
            if (cur_partition_meta->endpoint() == endpoint) {
                cur_partition_meta->set_is_alive(is_alive);
                has_update = true;
            }
        }
    }
    if (has_update) {
        std::string table_value;
        iter->second->SerializeToString(&table_value);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;         
        }
        NotifyTableChanged();
        PDLOG(INFO, "update table node[%s/%s]. value is [%s]", 
                        zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
    }            
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

std::shared_ptr<Task> NameServerImpl::CreateSelectLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t tid, uint32_t pid,
                    std::vector<std::string>& follower_endpoint) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kSelectLeader);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::SelectLeader, this, name, tid, pid, follower_endpoint, task->task_info_);
    PDLOG(INFO, "create SelectLeader task success. name[%s] tid[%u] pid[%u]", name.c_str(), tid, pid);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateChangeLeaderTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kChangeLeader);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::ChangeLeader, this, task->task_info_);
    PDLOG(INFO, "create ChangeLeader task success. name[%s] pid[%u]", name.c_str(), pid);
    return task;
}

std::shared_ptr<Task> NameServerImpl::CreateUpdateLeaderInfoTask(uint64_t op_index, ::rtidb::api::OPType op_type,
                    const std::string& name, uint32_t pid) {
    std::shared_ptr<Task> task = std::make_shared<Task>("", std::make_shared<::rtidb::api::TaskInfo>());
    task->task_info_->set_op_id(op_index);
    task->task_info_->set_op_type(op_type);
    task->task_info_->set_task_type(::rtidb::api::TaskType::kUpdateLeaderInfo);
    task->task_info_->set_status(::rtidb::api::TaskStatus::kInited);
    task->fun_ = boost::bind(&NameServerImpl::UpdateLeaderInfo, this, task->task_info_);
    PDLOG(INFO, "create UpdateLeaderInfo task success. name[%s] pid[%u]", name.c_str(), pid);
    return task;
}

std::shared_ptr<OPData> NameServerImpl::FindRunningOP(uint64_t op_id) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& op_list : task_vec_) {
        if (op_list.empty()) {
            continue;
        }
        if (op_list.front()->op_info_.op_id() == op_id) {
            return op_list.front();
        }
    }
    return std::shared_ptr<OPData>();
}

void NameServerImpl::SelectLeader(const std::string& name, uint32_t tid, uint32_t pid, 
            std::vector<std::string>& follower_endpoint, std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    uint64_t cur_term = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!zk_client_->SetNodeValue(zk_term_node_, std::to_string(term_ + 2))) {
            PDLOG(WARNING, "update leader id  node failed. table name[%s] pid[%u]", name.c_str(), pid);
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);
            return;
        }
        cur_term = term_ + 1;
        term_ += 2;
    }
    // select the max offset endpoint as leader
    uint64_t max_offset = 0;    
    std::vector<std::string> leader_endpoint_vec;
    for (const auto& endpoint : follower_endpoint) {
        std::shared_ptr<TabletInfo> tablet_ptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = tablets_.find(endpoint);
            if (it == tablets_.end() || it->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {

                PDLOG(WARNING, "endpoint[%s] is offline. table[%s] pid[%u]", 
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
        PDLOG(INFO, "FollowOfNoOne ok. term[%lu] offset[%lu] name[%s] tid[%u] pid[%u] endpoint[%s]", 
                     cur_term, offset, name.c_str(), tid, pid, endpoint.c_str());
        if (offset > max_offset || leader_endpoint_vec.empty()) {
            max_offset = offset;
            leader_endpoint_vec.clear();
            leader_endpoint_vec.push_back(endpoint);
        } else if (offset == max_offset) {
            leader_endpoint_vec.push_back(endpoint);
        }
    }
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. name[%s] pid[%u] data[%s]", 
                        name.c_str(), pid, op_data->op_info_.data().c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    std::string leader_endpoint;
    if (change_leader_data.has_candidate_leader()) {
        std::string candidate_leader = change_leader_data.candidate_leader();
        if (std::find(leader_endpoint_vec.begin(), leader_endpoint_vec.end(), candidate_leader) != leader_endpoint_vec.end()) {
            leader_endpoint = candidate_leader;
        } else {
            PDLOG(WARNING, "select leader failed, candidate_leader[%s] is not in leader_endpoint_vec. tid[%u] pid[%u]", 
                            candidate_leader.c_str(), tid, pid);
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
    } else {
        leader_endpoint = leader_endpoint_vec[rand_.Next() % leader_endpoint_vec.size()];
    }
    change_leader_data.set_leader(leader_endpoint);
    change_leader_data.set_offset(max_offset);
    change_leader_data.set_term(cur_term + 1);
    std::string value;
    change_leader_data.SerializeToString(&value);
    op_data->op_info_.set_data(value);
    PDLOG(INFO, "new leader is[%s]. name[%s] tid[%u] pid[%u] offset[%lu]", 
                leader_endpoint.c_str(), name.c_str(), tid, pid, max_offset);
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
 }            

void NameServerImpl::ChangeLeader(std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]", 
                        task_info->op_id(), op_data->op_info_.data().c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
        return;
    }
    std::string leader_endpoint = change_leader_data.leader();
    std::vector<std::string> follower_endpoint;
    for (int idx = 0; idx < change_leader_data.follower_size(); idx++) {
        follower_endpoint.push_back(change_leader_data.follower(idx));
    }
    std::shared_ptr<TabletInfo> tablet_ptr;
    uint64_t cur_term = change_leader_data.term();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tablets_.find(leader_endpoint);           
        if (iter == tablets_.end() || iter->second->state_ != ::rtidb::api::TabletState::kTabletHealthy) {
            PDLOG(WARNING, "endpoint[%s] is offline", leader_endpoint.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
        follower_endpoint.erase(std::find(follower_endpoint.begin(), follower_endpoint.end(), leader_endpoint));
        tablet_ptr = iter->second;
    }
    if (!tablet_ptr->client_->ChangeRole(change_leader_data.tid(), change_leader_data.pid(), true, 
                        follower_endpoint, cur_term)) {
        PDLOG(WARNING, "change leader failed. name[%s] tid[%u] pid[%u] endpoint[%s]", 
                        change_leader_data.name().c_str(), change_leader_data.tid(), 
                        change_leader_data.pid(), leader_endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    PDLOG(INFO, "change leader ok. name[%s] tid[%u] pid[%u] leader[%s] term[%lu]", 
                change_leader_data.name().c_str(), change_leader_data.tid(), 
                change_leader_data.pid(), leader_endpoint.c_str(), cur_term);
    task_info->set_status(::rtidb::api::TaskStatus::kDone);
    PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                task_info->op_id(), 
                ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
}

void NameServerImpl::UpdateTTL(RpcController* controller,
        const ::rtidb::nameserver::UpdateTTLRequest* request,
        ::rtidb::nameserver::UpdateTTLResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    if (!running_.load(std::memory_order_acquire)) {
        response->set_code(-1);
        response->set_msg("nameserver is not leader");
        PDLOG(WARNING, "cur nameserver is not leader");
        return;
    }
    std::shared_ptr<TableInfo> table = GetTableInfo(request->name());
    if (!table) {
        PDLOG(WARNING, "table with name %s does not exist", request->name().c_str());
        response->set_code(-1);
        response->set_msg("table does not exist");
        return;
    }
    // validation
    if (table->ttl_type() != request->ttl_type()) {
        PDLOG(WARNING, "table ttl type mismatch, expect %s bug %s", table->ttl_type().c_str(), request->ttl_type().c_str());
        response->set_code(-2);
        response->set_msg("table ttl type mismatch");
        return;
    }

    ::rtidb::api::TTLType ttl_type;
    bool ok = ::rtidb::api::TTLType_Parse(request->ttl_type(), &ttl_type);
    if (!ok) {
        PDLOG(WARNING, "fail to parse ttl_type %s", request->ttl_type().c_str());
        response->set_code(-3);
        response->set_msg("table ttl type invalid");
        return;
    }
    uint64_t old_ttl = table->ttl();
    table->set_ttl(request->value());

    // update the tablet
    bool all_ok = true;
    for (int32_t i = 0; i < table->table_partition_size(); i++) {
        if (!all_ok) {
            break;
        }
        const TablePartition& table_partition = table->table_partition(i);
        for (int32_t j = 0; j < table_partition.partition_meta_size(); j++) {
            const PartitionMeta& meta = table_partition.partition_meta(j);
            all_ok = all_ok && UpdateTTLOnTablet(meta.endpoint(), table->tid(), table_partition.pid(), ttl_type, request->value()); 
        }
    }

    if (!all_ok) {
        table->set_ttl(old_ttl);
        response->set_code(-4);
        response->set_msg("fail to update ttl from tablet");
        return;
    }
    // update zookeeper
    std::string table_value;
    table->SerializeToString(&table_value);
    if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + table->name(), table_value)) {
        PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                        zk_table_data_path_.c_str(), table->name().c_str(), table_value.c_str());
        response->set_code(-5);
        response->set_msg("save ttl to zk failed");
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void NameServerImpl::UpdateLeaderInfo(std::shared_ptr<::rtidb::api::TaskInfo> task_info) {
    std::shared_ptr<OPData> op_data = FindRunningOP(task_info->op_id());
    if (!op_data) {
        PDLOG(WARNING, "cannot find op[%lu] in running op", task_info->op_id());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);
        return;
    }
    ChangeLeaderData change_leader_data;
    if (!change_leader_data.ParseFromString(op_data->op_info_.data())) {
        PDLOG(WARNING, "parse change leader data failed. op_id[%lu] data[%s]", 
                        task_info->op_id(), op_data->op_info_.data().c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
        return;
    }
    std::string leader_endpoint = change_leader_data.leader();
    std::string name = change_leader_data.name();
    uint32_t pid = change_leader_data.pid();

    std::lock_guard<std::mutex> lock(mu_);
    auto table_iter = table_info_.find(name);
    if (table_iter == table_info_.end()) {
        PDLOG(WARNING, "not found table[%s] in table_info map", name.c_str());
        return;
    }
    int old_leader_index = -1;
    int new_leader_index = -1;
    for (int idx = 0; idx < table_iter->second->table_partition_size(); idx++) {
        if (table_iter->second->table_partition(idx).pid() != pid) {
            continue;
        }
        for (int meta_idx = 0; meta_idx < table_iter->second->table_partition(idx).partition_meta_size(); meta_idx++) {
            if (table_iter->second->table_partition(idx).partition_meta(meta_idx).is_leader() && 
                    table_iter->second->table_partition(idx).partition_meta(meta_idx).is_alive()) {
                old_leader_index = meta_idx;
            } else if (table_iter->second->table_partition(idx).partition_meta(meta_idx).endpoint() == leader_endpoint) {
                new_leader_index = meta_idx;
            }
        }
        ::rtidb::nameserver::TablePartition* table_partition = 
                table_iter->second->mutable_table_partition(idx);
        if (old_leader_index >= 0) {
            ::rtidb::nameserver::PartitionMeta* old_leader_meta = 
                    table_partition->mutable_partition_meta(old_leader_index);
            old_leader_meta->set_is_alive(false);
        }
        if (new_leader_index < 0) {
            PDLOG(WARNING, "endpoint[%s] is not exist. name[%s] pid[%u]", 
                            leader_endpoint.c_str(),  name.c_str(), pid);
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return;
        }
        ::rtidb::nameserver::PartitionMeta* new_leader_meta = 
                table_partition->mutable_partition_meta(new_leader_index);
        new_leader_meta->set_is_leader(true);
        ::rtidb::nameserver::TermPair* term_offset = table_partition->add_term_offset();
        term_offset->set_term(change_leader_data.term());
        term_offset->set_offset(change_leader_data.offset() + 1);
        std::string table_value;
        table_iter->second->SerializeToString(&table_value);
        if (!zk_client_->SetNodeValue(zk_table_data_path_ + "/" + name, table_value)) {
            PDLOG(WARNING, "update table node[%s/%s] failed! value[%s]", 
                            zk_table_data_path_.c_str(), name.c_str(), table_value.c_str());
            task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
            return; 
        }
        PDLOG(INFO, "change leader success. name[%s] pid[%u] new leader[%s]", 
                    name.c_str(), pid, leader_endpoint.c_str());
        task_info->set_status(::rtidb::api::TaskStatus::kDone);
        // notify client to update table partition information
        PDLOG(INFO, "update task status from[kDoing] to[kDone]. op_id[%lu], task_type[%s]", 
                    task_info->op_id(), 
                    ::rtidb::api::TaskType_Name(task_info->task_type()).c_str());
        NotifyTableChanged();
        return;
    }
    PDLOG(WARNING, "partition[%u] is not exist. name[%s]", pid, name.c_str());
    task_info->set_status(::rtidb::api::TaskStatus::kFailed);                
}

void NameServerImpl::NotifyTableChanged() {
    std::string value;
    bool ok = zk_client_->GetNodeValue(zk_table_changed_notify_node_, value);
    if (!ok) {
        PDLOG(WARNING, "get zk table changed notify node value failed");
        return;
    }
    uint64_t counter = std::stoull(value) + 1;
    ok = zk_client_->SetNodeValue(zk_table_changed_notify_node_, std::to_string(counter));
    if (!ok) {
        PDLOG(WARNING, "incr zk table changed notify node value failed");
    }
    PDLOG(INFO, "notify table changed ok, update counter from %s to %lu", value.c_str(), counter);
}

std::shared_ptr<TableInfo> NameServerImpl::GetTableInfo(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<TableInfo> table;
    std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>>::iterator it = table_info_.find(name);
    if (it == table_info_.end()) {
        return table;
    }
    table = it->second;
    return table;
}

std::shared_ptr<TabletInfo> NameServerImpl::GetTabletInfo(const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<TabletInfo> tablet;
    std::map<std::string, std::shared_ptr<TabletInfo>>::iterator it = tablets_.find(endpoint);
    if (it == tablets_.end()) {
        return tablet;
    }
    tablet = it->second;
    return tablet;
}

bool NameServerImpl::UpdateTTLOnTablet(const std::string& endpoint,
        int32_t tid, int32_t pid, const ::rtidb::api::TTLType& type,
        uint64_t ttl) {
    std::shared_ptr<TabletInfo> tablet = GetTabletInfo(endpoint);
    if (!tablet) {
        PDLOG(WARNING, "tablet with endpoint %s is not found", endpoint.c_str());
        return false;
    }

    if (!tablet->client_) {
        PDLOG(WARNING, "tablet with endpoint %s has not client", endpoint.c_str());
        return false;
    }
    bool ok = tablet->client_->UpdateTTL(tid, pid, type, ttl);
    if (!ok) {
        PDLOG(WARNING, "fail to update ttl with tid %d, pid %d , ttl %ld", tid, pid, ttl);
    }else {
        PDLOG(INFO, "update ttl with tid %d pid %d ttl %ld ok", tid, pid, ttl);
    }
    return ok;
}

}
}
