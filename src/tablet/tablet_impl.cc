//
// tablet_impl.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01
//

#include "tablet/tablet_impl.h"

#include "config.h"
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <gflags/gflags.h>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#ifdef TCMALLOC_ENABLE 
#include "gperftools/malloc_extension.h"
#endif
#include "base/codec.h"
#include "base/strings.h"
#include "base/file_util.h"
#include "logging.h"
#include "timer.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;
using ::rtidb::storage::Table;
using ::rtidb::storage::DataBlock;

DECLARE_int32(gc_interval);
DECLARE_int32(gc_pool_size);
DECLARE_int32(gc_safe_offset);
DECLARE_int32(statdb_ttl);
DECLARE_uint32(scan_max_bytes_size);
DECLARE_double(mem_release_rate);
DECLARE_string(db_root_path);
DECLARE_string(binlog_root_path);
DECLARE_bool(enable_statdb);
DECLARE_bool(binlog_notify_on_put);
DECLARE_string(snapshot_root_path);

// cluster config
DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace tablet {

TabletImpl::TabletImpl():tables_(),mu_(), gc_pool_(FLAGS_gc_pool_size),
    metric_(NULL), replicators_(), snapshots_(), zk_client_(NULL),
    keep_alive_pool_(1){}

TabletImpl::~TabletImpl() {
    if (FLAGS_enable_statdb) {
        Table* table = GetTable(0, 0);
        if (table != NULL) {
            table->Release();
            table->UnRef();
            table->UnRef();
        }
        tables_.erase(0);
        delete metric_;
    }
}

bool TabletImpl::Init() {
    MutexLock lock(&mu_);
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
            LOG(WARNING, "fail to register tablet with endpoint %s", FLAGS_endpoint.c_str());
            return false;
        }
        LOG(INFO, "tablet with endpoint %s register to zk cluster %s ok", FLAGS_endpoint.c_str(), FLAGS_zk_cluster.c_str());
        keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));
    }else {
        LOG(INFO, "zk cluster disabled");
    }
    if (FLAGS_enable_statdb) {
        // Create a dbstat table with tid = 0 and pid = 0
        Table* dbstat = new Table("dbstat", 0, 0, 8, FLAGS_statdb_ttl);
        dbstat->Init();
        dbstat->Ref();
        tables_[0].insert(std::make_pair(0, dbstat));
        if (FLAGS_statdb_ttl > 0) {
            gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000,
                    boost::bind(&TabletImpl::GcTable, this, 0, 0));
        }
        // For tablet metric
        dbstat->Ref();
        metric_ = new TabletMetric(dbstat);
        metric_->Init();
    }
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    tcmalloc->SetMemoryReleaseRate(FLAGS_mem_release_rate);
#endif 
    return true;
}

void TabletImpl::Put(RpcController* controller,
        const ::rtidb::api::PutRequest* request,
        ::rtidb::api::PutResponse* response,
        Closure* done) {
    if (request->tid() < 1) {
        LOG(WARNING, "invalid table tid %ld", request->tid());
        response->set_code(11);
        response->set_msg("invalid table id");
        done->Run();
        return;
    }
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL) {
        LOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (!table->IsLeader()) {
        LOG(WARNING, "table with tid %ld, pid %ld is follower and it's readonly ", request->tid(),
                request->pid());
        response->set_code(20);
        response->set_msg("table is follower, and it's readonly");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        table->UnRef();
        LOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        done->Run();
        return;
    }
    uint64_t size = request->value().size();
    table->Put(request->pk(), request->time(), request->value().c_str(),
            request->value().length());
    response->set_code(0);
    LOG(DEBUG, "put key %s ok ts %lld", request->pk().c_str(), request->time());
    bool leader = table->IsLeader();
    LogReplicator* replicator = NULL;
    if (leader) {
        do {
            replicator = GetReplicator(request->tid(), request->pid());
            if (replicator == NULL) {
                LOG(WARNING, "fail to find table tid %ld pid %ld leader's log replicator", request->tid(),
                        request->pid());
                break;
            }
            ::rtidb::api::LogEntry entry;
            entry.set_ts(request->time());
            entry.set_pk(request->pk());
            entry.set_value(request->value());
            replicator->AppendEntry(entry);
        } while(false);
    }
    table->UnRef();
    done->Run();
    if (FLAGS_enable_statdb) {
        metric_->IncrThroughput(1, size, 0, 0);
    }
    if (replicator != NULL) {
        if (FLAGS_binlog_notify_on_put) {
            replicator->Notify(); 
        }
        replicator->UnRef();
    }
}

void TabletImpl::BatchGet(RpcController* controller, 
        const ::rtidb::api::BatchGetRequest* request,
        ::rtidb::api::BatchGetResponse* response,
        Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL) {
        LOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        table->UnRef();
        LOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        done->Run();
        return;
    }
    std::vector<std::string> keys;
    for (int32_t i = 0; i < request->keys_size(); i++) {
        keys.push_back(request->keys(i));
    }
    std::map<uint32_t, DataBlock*> datas;
    ::rtidb::storage::Ticket ticket;
    table->BatchGet(keys, datas, ticket);
    uint32_t total_block_size = 0;
    std::map<uint32_t, DataBlock*>::iterator it = datas.begin();
    for (; it != datas.end(); ++it) {
        total_block_size += it->second->size;
    }
    uint32_t total_size = datas.size() * (8+4) + total_block_size;
    std::string* pairs = response->mutable_pairs();
    if (datas.size() <= 0) {
        pairs->resize(0);
    }else {
        pairs->resize(total_size);
    }
    LOG(DEBUG, "batch get count %d", datas.size());
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    it = datas.begin();
    for (; it != datas.end(); ++it) {
        LOG(DEBUG, "decode key %lld value %s", it->first, it->second->data);
        ::rtidb::base::Encode((uint64_t)it->first, it->second, rbuffer, offset);
        offset += (4 + 8 + it->second->size);
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
    table->UnRef();
}

inline bool TabletImpl::CheckScanRequest(const rtidb::api::ScanRequest* request) {
    if (request->st() < request->et()) {
        return false;
    }
    return true;
}

inline bool TabletImpl::CheckCreateRequest(const rtidb::api::CreateTableRequest* request) {
    if (request->name().size() <= 0) {
        return false;
    }
    if (request->tid() <= 0) {
        return false;
    }
    return true;
}

void TabletImpl::Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done) {

    if (!CheckScanRequest(request)) {
        response->set_code(8);
        response->set_msg("bad scan request");
        done->Run();
        return;
    }

    ::rtidb::api::RpcMetric* metric = response->mutable_metric();
    metric->CopyFrom(request->metric());
    metric->set_rqtime(::baidu::common::timer::get_micros());
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL) {
        LOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        table->UnRef();
        LOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        done->Run();
        return;
    }

    metric->set_sctime(::baidu::common::timer::get_micros());
    // Use seek to process scan request
    // the first seek to find the total size to copy
    ::rtidb::storage::Ticket ticket;
    Table::Iterator* it = table->NewIterator(request->pk(), ticket);
    it->Seek(request->st());
    metric->set_sitime(::baidu::common::timer::get_micros());
    std::vector<std::pair<uint64_t, DataBlock*> > tmp;
    uint32_t total_block_size = 0;
    uint64_t end_time = request->et();
    bool remove_duplicated_record = false;
    if (request->has_enable_remove_duplicated_record()) {
        remove_duplicated_record = request->enable_remove_duplicated_record();
    }
    LOG(DEBUG, "scan pk %s st %lld et %lld", request->pk().c_str(), request->st(), end_time);
    uint32_t scount = 0;
    uint64_t last_time = 0;
    while (it->Valid()) {
        scount ++;
        LOG(DEBUG, "scan key %lld value %s", it->GetKey(), it->GetValue()->data);
        if (it->GetKey() <= end_time) {
            break;
        }
        // skip duplicate record 
        if (remove_duplicated_record && scount > 1 && last_time == it->GetKey()) {
            LOG(DEBUG, "filter duplicate record for key %s with ts %lld", request->pk().c_str(), it->GetKey());
            last_time = it->GetKey();
            it->Next();
            continue;
        }
        last_time = it->GetKey();
        tmp.push_back(std::make_pair(it->GetKey(), it->GetValue()));
        total_block_size += it->GetValue()->size;
        it->Next();
        if (request->limit() > 0 && request->limit() <= scount) {
            break;
        }
    }
    delete it;
    metric->set_setime(::baidu::common::timer::get_micros());
    uint32_t total_size = tmp.size() * (8+4) + total_block_size;
    // check reach the max bytes size
    if (total_size > FLAGS_scan_max_bytes_size) {
        response->set_code(31);
        response->set_msg("reache the scan max bytes size " + ::rtidb::base::HumanReadableString(total_size));
        done->Run();
        table->UnRef();
        return;
    }

    std::string* pairs = response->mutable_pairs();
    if (tmp.size() <= 0) {
        pairs->resize(0);
    }else {
        pairs->resize(total_size);
    }

    LOG(DEBUG, "scan count %d", tmp.size());
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    std::vector<std::pair<uint64_t, DataBlock*> >::iterator lit = tmp.begin();
    for (; lit != tmp.end(); ++lit) {
        std::pair<uint64_t, DataBlock*>& pair = *lit;
        LOG(DEBUG, "decode key %lld value %s", pair.first, pair.second->data);
        ::rtidb::base::Encode(pair.first, pair.second, rbuffer, offset);
        offset += (4 + 8 + pair.second->size);
    }

    response->set_code(0);
    response->set_count(tmp.size());
    metric->set_sptime(::baidu::common::timer::get_micros()); 
    done->Run();
    table->UnRef();
    if (FLAGS_enable_statdb) {
        metric_->IncrThroughput(0, 0, 1, total_size);
    }
}

void TabletImpl::PauseSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL ||
        !table->IsLeader()) {
        if (table) {
            table->UnRef();
        }
        LOG(WARNING, "table not exist or table is leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    if (table->GetTableStat() != ::rtidb::storage::kNormal) {
        LOG(WARNING, "table status is [%u], cann't pause. tid[%u] pid[%u]", 
                table->GetTableStat(), request->tid(), request->pid());
        table->UnRef();
        response->set_code(-2);
        response->set_msg("table status is not kNormal");
        done->Run();
        return;
    }
    table->SetTableStat(::rtidb::storage::kPausing);
    LOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", 
               table->GetTableStat(), request->tid(), request->pid());
    table->UnRef();
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::RecoverSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL ||
        !table->IsLeader()) {
        if (table) {
            table->UnRef();
        }
        LOG(WARNING, "table not exist or table is leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    if (table->GetTableStat() != ::rtidb::storage::kPaused) {
        LOG(WARNING, "table status is [%u], cann't recover. tid[%u] pid[%u]", 
                table->GetTableStat(), request->tid(), request->pid());
        table->UnRef();
        response->set_code(-2);
        response->set_msg("table status is not kPaused");
        done->Run();
        return;
    }
    table->SetTableStat(::rtidb::storage::kNormal);
    LOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", 
               table->GetTableStat(), request->tid(), request->pid());
    table->UnRef();
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::ChangeRole(RpcController* controller, 
            const ::rtidb::api::ChangeRoleRequest* request,
            ::rtidb::api::ChangeRoleResponse* response,
            Closure* done) {
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    bool is_leader = false;
    if (request->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> vec;
    for (int idx = 0; idx < request->replicas_size(); idx++) {
        vec.push_back(request->replicas(idx).c_str());
    }
    if (is_leader) {
        if (ChangeToLeader(tid, pid, vec) < 0) {
            response->set_code(-1);
            response->set_msg("table change to leader failed!");
            done->Run();
            return;
        }
        response->set_code(0);
        response->set_msg("ok");
        done->Run();
    } else {
        response->set_code(-1);
        response->set_msg("not support change to follower");
        done->Run();
    }
}

int TabletImpl::ChangeToLeader(uint32_t tid, uint32_t pid, const std::vector<std::string>& replicas) {
    Table* table = NULL;
    LogReplicator* replicator = NULL;
    {
        MutexLock lock(&mu_);
        table = GetTableUnLock(tid, pid);
        if (!table) {
            LOG(WARNING, "table is not exisit. tid[%u] pid[%u]", tid, pid);
            return -1;
        }
        if (table->IsLeader() || table->GetTableStat() != ::rtidb::storage::kNormal) {
            LOG(WARNING, "table is leader or  state[%u] can not change role. tid[%u] pid[%u]", 
                        table->GetTableStat(), tid, pid);
            table->UnRef();
            return -1;
        }
        replicator = GetReplicatorUnLock(tid, pid);
        if (replicator == NULL) {
            LOG(WARNING,"no replicator for table tid[%u] pid[%u]", tid, pid);
            table->UnRef();
            return -1;
        }
        table->SetLeader(true);
        table->SetReplicas(replicas);
        replicator->SetRole(ReplicatorRole::kLeaderNode);
    }
    for (auto iter = replicas.begin(); iter != replicas.end(); ++iter) {
        if (!replicator->AddReplicateNode(*iter)) {
            LOG(WARNING,"add replicator[%s] for table tid[%u] pid[%u] failed!", 
                        iter->c_str(), tid, pid);
        }
    }
    table->UnRef();
    replicator->UnRef();
    return 0;
}

void TabletImpl::AddReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::AddReplicaResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL ||
        !table->IsLeader()) {
        if (table) {
            table->UnRef();
        }
        LOG(WARNING, "table not exist or table is not leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    LogReplicator* replicator = GetReplicator(request->tid(), request->pid());
    if (replicator == NULL) {
        table->UnRef();
        response->set_code(-2);
        response->set_msg("no replicator for table");
        LOG(WARNING,"no replicator for table %d, pid %d", request->tid(), request->pid());
        done->Run();
        return;
    }
    bool ok = replicator->AddReplicateNode(request->endpoint());
    replicator->UnRef();
    if (ok) {
        response->set_code(0);
        response->set_msg("ok");
        done->Run();
    }else {
        response->set_code(-3);
        LOG(WARNING, "fail to add endpoint for table %d pid %d", request->tid(), request->pid());
        response->set_msg("fail to add endpoint");
        done->Run();
    }  
    table->SetTableStat(::rtidb::storage::kNormal);
    table->UnRef();
}

void TabletImpl::DelReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL ||
        !table->IsLeader()) {
        if (table) {
            table->UnRef();
        }
        LOG(WARNING, "table not exist or table is not leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    table->UnRef();
    LogReplicator* replicator = GetReplicator(request->tid(), request->pid());
    if (replicator == NULL) {
        response->set_code(-2);
        response->set_msg("no replicator for table");
        LOG(WARNING,"no replicator for table %d, pid %d", request->tid(), request->pid());
        done->Run();
        return;
    }
    bool ok = replicator->DelReplicateNode(request->endpoint());
    replicator->UnRef();
    if (ok) {
        response->set_code(0);
        response->set_msg("ok");
        done->Run();
    } else {
        response->set_code(-3);
        LOG(WARNING, "fail to del endpoint for table %d pid %d", request->tid(), request->pid());
        response->set_msg("fail to del endpoint");
        done->Run();
    }  
}

void TabletImpl::AppendEntries(RpcController* controller,
        const ::rtidb::api::AppendEntriesRequest* request,
        ::rtidb::api::AppendEntriesResponse* response,
        Closure* done) {
    Table* table = NULL;
    LogReplicator* replicator = NULL;
    do {
        table = GetTable(request->tid(), request->pid());
        if (table == NULL ||
            table->IsLeader()) {
            LOG(WARNING, "table not exist or table is leader tid %d, pid %d", request->tid(),
                    request->pid());
            response->set_code(-1);
            response->set_msg("table not exist or table is leader");
            done->Run();
            break;
        }
        replicator = GetReplicator(request->tid(), request->pid());
        if (replicator == NULL) {
            response->set_code(-2);
            response->set_msg("no replicator for table");
            done->Run();
            break;
        }
        bool ok = replicator->AppendEntries(request, response);
        if (!ok) {
            response->set_code(-1);
            response->set_msg("fail to append entries to replicator");
            done->Run();
        }else {
            response->set_code(0);
            response->set_msg("ok");
            done->Run();
        }
    }while(false);
    if (table != NULL) {
        table->UnRef();
    }
    if (replicator != NULL) {
        replicator->UnRef();
    }
}

void TabletImpl::GetTableStatus(RpcController* controller,
            const ::rtidb::api::GetTableStatusRequest* request,
            ::rtidb::api::GetTableStatusResponse* response,
            Closure* done) {

    MutexLock lock(&mu_);
    Tables::iterator it = tables_.begin();
    for (; it != tables_.end(); ++it) {
        std::map<uint32_t, Table*>::iterator pit = it->second.begin();
        for (; pit != it->second.end(); ++pit) {
            Table* table = pit->second;
            table->Ref();
            ::rtidb::api::TableStatus* status = response->add_all_table_status();
            status->set_mode(::rtidb::api::TableMode::kTableFollower);
            if (table->IsLeader()) {
                status->set_mode(::rtidb::api::TableMode::kTableLeader);
            }
            status->set_tid(table->GetId());
            status->set_pid(table->GetPid());
            status->set_ttl(table->GetTTL());
            if (::rtidb::api::TableState_IsValid(table->GetTableStat())) {
                status->set_state(::rtidb::api::TableState(table->GetTableStat()));
            }
            LogReplicator* replicator = GetReplicatorUnLock(table->GetId(), table->GetPid());
            if (replicator != NULL) {
                status->set_offset(replicator->GetOffset());
                replicator->UnRef();
            }
            table->UnRef();
        }
    }
    response->set_code(0);
    done->Run();
}


bool TabletImpl::ApplyLogToTable(uint32_t tid, uint32_t pid, const ::rtidb::api::LogEntry& log) {
    Table* table = GetTable(tid, pid);
    if (table == NULL) {
        LOG(WARNING, "table with tid %ld and pid %ld does not exist", tid, pid);
        return false; 
    }
    table->Put(log.pk(), log.ts(), log.value().c_str(), log.value().size());
    table->UnRef();
    return true;
}

bool TabletImpl::MakeSnapshot(uint32_t tid, uint32_t pid,
                              const std::string& entry,
                              const std::string& pk,
                              uint64_t offset,
                              uint64_t ts) {
    std::shared_ptr<Snapshot> snapshot = GetSnapshot(tid, pid);
    return true;
}

void TabletImpl::LoadTable(RpcController* controller,
            const ::rtidb::api::LoadTableRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    if (request->name().size() <= 0 || request->tid() <= 0) {
        response->set_code(8);
        response->set_msg("table name is empty");
        done->Run();
        return;
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    uint32_t ttl = request->ttl();
    std::string name = request->name();
    uint32_t seg_cnt = 8;
    if (request->seg_cnt() > 0 && request->seg_cnt() < 32) {
        seg_cnt = request->seg_cnt();
    }

    {
        MutexLock lock(&mu_);
        Table* table = GetTableUnLock(tid, pid);
        std::shared_ptr<Snapshot> snapshot = GetSnapshot(tid, pid);
        if (table == NULL && snapshot) {
            LoadTableInternal(request, response);
        } else {
            if (table) {
                LOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
                table->UnRef();
            }
            response->set_code(1);
            response->set_msg("table with tid and pid exists");
            done->Run();
            return;
        }       
    }
    done->Run();
    LOG(INFO, "create table with id %d pid %d name %s seg_cnt %d ttl %d", tid, 
            pid, name.c_str(), seg_cnt, ttl);
    
    // load snapshot data
    Table* table = GetTable(tid, pid);        
    if (table == NULL) {
        LOG(WARNING, "table with tid %ld and pid %ld does not exist", tid, pid);
        return; 
    }
    std::shared_ptr<Snapshot> snapshot = GetSnapshot(tid, pid);
    if (!snapshot) {
        table->UnRef();
        LOG(WARNING, "snapshot with tid %ld and pid %ld does not exist", tid, pid);
        return; 
    }
    LogReplicator* replicator = GetReplicator(request->tid(), request->pid());
    if (replicator == NULL) {
        table->UnRef();
        LOG(WARNING, "replicator with tid %ld and pid %ld does not exist", tid, pid);
        return;
    }
    // snapshot->Recover(table);
    table->SetTableStat(::rtidb::storage::kNormal);
    replicator->SetOffset(snapshot->GetOffset());
    // start replicate task
    replicator->MatchLogOffset();
    replicator->UnRef();
    table->SchedGc();
    table->UnRef();
    if (ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
        LOG(INFO, "table %s with tid %ld pid %ld enable ttl %ld", name.c_str(), tid, pid, ttl);
    }
}

void TabletImpl::LoadTableInternal(const ::rtidb::api::LoadTableRequest* request,
        ::rtidb::api::GeneralResponse* response) {
    mu_.AssertHeld();
    uint32_t seg_cnt = 8;
    std::string name = request->name();
    if (request->seg_cnt() > 0 && request->seg_cnt() < 32) {
        seg_cnt = request->seg_cnt();
    }
    bool is_leader = false;
    if (request->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> endpoints;
    for (int32_t i = 0; i < request->replicas_size(); i++) {
        endpoints.push_back(request->replicas(i));
    }
    Table* table = new Table(request->name(), request->tid(),
                             request->pid(), seg_cnt, 
                             request->ttl(), is_leader,
                             endpoints, request->wal());
    table->Init();
    table->SetGcSafeOffset(FLAGS_gc_safe_offset);
    // for tables_ 
    table->Ref();
    table->SetTerm(request->term());
    table->SetTableStat(::rtidb::storage::kLoading);
    std::string table_binlog_path = FLAGS_binlog_root_path + "/" + boost::lexical_cast<std::string>(request->tid()) +"_" + boost::lexical_cast<std::string>(request->pid());
    LogReplicator* replicator = NULL;
    if (table->IsLeader() && table->GetWal()) {
        replicator = new LogReplicator(table_binlog_path, table->GetReplicas(), ReplicatorRole::kLeaderNode, table);
    } else if (table->GetWal()) {
        replicator = new LogReplicator(table_binlog_path, std::vector<std::string>(), ReplicatorRole::kFollowerNode, table);
    }
    if (replicator) {
        replicator->Ref();
    }
    if (!replicator || !replicator->Init()) {
        LOG(WARNING, "fail to create table tid %ld, pid %ld replicator", request->tid(), request->pid());
        // clean memory
        table->Release();
        table->UnRef();
        if (replicator) {
            replicator->UnRef();
        }
        response->set_code(-1);
        response->set_msg("fail create replicator for table");
        return;
    }
    tables_[request->tid()].insert(std::make_pair(request->pid(), table));
    replicators_[request->tid()].insert(std::make_pair(request->pid(), replicator));
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done) {
    if (!CheckCreateRequest(request)) {
        response->set_code(8);
        response->set_msg("table name is empty");
        done->Run();
        return;
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    uint32_t ttl = request->ttl();
    std::string name = request->name();
    uint32_t seg_cnt = 8;
    if (request->seg_cnt() > 0 && request->seg_cnt() < 32) {
        seg_cnt = request->seg_cnt();
    }
    // Note after create , request and response is unavaliable
    {
        MutexLock lock(&mu_);
        Table* table = GetTableUnLock(tid, pid);
        std::shared_ptr<Snapshot> snapshot = GetSnapshotUnLock(tid, pid);
        if (table != NULL || snapshot) {
            if (table) {
                LOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
                table->UnRef();
            }
            if (snapshot) {
                LOG(WARNING, "snapshot with tid[%u] and pid[%u] exists", tid, pid);
            }
            response->set_code(1);
            response->set_msg("table with tid and pid exists");
            done->Run();
            return;
        }       
        CreateTableInternal(request, response);
    }
    done->Run();
    LOG(INFO, "create table with id %d pid %d name %s seg_cnt %d ttl %d", tid, 
            pid, name.c_str(), seg_cnt, ttl);
    if (ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
        LOG(INFO, "table %s with tid %ld pid %ld enable ttl %ld", name.c_str(), tid, pid, ttl);
    }
}

void TabletImpl::CreateTableInternal(const ::rtidb::api::CreateTableRequest* request,
        ::rtidb::api::CreateTableResponse* response) {
    mu_.AssertHeld();
    uint32_t seg_cnt = 8;
    std::string name = request->name();
    if (request->seg_cnt() > 0 && request->seg_cnt() < 32) {
        seg_cnt = request->seg_cnt();
    }
    bool is_leader = false;
    if (request->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> endpoints;
    for (int32_t i = 0; i < request->replicas_size(); i++) {
        endpoints.push_back(request->replicas(i));
    }
    Table* table = new Table(request->name(), request->tid(),
                             request->pid(), seg_cnt, 
                             request->ttl(), is_leader,
                             endpoints, request->wal());
    table->Init();
    table->SetGcSafeOffset(FLAGS_gc_safe_offset);
    // for tables_ 
    table->Ref();
    table->SetTerm(request->term());
    table->SetTableStat(::rtidb::storage::kNormal);
    std::string table_binlog_path = FLAGS_binlog_root_path + "/" + boost::lexical_cast<std::string>(request->tid()) +"_" + boost::lexical_cast<std::string>(request->pid());
    std::shared_ptr<Snapshot> snapshot = std::make_shared<Snapshot>(request->tid(), request->pid());
    bool ok = snapshot->Init();
    if (!ok) {
        LOG(WARNING, "fail to init snapshot for tid %d, pid %d", request->tid(), request->pid());
        table->Release();
        table->UnRef();
        response->set_code(1);
        response->set_msg("fail to init snapshot");
        return;

    }
    LogReplicator* replicator = NULL;
    if (table->IsLeader() && table->GetWal()) {
        replicator = new LogReplicator(table_binlog_path, table->GetReplicas(), ReplicatorRole::kLeaderNode, table);
    }else if(table->GetWal()) {
        replicator = new LogReplicator(table_binlog_path, std::vector<std::string>(), ReplicatorRole::kFollowerNode, table);
    }
    if (replicator == NULL) {
        tables_[request->tid()].insert(std::make_pair(request->pid(), table));
        snapshots_[request->tid()].insert(std::make_pair(request->pid(), snapshot));
        response->set_code(0);
        response->set_msg("ok");
        return;
    }
    replicator->Ref();
    ok = replicator->Init();
    if (!ok) {
        LOG(WARNING, "fail to create table tid %ld, pid %ld replicator", request->tid(), request->pid());
        // clean memory
        table->Release();
        table->UnRef();
        replicator->UnRef();
        response->set_code(-1);
        response->set_msg("fail create replicator for table");
        return;
    }
    replicator->MatchLogOffset();
    tables_[request->tid()].insert(std::make_pair(request->pid(), table));
    snapshots_[request->tid()].insert(std::make_pair(request->pid(), snapshot));
    replicators_[request->tid()].insert(std::make_pair(request->pid(), replicator));
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::DropTable(RpcController* controller,
            const ::rtidb::api::DropTableRequest* request,
            ::rtidb::api::DropTableResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL) {
        response->set_code(-1);
        response->set_msg("table does not exist");
        done->Run();
        return;
    }
    LogReplicator* replicator = GetReplicator(request->tid(), 
            request->pid());
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    // do block other requests
    {
        MutexLock lock(&mu_);
        tables_[tid].erase(pid);
        replicators_[tid].erase(pid);
        snapshots_[tid].erase(pid);
        response->set_code(0);
        done->Run();
    }
    // unref table, let it release memory
    table->UnRef();
    table->UnRef();
    if (replicator != NULL) {
        replicator->Stop();
        replicator->UnRef();
        replicator->UnRef();
        LOG(INFO, "drop replicator for tid %d, pid %d", tid, pid);
    }
}

void TabletImpl::GcTable(uint32_t tid, uint32_t pid) {
    Table* table = GetTable(tid, pid);
    if (table == NULL) {
        return;
    }
    table->SchedGc();
    table->UnRef();
    gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshot(uint32_t tid, uint32_t pid) {
    MutexLock lock(&mu_);
    return GetSnapshotUnLock(tid, pid);
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshotUnLock(uint32_t tid, uint32_t pid) {
    mu_.AssertHeld();
    Snapshots::iterator it = snapshots_.find(tid);
    if (it != snapshots_.end()) {
        std::map<uint32_t, std::shared_ptr<Snapshot>>::iterator tit = it->second.find(pid);
        if (tit == it->second.end()) {
            return std::shared_ptr<Snapshot>();
        }
        return tit->second;
    }
    return std::shared_ptr<Snapshot>();
}

LogReplicator* TabletImpl::GetReplicatorUnLock(uint32_t tid, uint32_t pid) {
    mu_.AssertHeld();
    Replicators::iterator it = replicators_.find(tid);
    if (it != replicators_.end()) {
        std::map<uint32_t, LogReplicator*>::iterator tit = it->second.find(pid);
        if (tit == it->second.end()) {
            return NULL;
        }
        LogReplicator* replicator = tit->second;
        replicator->Ref();
        return replicator;
    }
    return NULL;
}

LogReplicator* TabletImpl::GetReplicator(uint32_t tid, uint32_t pid) {
    MutexLock lock(&mu_);
    return GetReplicatorUnLock(tid, pid);
}

Table* TabletImpl::GetTable(uint32_t tid, uint32_t pid) {
    MutexLock lock(&mu_);
    return GetTableUnLock(tid, pid);
}

Table* TabletImpl::GetTableUnLock(uint32_t tid, uint32_t pid) {
    mu_.AssertHeld();
    Tables::iterator it = tables_.find(tid);
    if (it != tables_.end()) {
        std::map<uint32_t, Table*>::iterator tit = it->second.find(pid);
        if (tit == it->second.end()) {
            return NULL;
        }
        Table* table = tit->second;
        table->Ref();
        return table;
    }
    return NULL;
}


// http action
bool TabletImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {
    const std::string& path = request.path; 
    if (path == "/tablet/show") {
       ShowTables(request, response); 
    }else if (path == "/tablet/metric") {
       ShowMetric(request, response);
    }else if (path == "/tablet/memory") {
       ShowMemPool(request, response);
    }
    return true;
}

void TabletImpl::ShowTables(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {

    std::vector<Table*> tmp_tables;
    {
        MutexLock lock(&mu_);
        Tables::iterator it = tables_.begin();
        for (; it != tables_.end(); ++it) {
            std::map<uint32_t, Table*>::iterator tit = it->second.begin();
            for (; tit != it->second.end(); ++tit) {
                Table* table = tit->second;
                table->Ref();
                tmp_tables.push_back(table);
            }
        }
    }

    ::rapidjson::StringBuffer sb;
    ::rapidjson::Writer<::rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("tables");
    writer.StartArray();
    LogReplicator* replicator = NULL;
    for (size_t i = 0; i < tmp_tables.size(); i++) {
        Table* table = tmp_tables[i];
        writer.StartObject();
        writer.Key("name");
        writer.String(table->GetName().c_str());
        writer.Key("tid");
        writer.Uint(table->GetId());
        writer.Key("pid");
        writer.Uint(table->GetPid());
        replicator = GetReplicator(table->GetId(), table->GetPid());
        if (replicator != NULL) {
            writer.Key("log_offset");
            writer.Uint(replicator->GetLogOffset());
            replicator->UnRef();
        }
        writer.Key("seg_cnt");
        writer.Uint(table->GetSegCnt());
        uint64_t total = 0;
        uint64_t* stat = NULL;
        uint32_t size = 0;
        table->GetDataCnt(&stat, &size);
        if (stat != NULL) {
            writer.Key("data_cnt_stat");
            writer.StartObject();
            writer.Key("stat");
            writer.StartArray();
            for (size_t k = 0; k < size; k++) {
                writer.Uint(stat[k]);
                total += stat[k];
            }
            writer.EndArray();
            writer.Key("total");
            writer.Uint(total);
            writer.EndObject();
            delete stat;
        }
        writer.EndObject();
        table->UnRef();
    }
    writer.EndArray();
    writer.EndObject();
    response.content->Append(sb.GetString());
}

void TabletImpl::ShowMetric(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response) {
    const std::string key = "key";
    ::rapidjson::StringBuffer sb;
    ::rapidjson::Writer<::rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("datapoints");
    writer.StartArray();

    std::map<const std::string, std::string>::const_iterator qit = request.query_params->find(key);
    if (qit == request.query_params->end()) {
        writer.EndArray();
        writer.EndObject();
        response.content->Append(sb.GetString());
        return;
    }

    const std::string& pk = qit->second;;
    Table* stat = GetTable(0, 0);
    if (stat == NULL) {
        writer.EndArray();
        writer.EndObject();
        response.content->Append(sb.GetString());
        return;
    }

    ::rtidb::storage::Ticket ticket;
    Table::Iterator* it = stat->NewIterator(pk, ticket);
    it->SeekToFirst();

    while (it->Valid()) {
        writer.StartArray();
        uint32_t val = 0;
        memcpy(static_cast<void*>(&val), it->GetValue()->data, 4);
        writer.Uint(val);
        writer.Uint(it->GetKey());
        writer.EndArray();
        it->Next();
    }
    writer.EndArray();
    writer.EndObject();
    response.content->Append(sb.GetString());
    stat->UnRef();
}

void TabletImpl::ShowMemPool(const sofa::pbrpc::HTTPRequest& request,
    sofa::pbrpc::HTTPResponse& response) {
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    std::string stat;
    stat.resize(1024);
    char* buffer = reinterpret_cast<char*>(& (stat[0]));
    tcmalloc->GetStats(buffer, 1024);
    response.content->Append("<html><head><title>Mem Stat</title></head><body><pre>");
    response.content->Append(stat);
    response.content->Append("</pre></body></html>");
#endif
}

void TabletImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        bool ok = zk_client_->Reconnect();
        if (ok) {
            zk_client_->Register();
        }
    }
    keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));

}

}
}



