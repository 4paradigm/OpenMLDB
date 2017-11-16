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
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

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
DECLARE_uint32(scan_reserve_size);
DECLARE_double(mem_release_rate);
DECLARE_string(db_root_path);
DECLARE_bool(enable_statdb);
DECLARE_bool(binlog_notify_on_put);
DECLARE_int32(task_pool_size);
DECLARE_int32(make_snapshot_time);
DECLARE_int32(make_snapshot_check_interval);
DECLARE_uint32(metric_max_record_cnt);
DECLARE_string(recycle_bin_root_path);

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
    keep_alive_pool_(1), task_pool_(FLAGS_task_pool_size){}

TabletImpl::~TabletImpl() {
    if (FLAGS_enable_statdb) {
        tables_.erase(0);
        delete metric_;
    }
}

bool TabletImpl::Init() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!FLAGS_zk_cluster.empty()) {
        zk_client_ = new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
                FLAGS_endpoint, FLAGS_zk_root_path);
        bool ok = zk_client_->Init();
        if (!ok) {
            PDLOG(WARNING, "fail to init zookeeper with cluster %s", FLAGS_zk_cluster.c_str());
            return false;
        }
        ok = zk_client_->Register();
        if (!ok) {
            PDLOG(WARNING, "fail to register tablet with endpoint %s", FLAGS_endpoint.c_str());
            return false;
        }
        PDLOG(INFO, "tablet with endpoint %s register to zk cluster %s ok", FLAGS_endpoint.c_str(), FLAGS_zk_cluster.c_str());
        keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));
    }else {
        PDLOG(INFO, "zk cluster disabled");
    }
    bool ok = ::rtidb::base::MkdirRecur(FLAGS_recycle_bin_root_path);
    if (!ok) {
        PDLOG(WARNING, "fail to create recycle bin path %s", FLAGS_recycle_bin_root_path.c_str());
        return false;
    }
    if (FLAGS_enable_statdb) {
        // Create a dbstat table with tid = 0 and pid = 0
        std::shared_ptr<Table> dbstat = std::make_shared<Table>("dbstat", 0, 0, 8, FLAGS_statdb_ttl);
        dbstat->Init();
        tables_[0].insert(std::make_pair(0, dbstat));
        if (FLAGS_statdb_ttl > 0) {
            gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000,
                    boost::bind(&TabletImpl::GcTable, this, 0, 0));
        }
        // For tablet metric
        metric_ = new TabletMetric(dbstat);
        metric_->Init();
    }
    if (FLAGS_make_snapshot_time < 0 || FLAGS_make_snapshot_time > 23) {
        PDLOG(WARNING, "make_snapshot_time[%d] is illegal.", FLAGS_make_snapshot_time);
        return false;
    }
    task_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    tcmalloc->SetMemoryReleaseRate(FLAGS_mem_release_rate);
#endif 
    return true;
}

void TabletImpl::Get(RpcController* controller,
             const ::rtidb::api::GetRequest* request,
             ::rtidb::api::GetResponse* response,
             Closure* done) {
    if (request->tid() < 1) {
        PDLOG(WARNING, "invalid table tid %ld", request->tid());
        response->set_code(11);
        response->set_msg("invalid table id");
        done->Run();
        return;
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        done->Run();
        return;
    }
    ::rtidb::storage::Ticket ticket;
    Table::Iterator* it = table->NewIterator(request->key(), ticket);
    if (request->ts() > 0) {
        it->Seek(request->ts());
    }else {
        it->SeekToFirst();
    }
    if (it->Valid()) {
        response->set_code(0);
        response->set_msg("ok");
        response->set_key(request->key());
        response->set_ts(it->GetKey());
        response->set_value(it->GetValue()->data, it->GetValue()->size);
        PDLOG(DEBUG, "Get key %s ts %lu value %s", request->key().c_str(),
                request->ts(), it->GetValue()->data);
    }else {
        response->set_code(1);
        response->set_msg("Not Found");
        PDLOG(DEBUG, "not found key %s ts %lu ", request->key().c_str(),
                request->ts());

    }
    done->Run();
}

void TabletImpl::Put(RpcController* controller,
        const ::rtidb::api::PutRequest* request,
        ::rtidb::api::PutResponse* response,
        Closure* done) {
    if (request->tid() < 1) {
        PDLOG(WARNING, "invalid table tid %ld", request->tid());
        response->set_code(11);
        response->set_msg("invalid table id");
        done->Run();
        return;
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (!table->IsLeader()) {
        PDLOG(WARNING, "table with tid %ld, pid %ld is follower and it's readonly ", request->tid(),
                request->pid());
        response->set_code(20);
        response->set_msg("table is follower, and it's readonly");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
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
    PDLOG(DEBUG, "put key %s ok ts %lld", request->pk().c_str(), request->time());
    bool leader = table->IsLeader();
    std::shared_ptr<LogReplicator> replicator;
    if (leader) {
        do {
            replicator = GetReplicator(request->tid(), request->pid());
            if (!replicator) {
                PDLOG(WARNING, "fail to find table tid %ld pid %ld leader's log replicator", request->tid(),
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
    done->Run();
    if (FLAGS_enable_statdb) {
        metric_->IncrThroughput(1, size, 0, 0);
    }
    if (replicator) {
        if (FLAGS_binlog_notify_on_put) {
            replicator->Notify(); 
        }
    }
}

void TabletImpl::BatchGet(RpcController* controller, 
        const ::rtidb::api::BatchGetRequest* request,
        ::rtidb::api::BatchGetResponse* response,
        Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
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
    PDLOG(DEBUG, "batch get count %d", datas.size());
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    it = datas.begin();
    for (; it != datas.end(); ++it) {
        PDLOG(DEBUG, "decode key %lld value %s", it->first, it->second->data);
        ::rtidb::base::Encode((uint64_t)it->first, it->second, rbuffer, offset);
        offset += (4 + 8 + it->second->size);
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

inline bool TabletImpl::CheckScanRequest(const rtidb::api::ScanRequest* request) {
    if (request->st() < request->et()) {
        return false;
    }
    return true;
}

inline bool TabletImpl::CheckTableMeta(const rtidb::api::TableMeta* table_meta) {
    if (table_meta->name().size() <= 0) {
        return false;
    }
    if (table_meta->tid() <= 0) {
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
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %ld, pid %ld is unavailable now", 
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
    // reduce the times of memcpy in vector
    tmp.reserve(FLAGS_scan_reserve_size);
    uint32_t total_block_size = 0;
    uint64_t end_time = request->et();
    bool remove_duplicated_record = false;
    if (request->has_enable_remove_duplicated_record()) {
        remove_duplicated_record = request->enable_remove_duplicated_record();
    }
    PDLOG(DEBUG, "scan pk %s st %lld et %lld", request->pk().c_str(), request->st(), end_time);
    uint32_t scount = 0;
    uint64_t last_time = 0;
    while (it->Valid()) {
        scount ++;
        PDLOG(DEBUG, "scan key %lld value %s", it->GetKey(), it->GetValue()->data);
        if (it->GetKey() <= end_time) {
            break;
        }
        // skip duplicate record 
        if (remove_duplicated_record && scount > 1 && last_time == it->GetKey()) {
            PDLOG(DEBUG, "filter duplicate record for key %s with ts %lld", request->pk().c_str(), it->GetKey());
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
        return;
    }
    std::string* pairs = response->mutable_pairs();
    if (tmp.size() <= 0) {
        pairs->resize(0);
    }else {
        pairs->resize(total_size);
    }
    PDLOG(DEBUG, "scan count %d", tmp.size());
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    std::vector<std::pair<uint64_t, DataBlock*> >::iterator lit = tmp.begin();
    for (; lit != tmp.end(); ++lit) {
        std::pair<uint64_t, DataBlock*>& pair = *lit;
        PDLOG(DEBUG, "decode key %lld value %s", pair.first, pair.second->data);
        ::rtidb::base::Encode(pair.first, pair.second, rbuffer, offset);
        offset += (4 + 8 + pair.second->size);
    }

    response->set_code(0);
    response->set_count(tmp.size());
    metric->set_sptime(::baidu::common::timer::get_micros()); 
    done->Run();
    if (FLAGS_enable_statdb) {
        metric_->IncrThroughput(0, 0, 1, total_size);
    }
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
    std::shared_ptr<Table> table;
    std::shared_ptr<LogReplicator> replicator;
    {
        std::lock_guard<std::mutex> lock(mu_);
        table = GetTableUnLock(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table is not exisit. tid[%u] pid[%u]", tid, pid);
            return -1;
        }
        if (table->IsLeader() || table->GetTableStat() != ::rtidb::storage::kNormal) {
            PDLOG(WARNING, "table is leader or  state[%u] can not change role. tid[%u] pid[%u]", 
                        table->GetTableStat(), tid, pid);
            return -1;
        }
        replicator = GetReplicatorUnLock(tid, pid);
        if (!replicator) {
            PDLOG(WARNING,"no replicator for table tid[%u] pid[%u]", tid, pid);
            return -1;
        }
        table->SetLeader(true);
        table->SetReplicas(replicas);
        replicator->SetRole(ReplicatorRole::kLeaderNode);
    }
    for (auto iter = replicas.begin(); iter != replicas.end(); ++iter) {
        if (!replicator->AddReplicateNode(*iter)) {
            PDLOG(WARNING,"add replicator[%s] for table tid[%u] pid[%u] failed!", 
                        iter->c_str(), tid, pid);
        }
    }
    return 0;
}

void TabletImpl::AddReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::AddReplicaResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table || !table->IsLeader()) {
        PDLOG(WARNING, "table not exist or table is not leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
    if (!replicator) {
        response->set_code(-2);
        response->set_msg("no replicator for table");
        PDLOG(WARNING,"no replicator for table %d, pid %d", request->tid(), request->pid());
        done->Run();
        return;
    }
    bool ok = replicator->AddReplicateNode(request->endpoint());
    if (ok) {
        response->set_code(0);
        response->set_msg("ok");
        done->Run();
    }else {
        response->set_code(-3);
        PDLOG(WARNING, "fail to add endpoint for table %d pid %d", request->tid(), request->pid());
        response->set_msg("fail to add endpoint");
        done->Run();
    }  
    table->SetTableStat(::rtidb::storage::kNormal);
}

void TabletImpl::DelReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table || !table->IsLeader()) {
        PDLOG(WARNING, "table not exist or table is not leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
    if (!replicator) {
        response->set_code(-2);
        response->set_msg("no replicator for table");
        PDLOG(WARNING,"no replicator for table %d, pid %d", request->tid(), request->pid());
        done->Run();
        return;
    }
    bool ok = replicator->DelReplicateNode(request->endpoint());
    if (ok) {
        response->set_code(0);
        response->set_msg("ok");
        done->Run();
    } else {
        response->set_code(-3);
        PDLOG(WARNING, "fail to del endpoint for table %d pid %d", request->tid(), request->pid());
        response->set_msg("fail to del endpoint");
        done->Run();
    }  
}

void TabletImpl::AppendEntries(RpcController* controller,
        const ::rtidb::api::AppendEntriesRequest* request,
        ::rtidb::api::AppendEntriesResponse* response,
        Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table || table->IsLeader()) {
        PDLOG(WARNING, "table not exist or table is leader tid %d, pid %d", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        response->set_code(-1);
        response->set_msg("table is loading now");
        PDLOG(WARNING, "table is loading now. tid %ld, pid %ld", request->tid(), request->pid());
        done->Run();
        return;
    }    
    std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
    if (!replicator) {
        response->set_code(-2);
        response->set_msg("no replicator for table");
        done->Run();
        return;
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
}

void TabletImpl::GetTableSchema(RpcController* controller,
            const ::rtidb::api::GetTableSchemaRequest* request,
            ::rtidb::api::GetTableSchemaResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        response->set_code(-1);
        response->set_msg("table not found");
        PDLOG(WARNING, "fail to find table with tid %d, pid %d", request->tid(),
                request->pid());
        done->Run();
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    response->set_schema(table->GetSchema());
    done->Run();
}

void TabletImpl::GetTableStatus(RpcController* controller,
            const ::rtidb::api::GetTableStatusRequest* request,
            ::rtidb::api::GetTableStatusResponse* response,
            Closure* done) {
    std::lock_guard<std::mutex> lock(mu_);        
    Tables::iterator it = tables_.begin();
    for (; it != tables_.end(); ++it) {
        auto pit = it->second.begin();
        for (; pit != it->second.end(); ++pit) {
            std::shared_ptr<Table> table = pit->second;
            ::rtidb::api::TableStatus* status = response->add_all_table_status();
            status->set_mode(::rtidb::api::TableMode::kTableFollower);
            if (table->IsLeader()) {
                status->set_mode(::rtidb::api::TableMode::kTableLeader);
            }
            status->set_tid(table->GetId());
            status->set_pid(table->GetPid());
            status->set_ttl(table->GetTTL());
            status->set_time_offset(table->GetTimeOffset());
            status->set_is_expire(table->GetExpireStatus());
            if (::rtidb::api::TableState_IsValid(table->GetTableStat())) {
                status->set_state(::rtidb::api::TableState(table->GetTableStat()));
            }
            std::shared_ptr<LogReplicator> replicator = GetReplicatorUnLock(table->GetId(), table->GetPid());
            if (replicator) {
                status->set_offset(replicator->GetOffset());
            }
        }
    }
    response->set_code(0);
    done->Run();
}

void TabletImpl::SetExpire(RpcController* controller,
            const ::rtidb::api::SetExpireRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table not exist. tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(-1);
        response->set_msg("table not exist");
        done->Run();
        return;
    }
	table->SetExpire(request->is_expire());
    PDLOG(INFO, "set table expire[%d]. tid[%u] pid[%u]", request->is_expire(), request->tid(), request->pid());
	response->set_code(0);
	response->set_msg("ok");
	done->Run();
}

void TabletImpl::SetTTLClock(RpcController* controller,
            const ::rtidb::api::SetTTLClockRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table not exist. tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(-1);
        response->set_msg("table not exist");
        done->Run();
        return;
    }
    int64_t cur_time = ::baidu::common::timer::get_micros() / 1000000;
    int64_t offset = (int64_t)request->timestamp() - cur_time;
	table->SetTimeOffset(offset);
    PDLOG(INFO, "set table virtual timestamp[%lu] cur timestamp[%lu] offset[%ld]. tid[%u] pid[%u]", 
                request->timestamp(), cur_time, offset, request->tid(), request->pid());
	response->set_code(0);
	response->set_msg("ok");
	done->Run();
}

bool TabletImpl::ApplyLogToTable(uint32_t tid, uint32_t pid, const ::rtidb::api::LogEntry& log) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        PDLOG(WARNING, "table with tid %ld and pid %ld does not exist", tid, pid);
        return false; 
    }
    table->Put(log.pk(), log.ts(), log.value().c_str(), log.value().size());
    return true;
}

void TabletImpl::MakeSnapshotInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task) {
    std::shared_ptr<Table> table;
    std::shared_ptr<Snapshot> snapshot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        table = GetTableUnLock(tid, pid);
        bool has_error = true;
        do {
            if (!table) {
                PDLOG(WARNING, "table is not exisit. tid[%u] pid[%u]", tid, pid);
                break;
            }
            if (table->GetTableStat() != ::rtidb::storage::kNormal) {
                PDLOG(WARNING, "table state is %d, cannot make snapshot. %ld, pid %ld", 
                             table->GetTableStat(), tid, pid);
                break;
            }    
            snapshot = GetSnapshotUnLock(tid, pid);
            if (!snapshot) {
                PDLOG(WARNING, "snapshot is not exisit. tid[%u] pid[%u]", tid, pid);
                break;
            }
            has_error = false;
        } while (0);
        if (has_error) {
            if (task) {
                task->set_status(::rtidb::api::kFailed);
            }    
            return;
        }
        table->SetTableStat(::rtidb::storage::kMakingSnapshot);
    }    
    uint64_t offset = 0;
    int ret = snapshot->MakeSnapshot(table, offset);
    if (ret == 0) {
        std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
        if (replicator) {
            replicator->SetSnapshotLogPartIndex(offset);
        }    
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        table->SetTableStat(::rtidb::storage::kNormal);
        if (task) {
            if (ret == 0) {
                task->set_status(::rtidb::api::kDone);
            } else {
                task->set_status(::rtidb::api::kFailed);
            }    
        }
    }
}

void TabletImpl::MakeSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    uint32_t tid = request->tid();        
    uint32_t pid = request->pid();        
    bool has_error = true;
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<Snapshot> snapshot = GetSnapshotUnLock(tid, pid);
    do {
        if (!snapshot) {
            response->set_code(-1);
            response->set_msg("snapshot is not exisit!");
            PDLOG(WARNING, "snapshot is not exisit! tid[%u] pid[%u]", tid, pid);
            done->Run();
            break;
        }
        std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
        if (!table) {
            PDLOG(WARNING, "fail to find table with tid %ld, pid %ld", tid, pid);
            response->set_code(-1);
            response->set_msg("table not found");
            done->Run();
            break;
        }
        if (table->GetTableStat() != ::rtidb::storage::kNormal) {
            response->set_code(-1);
            response->set_msg("table status is not normal");
            PDLOG(WARNING, "table state is %d, cannot make snapshot. %ld, pid %ld", 
                         table->GetTableStat(), tid, pid);
            done->Run();
            break;
        }
        has_error = false;
    } while (0);
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (request->task_info().task_type() != ::rtidb::api::TaskType::kMakeSnapshot) {
            response->set_code(-1);
            response->set_msg("task type is not match");
            PDLOG(WARNING, "task type is not match. type is[%s]", 
                            ::rtidb::api::TaskType_Name(request->task_info().task_type()).c_str());
            done->Run();
            has_error = true;
        } else if (FindTask(request->task_info().op_id(), request->task_info().task_type())) {
            response->set_code(-1);
            response->set_msg("task is running");
            PDLOG(WARNING, "task is running. op_id[%lu] op_type[%s] task_type[%s]", 
                            request->task_info().op_id(),
                            ::rtidb::api::OPType_Name(request->task_info().op_type()).c_str(),
                            ::rtidb::api::TaskType_Name(request->task_info().task_type()).c_str());
            has_error = true;
            done->Run();
        }
        task_ptr.reset(request->task_info().New());
        task_ptr->CopyFrom(request->task_info());
        if (has_error) {
            task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
        } else {
            task_ptr->set_status(::rtidb::api::TaskStatus::kDoing);
        }
        AddOPTask(task_ptr);
    }
    if (has_error) {
        return;
    }
    task_pool_.AddTask(boost::bind(&TabletImpl::MakeSnapshotInternal, this, tid, pid, task_ptr));
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::SchedMakeSnapshot() {
    int now_hour = ::rtidb::base::GetNowHour();
    if (now_hour != FLAGS_make_snapshot_time) {
        task_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
        return;
    }
    std::vector<std::pair<uint32_t, uint32_t> > table_set;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto iter = tables_.begin(); iter != tables_.end(); ++iter) {
            for (auto inner = iter->second.begin(); inner != iter->second.end(); ++ inner) {
                if (iter->first == 0 && inner->first == 0) {
                    continue;
                }
                table_set.push_back(std::make_pair(iter->first, inner->first));
            }
        }
    }
    for (auto iter = table_set.begin(); iter != table_set.end(); ++iter) {
        PDLOG(INFO, "start make snapshot tid[%u] pid[%u]", iter->first, iter->second);
        MakeSnapshotInternal(iter->first, iter->second, std::shared_ptr<::rtidb::api::TaskInfo>());
    }
    // delay task one hour later avoid execute  more than one time
    task_pool_.DelayTask(FLAGS_make_snapshot_check_interval + 60 * 60 * 1000, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
}

void TabletImpl::PauseSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table not exist. tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(-1);
        response->set_msg("table not exist");
        done->Run();
        return;
    }
	{
        std::lock_guard<std::mutex> lock(mu_);
		if (table->GetTableStat() != ::rtidb::storage::kNormal) {
			PDLOG(WARNING, "table status is [%u], cann't pause. tid[%u] pid[%u]", 
					table->GetTableStat(), request->tid(), request->pid());
			response->set_code(-2);
			response->set_msg("table status is not kNormal");
			done->Run();
			return;
		}
		table->SetTableStat(::rtidb::storage::kSnapshotPaused);
		PDLOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", 
				   table->GetTableStat(), request->tid(), request->pid());
	}
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::RecoverSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table not exist tid %ld, pid %ld", request->tid(), request->pid());
        response->set_code(-1);
        response->set_msg("table not exist");
        done->Run();
        return;
    }
	{
        std::lock_guard<std::mutex> lock(mu_);
		if (table->GetTableStat() != ::rtidb::storage::kSnapshotPaused) {
			PDLOG(WARNING, "table status is [%u], cann't recover. tid[%u] pid[%u]", 
					table->GetTableStat(), request->tid(), request->pid());
			response->set_code(-1);
			response->set_msg("table status is not kSnapshotPaused");
			done->Run();
			return;
		}
		table->SetTableStat(::rtidb::storage::kNormal);
		PDLOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", 
				   table->GetTableStat(), request->tid(), request->pid());
	}
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::LoadTable(RpcController* controller,
            const ::rtidb::api::LoadTableRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.CopyFrom(request->table_meta());
    if (!CheckTableMeta(&table_meta)) {
        response->set_code(8);
        response->set_msg("table name is empty");
        done->Run();
        return;
    }
    uint32_t tid = table_meta.tid();
    uint32_t pid = table_meta.pid();

    std::string db_path = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid) + 
        "_" + boost::lexical_cast<std::string>(pid);
    if (!::rtidb::base::IsExists(db_path)) {
        PDLOG(WARNING, "no db data for table tid %u, pid %u", tid, pid);
        response->set_code(-1);
        response->set_msg("no db data for table");
        done->Run();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
        if (!table) {
            UpdateTableMeta(db_path, &table_meta);
            if (WriteTableMeta(db_path, &table_meta) < 0) {
                PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
                response->set_code(-1);
                response->set_msg("write table_meta failed");
                done->Run();
                return;
            }
            std::string msg;
            if (CreateTableInternal(&table_meta, msg) < 0) {
                response->set_code(-1);
                response->set_msg(msg.c_str());
                done->Run();
                return;
            }
        } else {
            response->set_code(1);
            response->set_msg("table with tid and pid exists");
            done->Run();
            return;
        }
    }
    done->Run();
    uint64_t ttl = table_meta.ttl();
    std::string name = table_meta.name();
    uint32_t seg_cnt = 8;
    if (table_meta.seg_cnt() > 0) {
        seg_cnt = table_meta.seg_cnt();
    }
    PDLOG(INFO, "create table with id %d pid %d name %s seg_cnt %d ttl %llu", tid, 
            pid, name.c_str(), seg_cnt, ttl);
    
    // load snapshot data
    std::shared_ptr<Table> table = GetTable(tid, pid);        
    if (!table) {
        PDLOG(WARNING, "table with tid %ld and pid %ld does not exist", tid, pid);
        return; 
    }
    std::shared_ptr<Snapshot> snapshot = GetSnapshot(tid, pid);
    if (!snapshot) {
        PDLOG(WARNING, "snapshot with tid %ld and pid %ld does not exist", tid, pid);
        return; 
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        PDLOG(WARNING, "replicator with tid %ld and pid %ld does not exist", tid, pid);
        return;
    }
    uint64_t latest_offset = 0;
    bool ok = snapshot->Recover(table, latest_offset);
    if (ok) {
        table->SetTableStat(::rtidb::storage::kNormal);
        replicator->SetOffset(latest_offset);
        replicator->SetSnapshotLogPartIndex(snapshot->GetOffset());
        replicator->MatchLogOffset();
        table->SchedGc();
        if (ttl > 0) {
            gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
            PDLOG(INFO, "table %s with tid %ld pid %ld enable ttl %ld", name.c_str(), tid, pid, ttl);
        }
    }else {
       DeleteTableInternal(tid, pid);
    }
}

int32_t TabletImpl::DeleteTableInternal(uint32_t tid, uint32_t pid) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        return -1;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    // do block other requests
    {
        std::lock_guard<std::mutex> lock(mu_);
        tables_[tid].erase(pid);
        replicators_[tid].erase(pid);
        snapshots_[tid].erase(pid);
    }

    if (replicator) {
        replicator->Stop();
        PDLOG(INFO, "drop replicator for tid %d, pid %d", tid, pid);
    }

    std::string source_path = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid) + "_" +
        boost::lexical_cast<std::string>(pid);

    if (!::rtidb::base::IsExists(source_path)) {
        return 0;
    }

    std::string recycle_path = FLAGS_recycle_bin_root_path + "/" + boost::lexical_cast<std::string>(tid) + 
        "_" + boost::lexical_cast<std::string>(pid) + "_" + ::rtidb::base::GetNowTime();
    ::rtidb::base::Rename(source_path, recycle_path);
    return 0;
}

void TabletImpl::CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done) {
    const ::rtidb::api::TableMeta* table_meta = &request->table_meta();
    if (!CheckTableMeta(table_meta)) {
        response->set_code(8);
        response->set_msg("table name is empty");
        done->Run();
        return;
    }
    uint32_t tid = table_meta->tid();
    uint32_t pid = table_meta->pid();
    PDLOG(INFO, "create table tid[%u] pid[%u]", tid, pid);
    uint64_t ttl = table_meta->ttl();
    std::string name = table_meta->name();
    uint32_t seg_cnt = 8;
    if (table_meta->seg_cnt() > 0) {
        seg_cnt = table_meta->seg_cnt();
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
        std::shared_ptr<Snapshot> snapshot = GetSnapshotUnLock(tid, pid);
        if (table || snapshot) {
            if (table) {
                PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
            }
            if (snapshot) {
                PDLOG(WARNING, "snapshot with tid[%u] and pid[%u] exists", tid, pid);
            }
            response->set_code(1);
            response->set_msg("table with tid and pid exists");
            done->Run();
            return;
        }       
    	std::string table_db_path = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid) +
                        "_" + boost::lexical_cast<std::string>(pid);
		if (WriteTableMeta(table_db_path, table_meta) < 0) {
        	PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
            response->set_code(-1);
            response->set_msg("write table_meta failed");
            done->Run();
            return;
		}
        std::string msg;
        if (CreateTableInternal(table_meta, msg) < 0) {
            response->set_code(-1);
            response->set_msg(msg.c_str());
            done->Run();
            return;
        }
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
    std::shared_ptr<Table> table = GetTable(tid, pid);        
    if (!table) {
        PDLOG(WARNING, "table with tid %ld and pid %ld does not exist", tid, pid);
        return; 
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        PDLOG(WARNING, "replicator with tid %ld and pid %ld does not exist", tid, pid);
        return;
    }
    table->SetTableStat(::rtidb::storage::kNormal);
    replicator->MatchLogOffset();
    PDLOG(INFO, "create table with id %d pid %d name %s seg_cnt %d ttl %llu", tid, 
            pid, name.c_str(), seg_cnt, ttl);
    if (ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
        PDLOG(INFO, "table %s with tid %ld pid %ld enable ttl %llu", name.c_str(), tid, pid, ttl);
    }
}

int TabletImpl::WriteTableMeta(const std::string& path, const ::rtidb::api::TableMeta* table_meta) {
	if (!::rtidb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return -1;
    }
	std::string full_path = path + "/table_meta.txt";
	std::string table_meta_info;
    google::protobuf::TextFormat::PrintToString(*table_meta, &table_meta_info);
    FILE* fd_write = fopen(full_path.c_str(), "w");
    if (fd_write == NULL) {
        PDLOG(WARNING, "fail to open file %s. err[%d: %s]", full_path.c_str(), errno, strerror(errno));
        return -1;
    }
	if (fputs(table_meta_info.c_str(), fd_write) == EOF) {
        PDLOG(WARNING, "write error. path[%s], err[%d: %s]", full_path.c_str(), errno, strerror(errno));
		fclose(fd_write);
		return -1;
    }
	fclose(fd_write);
	return 0;
}

int TabletImpl::UpdateTableMeta(const std::string& path, ::rtidb::api::TableMeta* table_meta) {
	std::string full_path = path + "/table_meta.txt";
    int fd = open(full_path.c_str(), O_RDONLY);
	::rtidb::api::TableMeta old_meta;
    if (fd < 0) {
        PDLOG(WARNING, "[%s] is not exisit", "table_meta.txt");
        return 1;
    } else {
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        if (!google::protobuf::TextFormat::Parse(&fileInput, &old_meta)) {
            PDLOG(WARNING, "parse table_meta failed");
            return -1;
        }
    }
	// use replicas in LoadRequest
	old_meta.clear_replicas();
	old_meta.MergeFrom(*table_meta);
	table_meta->CopyFrom(old_meta);
	std::string new_name = full_path + "." + ::rtidb::base::GetNowTime();
	rename(full_path.c_str(), new_name.c_str());
    return 0;
}

int TabletImpl::CreateTableInternal(const ::rtidb::api::TableMeta* table_meta, std::string& msg) {
    uint32_t seg_cnt = 8;
    std::string name = table_meta->name();
    if (table_meta->seg_cnt() > 0) {
        seg_cnt = table_meta->seg_cnt();
    }
    bool is_leader = false;
    if (table_meta->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> endpoints;
    for (int32_t i = 0; i < table_meta->replicas_size(); i++) {
        endpoints.push_back(table_meta->replicas(i));
    }
    std::shared_ptr<Table> table = std::make_shared<Table>(table_meta->name(), table_meta->tid(),
                             table_meta->pid(), seg_cnt, 
                             table_meta->ttl(), is_leader,
                             endpoints, table_meta->wal());
    table->Init();
    table->SetGcSafeOffset(FLAGS_gc_safe_offset * 60 * 1000);
    table->SetTerm(table_meta->term());
    table->SetSchema(table_meta->schema());
    std::string table_db_path = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(table_meta->tid()) +
                "_" + boost::lexical_cast<std::string>(table_meta->pid());
    std::shared_ptr<LogReplicator> replicator;
    if (table->IsLeader() && table->GetWal()) {
        replicator = std::make_shared<LogReplicator>(table_db_path, table->GetReplicas(), ReplicatorRole::kLeaderNode, table);
    } else if(table->GetWal()) {
        replicator = std::make_shared<LogReplicator>(table_db_path, std::vector<std::string>(), ReplicatorRole::kFollowerNode, table);
    }
    if (!replicator) {
        PDLOG(WARNING, "fail to create replicator for table tid %ld, pid %ld", table_meta->tid(), table_meta->pid());
        msg.assign("fail create replicator for table");
        return -1;
    }
    bool ok = replicator->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init replicator for table tid %ld, pid %ld", table_meta->tid(), table_meta->pid());
        // clean memory
        msg.assign("fail init replicator for table");
        return -1;
    }
    std::shared_ptr<Snapshot> snapshot = std::make_shared<Snapshot>(table_meta->tid(), table_meta->pid(), replicator->GetLogPart());
    ok = snapshot->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init snapshot for tid %d, pid %d", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init snapshot");
        return -1;

    }
    tables_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), table));
    snapshots_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), snapshot));
    replicators_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), replicator));
    return 0;
}

void TabletImpl::DropTable(RpcController* controller,
            const ::rtidb::api::DropTableRequest* request,
            ::rtidb::api::DropTableResponse* response,
            Closure* done) {
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        response->set_code(-1);
        response->set_msg("table dose not exists");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kMakingSnapshot) {
        PDLOG(WARNING, "making snapshot task is running now. tid[%u] pid[%u]", tid, pid);
        response->set_code(-1);
        response->set_msg("table is making snapshot");
        done->Run();
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
    DeleteTableInternal(tid, pid);
}

void TabletImpl::GetTaskStatus(RpcController* controller,
        const ::rtidb::api::TaskStatusRequest* request,
        ::rtidb::api::TaskStatusResponse* response,
        Closure* done) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto iter = task_list_.begin(); iter != task_list_.end(); ++iter) {
        ::rtidb::api::TaskInfo* task = response->add_task();
        task->CopyFrom(**iter);
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::DeleteOPTask(RpcController* controller,
		const ::rtidb::api::DeleteTaskRequest* request,
		::rtidb::api::GeneralResponse* response,
		Closure* done) {
    std::lock_guard<std::mutex> lock(mu_);
	for (int idx = 0; idx < request->op_id_size(); idx++) {
		for (auto iter = task_list_.begin(); iter != task_list_.end(); ) {
			if ((*iter)->op_id() == request->op_id(idx)) {
                PDLOG(INFO, "delete task. op_id[%lu] op_type[%s] task_type[%s] endpoint[%s]", 
                          (*iter)->op_id(), ::rtidb::api::OPType_Name((*iter)->op_type()).c_str(),
                          ::rtidb::api::TaskType_Name((*iter)->task_type()).c_str(), FLAGS_endpoint.c_str());
				iter = task_list_.erase(iter);
				continue;
			}
			iter++;
		}
	}
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::AddOPTask(std::shared_ptr<::rtidb::api::TaskInfo> task) {
    task_list_.push_back(task);
}

std::shared_ptr<::rtidb::api::TaskInfo> TabletImpl::FindTask(
        uint64_t op_id, ::rtidb::api::TaskType task_type) {
    for (auto& task : task_list_) {
        if (task->op_id() == op_id && task->task_type() == task_type) {
            return task;
        }
    }
    return std::shared_ptr<::rtidb::api::TaskInfo>();
}

void TabletImpl::GcTable(uint32_t tid, uint32_t pid) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        return;
    }
    table->SchedGc();
    gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshot(uint32_t tid, uint32_t pid) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetSnapshotUnLock(tid, pid);
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshotUnLock(uint32_t tid, uint32_t pid) {
    Snapshots::iterator it = snapshots_.find(tid);
    if (it != snapshots_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<Snapshot>();
}

std::shared_ptr<LogReplicator> TabletImpl::GetReplicatorUnLock(uint32_t tid, uint32_t pid) {
    Replicators::iterator it = replicators_.find(tid);
    if (it != replicators_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<LogReplicator>();
}

std::shared_ptr<LogReplicator> TabletImpl::GetReplicator(uint32_t tid, uint32_t pid) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetReplicatorUnLock(tid, pid);
}

std::shared_ptr<Table> TabletImpl::GetTable(uint32_t tid, uint32_t pid) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetTableUnLock(tid, pid);
}

std::shared_ptr<Table> TabletImpl::GetTableUnLock(uint32_t tid, uint32_t pid) {
    Tables::iterator it = tables_.find(tid);
    if (it != tables_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<Table>();
}


// http action
/*bool TabletImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
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

    std::vector<std::shared_ptr<Table> > tmp_tables;
    {
        std::lock_guard<std::mutex> lock(mu_);
        Tables::iterator it = tables_.begin();
        for (; it != tables_.end(); ++it) {
            auto tit = it->second.begin();
            for (; tit != it->second.end(); ++tit) {
                tmp_tables.push_back(tit->second);
            }
        }
    }

    ::rapidjson::StringBuffer sb;
    ::rapidjson::Writer<::rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("tables");
    writer.StartArray();
    for (size_t i = 0; i < tmp_tables.size(); i++) {
        std::shared_ptr<Table> table = tmp_tables[i];
        writer.StartObject();
        writer.Key("name");
        writer.String(table->GetName().c_str());
        writer.Key("tid");
        writer.Uint(table->GetId());
        writer.Key("pid");
        writer.Uint(table->GetPid());
        std::shared_ptr<LogReplicator> replicator = GetReplicator(table->GetId(), table->GetPid());
        if (replicator) {
            writer.Key("log_offset");
            writer.Uint(replicator->GetLogOffset());
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
    std::shared_ptr<Table> stat = GetTable(0, 0);
    if (!stat) {
        writer.EndArray();
        writer.EndObject();
        response.content->Append(sb.GetString());
        return;
    }

    ::rtidb::storage::Ticket ticket;
    Table::Iterator* it = stat->NewIterator(pk, ticket);
    it->SeekToFirst();
    uint32_t read_cnt = 0;
    while (it->Valid() && read_cnt < FLAGS_metric_max_record_cnt) {
        writer.StartArray();
        uint32_t val = 0;
        memcpy(static_cast<void*>(&val), it->GetValue()->data, 4);
        writer.Uint(val);
        writer.Uint(it->GetKey());
        writer.EndArray();
        it->Next();
        read_cnt++;
    }
    writer.EndArray();
    writer.EndObject();
    response.content->Append(sb.GetString());
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
}*/

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



