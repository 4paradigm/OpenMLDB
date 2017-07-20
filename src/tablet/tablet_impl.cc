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

namespace rtidb {
namespace tablet {

TabletImpl::TabletImpl():tables_(),mu_(), gc_pool_(FLAGS_gc_pool_size),
    metric_(NULL), replicators_(){}

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

void TabletImpl::Init() {
    MutexLock lock(&mu_);
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
    metric->set_sctime(::baidu::common::timer::get_micros());
    // Use seek to process scan request
    // the first seek to find the total size to copy
    ::rtidb::storage::Ticket ticket;
    Table::Iterator* it = table->NewIterator(request->pk(), ticket);
    it->Seek(request->st());
    metric->set_sitime(::baidu::common::timer::get_micros());
    std::vector<std::pair<uint64_t, DataBlock*> > tmp;
    // TODO(wangtaize) controle the max size
    uint32_t total_block_size = 0;
    uint64_t end_time = request->et();
    LOG(DEBUG, "scan pk %s st %lld et %lld", request->pk().c_str(), request->st(), end_time);
    uint32_t scount = 0;
    while (it->Valid()) {
        scount ++;
        LOG(DEBUG, "scan key %lld value %s", it->GetKey(), it->GetValue()->data);
        if (it->GetKey() <= end_time) {
            break;
        }
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

void TabletImpl::AddReplica(RpcController* controller, 
            const ::rtidb::api::AddReplicaRequest* request,
            ::rtidb::api::AddReplicaResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL ||
        !table->IsLeader()) {
        LOG(WARNING, "table not exist or table is leader tid %ld, pid %ld", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    LogReplicator* replicator = GetReplicator(request->tid(), request->pid());
    if (replicator == NULL) {
        response->set_code(-2);
        response->set_msg("no replicator for table");
        LOG(WARNING,"no replicator for table %d, pid %d", request->tid(), request->pid());
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
        LOG(WARNING, "fail to add endpoint for table %d pid %d", request->tid(), request->pid());
        response->set_msg("fail to add endpoint");
        done->Run();
    }
}

void TabletImpl::AppendEntries(RpcController* controller,
        const ::rtidb::api::AppendEntriesRequest* request,
        ::rtidb::api::AppendEntriesResponse* response,
        Closure* done) {
    Table* table = GetTable(request->tid(), request->pid());
    if (table == NULL ||
        table->IsLeader()) {
        LOG(WARNING, "table not exist or table is leader tid %d, pid %d", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        done->Run();
        return;
    }
    LogReplicator* replicator = GetReplicator(request->tid(), request->pid());
    if (replicator == NULL) {
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
    Table* exist = GetTable(request->tid(), request->pid());
    if (exist != NULL) {
        exist->UnRef();
        LOG(WARNING, "table with tid %ld and pid %ld exists", request->tid(),
                request->pid());
        response->set_code(1);
        response->set_msg("table with tid and pid exists");
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
                             endpoints);
    table->Init();
    table->SetGcSafeOffset(FLAGS_gc_safe_offset);
    // for tables_ 
    table->Ref();
    std::string table_binlog_path = FLAGS_binlog_root_path + "/" + boost::lexical_cast<std::string>(request->tid()) +"_" + boost::lexical_cast<std::string>(request->pid());
    LogReplicator* replicator = NULL;
    if (table->IsLeader()) {
        replicator = new LogReplicator(table_binlog_path, table->GetReplicas(), 
                ReplicatorRole::kLeaderNode, request->tid(), request->pid());
    }else {
        replicator = new LogReplicator(table_binlog_path, 
                boost::bind(&TabletImpl::ApplyLogToTable, this, request->tid(), request->pid(), _1), 
                ReplicatorRole::kFollowerNode, request->tid(), request->pid());
    }
    if (replicator == NULL) {
        tables_[request->tid()].insert(std::make_pair(request->pid(), table));
        response->set_code(0);
        response->set_msg("ok");
        return;
    }
    replicator->Ref();
    bool ok = replicator->Init();
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
    tables_[request->tid()].insert(std::make_pair(request->pid(), table));
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
        response->set_code(0);
        done->Run();
    }
    uint64_t size = table->Release();
    LOG(INFO, "drop table %d pid %d with bytes %lld released", tid, pid, size);
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

LogReplicator* TabletImpl::GetReplicator(uint32_t tid, uint32_t pid) {
    MutexLock lock(&mu_);
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

Table* TabletImpl::GetTable(uint32_t tid, uint32_t pid) {
    MutexLock lock(&mu_);
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
    for (size_t i = 0; i < tmp_tables.size(); i++) {
        Table* table = tmp_tables[i];
        writer.StartObject();
        writer.Key("name");
        writer.String(table->GetName().c_str());
        writer.Key("tid");
        writer.Uint(table->GetId());
        writer.Key("pid");
        writer.Uint(table->GetPid());
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

}
}



