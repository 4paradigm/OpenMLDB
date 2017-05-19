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
DECLARE_string(db_root_path);

namespace rtidb {
namespace tablet {

TabletImpl::TabletImpl():tables_(),mu_(), gc_pool_(FLAGS_gc_pool_size),
    metric_(NULL){}

TabletImpl::~TabletImpl() {}

void TabletImpl::Init() {
    MutexLock lock(&mu_);
    // Create a dbstat table with tid = 0 and pid = 0
    Table* dbstat = new Table("dbstat", 0, 0, 8, FLAGS_statdb_ttl);
    dbstat->Init();
    dbstat->Ref();
    tables_.insert(std::make_pair(0, dbstat));
    if (FLAGS_statdb_ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000,
                boost::bind(&TabletImpl::GcTable, this, 0));
    }
    // For tablet metric
    dbstat->Ref();
    metric_ = new TabletMetric(dbstat);
    metric_->Init();
}

void TabletImpl::Put(RpcController* controller,
        const ::rtidb::api::PutRequest* request,
        ::rtidb::api::PutResponse* response,
        Closure* done) {
    Table* table = GetTable(request->tid());
    if (table == NULL) {
        LOG(WARNING, "fail to find table with id %d", request->tid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    bool pers = table->Persistence();
    ::rtidb::api::TableRow row;
    TableDataHA* ha = NULL;
    if (pers) {
        ha = GetTableHa(request->tid());
        row.set_pk(request->pk());
        row.set_data(request->value());
        row.set_time(request->time());
    }
    uint64_t size = request->value().length();
    table->Put(request->pk(), request->time(), request->value().c_str(),
            request->value().length());
    response->set_code(0);
    LOG(DEBUG, "put key %s ok ts %lld", request->pk().c_str(), request->time());
    table->UnRef();
    done->Run();
    metric_->IncrThroughput(1, size, 0, 0);
    if (pers && ha != NULL) {
        ha->Put(row);
        ha->UnRef();
    }
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
    Table* table = GetTable(request->tid());
    if (table == NULL) {
        LOG(WARNING, "fail to find table with id %d", request->tid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    metric->set_sctime(::baidu::common::timer::get_micros());
    // Use seek to process scan request
    // the first seek to find the total size to copy
    Table::Iterator* it = table->NewIterator(request->pk());
    it->Seek(request->st());
    metric->set_sitime(::baidu::common::timer::get_micros());
    std::vector<std::pair<uint64_t, DataBlock*> > tmp;
    // TODO(wangtaize) controle the max size
    uint32_t total_block_size = 0;
    uint64_t end_time = request->et();
    if (table->GetTTL() > 0) {
        uint64_t ttl_end_time = ::baidu::common::timer::get_micros() / 1000 - table->GetTTL() * 60 * 1000;
        end_time = ttl_end_time > (uint64_t)request->et() ? ttl_end_time : (uint64_t)request->et();
    }
    LOG(DEBUG, "scan pk %s st %lld et %lld", request->pk().c_str(), request->st(), end_time);
    while (it->Valid()) {
        LOG(DEBUG, "scan key %lld value %s", it->GetKey(), it->GetValue()->data);
        if (it->GetKey() < end_time) {
            break;
        }
        tmp.push_back(std::make_pair(it->GetKey(), it->GetValue()));
        total_block_size += it->GetValue()->size;
        it->Next();
    }
    metric->set_setime(::baidu::common::timer::get_micros());
    uint32_t total_size = tmp.size() * (8+4) + total_block_size;
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
    delete it;
    metric_->IncrThroughput(0, 0, 1, total_size);
}

void TabletImpl::CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done) {
    if (!CheckCreateRequest(request)) {
         // table exists
        response->set_code(8);
        response->set_msg("table name is empty");
        done->Run();
        return;
    }
    MutexLock lock(&mu_);
    // check table if it exist
    if (tables_.find(request->tid()) != tables_.end()) {
        // table exists
        response->set_code(-2);
        response->set_msg("table exists");
        done->Run();
        return;
    }
    uint32_t tid = request->tid();
    uint32_t ttl = request->ttl();
    uint32_t seg_cnt = 8;
    std::string name = request->name();
    if (request->seg_cnt() > 0 && request->seg_cnt() < 32) {
        seg_cnt = request->seg_cnt();
    }

    // parameter validation 
    Table* table = new Table(request->name(), request->tid(),
                             request->pid(), seg_cnt, 
                             request->ttl());
    table->Init();
    table->SetGcSafeOffset(FLAGS_gc_safe_offset);
    // for tables_ 
    table->Ref();
    table->Persistence(request->ha());
    if (request->ha()) {
        std::string db_path = FLAGS_db_root_path + "/" + request->name();
        TableDataHA* table_ha = new TableDataHA(db_path,
                request->name());
        bool ok = table_ha->Init();
        if (!ok) {
            table->UnRef();
            response->set_code(-2);
            response->set_msg("table ha err");
            done->Run();
            return;
        }
        table_ha->Ref();
        ::rtidb::api::TableMeta meta;
        meta.set_tid(request->tid());
        meta.set_name(request->name());
        meta.set_pid(request->pid());
        meta.set_ttl(request->ttl());
        meta.set_seg_cnt(request->seg_cnt());
        table_ha->SaveMeta(meta);
        table_has_.insert(std::make_pair(request->tid(), table_ha));
    }
    tables_.insert(std::make_pair(request->tid(), table));
    response->set_code(0);
    LOG(INFO, "create table with id %d pid %d name %s seg_cnt %d ttl %d", request->tid(), 
            request->pid(), request->name().c_str(), request->seg_cnt(), request->ttl());
    done->Run();
    if (ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid));
        LOG(INFO, "table %s with %d enable ttl %d", name.c_str(), tid, ttl);
    }
}

void TabletImpl::DropTable(RpcController* controller,
            const ::rtidb::api::DropTableRequest* request,
            ::rtidb::api::DropTableResponse* response,
            Closure* done) {
    Table* table = GetTable(request->tid());
    if (table == NULL) {
        response->set_code(-1);
        response->set_msg("table does not exist");
        done->Run();
        return;
    }
    uint32_t tid = request->tid();
    // do block other requests
    {
        MutexLock lock(&mu_);
        tables_.erase(request->tid());
        response->set_code(0);
        done->Run();
    }
    uint64_t size = table->Release();
    LOG(INFO, "delete table %d with bytes %lld released", tid, size);
    // unref table, let it release memory
    table->UnRef();
    table->UnRef();
}

void TabletImpl::RelMem(RpcController* controller,
        const ::rtidb::api::RelMemRequest*,
        ::rtidb::api::RelMemResponse*,
        Closure* done) {
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    tcmalloc->ReleaseFreeMemory();
#endif
}


void TabletImpl::GcTable(uint32_t tid) {
    Table* table = GetTable(tid);
    if (table == NULL) {
        return;
    }
    table->SchedGc();
    table->UnRef();
    gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid));
}


Table* TabletImpl::GetTable(uint32_t tid) {
    MutexLock lock(&mu_);
    std::map<uint32_t, Table*>::iterator it = tables_.find(tid);
    if (it != tables_.end()) {
        Table* table = it->second;
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
    }
    return true;
}

void TabletImpl::ShowTables(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {

    std::vector<Table*> tmp_tables;
    {
        MutexLock lock(&mu_);
        std::map<uint32_t, Table*>::iterator it = tables_.begin();
        for (; it != tables_.end(); ++it) {
            Table* table = it->second;
            table->Ref();
            tmp_tables.push_back(table);
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
        writer.Key("data_byte_size");
        writer.Uint(table->GetByteSize());
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
    Table* stat = GetTable(0);
    if (stat == NULL) {
        writer.EndArray();
        writer.EndObject();
        response.content->Append(sb.GetString());
        return;
    }

    Table::Iterator* it = stat->NewIterator(pk);
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

TableDataHA* TabletImpl::GetTableHa(uint32_t tid) {
    MutexLock lock(&mu_);
    std::map<uint32_t, ::rtidb::tablet::TableDataHA*>::iterator it = table_has_.find(tid);
    if (it == table_has_.end()) {
        return NULL;
    }
    TableDataHA* table_ha = it->second;
    table_ha->Ref();
    return table_ha;
}

}
}



