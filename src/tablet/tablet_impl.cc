//
// tablet_impl.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01
//

#include "tablet/tablet_impl.h"

#include <gflags/gflags.h>
#include <boost/bind.hpp>
#include "base/codec.h"
#include "base/strings.h"
#include "logging.h"
#include "timer.h"
#include <vector>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;
using ::rtidb::storage::Table;
using ::rtidb::storage::DataBlock;

DECLARE_int32(gc_interval);
DECLARE_int32(gc_pool_size);

namespace rtidb {
namespace tablet {

TabletImpl::TabletImpl():tables_(),mu_(), dbstat_(NULL), gc_pool_(FLAGS_gc_pool_size){}

TabletImpl::~TabletImpl() {}

void TabletImpl::Init() {
    //Create a dbstat table with tid = 0 and pid = 0
    //dbstat_ = new Table("dbstat", 0, 0, 8);
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

    table->Put(request->pk(), request->time(), request->value().c_str(),
            request->value().length());
    response->set_code(0);
    LOG(DEBUG, "put key %s ok", request->pk().c_str());
    done->Run();
}

void TabletImpl::Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done) {
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
    // TODO(wangtaize) config the tmp init size
    std::vector<std::pair<uint64_t, DataBlock*> > tmp;
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
    // Experiment reduce memory alloc times
    uint32_t total_size = tmp.size() * (8+4) + total_block_size;
    std::string* pairs = response->mutable_pairs();
    pairs->resize(total_size);
    LOG(DEBUG, "scan count %d", tmp.size());
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    for (size_t i = 0; i < tmp.size(); i++) {
        std::pair<uint64_t, DataBlock*>& pair = tmp[i];
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
}

void TabletImpl::CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done) {
    MutexLock lock(&mu_);
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
    if (request->seg_cnt() > 0 && request->seg_cnt() < 32) {
        seg_cnt = request->seg_cnt();
    }
    //TODO(wangtaize) config segment count option
    // parameter validation 
    Table* table = new Table(request->name(), request->tid(),
                             request->pid(), seg_cnt, 
                             request->ttl());
    table->Init();
    // for tables_ 
    table->Ref();
    tables_.insert(std::make_pair(request->tid(), table));
    response->set_code(0);
    LOG(INFO, "create table with id %d pid %d name %s seg_cnt %d ttl %d", request->tid(), 
            request->pid(), request->name().c_str(), request->seg_cnt(), request->ttl());
    done->Run();
    if (ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid));
    }
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

}
}



