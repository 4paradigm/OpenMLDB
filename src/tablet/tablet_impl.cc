//
// tablet_impl.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01
//

#include "tablet/tablet_impl.h"
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
namespace rtidb {
namespace tablet {

TabletImpl::TabletImpl():tables_(),mu_(), dbstat_(NULL){}

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
    LOG(DEBUG, "scan pk %s st %lld et %lld", request->pk().c_str(), request->st(), request->et());
    // Use seek to process scan request
    // the first seek to find the total size to copy
    Table::Iterator* it = table->NewIterator(request->pk());
    it->Seek(request->st());
    metric->set_sitime(::baidu::common::timer::get_micros());
    // TODO(wangtaize) config the tmp init size
    std::vector<std::pair<uint64_t, DataBlock*> > tmp;
    uint32_t total_block_size = 0;
    uint32_t count = 0;
    while (it->Valid()) {
        LOG(DEBUG, "scan key %lld value %s", it->GetKey(), it->GetValue()->data);
        if (it->GetKey() < (uint64_t)request->et()) {
            break;
        }
        tmp.push_back(std::make_pair(it->GetKey(), it->GetValue()));
        count++;
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
    for (size_t i = 0; i < count; i++) {
        std::pair<uint64_t, DataBlock*>& pair = tmp[i];
        LOG(DEBUG, "decode key %lld value %s", pair.first, pair.second->data);
        ::rtidb::base::Encode(pair.first, pair.second, rbuffer, offset);
        offset += (4 + 8 + pair.second->size);
    }
    response->set_code(0);
    response->set_count(count);
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
    //TODO(wangtaize) config segment count option
    Table* table = new Table(request->name(), request->tid(),
            request->pid(), 8);
    table->Init();
    // for tables_ 
    table->Ref();
    tables_.insert(std::make_pair(request->tid(), table));
    response->set_code(0);
    LOG(INFO, "create table with id %d pid %d name %s", request->tid(), 
            request->pid(), request->name().c_str());
    done->Run();
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



