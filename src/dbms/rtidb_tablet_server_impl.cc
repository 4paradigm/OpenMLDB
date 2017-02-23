//
// rtidb_tablet_server_impl.cc
// Copyright 2017 elasticlog <elasticlog01@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "dbms/rtidb_tablet_server_impl.h"

#include "storage/dbformat.h"
#include "util/comparator.h"
#include "util/slice.h"
#include "util/coding.h"

namespace rtidb {


RtiDBTabletServerImpl::RtiDBTabletServerImpl(uint32_t partitions):partitions_(partitions),
    tables_(),mu_() {}

RtiDBTabletServerImpl::~RtiDBTabletServerImpl() {}

bool RtiDBTabletServerImpl::Init() {
    for (uint32_t i = 0; i < partitions_; i++) {
         const Comparator* com = BytewiseComparator();
         InternalKeyComparator ic(com);
         MemTable* table = new MemTable(ic);
         // for the tables_ container
         table->Ref();
         Mutex* mu = new Mutex();
         tables_.push_back(std::make_pair(mu,table));
    }
}

void RtiDBTabletServerImpl::Put(RpcController* controller,
            const PutRequest* request,
            PutResponse* response,
            Closure* done) {
    MemTable* table = NULL;
    Mutex* table_lock = NULL;
    GetTable(request->pk(), &table, &table_lock);
    if (table == NULL || table_lock == NULL) {
        response->set_code(-1);
        response->set_msg("no table exist  for input pk");
        LOG(WARNING, "no table exist for pk %s", request->pk().c_str());
        done->Run();
        return;
    }
    MutexLock lock(table_lock);
    Slice key(request->pk()+"/"+request->sk());
    Slice value(request->value());
    table->Add(0, kTypeValue, key, value);
    response->set_code(0);
    response->set_msg("ok");
    LOG(DEBUG, "add key %s value %s ok", key.data(), value.data());
    done->Run();
    table->Unref();
}

void RtiDBTabletServerImpl::Scan(RpcController* controller,
        const ScanRequest* request,
        ScanResponse* response,
        Closure* done) {
    RpcMetric* metric = response->mutable_metric();
    metric->CopyFrom(request->metric());
    metric->set_rqtime(::baidu::common::timer::get_micros());
    MemTable* table = NULL;
    Mutex* table_lock = NULL;
    GetTable(request->pk(), &table, &table_lock);
    if (table == NULL || table_lock == NULL) {
        response->set_code(-1);
        response->set_msg("no table exist  for input pk");
        LOG(WARNING, "no table exist for pk %s", request->pk().c_str());
        done->Run();
        return;
    }
    metric->set_sctime(::baidu::common::timer::get_micros());
    LOG(DEBUG, "scan table with pk %s  sk %s ek %s",request->pk().c_str(),
            request->sk().c_str(), request->ek().c_str());
    Iterator* it = table->NewIterator();
    Slice sk(request->pk() + "/" + request->sk());
    LookupKey lsk(sk, 0);
    Slice ek(request->pk() + "/" + request->ek());
    InternalKey iek(ek, 0, kTypeValue);
    it->Seek(lsk.memtable_key());
    while (it->Valid()) {
        LOG(DEBUG, "scan k:%s v:%s", it->key().data(), it->value().data());
        if (it->key().compare(iek.Encode()) > 0) {
            break;
        }
        KvPair* pair = response->add_pairs();
        pair->set_sk(it->key().data());
        pair->set_value(it->value().data());
        it->Next();
    }
    delete it;
    response->set_code(0);
    response->set_msg("ok");
    table->Unref();
    metric->set_sptime(::baidu::common::timer::get_micros());
    done->Run();
}


void RtiDBTabletServerImpl::GetTable(const std::string& pk, MemTable** table,
        Mutex** table_lock) {
    uint32_t index = ::rtidb::DecodeFixed32(pk.c_str()) % partitions_;
    LOG(DEBUG, "hash pk %s with index %ld", pk.c_str(), index);
    MutexLock lock(&mu_);
    if (index < tables_.size()) {
        std::pair<Mutex*, MemTable*> pair = tables_[index];
        pair.second->Ref();
        *table = pair.second;
        *table_lock = pair.first;
    }
}

}


