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
#include "util/hash.h"

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
    GetTable(request->pk(), table, table_lock);
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
    done->Run();
    table->Unref();
}

void RtiDBTabletServerImpl::Scan(RpcController* controller,
        const ScanRequest* request,
        ScanResponse* response,
        Closure* done) {
    MemTable* table = NULL;
    Mutex* table_lock = NULL;
    GetTable(request->pk(), table, table_lock);
    if (table == NULL || table_lock == NULL) {
        response->set_code(-1);
        response->set_msg("no table exist  for input pk");
        LOG(WARNING, "no table exist for pk %s", request->pk().c_str());
        done->Run();
        return;
    }
    Iterator* it = table->NewIterator();
    Slice sk(request->pk() + "/" + request->sk());
    Slice ek(request->pk() + "/" + request->ek());
    it->Seek(sk);
    while (it->Valid()) {
        if (it->value().compare(ek) > 0) {
            break;
        }
        KvPair* pair = response->add_pairs();
        pair->set_sk(it->key().ToString());
        pair->set_value(it->value().ToString());
        it->Next();
    }
    response->set_code(0);
    response->set_msg("ok");
    table->Unref();
}


void RtiDBTabletServerImpl::GetTable(const std::string& pk, MemTable* table,
        Mutex* table_lock) {
    uint32_t index = ::rtidb::Hash(pk.c_str(), 32, 32) % partitions_;
    MutexLock lock(&mu_);
    if (index < tables_.size()) {
        std::pair<Mutex*, MemTable*> pair = tables_[index];
        table = pair.second;
        table_lock = pair.first;
        table->Ref();
    }
}

}


