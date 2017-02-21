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
    tables_() {}

RtiDBTabletServerImpl::~RtiDBTabletServerImpl() {}

bool RtiDBTabletServerImpl::Init() {
    for (uint32_t i = 0; i < partitions_; i++) {
         const Comparator* com = BytewiseComparator();
         InternalKeyComparator ic(com);
         MemTable* table = new MemTable(ic);
         Mutex* mu = new Mutex();
         tables_.push_back(std::make_pair(mu,table));
    }
}

void RtiDBTabletServerImpl::Put(RpcController* controller,
            const PutRequest* request,
            PutResponse* response,
            Closure* done) {
    uint32_t index = ::rtidb::Hash(request->pk().c_str(), 32, 32) % partitions_;
    std::pair<Mutex*, MemTable*> pair = tables_[index];
    MutexLock lock(pair.first);
    Slice key(request->pk() + request->sk());
    Slice value(request->value());
    pair.second->Add(0, kTypeValue, key, value);
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

}


