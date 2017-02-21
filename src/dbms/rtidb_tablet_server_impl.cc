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


namespace rtidb {


RtiDBTabletServerImpl::RtiDBTabletServerImpl(uint32_t partitions):partitions_(partitions),
    tables_() {}

RtiDBTabletServerImpl::~RtiDBTabletServerImpl() {}

bool RtiDBTabletServerImpl::Init() {

}

void RtiDBTabletServerImpl::Put(RpcController* controller,
            const PutRequest* request,
            PutResponse* response,
            Closure* done) {


}



}


