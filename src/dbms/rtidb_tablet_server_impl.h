//
// rtidb_tablet_server_impl.h 
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

#ifndef RTIDB_TABLET_SERVER_IMPL_H
#define RTIDB_TABLET_SERVER_IMPL_H

#include <vector>
#include <map>
#include "rtidb_tablet_server.pb.h"
#include "storage/memtable.h"
#include "dbms/baidu_common.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

namespace rtidb {

class RtiDBTabletServerImpl : public RtiDBTabletServer {

public:
    RtiDBTabletServerImpl(uint32_t partitions);

    ~RtiDBTabletServerImpl();

    bool Init();

    void Put(RpcController* controller,
            const PutRequest* request,
            PutResponse* response,
            Closure* done);
private:
    uint32_t partitions_;
    std::vector<std::pair<Mutex*, MemTable*> > tables_;
};

}
#endif /* !RTIDB_TABLET_SERVER_IMPL_H */
