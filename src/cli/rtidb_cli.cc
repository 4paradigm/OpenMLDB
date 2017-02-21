//
// rtidb_cli.cc
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


#include "rtidb_tablet_server.pb.h"

#include "rpc/rpc_client.h"

namespace ritdb {

class RtiDBClient {

public:
    RtiDBClient(const std::string& endpoint):endpoint_(endpoint),
    stub_(NULL),rpc_client_(NULL){}
    ~RtiDBClient(){}

    void Init() {
        rpc_client_ = new RpcClient();
        rpc_client_->GetStub(endpoint_, &stub_);
    }

    void Put(const std::string& pk,
             const std::string& sk,
             const std::string& val) {}

private:
    std::string endpoint_;
    RtiDBTabletServer_Stub* stub_;
    RpcClient* rpc_client_;
};

}



