//
// rtidb_client.h 
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

#ifndef RTIDB_CLIENT_H
#define RTIDB_CLIENT_H

#include "rtidb_tablet_server.pb.h"

#include "dbms/baidu_common.h"

namespace rtidb {

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
             const std::string& val) {
        int64_t consumed = ::baidu::common::timer::get_micros();
        PutRequest request;
        request.set_pk(pk);
        request.set_sk(sk);
        request.set_value(val);
        PutResponse response;
        bool ok = rpc_client_->SendRequest(stub_, &RtiDBTabletServer_Stub::Put,
                 &request, &response, 12, 1);
        if (ok && response.code() == 0) {
            consumed = ::baidu::common::timer::get_micros() - consumed;
            std::cout << "Put "<< sk <<" ok, latency "<< consumed / 1000  << " ms"<< std::endl;
            return ;
        }
        std::cout << "fail to call put request with error " << response.msg() << std::endl;
    }

    void Scan(const std::string& pk, const std::string& sk,
              const std::string& ek) {

        int64_t consumed = ::baidu::common::timer::get_micros();
        ScanRequest request;
        request.set_pk(pk);
        request.set_sk(sk);
        request.set_ek(ek);
        ScanResponse response;
        bool ok = rpc_client_->SendRequest(stub_, &RtiDBTabletServer_Stub::Scan,
                &request, &response, 12, 1);
        if (!ok || response.code() != 0) {
            std::cout << "fail to call scan request with error " << response.msg() << std::endl;
            return;
        }

        consumed = ::baidu::common::timer::get_micros() - consumed;
        for (int i = 0; i < response.pairs_size(); i++) {
            const ::rtidb::KvPair& pair = response.pairs(i);
            std::cout << "key:" << pair.sk()
                << "  value:" << pair.value() << std::endl;
        }
        std::cout << "scan " << response.pairs_size() << " records, latency " 
            << consumed / 1000 << " ms" << std::endl;
    }

private:
    std::string endpoint_;
    RtiDBTabletServer_Stub* stub_;
    RpcClient* rpc_client_;
};
}

#endif /* !RTIDB_CLIENT_H */
