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
              const std::string& ek, bool print) {

        int64_t consumed = ::baidu::common::timer::get_micros();
        ScanRequest request;
        request.set_pk(pk);
        request.set_sk(sk);
        request.set_ek(ek);
        RpcMetric* metric = request.mutable_metric();
        metric->set_sqtime(::baidu::common::timer::get_micros());
        ScanResponse response;
        bool ok = rpc_client_->SendRequest(stub_, &RtiDBTabletServer_Stub::Scan,
                &request, &response, 12, 1);
        if (!ok || response.code() != 0) {
            std::cout << "fail to call scan request with error " << response.msg() << std::endl;
            return;
        }
        RpcMetric* pmetric = response.mutable_metric();
        pmetric->set_rptime(::baidu::common::timer::get_micros());
        consumed = ::baidu::common::timer::get_micros() - consumed;
        if (!print) {
            return;
        }
        for (int i = 0; i < response.pairs_size(); i++) {
            const ::rtidb::KvPair& pair = response.pairs(i);
            std::cout << "key:" << pair.sk()
                << "  value:" << pair.value() << std::endl;
        }
        std::cout << "scan " << response.pairs_size() << " records, latency " 
            << consumed / 1000 << " ms" << std::endl;
        PrintMetric(pmetric);
    }

    void PrintMetric(const RpcMetric* metric) {
        uint64_t rpc_send_consumed = metric->rqtime() - metric->sqtime();
        uint64_t server_lock_consumed = metric->sctime() - metric->rqtime();
        uint64_t server_iterator_consumed = metric->sptime() - metric->sitime();
        uint64_t rpc_receive_consumed = metric->rptime() - metric->sptime();
        uint64_t server_seek_consumed = metric->sitime() - metric->sctime();
        std::cout << "rpc metric: #RpcSendConsumed=" << rpc_send_consumed << " "
                  << "#ServerLockConsumed=" << server_lock_consumed << " "
                  << "#ServerSeekConsumed="<<server_seek_consumed << " "
                  << "#ServerIteratorConsumed="<< server_iterator_consumed << " "
                  << "#RpcReceiveConsumed=" << rpc_receive_consumed << std::endl;
    }

private:
    std::string endpoint_;
    RtiDBTabletServer_Stub* stub_;
    RpcClient* rpc_client_;
};
}

#endif /* !RTIDB_CLIENT_H */
