/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CLIENT_NEARLINE_TABLET_CLIENT_H_
#define SRC_CLIENT_NEARLINE_TABLET_CLIENT_H_

#include <string>
#include "client/client.h"
#include "proto/nl_tablet.pb.h"
#include "rpc/rpc_client.h"

using Schema = ::google::protobuf::RepeatedPtrField<openmldb::common::ColumnDesc>;

namespace openmldb {

namespace client {

class NearLineTabletClient : public Client {
 public:
    NearLineTabletClient(const std::string& endpoint, const std::string& real_endpoint)
        : Client(endpoint, real_endpoint), client_(real_endpoint.empty() ? endpoint : real_endpoint) {}

    ~NearLineTabletClient() {}

    int Init() override {
        return client_.Init();
    }

    bool CreateTable(const std::string& db_name, const std::string& table_name,
            const std::string& partition_key, const Schema& schema);

 private:
    ::openmldb::RpcClient<::openmldb::nltablet::NLTabletServer_Stub> client_;
};

}  // namespace client
}  // namespace openmldb

#endif  // SRC_CLIENT_NEARLINE_TABLET_CLIENT_H_
