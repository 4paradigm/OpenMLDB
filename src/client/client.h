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

#ifndef SRC_CLIENT_CLIENT_H_
#define SRC_CLIENT_CLIENT_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "brpc/channel.h"
#include "rpc/rpc_client.h"

namespace openmldb::client {

class Client {
 public:
    Client(const std::string& endpoint, const std::string& real_endpoint)
        : endpoint_(endpoint), real_endpoint_(real_endpoint) {
        if (real_endpoint_.empty()) {
            real_endpoint_ = endpoint_;
        }
    }

    virtual ~Client() {}

    virtual int Init() = 0;

    const std::string& GetEndpoint() const {
        return endpoint_;
    }

    const std::string& GetRealEndpoint() const {
        return real_endpoint_;
    }

 private:
    std::string endpoint_;
    std::string real_endpoint_;
};

}  // namespace openmldb::client

#endif  // SRC_CLIENT_CLIENT_H_
