//
// rpc_client.h 
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

#ifndef RTIDB_RPC_CLIENT_H
#define RTIDB_RPC_CLIENT_H

#include <brpc/channel.h>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <mutex.h>
#include <thread_pool.h>
#include "logging.h"
#include <atomic>

using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;

namespace rtidb {
 
template <class T>
class RpcClient {
public:
    RpcClient(const std::string& endpoint) : endpoint_(endpoint), log_id_(0), stub_(NULL), channel_() {
    }
    ~RpcClient() {
        delete channel_;
        delete stub_;
    }

    int Init() {
        channel_ = new brpc::Channel();
        brpc::ChannelOptions options;
        if (channel_->Init(endpoint_.c_str(), "", &options) != 0) {
            return -1;
        }
        stub_.store(new T(channel_), std::memory_order_relaxed);
        return 0;
    }

    int Reconnect() {
        brpc::Channel* old_channel = channel_;
        T* old_stub_ptr = stub_.load(std::memory_order_relaxed);
        brpc::Channel* new_channel = new brpc::Channel();
        brpc::ChannelOptions options;
        if (new_channel->Init(endpoint_.c_str(), "", &options) != 0) {
            return -1;
        }
        stub_.store(new T(new_channel), std::memory_order_release);
        channel_ = new_channel;
        delete old_channel;
        delete old_stub_ptr;
        return 0;
    }

    template <class Request, class Response, class Callback>
    bool SendRequest(void(T::*func)(
                    google::protobuf::RpcController*,
                    const Request*, Response*, Callback*),
                    const Request* request, Response* response,
                    uint64_t rpc_timeout, int retry_times) {
        brpc::Controller cntl;
        cntl.set_log_id(log_id_++);
        if (rpc_timeout > 0) {
            cntl.set_timeout_ms(rpc_timeout * 1000);
        }
        if (retry_times > 0) {
            cntl.set_max_retry(retry_times);
        }
        if (stub_.load(std::memory_order_relaxed) == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return false;
        }
        (stub_.load(std::memory_order_relaxed)->*func)(&cntl, request, response, NULL);
        if (!cntl.Failed()) {
            return true;
        }
        PDLOG(WARNING, "request error. %s", cntl.ErrorText().c_str());
        return false;
    }
  
private:
    std::string endpoint_;
    uint64_t log_id_;
    std::atomic<T*> stub_;
    brpc::Channel* channel_;
};

} // namespace rtidb 

#endif /* !RPC_CLIENT_H */
