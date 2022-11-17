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

#ifndef SRC_RPC_RPC_CLIENT_H_
#define SRC_RPC_RPC_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/retry_policy.h>
#include <gflags/gflags.h>

#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <utility>

#include "base/glog_wrapper.h"
#include "base/status.h"
#include "proto/tablet.pb.h"

DECLARE_int32(request_sleep_time);

namespace openmldb {

class SleepRetryPolicy : public brpc::RetryPolicy {
 public:
    bool DoRetry(const brpc::Controller* controller) const {
        const int error_code = controller->ErrorCode();
        if (!error_code) {
            return false;
        }
        if (EHOSTDOWN == error_code) {
            PDLOG(WARNING, "error_code is EHOSTDOWN, sleep [%lu] ms", FLAGS_request_sleep_time);
            std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_request_sleep_time));
            return true;
        }
        return (brpc::EFAILEDSOCKET == error_code || brpc::EEOF == error_code || brpc::ELOGOFF == error_code ||
                ETIMEDOUT == error_code  // This is not timeout of RPC.
                || brpc::ELIMIT == error_code || ENOENT == error_code || EPIPE == error_code ||
                ECONNREFUSED == error_code || ECONNRESET == error_code || ENODATA == error_code ||
                brpc::EOVERCROWDED == error_code);
    }
};

static SleepRetryPolicy sleep_retry_policy;

template <class T>
class RpcClient {
 public:
    explicit RpcClient(const std::string& endpoint)
        : endpoint_(endpoint), use_sleep_policy_(false), log_id_(0), stub_(NULL), channel_(NULL) {}
    RpcClient(const std::string& endpoint, bool use_sleep_policy)
        : endpoint_(endpoint), use_sleep_policy_(use_sleep_policy), log_id_(0), stub_(NULL), channel_(NULL) {}
    ~RpcClient() {
        delete channel_;
        delete stub_;
    }

    int Init() {
        channel_ = new brpc::Channel();
        brpc::ChannelOptions options;
        if (use_sleep_policy_) {
            options.retry_policy = &sleep_retry_policy;
        }
        if (channel_->Init(endpoint_.c_str(), "", &options) != 0) {
            return -1;
        }
        stub_ = new T(channel_);
        return 0;
    }

    template <class Request, class Response, class Callback>
    bool SendRequest(void (T::*func)(google::protobuf::RpcController*, const Request*, Response*, Callback*),
                     brpc::Controller* cntl, const Request* request, Response* response, Callback* callback) {
        if (stub_ == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return false;
        }
        (stub_->*func)(cntl, request, response, callback);
        return true;
    }

    template <class Request, class Response, class Callback>
    bool SendRequest(void (T::*func)(google::protobuf::RpcController*, const Request*, Response*, Callback*),
                     brpc::Controller* cntl, const Request* request, Response* response) {
        if (stub_ == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return false;
        }
        (stub_->*func)(cntl, request, response, NULL);
        if (!cntl->Failed()) {
            return true;
        }
        PDLOG(WARNING, "request error. %s", cntl->ErrorText().c_str());
        return false;
    }
    template <class Request, class Response, class Callback>
    bool SendRequest(void (T::*func)(google::protobuf::RpcController*, const Request*, Response*, Callback*),
                     const Request* request, Response* response, uint64_t rpc_timeout, int retry_times) {
        brpc::Controller cntl;
        cntl.set_log_id(log_id_++);
        if (rpc_timeout > 0) {
            cntl.set_timeout_ms(rpc_timeout);
        }
        if (retry_times > 0) {
            cntl.set_max_retry(retry_times);
        }
        if (stub_ == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return false;
        }
        (stub_->*func)(&cntl, request, response, NULL);
        if (!cntl.Failed()) {
            return true;
        }
        PDLOG(WARNING, "request error. %s", cntl.ErrorText().c_str());
        return false;
    }
    
    template <class Request, class Response, class Callback>
    base::Status SendRequestSt(void (T::*func)(google::protobuf::RpcController*, const Request*, Response*, Callback*),
                     const Request* request, Response* response, uint64_t rpc_timeout, int retry_times) {
        base::Status status;
        brpc::Controller cntl;
        cntl.set_log_id(log_id_++);
        if (rpc_timeout > 0) {
            cntl.set_timeout_ms(rpc_timeout);
        }
        if (retry_times > 0) {
            cntl.set_max_retry(retry_times);
        }
        if (stub_ == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return {base::ReturnCode::kServerConnError, "stub is null"};
        }
        (stub_->*func)(&cntl, request, response, NULL);
        if (!cntl.Failed()) {
            return {};
        }
        return {base::ReturnCode::kRPCError, cntl.ErrorText().c_str()};
    }

    template <class Request, class Response, class Callback>
    bool SendRequestGetAttachment(void (T::*func)(google::protobuf::RpcController*, const Request*, Response*,
                                                  Callback*),
                                  const Request* request, Response* response, uint64_t rpc_timeout, int retry_times,
                                  butil::IOBuf* buff) {
        brpc::Controller cntl;
        cntl.set_log_id(log_id_++);
        if (rpc_timeout > 0) {
            cntl.set_timeout_ms(rpc_timeout);
        }
        if (retry_times > 0) {
            cntl.set_max_retry(retry_times);
        }
        if (stub_ == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return false;
        }
        (stub_->*func)(&cntl, request, response, NULL);
        if (cntl.Failed()) {
            PDLOG(WARNING, "request error. %s", cntl.ErrorText().c_str());
            return false;
        }
        buff->append(cntl.response_attachment());
        return true;
    }

    template <class Request, class Response>
    bool SendRequest(void (T::*func)(google::protobuf::RpcController*, const Request*, Response*,
                                     google::protobuf::Closure*),
                     brpc::Controller* cntl, const Request* request, Response* response,
                     google::protobuf::Closure* callback) {
        if (stub_ == NULL) {
            PDLOG(WARNING, "stub is null. client must be init before send request");
            return false;
        }
        (stub_->*func)(cntl, request, response, callback);
        return true;
    }

 private:
    std::string endpoint_;
    bool use_sleep_policy_;
    uint64_t log_id_;
    T* stub_;
    brpc::Channel* channel_;
};

template <class Response>
class RpcCallback : public google::protobuf::Closure {
 public:
    RpcCallback(const std::shared_ptr<Response>& response, const std::shared_ptr<brpc::Controller>& cntl)
        : response_(response), cntl_(cntl), is_done_(false), ref_count_(1) {}

    ~RpcCallback() {}

    void Run() override {
        is_done_.store(true, std::memory_order_release);
        UnRef();
    }

    inline const std::shared_ptr<Response>& GetResponse() const { return response_; }

    inline const std::shared_ptr<brpc::Controller>& GetController() const { return cntl_; }

    inline bool IsDone() const { return is_done_.load(std::memory_order_acquire); }

    void Ref() { ref_count_.fetch_add(1, std::memory_order_acq_rel); }

    void UnRef() {
        if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

 private:
    std::shared_ptr<Response> response_;
    std::shared_ptr<brpc::Controller> cntl_;
    std::atomic<bool> is_done_;
    std::atomic<uint32_t> ref_count_;
};

}  // namespace openmldb

#endif  // SRC_RPC_RPC_CLIENT_H_
