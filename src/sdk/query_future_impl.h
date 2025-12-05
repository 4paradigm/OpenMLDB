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

#ifndef SRC_SDK_QUERY_FUTURE_IMPL_H_
#define SRC_SDK_QUERY_FUTURE_IMPL_H_

#include <memory>
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "sdk/base.h"
#include "sdk/result_set_sql.h"
#include "sdk/sql_router.h"

namespace openmldb {
namespace sdk {

class QueryFutureImpl : public QueryFuture {
 public:
    explicit QueryFutureImpl(openmldb::RpcCallback<openmldb::api::QueryResponse>* callback) : callback_(callback) {
        if (callback_) {
            callback_->Ref();
        }
    }
    ~QueryFutureImpl() {
        if (callback_) {
            callback_->UnRef();
        }
    }

    std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(hybridse::sdk::Status* status) override {
        if (!status) {
            return nullptr;
        }
        if (!callback_ || !callback_->GetResponse() || !callback_->GetController()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error, response or controller null";
            return nullptr;
        }
        brpc::Join(callback_->GetController()->call_id());
        if (callback_->GetController()->Failed()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error, " + callback_->GetController()->ErrorText();
            return nullptr;
        }
        if (callback_->GetResponse()->code() != ::openmldb::base::kOk) {
            status->code = callback_->GetResponse()->code();
            status->msg = "request error, " + callback_->GetResponse()->msg();
            return nullptr;
        }
        auto rs = ResultSetSQL::MakeResultSet(callback_->GetResponse(), callback_->GetController(), status);
        return rs;
    }

    bool IsDone() const override {
        if (callback_) return callback_->IsDone();
        return false;
    }

 private:
    openmldb::RpcCallback<openmldb::api::QueryResponse>* callback_;
};

class BatchQueryFutureImpl : public QueryFuture {
 public:
    explicit BatchQueryFutureImpl(openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback)
        : callback_(callback) {
        if (callback_) {
            callback_->Ref();
        }
    }

    ~BatchQueryFutureImpl() {
        if (callback_) {
            callback_->UnRef();
        }
    }

    std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(hybridse::sdk::Status* status) override {
        if (!status) {
            return nullptr;
        }
        if (!callback_ || !callback_->GetResponse() || !callback_->GetController()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error, response or controller null";
            return nullptr;
        }
        brpc::Join(callback_->GetController()->call_id());
        if (callback_->GetController()->Failed()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error. " + callback_->GetController()->ErrorText();
            return nullptr;
        }
        auto rs = std::make_shared<openmldb::sdk::SQLBatchRequestResultSet>(callback_->GetResponse(),
                                                                      callback_->GetController());
        if (!rs->Init()) {
            status->code = -1;
            status->msg = "request error, resuletSetSQL init failed";
            return nullptr;
        }
        return rs;
    }

    bool IsDone() const override { return callback_->IsDone(); }

 private:
    openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback_;
};


}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_QUERY_FUTURE_IMPL_H_
