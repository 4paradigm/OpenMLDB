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

#include "sdk/table_reader_impl.h"

#include <memory>
#include <utility>

#include "base/hash.h"
#include "brpc/channel.h"
#include "client/tablet_client.h"
#include "proto/tablet.pb.h"
#include "sdk/result_set_sql.h"

namespace openmldb {
namespace sdk {

class ScanFutureImpl : public ScanFuture {
 public:
    ScanFutureImpl(openmldb::RpcCallback<openmldb::api::ScanResponse>* callback,
                            const ::google::protobuf::RepeatedField<uint32_t>& projection,
                            std::shared_ptr<::hybridse::vm::TableHandler> table_handler)
        : callback_(callback), schema_(), projection_(projection), table_handler_(table_handler) {
        if (callback_) {
            callback_->Ref();
        }
    }

    ~ScanFutureImpl() {
        if (callback_) {
            callback_->UnRef();
        }
    }

    bool IsDone() const override {
        if (callback_) return callback_->IsDone();
        return false;
    }

    std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(::hybridse::sdk::Status* status) override {
        if (status == nullptr) {
            return std::shared_ptr<hybridse::sdk::ResultSet>();
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

        auto rs = ResultSetSQL::MakeResultSet(callback_->GetResponse(), projection_, callback_->GetController(),
                                              table_handler_, status);
        return rs;
    }

 private:
    openmldb::RpcCallback<openmldb::api::ScanResponse>* callback_;
    hybridse::vm::Schema schema_;
    ::google::protobuf::RepeatedField<uint32_t> projection_;
    std::shared_ptr<::hybridse::vm::TableHandler> table_handler_;
};

TableReaderImpl::TableReaderImpl(DBSDK* cluster_sdk) : cluster_sdk_(cluster_sdk) {}

std::shared_ptr<openmldb::sdk::ScanFuture> TableReaderImpl::AsyncScan(const std::string& db, const std::string& table,
                                                                      const std::string& key, int64_t st, int64_t et,
                                                                      const ScanOption& so, int64_t timeout_ms,
                                                                      ::hybridse::sdk::Status* status) {
    auto table_handler = cluster_sdk_->GetCatalog()->GetTable(db, table);
    if (!table_handler) {
        LOG(WARNING) << "fail to get table " << table << "desc from catalog";
        return std::shared_ptr<openmldb::sdk::ScanFuture>();
    }

    auto sdk_table_handler = dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
    uint32_t pid_num = sdk_table_handler->GetPartitionNum();
    uint32_t pid = 0;
    if (pid_num > 0) {
        pid = ::openmldb::base::hash64(key) % pid_num;
    }
    auto accessor = sdk_table_handler->GetTablet(pid);
    if (!accessor) {
        LOG(WARNING) << "fail to get tablet for db " << db << " table " << table;
        return std::shared_ptr<openmldb::sdk::ScanFuture>();
    }
    auto client = accessor->GetClient();
    std::shared_ptr<openmldb::api::ScanResponse> response = std::make_shared<openmldb::api::ScanResponse>();
    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    cntl->set_timeout_ms(timeout_ms);
    openmldb::RpcCallback<openmldb::api::ScanResponse>* callback =
        new openmldb::RpcCallback<openmldb::api::ScanResponse>(response, cntl);

    ::openmldb::api::ScanRequest request;
    request.set_pk(key);
    request.set_tid(sdk_table_handler->GetTid());
    request.set_pid(pid);
    request.set_st(st);
    request.set_et(et);
    request.set_use_attachment(true);
    for (size_t i = 0; i < so.projection.size(); i++) {
        const std::string& col = so.projection.at(i);
        int32_t col_idx = sdk_table_handler->GetColumnIndex(col);
        if (col_idx < 0) {
            LOG(WARNING) << "fail to get col " << col << " from table " << table;
            return std::shared_ptr<openmldb::sdk::ScanFuture>();
        }
        request.add_projection(static_cast<uint32_t>(col_idx));
    }
    if (so.limit > 0) {
        request.set_limit(so.limit);
    }
    if (!so.idx_name.empty()) {
        request.set_idx_name(so.idx_name);
    }
    auto scan_future = std::make_shared<ScanFutureImpl>(callback, request.projection(), table_handler);
    client->AsyncScan(request, callback);
    return scan_future;
}

std::shared_ptr<hybridse::sdk::ResultSet> TableReaderImpl::Scan(const std::string& db, const std::string& table,
                                                                const std::string& key, int64_t st, int64_t et,
                                                                const ScanOption& so, ::hybridse::sdk::Status* status) {
    auto table_handler = cluster_sdk_->GetCatalog()->GetTable(db, table);
    if (!table_handler) {
        LOG(WARNING) << "fail to get table " << table << "desc from catalog";
        return std::shared_ptr<hybridse::sdk::ResultSet>();
    }

    auto sdk_table_handler = dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
    uint32_t pid_num = sdk_table_handler->GetPartitionNum();
    uint32_t pid = 0;
    if (pid_num > 0) {
        pid = ::openmldb::base::hash64(key) % pid_num;
    }
    auto accessor = sdk_table_handler->GetTablet(pid);
    if (!accessor) {
        LOG(WARNING) << "fail to get tablet for db " << db << " table " << table;
        return std::shared_ptr<hybridse::sdk::ResultSet>();
    }
    auto client = accessor->GetClient();
    ::openmldb::api::ScanRequest request;
    request.set_pk(key);
    request.set_tid(sdk_table_handler->GetTid());
    request.set_pid(pid);
    request.set_st(st);
    request.set_et(et);
    request.set_use_attachment(true);
    for (size_t i = 0; i < so.projection.size(); i++) {
        const std::string& col = so.projection.at(i);
        int32_t col_idx = sdk_table_handler->GetColumnIndex(col);
        if (col_idx < 0) {
            LOG(WARNING) << "fail to get col " << col << " from table " << table;
            return std::shared_ptr<hybridse::sdk::ResultSet>();
        }
        request.add_projection(static_cast<uint32_t>(col_idx));
    }
    if (so.limit > 0) {
        request.set_limit(so.limit);
    }
    if (!so.idx_name.empty()) {
        request.set_idx_name(so.idx_name);
    }
    auto response = std::make_shared<::openmldb::api::ScanResponse>();
    auto cntl = std::make_shared<::brpc::Controller>();
    client->Scan(request, cntl.get(), response.get());
    if (response->code() != 0) {
        status->code = response->code();
        status->msg = response->msg();
        return std::shared_ptr<hybridse::sdk::ResultSet>();
    }
    auto rs = ResultSetSQL::MakeResultSet(response, request.projection(), cntl, table_handler, status);
    return rs;
}

}  // namespace sdk
}  // namespace openmldb
