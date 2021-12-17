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

#include "sdk/result_set_sql.h"

#include <memory>
#include <string>
#include <utility>

#include "base/status.h"
#include "catalog/sdk_catalog.h"
#include "codec/fe_schema_codec.h"
#include "glog/logging.h"
#include "schema/schema_adapter.h"

namespace openmldb {
namespace sdk {

ResultSetSQL::ResultSetSQL(const ::hybridse::vm::Schema& schema, uint32_t record_cnt, uint32_t buf_size,
                           const std::shared_ptr<brpc::Controller>& cntl)
    : schema_(schema), record_cnt_(record_cnt), buf_size_(buf_size), cntl_(cntl), result_set_base_(nullptr) {}

ResultSetSQL::~ResultSetSQL() { delete result_set_base_; }

bool ResultSetSQL::Init() {
    std::unique_ptr<::hybridse::sdk::RowIOBufView> row_view(new ::hybridse::sdk::RowIOBufView(schema_));
    DLOG(INFO) << "init result set sql with record cnt " << record_cnt_ << " buf size " << buf_size_;
    result_set_base_ = new ResultSetBase(cntl_, record_cnt_, buf_size_, std::move(row_view), schema_);
    return true;
}

std::shared_ptr<::hybridse::sdk::ResultSet> ResultSetSQL::MakeResultSet(
    const std::shared_ptr<::openmldb::api::QueryResponse>& response, const std::shared_ptr<brpc::Controller>& cntl,
    hybridse::sdk::Status* status) {
    if (!status || !response || !cntl) {
        return std::shared_ptr<ResultSet>();
    }
    ::hybridse::vm::Schema schema;
    bool ok = ::hybridse::codec::SchemaCodec::Decode(response->schema(), &schema);
    if (!ok) {
        status->code = -1;
        status->msg = "request error, fail to decodec schema";
        return std::shared_ptr<ResultSet>();
    }
    std::shared_ptr<::openmldb::sdk::ResultSetSQL> rs =
        std::make_shared<openmldb::sdk::ResultSetSQL>(schema, response->count(), response->byte_size(), cntl);
    ok = rs->Init();
    if (!ok) {
        status->code = -1;
        status->msg = "request error, resuletSetSQL init failed";
        return std::shared_ptr<ResultSet>();
    }
    return rs;
}

std::shared_ptr<::hybridse::sdk::ResultSet> ResultSetSQL::MakeResultSet(
    const std::shared_ptr<::openmldb::api::ScanResponse>& response,
    const ::google::protobuf::RepeatedField<uint32_t>& projection, const std::shared_ptr<brpc::Controller>& cntl,
    std::shared_ptr<::hybridse::vm::TableHandler> table_handler, ::hybridse::sdk::Status* status) {
    if (!status || !response || !cntl) {
        return std::shared_ptr<ResultSet>();
    }
    auto sdk_table_handler = dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
    if (projection.size() > 0) {
        ::hybridse::vm::Schema schema;
        bool ok = ::openmldb::schema::SchemaAdapter::SubSchema(sdk_table_handler->GetSchema(), projection, &schema);
        if (!ok) {
            status->code = -1;
            status->msg = "fail to get sub schema";
        }

        std::shared_ptr<::openmldb::sdk::ResultSetSQL> rs = std::make_shared<openmldb::sdk::ResultSetSQL>(
            *(sdk_table_handler->GetSchema()), response->count(), response->buf_size(), cntl);
        ok = rs->Init();
        if (!ok) {
            status->code = -1;
            status->msg = "request error, resuletSetSQL init failed";
            return std::shared_ptr<ResultSet>();
        }
        return rs;
    } else {
        std::shared_ptr<::openmldb::sdk::ResultSetSQL> rs = std::make_shared<openmldb::sdk::ResultSetSQL>(
            *(sdk_table_handler->GetSchema()), response->count(), response->buf_size(), cntl);
        bool ok = rs->Init();
        if (!ok) {
            status->code = -1;
            status->msg = "request error, resuletSetSQL init failed";
            return std::shared_ptr<ResultSet>();
        }
        return rs;
    }
}

}  // namespace sdk
}  // namespace openmldb
