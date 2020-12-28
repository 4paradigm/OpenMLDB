/*
 * result_set_impl.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#include "base/fe_strings.h"
#include "base/status.h"
#include "codec/fe_schema_codec.h"
#include "glog/logging.h"

namespace rtidb {
namespace sdk {

ResultSetSQL::ResultSetSQL(const std::shared_ptr<::rtidb::api::QueryResponse>& response,
                           const std::shared_ptr<brpc::Controller>& cntl)
    : response_(response), cntl_(cntl), result_set_base_(nullptr) {}

ResultSetSQL::~ResultSetSQL() { delete result_set_base_; }

bool ResultSetSQL::Init() {
    if (!response_ || response_->code() != ::rtidb::base::kOk) {
        LOG(WARNING) << "bad response code " << response_->code();
        return false;
    }
    DLOG(INFO) << "byte size " << response_->byte_size() << " count " << response_->count();
    ::fesql::vm::Schema internal_schema;
    bool ok = ::fesql::codec::SchemaCodec::Decode(response_->schema(), &internal_schema);
    if (!ok) {
        LOG(WARNING) << "fail to decode response schema ";
        return false;
    }
    std::unique_ptr<::fesql::sdk::RowIOBufView> row_view(new ::fesql::sdk::RowIOBufView(internal_schema));
    result_set_base_ =
        new ResultSetBase(cntl_, response_->count(), response_->byte_size(), std::move(row_view), internal_schema);
    return true;
}

}  // namespace sdk
}  // namespace rtidb
