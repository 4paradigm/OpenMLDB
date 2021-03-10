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


#ifndef SRC_SDK_RESULT_SET_SQL_H_
#define SRC_SDK_RESULT_SET_SQL_H_

#include <memory>
#include <string>

#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "proto/tablet.pb.h"
#include "sdk/base_impl.h"
#include "sdk/codec_sdk.h"
#include "sdk/result_set.h"
#include "sdk/result_set_base.h"

namespace rtidb {
namespace sdk {

class ResultSetSQL : public ::fesql::sdk::ResultSet {
 public:
    ResultSetSQL(const ::fesql::vm::Schema& schema, uint32_t record_cnt, uint32_t buf_size,
                 const std::shared_ptr<brpc::Controller>& cntl);

    ~ResultSetSQL();

    static std::shared_ptr<::fesql::sdk::ResultSet> MakeResultSet(
        const std::shared_ptr<::rtidb::api::QueryResponse>& response, const std::shared_ptr<brpc::Controller>& cntl,
        ::fesql::sdk::Status* status);

    static std::shared_ptr<::fesql::sdk::ResultSet> MakeResultSet(
        const std::shared_ptr<::rtidb::api::ScanResponse>& response,
        const ::google::protobuf::RepeatedField<uint32_t>& projection, const std::shared_ptr<brpc::Controller>& cntl,
        std::shared_ptr<::fesql::vm::TableHandler> table_handler, ::fesql::sdk::Status* status);

    bool Init();

    bool Reset() { return result_set_base_->Reset(); }

    bool Next() { return result_set_base_->Next(); }

    bool IsNULL(int index) { return result_set_base_->IsNULL(index); }

    bool GetString(uint32_t index, std::string* str) { return result_set_base_->GetString(index, str); }

    bool GetBool(uint32_t index, bool* result) { return result_set_base_->GetBool(index, result); }

    bool GetChar(uint32_t index, char* result) { return result_set_base_->GetChar(index, result); }

    bool GetInt16(uint32_t index, int16_t* result) { return result_set_base_->GetInt16(index, result); }

    bool GetInt32(uint32_t index, int32_t* result) { return result_set_base_->GetInt32(index, result); }

    bool GetInt64(uint32_t index, int64_t* result) { return result_set_base_->GetInt64(index, result); }

    bool GetFloat(uint32_t index, float* result) { return result_set_base_->GetFloat(index, result); }

    bool GetDouble(uint32_t index, double* result) { return result_set_base_->GetDouble(index, result); }

    bool GetDate(uint32_t index, int32_t* date) { return result_set_base_->GetDate(index, date); }

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day) {
        return result_set_base_->GetDate(index, year, month, day);
    }

    bool GetTime(uint32_t index, int64_t* mills) { return result_set_base_->GetTime(index, mills); }

    const ::fesql::sdk::Schema* GetSchema() { return result_set_base_->GetSchema(); }

    int32_t Size() { return result_set_base_->Size(); }

 private:
    ::fesql::vm::Schema schema_;
    uint32_t record_cnt_;
    uint32_t buf_size_;
    std::shared_ptr<brpc::Controller> cntl_;
    ResultSetBase* result_set_base_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_RESULT_SET_SQL_H_
