/*
 * result_set_impl.h
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

#ifndef SRC_SDK_RESULT_SET_SQL_H_
#define SRC_SDK_RESULT_SET_SQL_H_

#include <memory>
#include <string>

#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "sdk/codec_sdk.h"
#include "proto/tablet.pb.h"
#include "sdk/base_impl.h"
#include "sdk/result_set.h"

namespace rtidb {
namespace sdk {

class ResultSetSQL : public ::fesql::sdk::ResultSet {
 public:
    ResultSetSQL(std::unique_ptr<::rtidb::api::QueryResponse> response,
                 std::unique_ptr<brpc::Controller> cntl);
    ~ResultSetSQL();

    bool Init();

    bool Reset();

    bool Next();

    bool IsNULL(int index);

    bool GetString(uint32_t index, std::string* str);

    bool GetBool(uint32_t index, bool* result);

    bool GetChar(uint32_t index, char* result);

    bool GetInt16(uint32_t index, int16_t* result);

    bool GetInt32(uint32_t index, int32_t* result);

    bool GetInt64(uint32_t index, int64_t* result);

    bool GetFloat(uint32_t index, float* result);

    bool GetDouble(uint32_t index, double* result);

    bool GetDate(uint32_t index, int32_t* date);

    bool GetDate(uint32_t index, int32_t* year, int32_t* month,
                         int32_t* day);

    bool GetTime(uint32_t index, int64_t* mills);

    inline const ::fesql::sdk::Schema* GetSchema() { return &schema_; }

    inline int32_t Size() { return response_->count(); }

 private:
    inline uint32_t GetRecordSize() { return response_->count(); }

 private:
    std::unique_ptr<::rtidb::api::QueryResponse> response_;
    int32_t index_;
    uint32_t byte_size_;
    uint32_t position_;
    std::unique_ptr<::fesql::sdk::RowIOBufView> row_view_;
    ::fesql::vm::Schema internal_schema_;
    ::fesql::sdk::SchemaImpl schema_;
    std::unique_ptr<brpc::Controller> cntl_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_RESULT_SET_SQL_H_
