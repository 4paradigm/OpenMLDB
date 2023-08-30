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

#ifndef SRC_SDK_BATCH_REQUEST_RESULT_SET_SQL_H_
#define SRC_SDK_BATCH_REQUEST_RESULT_SET_SQL_H_

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "proto/tablet.pb.h"
#include "sdk/base_impl.h"
#include "sdk/codec_sdk.h"
#include "sdk/result_set.h"

namespace openmldb {
namespace sdk {

class SQLBatchRequestResultSet : public ::hybridse::sdk::ResultSet {
 public:
    SQLBatchRequestResultSet(const std::shared_ptr<::openmldb::api::SQLBatchRequestQueryResponse>& response,
                             const std::shared_ptr<brpc::Controller>& cntl);
    ~SQLBatchRequestResultSet();

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

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day);

    bool GetTime(uint32_t index, int64_t* mills);

    inline const ::hybridse::sdk::Schema* GetSchema() { return &external_schema_; }

    inline int32_t Size() { return response_->count(); }

    int32_t GetDataLength() override { return cntl_->response_attachment().size(); }
    void CopyTo(hybridse::sdk::NIOBUFFER buf) override { cntl_->response_attachment().copy_to(reinterpret_cast<void*>(buf)); }

 private:
    inline uint32_t GetRecordSize() { return response_->count(); }

 private:
    bool IsCommonColumnIdx(size_t index) const;
    bool IsValidColumnIdx(size_t index) const;
    size_t GetCommonColumnNum() const;

    std::shared_ptr<::openmldb::api::SQLBatchRequestQueryResponse> response_;
    int32_t index_;
    uint32_t byte_size_;
    uint32_t position_;

    std::set<size_t> common_column_indices_;
    std::vector<size_t> column_remap_;

    std::unique_ptr<::hybridse::sdk::RowIOBufView> common_row_view_;
    std::unique_ptr<::hybridse::sdk::RowIOBufView> non_common_row_view_;

    ::hybridse::sdk::SchemaImpl external_schema_;
    ::hybridse::codec::Schema common_schema_;
    ::hybridse::codec::Schema non_common_schema_;

    size_t common_buf_size_ = 0;
    butil::IOBuf common_buf_;
    std::shared_ptr<brpc::Controller> cntl_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_BATCH_REQUEST_RESULT_SET_SQL_H_
