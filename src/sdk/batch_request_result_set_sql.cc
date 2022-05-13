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

#include "sdk/batch_request_result_set_sql.h"

#include <memory>
#include <string>
#include <utility>

#include "base/status.h"
#include "codec/fe_schema_codec.h"
#include "glog/logging.h"

namespace openmldb {
namespace sdk {

static const std::string EMPTY_STR;  // NOLINT
SQLBatchRequestResultSet::SQLBatchRequestResultSet(
    std::shared_ptr<const ::openmldb::api::SQLBatchRequestQueryResponse>& response,
    std::shared_ptr<const brpc::Controller>& cntl)
    : response_(response),
      index_(-1),
      byte_size_(0),
      position_(0),
      common_row_view_(),
      non_common_row_view_(),
      external_schema_(),
      cntl_(cntl) {}

SQLBatchRequestResultSet::~SQLBatchRequestResultSet() {}

bool SQLBatchRequestResultSet::Init() {
    if (!response_ || response_->code() != ::openmldb::base::kOk) {
        LOG(WARNING) << "bad response code " << response_->code();
        return false;
    }

    // Get all buffer byte size
    byte_size_ = 0;
    for (auto row_size : response_->row_sizes()) {
        if (row_size < 0) {
            LOG(WARNING) << "illegal row size field";
            return false;
        }
        byte_size_ += row_size;
    }
    DLOG(INFO) << "byte size " << byte_size_ << " count " << response_->count();

    // Decode schema
    ::hybridse::codec::Schema schema;
    ::hybridse::codec::SchemaCodec::Decode(response_->schema(), &schema);
    external_schema_.SetSchema(schema);

    if (byte_size_ <= 0) return true;

    for (int i = 0; i < response_->common_column_indices().size(); ++i) {
        common_column_indices_.insert(response_->common_column_indices().Get(i));
    }
    column_remap_.resize(schema.size());
    for (int i = 0; i < schema.size(); ++i) {
        auto iter = common_column_indices_.find(i);
        if (iter != common_column_indices_.end()) {
            column_remap_[i] = common_schema_.size();
            *common_schema_.Add() = schema.Get(i);
        } else {
            column_remap_[i] = non_common_schema_.size();
            *non_common_schema_.Add() = schema.Get(i);
        }
    }

    common_row_view_ =
        std::unique_ptr<::hybridse::sdk::RowIOBufView>(new ::hybridse::sdk::RowIOBufView(common_schema_));
    non_common_row_view_ =
        std::unique_ptr<::hybridse::sdk::RowIOBufView>(new ::hybridse::sdk::RowIOBufView(non_common_schema_));

    if (!common_schema_.empty()) {
        uint32_t row_size = 0;
        cntl_->response_attachment().copy_to(&row_size, 4, 2);
        common_buf_size_ = row_size;
        position_ = row_size;
        cntl_->response_attachment().append_to(&common_buf_, row_size, 0);
        common_row_view_->Reset(common_buf_);
    }
    return true;
}

bool SQLBatchRequestResultSet::IsNULL(int index) {
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    if (IsCommonColumnIdx(index)) {
        return common_row_view_->IsNULL(mapped_index);
    } else {
        return non_common_row_view_->IsNULL(mapped_index);
    }
}

bool SQLBatchRequestResultSet::Next() {
    index_++;
    if (index_ < static_cast<int32_t>(response_->count()) && position_ < byte_size_) {
        if (non_common_schema_.empty()) {
            return true;
        }
        // get row size
        uint32_t row_size = 0;
        cntl_->response_attachment().copy_to(&row_size, 4, position_ + 2);
        DLOG(INFO) << "row size " << row_size << " position " << position_ << " byte size " << byte_size_;
        butil::IOBuf tmp;
        cntl_->response_attachment().append_to(&tmp, row_size, position_);
        position_ += row_size;
        bool ok = non_common_row_view_->Reset(tmp);
        if (!ok) {
            LOG(WARNING) << "reset row buf failed";
            return false;
        }
        return true;
    }
    return false;
}

bool SQLBatchRequestResultSet::Reset() {
    index_ = -1;
    position_ = common_buf_size_;
    return true;
}

bool SQLBatchRequestResultSet::IsCommonColumnIdx(size_t index) const {
    return common_column_indices_.find(index) != common_column_indices_.end();
}

size_t SQLBatchRequestResultSet::GetCommonColumnNum() const { return common_schema_.size(); }

bool SQLBatchRequestResultSet::IsValidColumnIdx(size_t index) const { return index < column_remap_.size(); }

bool SQLBatchRequestResultSet::GetString(uint32_t index, std::string* str) {
    if (str == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    butil::IOBuf tmp;
    int32_t ret = -1;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetString(mapped_index, &tmp);
    } else {
        ret = non_common_row_view_->GetString(mapped_index, &tmp);
    }
    if (ret == 0) {
        DLOG(INFO) << "get str size " << tmp.size();
        tmp.append_to(str, tmp.size(), 0);
        return true;
    }
    DLOG(INFO) << "fail to get string with ret " << ret;
    return false;
}

bool SQLBatchRequestResultSet::GetBool(uint32_t index, bool* val) {
    if (val == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetBool(mapped_index, val);
    } else {
        ret = non_common_row_view_->GetBool(mapped_index, val);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetChar(uint32_t index, char* result) { return false; }

bool SQLBatchRequestResultSet::GetInt16(uint32_t index, int16_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetInt16(mapped_index, result);
    } else {
        ret = non_common_row_view_->GetInt16(mapped_index, result);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetInt32(uint32_t index, int32_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetInt32(mapped_index, result);
    } else {
        ret = non_common_row_view_->GetInt32(mapped_index, result);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetInt64(uint32_t index, int64_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetInt64(mapped_index, result);
    } else {
        ret = non_common_row_view_->GetInt64(mapped_index, result);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetFloat(uint32_t index, float* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetFloat(mapped_index, result);
    } else {
        ret = non_common_row_view_->GetFloat(mapped_index, result);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetDouble(uint32_t index, double* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetDouble(mapped_index, result);
    } else {
        ret = non_common_row_view_->GetDouble(mapped_index, result);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetDate(uint32_t index, int32_t* date) {
    if (date == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetDate(mapped_index, date);
    } else {
        ret = non_common_row_view_->GetDate(mapped_index, date);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day) {
    if (day == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetDate(mapped_index, year, month, day);
    } else {
        ret = non_common_row_view_->GetDate(mapped_index, year, month, day);
    }
    return ret == 0;
}

bool SQLBatchRequestResultSet::GetTime(uint32_t index, int64_t* mills) {
    if (mills == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    if (!IsValidColumnIdx(index)) {
        LOG(WARNING) << "column idx out of bound " << index;
        return false;
    }
    size_t mapped_index = column_remap_[index];
    int32_t ret;
    if (IsCommonColumnIdx(index)) {
        ret = common_row_view_->GetTimestamp(mapped_index, mills);
    } else {
        ret = non_common_row_view_->GetTimestamp(mapped_index, mills);
    }
    return ret == 0;
}

}  // namespace sdk
}  // namespace openmldb
