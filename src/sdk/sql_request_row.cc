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

#include "sdk/sql_request_row.h"

#include <stdint.h>
#include <string>
#include <unordered_map>

#include "glog/logging.h"
#include "schema/schema_adapter.h"

namespace openmldb {
namespace sdk {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))
static constexpr uint8_t SDK_VERSION_LENGTH = 2;
static constexpr uint8_t SDK_SIZE_LENGTH = 4;
static constexpr uint8_t SDK_HEADER_LENGTH = SDK_VERSION_LENGTH + SDK_SIZE_LENGTH;
static constexpr uint32_t SDK_UINT24_MAX = (1 << 24) - 1;
static const std::unordered_map<::hybridse::sdk::DataType, uint8_t> SDK_TYPE_SIZE_MAP = {
    {::hybridse::sdk::kTypeBool, sizeof(bool)},         {::hybridse::sdk::kTypeInt16, sizeof(int16_t)},
    {::hybridse::sdk::kTypeInt32, sizeof(int32_t)},     {::hybridse::sdk::kTypeDate, sizeof(int32_t)},
    {::hybridse::sdk::kTypeFloat, sizeof(float)},       {::hybridse::sdk::kTypeInt64, sizeof(int64_t)},
    {::hybridse::sdk::kTypeTimestamp, sizeof(int64_t)}, {::hybridse::sdk::kTypeDouble, sizeof(double)}};

static inline uint8_t SDKGetAddrLength(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= SDK_UINT24_MAX) {
        return 3;
    } else {
        return 4;
    }
}

inline uint32_t SDKGetStartOffset(int32_t column_count) { return SDK_HEADER_LENGTH + BitMapSize(column_count); }

SQLRequestRow::SQLRequestRow(std::shared_ptr<hybridse::sdk::Schema> schema, const std::set<std::string>& record_cols)
    : schema_(schema),
      cnt_(0),
      size_(0),
      str_field_cnt_(0),
      str_addr_length_(0),
      str_field_start_offset_(0),
      str_offset_(0),
      offset_vec_(),
      val_(),
      buf_(NULL),
      str_length_expect_(0),
      str_length_current_(0),
      has_error_(false),
      is_ok_(false) {
    str_field_start_offset_ = SDK_HEADER_LENGTH + BitMapSize(schema->GetColumnCnt());
    for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
        auto type = schema->GetColumnType(idx);
        if (type == ::hybridse::sdk::kTypeString) {
            offset_vec_.push_back(str_field_cnt_);
            str_field_cnt_++;
        } else {
            auto iter = SDK_TYPE_SIZE_MAP.find(type);
            if (iter == SDK_TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << hybridse::sdk::DataTypeName(type) << " is not supported";
            } else {
                offset_vec_.push_back(str_field_start_offset_);
                str_field_start_offset_ += iter->second;
            }
        }
        if (!record_cols.empty() && record_cols.find(schema->GetColumnName(idx)) != record_cols.end()) {
            record_cols_.insert(static_cast<uint32_t>(idx));
        }
    }
}

bool SQLRequestRow::Init(int32_t str_length) {
    if (schema_->GetColumnCnt() == 0) {
        return true;
    }
    has_error_ = false;
    is_ok_ = false;
    str_length_expect_ = str_length;
    str_length_current_ = 0;
    uint32_t total_length = str_field_start_offset_;
    total_length += str_length;
    if (total_length + str_field_cnt_ <= UINT8_MAX) {
        total_length += str_field_cnt_;
    } else if (total_length + str_field_cnt_ * 2 <= UINT16_MAX) {
        total_length += str_field_cnt_ * 2;
    } else if (total_length + str_field_cnt_ * 3 <= SDK_UINT24_MAX) {
        total_length += str_field_cnt_ * 3;
    } else if (total_length + str_field_cnt_ * 4 <= UINT32_MAX) {
        total_length += str_field_cnt_ * 4;
    }
    // TODO(wangtaize) limit total length
    val_.resize(total_length);
    buf_ = reinterpret_cast<int8_t*>(&(val_[0]));
    size_ = total_length;
    *(buf_) = 1;      // FVersion
    *(buf_ + 1) = 1;  // SVersion
    *(reinterpret_cast<uint32_t*>(buf_ + SDK_VERSION_LENGTH)) = total_length;
    uint32_t bitmap_size = BitMapSize(schema_->GetColumnCnt());
    memset(buf_ + SDK_HEADER_LENGTH, 0, bitmap_size);
    cnt_ = 0;
    str_addr_length_ = SDKGetAddrLength(total_length);
    str_offset_ = str_field_start_offset_ + str_addr_length_ * str_field_cnt_;
    return true;
}

bool SQLRequestRow::Check(hybridse::sdk::DataType type) {
    if (buf_ == NULL) {
        LOG(WARNING) << "please init this object";
        return false;
    }
    if ((int32_t)cnt_ >= schema_->GetColumnCnt()) {
        LOG(WARNING) << "idx out of index: " << cnt_ << " size=" << schema_->GetColumnCnt();
        return false;
    }
    auto expected_type = schema_->GetColumnType(cnt_);
    if (expected_type != type) {
        LOG(WARNING) << "type mismatch required type " << hybridse::sdk::DataTypeName(expected_type)
                     << " but real type " << hybridse::sdk::DataTypeName(type);
        return false;
    }
    if (type != ::hybridse::sdk::kTypeString) {
        auto iter = SDK_TYPE_SIZE_MAP.find(type);
        if (iter == SDK_TYPE_SIZE_MAP.end()) {
            LOG(WARNING) << hybridse::sdk::DataTypeName(type) << " is not supported";
            return false;
        }
    }
    return true;
}

bool SQLRequestRow::AppendBool(bool val) {
    if (!Check(::hybridse::sdk::kTypeBool)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<uint8_t*>(ptr)) = val ? 1 : 0;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        if (val) {
            record_value_.emplace(schema_->GetColumnName(cnt_), "0");
        } else {
            record_value_.emplace(schema_->GetColumnName(cnt_), "1");
        }
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendInt32(int32_t val) {
    if (!Check(::hybridse::sdk::kTypeInt32)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendInt16(int16_t val) {
    if (!Check(::hybridse::sdk::kTypeInt16)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendInt64(int64_t val) {
    if (!Check(::hybridse::sdk::kTypeInt64)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendTimestamp(int64_t val) {
    if (!Check(::hybridse::sdk::kTypeTimestamp)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}
bool SQLRequestRow::AppendDate(int32_t val) {
    if (!Check(::hybridse::sdk::kTypeDate)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}
bool SQLRequestRow::AppendDate(int32_t year, int32_t month, int32_t day) {
    if (!Check(::hybridse::sdk::kTypeDate)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    int32_t date = 0;
    if (year < 1900 || year > 9999) {
        *(reinterpret_cast<int32_t*>(ptr)) = 0;
        cnt_++;
        return true;
    }
    if (month < 1 || month > 12) {
        *(reinterpret_cast<int32_t*>(ptr)) = 0;
        cnt_++;
        return true;
    }
    if (day < 1 || day > 31) {
        *(reinterpret_cast<int32_t*>(ptr)) = 0;
        cnt_++;
        return true;
    }
    date = (year - 1900) << 16;
    date = date | ((month - 1) << 8);
    date = date | day;
    *(reinterpret_cast<int32_t*>(ptr)) = date;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(date));
    }
    cnt_++;
    return true;
}
bool SQLRequestRow::AppendFloat(float val) {
    if (!Check(::hybridse::sdk::kTypeFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendDouble(double val) {
    if (!Check(::hybridse::sdk::kTypeDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = val;
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), std::to_string(val));
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendString(const std::string& val) { return AppendString(val.data(), val.size()); }

bool SQLRequestRow::AppendString(const char* string_buffer_var_name, uint32_t length) {
    if (!Check(::hybridse::sdk::kTypeString)) return false;
    if (str_offset_ + length > size_) return false;
    int8_t* ptr = buf_ + str_field_start_offset_ + str_addr_length_ * offset_vec_[cnt_];
    if (str_addr_length_ == 1) {
        *(reinterpret_cast<uint8_t*>(ptr)) = (uint8_t)str_offset_;
    } else if (str_addr_length_ == 2) {
        *(reinterpret_cast<uint16_t*>(ptr)) = (uint16_t)str_offset_;
    } else if (str_addr_length_ == 3) {
        *(reinterpret_cast<uint8_t*>(ptr)) = str_offset_ >> 16;
        *(reinterpret_cast<uint8_t*>(ptr + 1)) = (str_offset_ & 0xFF00) >> 8;
        *(reinterpret_cast<uint8_t*>(ptr + 2)) = str_offset_ & 0x00FF;
    } else {
        *(reinterpret_cast<uint32_t*>(ptr)) = str_offset_;
    }
    if (length != 0) {
        memcpy(reinterpret_cast<char*>(buf_ + str_offset_), string_buffer_var_name, length);
    }
    if (record_cols_.find(cnt_) != record_cols_.end()) {
        record_value_.emplace(schema_->GetColumnName(cnt_), string_buffer_var_name);
    }
    str_offset_ += length;
    cnt_++;
    str_length_current_ += length;
    return true;
}

bool SQLRequestRow::AppendNULL() {
    if (schema_->IsColumnNotNull(cnt_)) {
        has_error_ = true;
        return false;
    }
    int8_t* ptr = buf_ + SDK_HEADER_LENGTH + (cnt_ >> 3);
    *(reinterpret_cast<uint8_t*>(ptr)) |= 1 << (cnt_ & 0x07);
    auto type = schema_->GetColumnType(cnt_);
    if (type == ::hybridse::sdk::kTypeString) {
        ptr = buf_ + str_field_start_offset_ + str_addr_length_ * offset_vec_[cnt_];
        if (str_addr_length_ == 1) {
            *(reinterpret_cast<uint8_t*>(ptr)) = (uint8_t)str_offset_;
        } else if (str_addr_length_ == 2) {
            *(reinterpret_cast<uint16_t*>(ptr)) = (uint16_t)str_offset_;
        } else if (str_addr_length_ == 3) {
            *(reinterpret_cast<uint8_t*>(ptr)) = str_offset_ >> 16;
            *(reinterpret_cast<uint8_t*>(ptr + 1)) = (str_offset_ & 0xFF00) >> 8;
            *(reinterpret_cast<uint8_t*>(ptr + 2)) = str_offset_ & 0x00FF;
        } else {
            *(reinterpret_cast<uint32_t*>(ptr)) = str_offset_;
        }
    }
    cnt_++;
    return true;
}

bool SQLRequestRow::Build() {
    if (has_error_) {
        return false;
    }
    if (str_length_current_ != str_length_expect_) {
        LOG(WARNING) << "str_length_current_ != str_length_expect_ " << str_length_current_ << ", "
                     << str_length_expect_;
        return false;
    }
    int32_t cnt = cnt_;
    for (; cnt < schema_->GetColumnCnt(); cnt++) {
        bool ok = AppendNULL();
        if (!ok) return false;
    }
    is_ok_ = true;
    return true;
}

bool SQLRequestRow::GetRecordVal(const std::string& col, std::string* val) {
    if (val == nullptr) {
        return false;
    }
    auto iter = record_value_.find(col);
    if (iter != record_value_.end()) {
        val->assign(iter->second);
        return true;
    }
    return false;
}

std::shared_ptr<SQLRequestRow> SQLRequestRow::CreateSQLRequestRowFromColumnTypes(
    std::shared_ptr<hybridse::sdk::ColumnTypes> types) {
    hybridse::codec::Schema schema;
    for (size_t idx = 0; idx < types->GetTypeSize(); idx++) {
        hybridse::type::Type hybridse_type;
        if (!openmldb::schema::SchemaAdapter::ConvertType(types->GetColumnType(idx), &hybridse_type)) {
            LOG(WARNING) << "fail to create sql request row from column types: invalid type "
                         << hybridse::sdk::DataTypeName(types->GetColumnType(idx));
            return std::shared_ptr<SQLRequestRow>();
        }
        auto column = schema.Add();
        column->set_type(hybridse_type);
    }
    std::shared_ptr<const ::hybridse::sdk::Schema> schema_impl = std::make_shared<hybridse::sdk::SchemaImpl>(schema);
    return std::make_shared<SQLRequestRow>(schema_impl, std::set<std::string>());
}

::hybridse::type::Type ProtoTypeFromDataType(::hybridse::sdk::DataType type) {
    switch (type) {
        case hybridse::sdk::kTypeBool:
            return hybridse::type::kBool;
        case hybridse::sdk::kTypeInt16:
            return hybridse::type::kInt16;
        case hybridse::sdk::kTypeInt32:
            return hybridse::type::kInt32;
        case hybridse::sdk::kTypeInt64:
            return hybridse::type::kInt64;
        case hybridse::sdk::kTypeFloat:
            return hybridse::type::kFloat;
        case hybridse::sdk::kTypeDouble:
            return hybridse::type::kDouble;
        case hybridse::sdk::kTypeString:
            return hybridse::type::kVarchar;
        case hybridse::sdk::kTypeDate:
            return hybridse::type::kDate;
        case hybridse::sdk::kTypeTimestamp:
            return hybridse::type::kTimestamp;
        default:
            return hybridse::type::kNull;
    }
}

void ColumnIndicesSet::AddCommonColumnIdx(size_t idx) {
    if (idx >= bound_) {
        LOG(WARNING) << "Common column index out of bound: " << idx;
        return;
    }
    common_column_indices_.insert(idx);
}

SQLRequestRowBatch::SQLRequestRowBatch(std::shared_ptr<hybridse::sdk::Schema> schema,
                                       std::shared_ptr<ColumnIndicesSet> indices)
    : common_selector_(nullptr), non_common_selector_(nullptr) {
    if (schema == nullptr) {
        LOG(WARNING) << "Null input schema";
        return;
    }
    common_column_indices_ = indices->common_column_indices_;

    std::vector<size_t> common_indices_vec;
    std::vector<size_t> non_common_indices_vec;
    for (int i = 0; i < schema->GetColumnCnt(); ++i) {
        auto col_ref = request_schema_.Add();
        col_ref->set_name(schema->GetColumnName(i));
        col_ref->set_is_not_null(schema->IsColumnNotNull(i));
        col_ref->set_type(ProtoTypeFromDataType(schema->GetColumnType(i)));
        if (common_column_indices_.find(i) != common_column_indices_.end()) {
            common_indices_vec.push_back(i);
        } else {
            non_common_indices_vec.push_back(i);
        }
    }

    if (!common_column_indices_.empty()) {
        common_selector_ = std::unique_ptr<::hybridse::codec::RowSelector>(
            new ::hybridse::codec::RowSelector(&request_schema_, common_indices_vec));
        non_common_selector_ = std::unique_ptr<::hybridse::codec::RowSelector>(
            new ::hybridse::codec::RowSelector(&request_schema_, non_common_indices_vec));
    }
}

bool SQLRequestRowBatch::AddRow(std::shared_ptr<SQLRequestRow> row) {
    if (row == nullptr || !row->OK()) {
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return false;
    }
    const std::string& row_str = row->GetRow();
    int8_t* input_buf = reinterpret_cast<int8_t*>(const_cast<char*>(&row_str[0]));
    size_t input_size = row_str.size();

    // non-common
    if (common_column_indices_.empty() ||
        common_column_indices_.size() == static_cast<size_t>(request_schema_.size())) {
        non_common_slices_.emplace_back(std::string(reinterpret_cast<char*>(input_buf), input_size));
        return true;
    }

    if (non_common_slices_.empty()) {
        int8_t* common_buf = nullptr;
        size_t common_size = 0;
        if (!common_selector_->Select(input_buf, input_size, &common_buf, &common_size)) {
            LOG(WARNING) << "Extract common slice failed";
            return false;
        }
        common_slice_ = std::string(reinterpret_cast<char*>(common_buf), common_size);
        free(common_buf);
    }
    int8_t* non_common_buf = nullptr;
    size_t non_common_size = 0;
    if (!non_common_selector_->Select(input_buf, input_size, &non_common_buf, &non_common_size)) {
        LOG(WARNING) << "Extract non-common slice failed";
        return false;
    }
    non_common_slices_.emplace_back(std::string(reinterpret_cast<char*>(non_common_buf), non_common_size));
    free(non_common_buf);
    return true;
}

}  // namespace sdk
}  // namespace openmldb
