/*
 * codec.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "base/codec.h"
#include <unordered_map>
#include "logging.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace base {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))

static const std::unordered_map<::rtidb::type::DataType, uint8_t> TYPE_SIZE_MAP = {
    {::rtidb::type::kBool, sizeof(bool)},
    {::rtidb::type::kInt16, sizeof(int16_t)},
    {::rtidb::type::kInt32, sizeof(int32_t)},
    {::rtidb::type::kFloat, sizeof(float)},
    {::rtidb::type::kInt64, sizeof(int64_t)},
    {::rtidb::type::kTimestamp, sizeof(int64_t)},
    {::rtidb::type::kDouble, sizeof(double)}};

static inline uint8_t GetAddrLength(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= UINT24_MAX) {
        return 3;
    } else {
        return 4;
    }
}

RowBuilder::RowBuilder(const Schema& schema)
    : schema_(schema),
      buf_(NULL),
      cnt_(0),
      size_(0),
      str_field_cnt_(0),
      str_addr_length_(0),
      str_field_start_offset_(0),
      str_offset_(0) {
    str_field_start_offset_ = HEADER_LENGTH + BitMapSize(schema.size());
    for (int idx = 0; idx < schema.size(); idx++) {
        const ::rtidb::common::ColumnDesc& column = schema.Get(idx);
        if (column.data_type() == ::rtidb::type::kVarchar) {
            offset_vec_.push_back(str_field_cnt_);
            str_field_cnt_++;
        } else {
            auto iter = TYPE_SIZE_MAP.find(column.data_type());
            if (iter == TYPE_SIZE_MAP.end()) {
                PDLOG(WARNING, "type is not supported");
            } else {
                offset_vec_.push_back(str_field_start_offset_);
                str_field_start_offset_ += iter->second;
            }
        }
    }
}

bool RowBuilder::SetBuffer(int8_t* buf, uint32_t size) {
    if (buf == NULL || size == 0 ||
        size < str_field_start_offset_ + str_field_cnt_) {
        return false;
    }
    buf_ = buf;
    size_ = size;
    *(buf_) = 1;      // FVersion
    *(buf_ + 1) = 1;  // SVersion
    *(reinterpret_cast<uint32_t*>(buf_ + VERSION_LENGTH)) = size;
    uint32_t bitmap_size = BitMapSize(schema_.size());
    memset(buf_ + HEADER_LENGTH, 0, bitmap_size);
    cnt_ = 0;
    str_addr_length_ = GetAddrLength(size);
    str_offset_ = str_field_start_offset_ + str_addr_length_ * str_field_cnt_;
    return true;
}

uint32_t RowBuilder::CalTotalLength(uint32_t string_length) {
    if (schema_.size() == 0) {
        return 0;
    }
    uint32_t total_length = str_field_start_offset_;
    total_length += string_length;
    if (total_length + str_field_cnt_ <= UINT8_MAX) {
        return total_length + str_field_cnt_;
    } else if (total_length + str_field_cnt_ * 2 <= UINT16_MAX) {
        return total_length + str_field_cnt_ * 2;
    } else if (total_length + str_field_cnt_ * 3 <= UINT24_MAX) {
        return total_length + str_field_cnt_ * 3;
    } else if (total_length + str_field_cnt_ * 4 <= UINT32_MAX) {
        return total_length + str_field_cnt_ * 4;
    }
    return 0;
}

bool RowBuilder::Check(::rtidb::type::DataType type) {
    if ((int32_t)cnt_ >= schema_.size()) {
        return false;
    }
    const ::rtidb::common::ColumnDesc& column = schema_.Get(cnt_);
    if (column.data_type() != type) {
        return false;
    }
    if (column.data_type() != ::rtidb::type::kVarchar) {
        auto iter = TYPE_SIZE_MAP.find(column.data_type());
        if (iter == TYPE_SIZE_MAP.end()) {
            return false;
        }
    }
    return true;
}

bool RowBuilder::AppendNULL() {
    int8_t* ptr = buf_ + HEADER_LENGTH + (cnt_ >> 3);
    *(reinterpret_cast<uint8_t*>(ptr)) |= 1 << (cnt_ & 0x07);
    const ::rtidb::common::ColumnDesc& column = schema_.Get(cnt_);
    if (column.data_type() == ::rtidb::type::kVarchar) {
        ptr = buf_ + str_field_start_offset_ +
              str_addr_length_ * offset_vec_[cnt_];
        if (str_addr_length_ == 1) {
            *(reinterpret_cast<uint8_t*>(ptr)) = (uint8_t)str_offset_;
        } else if (str_addr_length_ == 2) {
            *(reinterpret_cast<uint16_t*>(ptr)) = (uint16_t)str_offset_;
        } else if (str_addr_length_ == 3) {
            *(reinterpret_cast<uint8_t*>(ptr)) = str_offset_ >> 16;
            *(reinterpret_cast<uint8_t*>(ptr + 1)) =
                (str_offset_ & 0xFF00) >> 8;
            *(reinterpret_cast<uint8_t*>(ptr + 2)) = str_offset_ & 0x00FF;
        } else {
            *(reinterpret_cast<uint32_t*>(ptr)) = str_offset_;
        }
    }
    cnt_++;
    return true;
}

bool RowBuilder::AppendBool(bool val) {
    if (!Check(::rtidb::type::kBool)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<uint8_t*>(ptr)) = val ? 1 : 0;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt32(int32_t val) {
    if (!Check(::rtidb::type::kInt32)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt16(int16_t val) {
    if (!Check(::rtidb::type::kInt16)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendTimestamp(int64_t val) {
    if (!Check(::rtidb::type::kTimestamp)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt64(int64_t val) {
    if (!Check(::rtidb::type::kInt64)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendFloat(float val) {
    if (!Check(::rtidb::type::kFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendDouble(double val) {
    if (!Check(::rtidb::type::kDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendString(const char* val, uint32_t length) {
    if (val == NULL || !Check(::rtidb::type::kVarchar)) return false;
    if (str_offset_ + length > size_) return false;
    int8_t* ptr =
        buf_ + str_field_start_offset_ + str_addr_length_ * offset_vec_[cnt_];
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
        memcpy(reinterpret_cast<char*>(buf_ + str_offset_), val, length);
    }
    str_offset_ += length;
    cnt_++;
    return true;
}

RowView::RowView(const Schema& schema)
    : str_addr_length_(0),
      is_valid_(true),
      string_field_cnt_(0),
      str_field_start_offset_(0),
      size_(0),
      row_(NULL),
      schema_(schema),
      offset_vec_() {
    Init();
}

RowView::RowView(const Schema& schema, const int8_t* row, uint32_t size)
    : str_addr_length_(0),
      is_valid_(true),
      string_field_cnt_(0),
      str_field_start_offset_(0),
      size_(size),
      row_(row),
      schema_(schema),
      offset_vec_() {
    if (schema_.size() == 0) {
        is_valid_ = false;
        return;
    }
    if (Init()) {
        Reset(row, size);
    }
}

bool RowView::Init() {
    uint32_t offset = HEADER_LENGTH + BitMapSize(schema_.size());
    for (int idx = 0; idx < schema_.size(); idx++) {
        const ::rtidb::common::ColumnDesc& column = schema_.Get(idx);
        if (column.data_type() == ::rtidb::type::kVarchar) {
            offset_vec_.push_back(string_field_cnt_);
            string_field_cnt_++;
        } else {
            auto iter = TYPE_SIZE_MAP.find(column.data_type());
            if (iter == TYPE_SIZE_MAP.end()) {
                is_valid_ = false;
                return false;
            } else {
                offset_vec_.push_back(offset);
                offset += iter->second;
            }
        }
    }
    str_field_start_offset_ = offset;
    return true;
}

bool RowView::Reset(const int8_t* row, uint32_t size) {
    if (schema_.size() == 0 || row == NULL || size <= HEADER_LENGTH ||
        *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH)) != size) {
        is_valid_ = false;
        return false;
    }
    row_ = row;
    size_ = size;
    str_addr_length_ = GetAddrLength(size_);
    return true;
}

bool RowView::Reset(const int8_t* row) {
    if (schema_.size() == 0 || row == NULL) {
        is_valid_ = false;
        return false;
    }
    row_ = row;
    size_ = *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH));
    if (size_ <= HEADER_LENGTH) {
        is_valid_ = false;
        return false;
    }
    str_addr_length_ = GetAddrLength(size_);
    return true;
}

bool RowView::CheckValid(uint32_t idx, ::rtidb::type::DataType type) {
    if (row_ == NULL || !is_valid_) {
        return false;
    }
    if ((int32_t)idx >= schema_.size()) {
        return false;
    }
    const ::rtidb::common::ColumnDesc& column = schema_.Get(idx);
    if (column.data_type() != type) {
        return false;
    }
    return true;
}

int32_t RowView::GetBool(uint32_t idx, bool* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kBool)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    int8_t v = v1::GetBoolField(row_, offset);
    if (v == 1) {
        *val = true;
    } else {
        *val = false;
    }
    return 0;
}

int32_t RowView::GetInt32(uint32_t idx, int32_t* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kInt32)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt32Field(row_, offset);
    return 0;
}

int32_t RowView::GetTimestamp(uint32_t idx, int64_t* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kTimestamp)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt64Field(row_, offset);
    return 0;
}

int32_t RowView::GetInt64(uint32_t idx, int64_t* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kInt64)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt64Field(row_, offset);
    return 0;
}

int32_t RowView::GetInt16(uint32_t idx, int16_t* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kInt16)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt16Field(row_, offset);
    return 0;
}

int32_t RowView::GetFloat(uint32_t idx, float* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kFloat)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetFloatField(row_, offset);
    return 0;
}

int32_t RowView::GetDouble(uint32_t idx, double* val) {
    if (val == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::rtidb::type::kDouble)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetDoubleField(row_, offset);
    return 0;
}

int32_t RowView::GetInteger(const int8_t* row, uint32_t idx,
                            ::rtidb::type::DataType type, int64_t* val) {
    int32_t ret = 0;
    switch (type) {
        case ::rtidb::type::kInt16: {
            int16_t tmp_val = 0;
            ret = GetValue(row, idx, type, &tmp_val);
            if (ret == 0) *val = tmp_val;
            break;
        }
        case ::rtidb::type::kInt32: {
            int32_t tmp_val = 0;
            GetValue(row, idx, type, &tmp_val);
            if (ret == 0) *val = tmp_val;
            break;
        }
        case ::rtidb::type::kTimestamp:
        case ::rtidb::type::kInt64: {
            int64_t tmp_val = 0;
            GetValue(row, idx, type, &tmp_val);
            if (ret == 0) *val = tmp_val;
            break;
        }
        default:
            return -1;
    }
    return ret;
}

int32_t RowView::GetValue(const int8_t* row, uint32_t idx,
                          ::rtidb::type::DataType type, void* val) {
    if (schema_.size() == 0 || row == NULL) {
        return -1;
    }
    if ((int32_t)idx >= schema_.size()) {
        return -1;
    }
    const ::rtidb::common::ColumnDesc& column = schema_.Get(idx);
    if (column.data_type() != type) {
        return -1;
    }
    if (GetSize(row) <= HEADER_LENGTH) {
        return -1;
    }
    if (IsNULL(row, idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    switch (type) {
        case ::rtidb::type::kBool: {
            int8_t v = v1::GetBoolField(row, offset);
            if (v == 1) {
                *(reinterpret_cast<bool*>(val)) = true;
            } else {
                *(reinterpret_cast<bool*>(val)) = false;
            }
            break;
        }
        case ::rtidb::type::kInt16:
            *(reinterpret_cast<int16_t*>(val)) = v1::GetInt16Field(row, offset);
            break;
        case ::rtidb::type::kInt32:
            *(reinterpret_cast<int32_t*>(val)) = v1::GetInt32Field(row, offset);
            break;
        case ::rtidb::type::kTimestamp:
        case ::rtidb::type::kInt64:
            *(reinterpret_cast<int64_t*>(val)) = v1::GetInt64Field(row, offset);
            break;
        case ::rtidb::type::kFloat:
            *(reinterpret_cast<float*>(val)) = v1::GetFloatField(row, offset);
            break;
        case ::rtidb::type::kDouble:
            *(reinterpret_cast<double*>(val)) = v1::GetDoubleField(row, offset);
            break;
        default:
            return -1;
    }
    return 0;
}

int32_t RowView::GetValue(const int8_t* row, uint32_t idx, char** val,
                          uint32_t* length) {
    if (schema_.size() == 0 || row == NULL || length == NULL) {
        return -1;
    }
    if ((int32_t)idx >= schema_.size()) {
        return false;
    }
    const ::rtidb::common::ColumnDesc& column = schema_.Get(idx);
    if (column.data_type() != ::rtidb::type::kVarchar) {
        return false;
    }
    uint32_t size = GetSize(row);
    if (size <= HEADER_LENGTH) {
        return -1;
    }
    if (IsNULL(row, idx)) {
        return 1;
    }
    uint32_t field_offset = offset_vec_.at(idx);
    uint32_t next_str_field_offset = 0;
    if (offset_vec_.at(idx) < string_field_cnt_ - 1) {
        next_str_field_offset = field_offset + 1;
    }
    return v1::GetStrField(row, field_offset, next_str_field_offset,
                           str_field_start_offset_, GetAddrLength(size),
                           reinterpret_cast<int8_t**>(val), length);
}

int32_t RowView::GetString(uint32_t idx, char** val, uint32_t* length) {
    if (val == NULL || length == NULL) {
        return -1;
    }

    if (!CheckValid(idx, ::rtidb::type::kVarchar)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    uint32_t field_offset = offset_vec_.at(idx);
    uint32_t next_str_field_offset = 0;
    if (offset_vec_.at(idx) < string_field_cnt_ - 1) {
        next_str_field_offset = field_offset + 1;
    }
    return v1::GetStrField(row_, field_offset, next_str_field_offset,
                           str_field_start_offset_, str_addr_length_,
                           reinterpret_cast<int8_t**>(val), length);
}

namespace v1{
int32_t GetStrField(const int8_t* row, uint32_t field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, int8_t** data, uint32_t* size) {
    if (row == NULL || data == NULL || size == NULL) return -1;
    const int8_t* row_with_offset = row + str_start_offset;
    uint32_t str_offset = 0;
    uint32_t next_str_offset = 0;
    switch (addr_space) {
        case 1: {
            str_offset =
                (uint8_t)(*(row_with_offset + field_offset * addr_space));
            if (next_str_field_offset > 0) {
                next_str_offset = (uint8_t)(
                    *(row_with_offset + next_str_field_offset * addr_space));
            }
            break;
        }
        case 2: {
            str_offset = *(reinterpret_cast<const uint16_t*>(
                row_with_offset + field_offset * addr_space));
            if (next_str_field_offset > 0) {
                next_str_offset = *(reinterpret_cast<const uint16_t*>(
                    row_with_offset + next_str_field_offset * addr_space));
            }
            break;
        }
        case 3: {
            const int8_t* cur_row_with_offset =
                row_with_offset + field_offset * addr_space;
            str_offset = (uint8_t)(*cur_row_with_offset);
            str_offset =
                (str_offset << 8) + (uint8_t)(*(cur_row_with_offset + 1));
            str_offset =
                (str_offset << 8) + (uint8_t)(*(cur_row_with_offset + 2));
            if (next_str_field_offset > 0) {
                const int8_t* next_row_with_offset =
                    row_with_offset + next_str_field_offset * addr_space;
                next_str_offset = (uint8_t)(*(next_row_with_offset));
                next_str_offset = (next_str_offset << 8) +
                                  (uint8_t)(*(next_row_with_offset + 1));
                next_str_offset = (next_str_offset << 8) +
                                  (uint8_t)(*(next_row_with_offset + 2));
            }
            break;
        }
        case 4: {
            str_offset = *(reinterpret_cast<const uint32_t*>(
                row_with_offset + field_offset * addr_space));
            if (next_str_field_offset > 0) {
                next_str_offset = *(reinterpret_cast<const uint32_t*>(
                    row_with_offset + next_str_field_offset * addr_space));
            }
            break;
        }
        default: {
            return -2;
        }
    }
    const int8_t* ptr = row + str_offset;
    *data = (int8_t*)(ptr);  // NOLINT
    if (next_str_field_offset <= 0) {
        uint32_t total_length =
            *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH));
        *size = total_length - str_offset;
    } else {
        *size = next_str_offset - str_offset;
    }
    return 0;
}

}  // namespace v1

}  // namespace base
}  // namespace rtidb
