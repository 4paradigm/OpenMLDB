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

#include "storage/codec.h"
#include <utility>
#include "glog/logging.h"
#include "storage/type_ir_builder.h"

namespace fesql {
namespace storage {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))

static const uint8_t VERSION_LENGTH = 2;
static const uint8_t SIZE_LENGTH = 4;
static const uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static const std::map<::fesql::type::Type, uint8_t> TYPE_SIZE_MAP = {
    {::fesql::type::kBool, sizeof(bool)},
    {::fesql::type::kInt16, sizeof(int16_t)},
    {::fesql::type::kInt32, sizeof(int32_t)},
    {::fesql::type::kFloat, sizeof(float)},
    {::fesql::type::kInt64, sizeof(int64_t)},
    {::fesql::type::kDouble, sizeof(double)}};

static uint8_t GetAddrLength(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= 1 << 24) {
        return 3;
    } else {
        return 4;
    }
}

// make sure buf is not NULL
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
        const ::fesql::type::ColumnDef& column = schema.Get(idx);
        if (column.type() == ::fesql::type::kVarchar) {
            offset_vec_.push_back(str_field_cnt_);
            str_field_cnt_++;
        } else {
            auto iter = TYPE_SIZE_MAP.find(column.type());
            if (iter == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
            } else {
                offset_vec_.push_back(str_field_start_offset_);
                str_field_start_offset_ += iter->second;
            }
        }
    }
}

bool RowBuilder::SetBuffer(int8_t* buf, uint32_t size) {
    if (size < str_field_start_offset_ + str_field_cnt_) {
        return false;
    }
    buf_ = buf;
    size_ = size;
    *(buf_) = 1;      // FVersion
    *(buf_ + 1) = 1;  // SVersion
    *(reinterpret_cast<uint32_t*>(buf_ + VERSION_LENGTH)) = size;
    uint32_t bitmap_size = BitMapSize(schema_.size());
    memset(buf_ + HEADER_LENGTH, 0, bitmap_size);
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
    } else if (total_length + str_field_cnt_ * 3 <= 1 << 24) {
        return total_length + str_field_cnt_ * 3;
    } else if (total_length + str_field_cnt_ * 4 <= UINT32_MAX) {
        return total_length + str_field_cnt_ * 4;
    }
    return 0;
}

bool RowBuilder::Check(::fesql::type::Type type) {
    if ((int32_t)cnt_ >= schema_.size()) {
        LOG(WARNING) << "idx out of index";
        return false;
    }
    const ::fesql::type::ColumnDef& column = schema_.Get(cnt_);
    if (column.type() != type) {
        LOG(WARNING) << "type mismatch required is "
                     << ::fesql::type::Type_Name(type) << " but is "
                     << fesql::type::Type_Name(column.type());
        return false;
    }
    if (column.type() != ::fesql::type::kVarchar) {
        auto iter = TYPE_SIZE_MAP.find(column.type());
        if (iter == TYPE_SIZE_MAP.end()) {
            LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                         << " is not supported";
            return false;
        }
    }
    return true;
}

bool RowBuilder::AppendNULL() {
    int8_t* ptr = buf_ + HEADER_LENGTH + (cnt_ >> 3);
    *(reinterpret_cast<uint8_t*>(ptr)) |= 1 << (cnt_ & 0x07);
    const ::fesql::type::ColumnDef& column = schema_.Get(cnt_);
    if (column.type() == ::fesql::type::kVarchar) {
        ptr = buf_ + str_field_start_offset_ +
                          str_addr_length_ * offset_vec_[cnt_];
        if (str_addr_length_ == 1) {
            *(reinterpret_cast<uint8_t*>(ptr)) = (uint8_t)str_offset_;
        } else if (str_addr_length_ == 2) {
            *(reinterpret_cast<uint16_t*>(ptr)) = (uint16_t)str_offset_;
        } else if (str_addr_length_ == 3) {
            *(reinterpret_cast<uint8_t*>(ptr)) = str_offset_ & 0x0F00;
            *(reinterpret_cast<uint8_t*>(ptr + 1)) = str_offset_ & 0x00F0;
            *(reinterpret_cast<uint8_t*>(ptr + 2)) = str_offset_ & 0x000F;
        } else {
            *(reinterpret_cast<uint32_t*>(ptr)) = str_offset_;
        }
    }
    cnt_++;
    return true;
}

bool RowBuilder::AppendBool(bool val) {
    if (!Check(::fesql::type::kBool)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<uint8_t*>(ptr)) = val ? 1 : 0;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt32(int32_t val) {
    if (!Check(::fesql::type::kInt32)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt16(int16_t val) {
    if (!Check(::fesql::type::kInt16)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt64(int64_t val) {
    if (!Check(::fesql::type::kInt64)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendFloat(float val) {
    if (!Check(::fesql::type::kFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendDouble(double val) {
    if (!Check(::fesql::type::kDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendString(const char* val, uint32_t length) {
    if (val == NULL || !Check(::fesql::type::kVarchar)) return false;
    if (str_offset_ + length > size_) return false;
    int8_t* ptr =
        buf_ + str_field_start_offset_ + str_addr_length_ * offset_vec_[cnt_];
    if (str_addr_length_ == 1) {
        *(reinterpret_cast<uint8_t*>(ptr)) = (uint8_t)str_offset_;
    } else if (str_addr_length_ == 2) {
        *(reinterpret_cast<uint16_t*>(ptr)) = (uint16_t)str_offset_;
    } else if (str_addr_length_ == 3) {
        *(reinterpret_cast<uint8_t*>(ptr)) = str_offset_ & 0x0F00;
        *(reinterpret_cast<uint8_t*>(ptr + 1)) = str_offset_ & 0x00F0;
        *(reinterpret_cast<uint8_t*>(ptr + 2)) = str_offset_ & 0x000F;
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

RowView::RowView(const Schema& schema, const int8_t* row, uint32_t size)
    : str_addr_length_(0),
      is_valid_(true),
      str_field_start_offset_(0),
      size_(size),
      row_(row),
      schema_(schema),
      offset_vec_() {
    if (schema_.size() == 0) {
        is_valid_ = false;
        return;
    }
    uint32_t offset = HEADER_LENGTH + BitMapSize(schema_.size());
    uint32_t string_field_cnt = 0;
    for (int idx = 0; idx < schema_.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema_.Get(idx);
        if (column.type() == ::fesql::type::kVarchar) {
            offset_vec_.push_back(string_field_cnt);
            next_str_pos_.insert(std::make_pair(idx, idx));
            string_field_cnt++;
        } else {
            auto iter = TYPE_SIZE_MAP.find(column.type());
            if (iter == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
                is_valid_ = false;
                return;
            } else {
                offset_vec_.push_back(offset);
                offset += iter->second;
            }
        }
    }
    uint32_t next_pos = 0;
    for (auto iter = next_str_pos_.rbegin(); iter != next_str_pos_.rend();
         iter++) {
        uint32_t tmp = iter->second;
        iter->second = next_pos;
        next_pos = tmp;
    }
    str_field_start_offset_ = offset;
    Reset(row, size);
}

void RowView::Reset(const int8_t* row, uint32_t size) {
    if (schema_.size() == 0 || row == NULL || size <= HEADER_LENGTH ||
        *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH)) != size) {
        is_valid_ = false;
        return;
    }
    row_ = row;
    size_ = size;
    str_addr_length_ = GetAddrLength(size_);
}

bool RowView::CheckValid(uint32_t idx, ::fesql::type::Type type) {
    if (!is_valid_) {
        LOG(WARNING) << "row is invalid";
        return false;
    }
    if ((int32_t)idx >= schema_.size()) {
        LOG(WARNING) << "idx out of index";
        return false;
    }
    const ::fesql::type::ColumnDef& column = schema_.Get(idx);
    if (column.type() != type) {
        LOG(WARNING) << "type mismatch required is "
                     << ::fesql::type::Type_Name(type) << " but is "
                     << fesql::type::Type_Name(column.type());
        return false;
    }
    return true;
}

bool RowView::IsNULL(uint32_t idx) {
    const int8_t* ptr = row_ + HEADER_LENGTH + (idx >> 3);
    return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
}

int32_t RowView::GetBool(uint32_t idx, bool* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kBool)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    int8_t v = v1::GetBoolField(row_, offset);
    if (v == 1){
        *val = true;
    }else {
        *val =false;
    }
    return 0;
}

int32_t RowView::GetInt32(uint32_t idx, int32_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kInt32)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val =  v1::GetInt32Field(row_, offset);
    return 0;
}

int32_t RowView::GetInt64(uint32_t idx, int64_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kInt64)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt64Field(row_, offset);
    return 0;
}

int32_t RowView::GetInt16(uint32_t idx, int16_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kInt16)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val =  v1::GetInt16Field(row_, offset);
    return 0;
}

int32_t RowView::GetFloat(uint32_t idx, float* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kFloat)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetFloatField(row_, offset);
    return 0;
}

int32_t RowView::GetDouble(uint32_t idx, double* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kDouble)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetDoubleField(row_, offset);
    return 0;
}

int32_t RowView::GetString(uint32_t idx, char** val, uint32_t* length) {

    if (val == NULL || length == NULL) {
        LOG(WARNING) << "output val or length is null";
        return -1;
    }

    if (!CheckValid(idx, ::fesql::type::kVarchar)) {
        return -1;
    }

    if (IsNULL(idx)) {
        return 1;
    }

    uint32_t field_offset =
        str_field_start_offset_ + offset_vec_.at(idx) * str_addr_length_;
    auto iter = next_str_pos_.find(idx);

    if (iter == next_str_pos_.end()) {
        return -1;
    }

    uint32_t next_str_field_offset = 0;
    if (iter->second != 0) {
        next_str_field_offset =
            str_field_start_offset_ +
            offset_vec_.at(iter->second) * str_addr_length_;
        if (next_str_field_offset > size_) {
            return -1;
        }
    }
    return v1::GetStrField(row_, field_offset, 
            next_str_field_offset, str_addr_length_,
            reinterpret_cast<int8_t**>(val), length);
}

}  // namespace storage
}  // namespace fesql
