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

#include "glog/logging.h"

namespace fesql {
namespace storage {

#define BitMapSize(size) (((size) >> 3) + !!((size) & 0x07))

static const uint8_t SIZE_LENGTH = 4;
static const uint8_t VERSION_LENGTH = 2;
static const uint8_t HEADER_LENGTH = SIZE_LENGTH + VERSION_LENGTH;
static const std::map<::fesql::type::Type, uint8_t> TYPE_SIZE_MAP = {
        {::fesql::type::kBool, 1},
        {::fesql::type::kInt16, 2},
        {::fesql::type::kInt32, 4},
        {::fesql::type::kFloat, 4},
        {::fesql::type::kInt64, 8},
        {::fesql::type::kDouble, 8}
};

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
RowBuilder::RowBuilder(const Schema& schema, 
                       int8_t* buf, uint32_t size):
                       schema_(schema), buf_(buf), 
                       cnt_(0), size_(size), offset_(0), 
                       str_addr_length_(0), str_start_offset_(0), str_offset_(0) {
                           
    (*(int32_t*)buf_) = size;                       
    *(buf_ + SIZE_LENGTH) = 1; //FVersion
    *(buf_ + SIZE_LENGTH + 1) = 1; //SVersion
    offset_ = HEADER_LENGTH;
    uint32_t bitmap_size = BitMapSize(schema.size());
    memset(buf_ + offset_, 0, bitmap_size);
    offset_ += bitmap_size; 
    str_start_offset_ = offset_;
    str_addr_length_ = GetAddrLength(size);
    for (int idx = 0; idx < schema.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema.Get(idx);
        if (column.type() == ::fesql::type::kString) {
            str_start_offset_ += str_addr_length_;
        } else {
            auto iter = TYPE_SIZE_MAP.find(column.type());
            if (iter == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
            } else {
                str_start_offset_ += iter->second;
            }
        }
    }
    str_offset_ = str_start_offset_;
}

uint32_t RowBuilder::CalTotalLength(const Schema& schema, uint32_t string_length) {
    if (schema.size() == 0) {
        return 0;
    }
    uint32_t total_length = HEADER_LENGTH + BitMapSize(schema.size());
    uint32_t string_field_cnt = 0;
    for (int idx = 0; idx < schema.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema.Get(idx);
        if (column.type() == ::fesql::type::kString) {
            string_field_cnt++;
        } else {
            auto iter = TYPE_SIZE_MAP.find(column.type());
            if (iter == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
                return 0;
            } else {
               total_length += iter->second;
            }
        }       
    }
    total_length += string_length;
    if (total_length + string_field_cnt <= UINT8_MAX) {
        return total_length + string_field_cnt;
    } else if (total_length + string_field_cnt * 2 <= UINT16_MAX) {
        return total_length + string_field_cnt * 2;
    } else if (total_length + string_field_cnt * 3 <= 1 << 24) {
        return total_length + string_field_cnt * 3;
    } else {
        return total_length + string_field_cnt * 4;
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
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(type)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }
    uint32_t delta = 0;
    if (column.type() == ::fesql::type::kString) {
        delta = str_addr_length_;
    } else {
        auto iter = TYPE_SIZE_MAP.find(column.type());
        if (iter == TYPE_SIZE_MAP.end()) {
            LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                         << " is not supported";
            return false;
        } else {
            delta = iter->second;
        }
    }
    if (offset_ + delta <= str_start_offset_ && str_offset_ <= size_) return true;
    return false;
}

bool RowBuilder::AppendNULL() {
    int8_t* ptr = buf_ + HEADER_LENGTH + (cnt_ >> 3);
    *((uint8_t*)ptr) |= 1 << (cnt_ & 0x07); 
    const ::fesql::type::ColumnDef& column = schema_.Get(cnt_);
    if (column.type() == ::fesql::type::kString) {
        ptr = buf_ + offset_;
        if (str_addr_length_ == 1) {
            (*(uint8_t*)ptr) = (uint8_t)str_offset_;
        } else if (str_addr_length_ == 2) {
            (*(uint16_t*)ptr) = (uint16_t)str_offset_;
        } else if (str_addr_length_ == 3) {
            (*(uint8_t*)ptr) = str_offset_ & 0x0F00;
            (*(uint8_t*)(ptr + 1)) = str_offset_ & 0x00F0;
            (*(uint8_t*)(ptr + 2)) = str_offset_ & 0x000F;
        } else {
            (*(uint32_t*)ptr) = str_offset_;
        }
        offset_ += str_addr_length_;
    } else {
        auto iter = TYPE_SIZE_MAP.find(column.type());
        if (iter == TYPE_SIZE_MAP.end()) {
            LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                         << " is not supported";
            return false;
        } else {
            offset_ += iter->second;
        }
    }
    cnt_++;
    return true;
}

bool RowBuilder::AppendBool(bool val) {
    if (!Check(::fesql::type::kBool)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int8_t*)ptr) = val ? 1 : 0;
    offset_ += 1;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt32(int32_t val) {
    if (!Check(::fesql::type::kInt32)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int32_t*)ptr) = val;
    offset_ += 4;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt16(int16_t val) {
    if (!Check(::fesql::type::kInt16))  return false;
    int8_t* ptr = buf_ + offset_;
    (*(int16_t*)ptr) = val;
    offset_ += 2;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt64(int64_t val) {
    if (!Check(::fesql::type::kInt64)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int64_t*)ptr) = val;
    offset_ += 8;
    cnt_++;
    return true;
}

bool RowBuilder::AppendFloat(float val) {
    if (!Check(::fesql::type::kFloat)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(float*)ptr) = val;
    offset_ += 4;
    cnt_++;
    return true;
}

bool RowBuilder::AppendDouble(double val) {
    if (!Check(::fesql::type::kDouble)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(double*)ptr) = val;
    offset_ += 8;
    cnt_++;
    return true;
}

bool RowBuilder::AppendString(const char* val, uint32_t length) {
    if (val == NULL || !Check(::fesql::type::kString)) return false;
    if (str_offset_ + length > size_) return false;
    int8_t* ptr = buf_ + offset_;
    if (str_addr_length_ == 1) {
        (*(uint8_t*)ptr) = (uint8_t)str_offset_;
    } else if (str_addr_length_ == 2) {
        (*(uint16_t*)ptr) = (uint16_t)str_offset_;
    } else if (str_addr_length_ == 3) {
        (*(uint8_t*)ptr) = str_offset_ & 0x0F00;
        (*(uint8_t*)(ptr + 1)) = str_offset_ & 0x00F0;
        (*(uint8_t*)(ptr + 2)) = str_offset_ & 0x000F;
    } else {
        (*(uint32_t*)ptr) = str_offset_;
    }
    if (length != 0) {
        memcpy((char*)(buf_ + str_offset_), val, length);
    }
    offset_ += str_addr_length_;
    str_offset_ += length;
    cnt_++;
    return true;
}

RowView::RowView(const Schema& schema,
                 const int8_t* row,
                 uint32_t size):
        str_addr_length_(0), is_valid_(true), size_(size), row_(row), schema_(schema),  
        offset_vec_(), str_length_map_() {
    if (schema_.size() == 0 || row_ == NULL || size <= HEADER_LENGTH ||
            *((uint32_t*)row) != size) {
        is_valid_ = false;
        return;
    }
    uint32_t offset = HEADER_LENGTH + BitMapSize(schema_.size());
    str_addr_length_ = GetAddrLength(size);
    for (int idx = 0; idx < schema_.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema_.Get(idx);
        if (column.type() == ::fesql::type::kString) {
            if (str_addr_length_ == 1) {
                uint8_t str_offset = (uint8_t)(*(row + offset));
                offset_vec_.push_back(str_offset);
            } else if (str_addr_length_ == 2) {
                uint16_t str_offset = (uint16_t)(*(row + offset));
                offset_vec_.push_back(str_offset);
            } else if (str_addr_length_ == 3) {
                uint32_t str_offset = (uint8_t)(*(row + offset));
                str_offset = (str_offset << 8) + (uint8_t)(*(row + offset + 1));
                str_offset = (str_offset << 8) + (uint8_t)(*(row + offset + 2));
                offset_vec_.push_back(str_offset);
            } else if (str_addr_length_ == 4) {
                uint32_t str_offset = (uint32_t)(*(row + offset));
                offset_vec_.push_back(str_offset);
            } else {
                is_valid_ = false;
                return;
            }
            str_length_map_.insert(std::make_pair(offset_vec_.size() - 1, offset_vec_.back()));
            offset += str_addr_length_;
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
    if (!str_length_map_.empty()) {
        uint32_t last_offset = size_;
        for (auto iter = str_length_map_.rbegin(); iter != str_length_map_.rend(); iter++) {
            if (iter->second <= last_offset) {
                uint32_t tmp_offset = last_offset;
                last_offset = iter->second;
                iter->second = tmp_offset - iter->second;
            } else {
                is_valid_ = false;
                break;
            }
        }
    }
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
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(type)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }
    return true;
}

bool RowView::IsNULL(uint32_t idx) {
    const int8_t* ptr = row_ + HEADER_LENGTH + (idx >> 3);
    return *((uint8_t*)ptr) & (1 << (idx & 0x07)); 
}

int RowView::GetBool(uint32_t idx, bool* val) {
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
    const int8_t* ptr = row_ + offset;
    uint8_t value = *((uint8_t*)ptr);
    value == 1 ? *val = true : *val = false;
    return 0;
}

int RowView::GetInt32(uint32_t idx, int32_t* val) {
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
    const int8_t* ptr = row_ + offset;
    *val = *((const int32_t*)ptr);
    return 0;
}

int RowView::GetInt64(uint32_t idx, int64_t* val) {
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
    const int8_t* ptr = row_ + offset;
    *val = *((const int64_t*)ptr);
    return 0;
}

int RowView::GetInt16(uint32_t idx, int16_t* val) {
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
    const int8_t* ptr = row_ + offset;
    *val = *((const int16_t*)ptr);
    return 0;
}

int RowView::GetFloat(uint32_t idx, float* val) {
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
    const int8_t* ptr = row_ + offset;
    *val = *((const float*)ptr);
    return 0;
}

int RowView::GetDouble(uint32_t idx, double* val) {
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
    const int8_t* ptr = row_ + offset;
    *val = *((const double*)ptr);
    return 0;
}

int RowView::GetString(uint32_t idx, char** val, uint32_t* length) {
    if (val == NULL || length == 0) {
        LOG(WARNING) << "output val is null or lenght is zero";
        return -1;
    }
    if (!CheckValid(idx, ::fesql::type::kString)) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = (char*)ptr;
    auto iter = str_length_map_.find(idx);
    if (iter == str_length_map_.end()) {
        LOG(WARNING) << "not found idx" << idx << "in str_length_map";
        return -1;
    }
    *length = iter->second;
    return 0;
}

}  // namespace storage
}  // namespace fesql

