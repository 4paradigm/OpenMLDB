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

// make sure buf is not NULL    
RowBuilder::RowBuilder(const Schema& schema, 
                       int8_t* buf, uint32_t size):
                       schema_(schema), buf_(buf), 
                       size_(size), offset_(0), 
                       str_addr_length_(0), str_start_offset_(0), str_offset_(0) {
    *buf_ = 1; //FVersion
    *(buf_ + 1) = 1; //SVersion
    if (size <= UINT8_MAX) {
        str_addr_length_ = 1;
    } else if (size <= UINT16_MAX) {
        str_addr_length_ = 2;
    } else if (size <= 1 << 24) {
        str_addr_length_ = 3;
    } else {
        str_addr_length_ = 4;
    }
    *(buf_ + 2) = str_addr_length_;
    offset_ = 3;
    str_start_offset_ = offset_;
    for (int idx = 0; idx < schema.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema.Get(idx);
        switch (column.type()) {
            case ::fesql::type::kBool:
                str_start_offset_ += 1;
                break;
            case ::fesql::type::kInt16:
                str_start_offset_ += 2;
                break;
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                str_start_offset_ += 4;
                break;
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
                str_start_offset_ += 8;
                break;
            case ::fesql::type::kString:
                str_start_offset_ += str_addr_length_;
                break;
            default:
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
        }       
    }
    str_offset_ = str_start_offset_;
}

uint32_t RowBuilder::CalTotalLength(const Schema& schema, uint32_t string_length) {
    if (schema.size() == 0) {
        return 0;
    }
    uint32_t total_length = 2 + 1;
    uint32_t string_filed_cnt = 0;
    for (int idx = 0; idx < schema.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema.Get(idx);
        switch (column.type()) {
            case ::fesql::type::kBool:
                total_length += 1;
                break;
            case ::fesql::type::kInt16:
                total_length += 2;
                break;
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                total_length += 4;
                break;
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
                total_length += 8;
                break;
            case ::fesql::type::kString:
                string_filed_cnt++;
                break;
            default:
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
                return 0;
        }       
    }
    total_length += string_length;
    if (total_length + string_filed_cnt <= UINT8_MAX) {
        return total_length + string_filed_cnt;
    } else if (total_length + string_filed_cnt * 2 <= UINT16_MAX) {
        return total_length + string_filed_cnt * 2;
    } else if (total_length + string_filed_cnt * 3 <= 1 << 24) {
        return total_length + string_filed_cnt * 3;
    } else {
        return total_length + string_filed_cnt * 4;
    }
    return 0;
}

bool RowBuilder::Check(uint32_t delta) {
    if (offset_ + delta <= str_start_offset_ && str_offset_ <= size_) return true;
    return false;
}

bool RowBuilder::AppendBool(bool val) {
    if (!Check(1)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int8_t*)ptr) = val ? 1 : 0;
    offset_ += 1;
    return true;
}

bool RowBuilder::AppendInt32(int32_t val) {
    if (!Check(4)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int32_t*)ptr) = val;
    offset_ += 4;
    return true;
}

bool RowBuilder::AppendInt16(int16_t val) {
    if (!Check(2))  return false;
    int8_t* ptr = buf_ + offset_;
    (*(int16_t*)ptr) = val;
    offset_ += 2;
    return true;
}

bool RowBuilder::AppendInt64(int64_t val) {
    if (!Check(8)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int64_t*)ptr) = val;
    offset_ += 8;
    return true;
}

bool RowBuilder::AppendFloat(float val) {
    if (!Check(4)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(float*)ptr) = val;
    offset_ += 4;
    return true;
}

bool RowBuilder::AppendDouble(double val) {
    if (!Check(8)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(double*)ptr) = val;
    offset_ += 8;
    return true;
}

bool RowBuilder::AppendString(const char* val, uint32_t length) {
    if (val == NULL || length == 0 || !Check(str_addr_length_)) return false;
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
    memcpy((char*)(buf_ + str_offset_), val, length);
    offset_ += str_addr_length_;
    str_offset_ += length;
    return true;
}

RowView::RowView(const Schema& schema,
                 const int8_t* row,
                 uint32_t size):
        str_addr_length_(0), is_valid_(true), size_(size), row_(row), schema_(schema),  
        offset_vec_(), str_length_map_() {
    if (schema_.size() == 0 || row_ == NULL || size <= 3) {
        is_valid_ = false;
        return;
    }
    uint32_t offset = 2;
    str_addr_length_ = (uint8_t)(*(row + offset));
    offset++;
    for (int idx = 0; idx < schema_.size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema_.Get(idx);
        switch (column.type()) {
            case ::fesql::type::kBool:
                offset_vec_.push_back(offset);
                offset += 1;
                break;
            case ::fesql::type::kInt16: {
                offset_vec_.push_back(offset);
                offset += 2;
                break;
            }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat: {
                offset_vec_.push_back(offset);
                offset += 4;
                break;
            }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble: {
                offset_vec_.push_back(offset);
                offset += 8;
                break;
            }
            case ::fesql::type::kString:
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
                break;
            default: {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
                is_valid_ = false;
                return;
            }
        }
    }
    if (!str_length_map_.empty()) {
        uint32_t last_offset = size_;
        for (auto iter = str_length_map_.rbegin(); iter != str_length_map_.rend(); iter++) {
            if (iter->second < last_offset) {
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

bool RowView::GetBool(uint32_t idx, bool* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kBool)) {
        return false;
    }    
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    uint8_t value = *((uint8_t*)ptr);
    value == 1 ? *val = true : *val = false;
    return true;
}

bool RowView::GetInt32(uint32_t idx, int32_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kInt32)) {
        return false;
    }    
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = *((const int32_t*)ptr);
    return true;
}

bool RowView::GetInt64(uint32_t idx, int64_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kInt64)) {
        return false;
    }
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = *((const int64_t*)ptr);
    return true;
}

bool RowView::GetInt16(uint32_t idx, int16_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kInt16)) {
        return false;
    }
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = *((const int16_t*)ptr);
    return true;
}

bool RowView::GetFloat(uint32_t idx, float* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kFloat)) {
        return false;
    }
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = *((const float*)ptr);
    return true;
}

bool RowView::GetDouble(uint32_t idx, double* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kDouble)) {
        return false;
    }
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = *((const double*)ptr);
    return true;
}

bool RowView::GetString(uint32_t idx, char** val, uint32_t* length) {
    if (val == NULL || length == 0) {
        LOG(WARNING) << "output val is null or lenght is zero";
        return false;
    }
    if (!CheckValid(idx, ::fesql::type::kString)) {
        return false;
    }
    uint32_t offset = offset_vec_.at(idx);
    const int8_t* ptr = row_ + offset;
    *val = (char*)ptr;
    auto iter = str_length_map_.find(idx);
    if (iter == str_length_map_.end()) {
        LOG(WARNING) << "not found idx" << idx << "in str_length_map";
        return false;
    }
    *length = iter->second;
    return true;
}

}  // namespace storage
}  // namespace fesql

