/*
 * sql_insert_row.cc
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

#include "sdk/sql_insert_row.h"

#include <stdint.h>

#include <string>
#include <unordered_map>

#include "base/fe_strings.h"
#include "glog/logging.h"

namespace rtidb {
namespace sdk {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))
static constexpr uint8_t SDK_VERSION_LENGTH = 2;
static constexpr uint8_t SDK_SIZE_LENGTH = 4;
static constexpr uint8_t SDK_HEADER_LENGTH =
    SDK_VERSION_LENGTH + SDK_SIZE_LENGTH;
static constexpr uint32_t SDK_UINT24_MAX = (1 << 24) - 1;
static const std::unordered_map<::fesql::sdk::DataType, uint8_t>
    SDK_TYPE_SIZE_MAP = {{::fesql::sdk::kTypeBool, sizeof(bool)},
                         {::fesql::sdk::kTypeInt16, sizeof(int16_t)},
                         {::fesql::sdk::kTypeInt32, sizeof(int32_t)},
                         {::fesql::sdk::kTypeDate, sizeof(int32_t)},
                         {::fesql::sdk::kTypeFloat, sizeof(float)},
                         {::fesql::sdk::kTypeInt64, sizeof(int64_t)},
                         {::fesql::sdk::kTypeTimestamp, sizeof(int64_t)},
                         {::fesql::sdk::kTypeDouble, sizeof(double)}};

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

inline uint32_t SDKGetStartOffset(int32_t column_count) {
    return SDK_HEADER_LENGTH + BitMapSize(column_count);
}

SQLInsertRows::SQLInsertRows(
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
    std::shared_ptr<std::map<uint32_t, DefaultValue>> default_map,
    uint32_t default_str_length)
    : table_info_(table_info),
      default_map_(default_map),
      default_str_length_(default_str_length) {}

std::shared_ptr<SQLInsertRow> SQLInsertRows::NewRow() {
    if (row_cnt_ != 0 && !rows_.back()->IsComplete()) {
        return std::shared_ptr<SQLInsertRow>();
    }
    row_cnt_++;
    std::shared_ptr<SQLInsertRow> row = std::make_shared<SQLInsertRow>(
        table_info_, default_map_, default_str_length_);
    rows_.push_back(row);
    return row;
}

SQLInsertRow::SQLInsertRow(
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
    std::shared_ptr<std::map<uint32_t, DefaultValue>> default_map,
    uint32_t default_str_length)
    : table_info_(table_info),
      default_map_(default_map),
      cnt_(0),
      size_(0),
      str_field_cnt_(0),
      str_addr_length_(0),
      str_field_start_offset_(0),
      str_offset_(0),
      offset_vec_(),
      val_(),
      buf_(NULL) {
    str_field_start_offset_ = SDK_HEADER_LENGTH +
                              BitMapSize(table_info_->column_desc_v1_size()) +
                              default_str_length;
    for (int idx = 0; idx < table_info_->column_desc_v1_size(); idx++) {
        auto type = ConvertType(table_info_->column_desc_v1(idx).data_type());
        if (type == ::fesql::sdk::kTypeString) {
            offset_vec_.push_back(str_field_cnt_);
            str_field_cnt_++;
        } else {
            auto iter = SDK_TYPE_SIZE_MAP.find(type);
            if (iter == SDK_TYPE_SIZE_MAP.end()) {
                LOG(WARNING)
                    << fesql::sdk::DataTypeName(type) << " is not supported";
            } else {
                offset_vec_.push_back(str_field_start_offset_);
                str_field_start_offset_ += iter->second;
            }
        }
        if (table_info_->column_desc_v1(idx).is_ts_col()) {
            ts_set_.insert(idx);
        } else if (table_info_->column_desc_v1(idx).add_ts_idx()) {
            index_set_.insert(idx);
        }
    }
}

bool SQLInsertRow::Init(int str_length) {
    if (table_info_->column_desc_v1_size() == 0) {
        return true;
    }
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
    val_.resize(total_length);
    buf_ = reinterpret_cast<int8_t*>(&(val_[0]));
    size_ = total_length;
    *(buf_) = 1;      // FVersion
    *(buf_ + 1) = 1;  // SVersion
    *(reinterpret_cast<uint32_t*>(buf_ + SDK_VERSION_LENGTH)) = total_length;
    uint32_t bitmap_size = BitMapSize(table_info_->column_desc_v1_size());
    memset(buf_ + SDK_HEADER_LENGTH, 0, bitmap_size);
    cnt_ = 0;
    str_addr_length_ = SDKGetAddrLength(total_length);
    str_offset_ = str_field_start_offset_ + str_addr_length_ * str_field_cnt_;
    MakeDefault();
    return true;
}

bool SQLInsertRow::Check(fesql::sdk::DataType type) {
    if (buf_ == NULL) {
        LOG(WARNING) << "please init this object";
        return false;
    }
    if ((int32_t)cnt_ >= table_info_->column_desc_v1_size()) {
        LOG(WARNING) << "idx out of index: " << cnt_
                     << " size=" << table_info_->column_desc_v1_size();
        return false;
    }
    auto expected_type =
        ConvertType(table_info_->column_desc_v1(cnt_).data_type());
    if (expected_type != type) {
        LOG(WARNING) << "type mismatch required "
                     << fesql::sdk::DataTypeName(expected_type) << " get "
                     << fesql::sdk::DataTypeName(type);
        return false;
    }
    if (type != ::fesql::sdk::kTypeString) {
        auto iter = SDK_TYPE_SIZE_MAP.find(type);
        if (iter == SDK_TYPE_SIZE_MAP.end()) {
            LOG(WARNING) << fesql::sdk::DataTypeName(type)
                         << " is not supported";
            return false;
        }
    }
    return true;
}

bool SQLInsertRow::GetIndex(const std::string& val) {
    auto index_it = index_set_.find(cnt_);
    if (index_it != index_set_.end()) {
        dimensions_.push_back(std::make_pair(val, cnt_));
        return true;
    }
    return false;
}

bool SQLInsertRow::GetTs(uint64_t ts) {
    auto ts_it = ts_set_.find(cnt_);
    if (ts_it != ts_set_.end()) {
        ts_.push_back(ts);
        return true;
    }
    return false;
}

bool SQLInsertRow::MakeDefault() {
    auto it = default_map_->find(cnt_);
    if (it != default_map_->end()) {
        switch (it->second.type) {
            case ::fesql::sdk::kTypeBool:
                if (it->second.value == "true") {
                    return AppendBool(true);
                } else if (it->second.value == "false") {
                    return AppendBool(false);
                } else {
                    return false;
                }
            case ::fesql::sdk::kTypeInt16:
                return AppendInt16(it->second.value);
            case ::fesql::sdk::kTypeInt32:
                return AppendInt32(it->second.value);
            case ::fesql::sdk::kTypeInt64:
                return AppendInt64(it->second.value);
            case ::fesql::sdk::kTypeFloat:
                return AppendFloat(it->second.value);
            case ::fesql::sdk::kTypeDouble:
                return AppendDouble(it->second.value);
            case ::fesql::sdk::kTypeTimestamp:
                return AppendInt64(it->second.value);
            case ::fesql::sdk::kTypeString:
                return AppendString(it->second.value);
            default:
                return false;
        }
    }
    return true;
}

bool SQLInsertRow::AppendBool(bool val) {
    if (!Check(::fesql::sdk::kTypeBool)) return false;
    GetIndex(val ? "true" : "false");
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<uint8_t*>(ptr)) = val ? 1 : 0;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendBool(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeBool)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    if (val == "true") {
        *(reinterpret_cast<uint8_t*>(ptr)) = 1;
        GetIndex("true");
    } else if (val == "false") {
        *(reinterpret_cast<uint8_t*>(ptr)) = 0;
        GetIndex("false");
    } else {
        return false;
    }
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendInt32(int32_t val) {
    if (!Check(::fesql::sdk::kTypeInt32)) return false;
    GetIndex(std::to_string(val));
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendInt32(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeInt32)) return false;
    GetIndex(val);
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = boost::lexical_cast<int32_t>(val);
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendInt16(int16_t val) {
    if (!Check(::fesql::sdk::kTypeInt16)) return false;
    GetIndex(std::to_string(val));
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = val;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendInt16(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeInt16)) return false;
    GetIndex(val);
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = boost::lexical_cast<int16_t>(val);
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendInt64(int64_t val) {
    if (!Check(::fesql::sdk::kTypeInt64)) return false;
    GetIndex(std::to_string(val));
    GetTs(val);
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendInt64(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeInt64)) return false;
    GetIndex(val);
    GetTs(boost::lexical_cast<int64_t>(val));
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = boost::lexical_cast<int64_t>(val);
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendTimestamp(int64_t val) {
    if (!Check(::fesql::sdk::kTypeTimestamp)) return false;
    GetIndex(std::to_string(val));
    GetTs(val);
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendTimestamp(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeTimestamp)) return false;
    GetIndex(val);
    GetTs(boost::lexical_cast<int64_t>(val));
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = boost::lexical_cast<int64_t>(val);
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendFloat(float val) {
    if (!Check(::fesql::sdk::kTypeFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = val;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendFloat(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = boost::lexical_cast<float>(val);
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendDouble(double val) {
    if (!Check(::fesql::sdk::kTypeDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = val;
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendDouble(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = boost::lexical_cast<double>(val);
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendString(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeString)) return false;
    if (str_offset_ + val.size() > size_) return false;
    GetIndex(val);
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
    if (val.size() != 0) {
        memcpy(reinterpret_cast<char*>(buf_ + str_offset_), val.c_str(),
               val.size());
    }
    str_offset_ += val.size();
    cnt_++;
    return MakeDefault();
}

bool SQLInsertRow::AppendNULL() {
    // todo: deal with null index and ts
    if (GetIndex("")) return false;
    if (GetTs(0)) return false;
    int8_t* ptr = buf_ + SDK_HEADER_LENGTH + (cnt_ >> 3);
    *(reinterpret_cast<uint8_t*>(ptr)) |= 1 << (cnt_ & 0x07);
    auto type = ConvertType(table_info_->column_desc_v1(cnt_).data_type());
    if (type == ::fesql::sdk::kTypeString) {
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
    return MakeDefault();
}

bool SQLInsertRow::Build() {
    int32_t cnt = cnt_;
    for (; cnt < table_info_->column_desc_v1_size(); cnt++) {
        bool ok = AppendNULL();
        if (!ok) return false;
    }
    return true;
}

bool SQLInsertRow::IsComplete() {
    return cnt_ == (uint32_t)table_info_->column_desc_v1_size();
}

}  // namespace sdk
}  // namespace rtidb
