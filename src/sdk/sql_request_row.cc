/*
 * sql_request_row.cc
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

#include "sdk/sql_request_row.h"

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

SQLRequestRow::SQLRequestRow(std::shared_ptr<fesql::sdk::Schema> schema)
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
      has_error_(false) {
    str_field_start_offset_ =
        SDK_HEADER_LENGTH + BitMapSize(schema->GetColumnCnt());
    for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
        auto type = schema->GetColumnType(idx);
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
    }
}

bool SQLRequestRow::Init(int32_t str_length) {
    if (schema_->GetColumnCnt() == 0) {
        return true;
    }
    str_length_expect_ = str_length;
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

bool SQLRequestRow::Check(fesql::sdk::DataType type) {
    if (buf_ == NULL) {
        LOG(WARNING) << "please init this object";
        return false;
    }
    if ((int32_t)cnt_ >= schema_->GetColumnCnt()) {
        LOG(WARNING) << "idx out of index: " << cnt_
                     << " size=" << schema_->GetColumnCnt();
        return false;
    }
    auto expected_type = schema_->GetColumnType(cnt_);
    if (expected_type != type) {
        LOG(WARNING) << "type mismatch required type "
                     << fesql::sdk::DataTypeName(expected_type)
                     << " but real type " << fesql::sdk::DataTypeName(type);
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

bool SQLRequestRow::AppendBool(bool val) {
    if (!Check(::fesql::sdk::kTypeBool)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<uint8_t*>(ptr)) = val ? 1 : 0;
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendInt32(int32_t val) {
    if (!Check(::fesql::sdk::kTypeInt32)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendInt16(int16_t val) {
    if (!Check(::fesql::sdk::kTypeInt16)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendInt64(int64_t val) {
    if (!Check(::fesql::sdk::kTypeInt64)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendTimestamp(int64_t val) {
    if (!Check(::fesql::sdk::kTypeTimestamp)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}
bool SQLRequestRow::AppendDate(int32_t val) {
    if (!Check(::fesql::sdk::kTypeDate)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    cnt_++;
    return true;
}
bool SQLRequestRow::AppendDate(int32_t year, int32_t month, int32_t day) {
    if (!Check(::fesql::sdk::kTypeDate)) return false;
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
    cnt_++;
    return true;
}
bool SQLRequestRow::AppendFloat(float val) {
    if (!Check(::fesql::sdk::kTypeFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = val;
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendDouble(double val) {
    if (!Check(::fesql::sdk::kTypeDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = val;
    cnt_++;
    return true;
}

bool SQLRequestRow::AppendString(const std::string& val) {
    if (!Check(::fesql::sdk::kTypeString)) return false;
    if (str_offset_ + val.size() > size_) return false;
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
    str_length_current_ += val.size();
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
    return true;
}

bool SQLRequestRow::Build() {
    if (has_error_) return false;
    if (str_length_current_ != str_length_expect_) return false;
    int32_t cnt = cnt_;
    for (; cnt < schema_->GetColumnCnt(); cnt++) {
        bool ok = AppendNULL();
        if (!ok) return false;
    }
    return true;
}

}  // namespace sdk
}  // namespace rtidb
