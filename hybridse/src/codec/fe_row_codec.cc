/*
 * Copyright 2021 4Paradigm
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

#include "codec/fe_row_codec.h"
#include <string>
#include <utility>
#include "codec/type_codec.h"
#include "glog/logging.h"

DECLARE_bool(enable_spark_unsaferow_format);

namespace hybridse {
namespace codec {

const uint32_t BitMapSize(uint32_t size) {
    if (FLAGS_enable_spark_unsaferow_format) {
        // For UnsafeRow opt, the nullbit set increases by 8 bytes
        return ((size >> 6) + !!(size&0x7f)) * 8;
    } else {
        return (size >> 3) + !!(size&0x07);
    }
}

const std::unordered_map<::hybridse::type::Type, uint8_t>&
    DEFAULT_TYPE_SIZE_MAP = {{::hybridse::type::kBool, sizeof(bool)},
                             {::hybridse::type::kInt16, sizeof(int16_t)},
                             {::hybridse::type::kInt32, sizeof(int32_t)},
                             {::hybridse::type::kFloat, sizeof(float)},
                             {::hybridse::type::kInt64, sizeof(int64_t)},
                             {::hybridse::type::kTimestamp, sizeof(int64_t)},
                             {::hybridse::type::kDate, sizeof(int32_t)},
                             {::hybridse::type::kDouble, sizeof(double)}};

const std::unordered_map<::hybridse::type::Type, uint8_t>&
    SPARK_UNSAFEROW_TYPE_SIZE_MAP = {
        {::hybridse::type::kBool, 8},  {::hybridse::type::kInt16, 8},
        {::hybridse::type::kInt32, 8}, {::hybridse::type::kFloat, 8},
        {::hybridse::type::kInt64, 8}, {::hybridse::type::kTimestamp, 8},
        {::hybridse::type::kDate, 8},  {::hybridse::type::kDouble, 8}};

const std::unordered_map<::hybridse::type::Type, uint8_t>& GetTypeSizeMap() {
    if (FLAGS_enable_spark_unsaferow_format) {
        return SPARK_UNSAFEROW_TYPE_SIZE_MAP;
    } else {
        return DEFAULT_TYPE_SIZE_MAP;
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
        const ::hybridse::type::ColumnDef& column = schema.Get(idx);
        if (column.type() == ::hybridse::type::kVarchar) {
            if (FLAGS_enable_spark_unsaferow_format) {
                offset_vec_.push_back(str_field_start_offset_);
                str_field_start_offset_ += 8;
            } else {
                offset_vec_.push_back(str_field_cnt_);
            }
            str_field_cnt_++;
        } else {
            auto TYPE_SIZE_MAP = GetTypeSizeMap();
            auto iter = TYPE_SIZE_MAP.find(column.type());
            if (iter == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::hybridse::type::Type_Name(column.type())
                             << " is not supported";
            } else {
                offset_vec_.push_back(str_field_start_offset_);
                str_field_start_offset_ += iter->second;
            }
        }
    }
}

bool RowBuilder::SetBuffer(int64_t buf_handle, uint32_t size) {
    return SetBuffer(reinterpret_cast<int8_t*>(buf_handle), size);
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
    if (FLAGS_enable_spark_unsaferow_format) {
        str_offset_ = str_field_start_offset_;
    } else {
        str_offset_ = str_field_start_offset_ + str_addr_length_ * str_field_cnt_;
    }
    return true;
}

bool RowBuilder::SetBuffer(const hybridse::base::RawBuffer& buf) {
    return this->SetBuffer(reinterpret_cast<int8_t*>(buf.addr), buf.size);
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

bool RowBuilder::Check(::hybridse::type::Type type) {
    if ((int32_t)cnt_ >= schema_.size()) {
        LOG(WARNING) << "idx out of index: " << cnt_
                     << " size=" << schema_.size();
        return false;
    }
    const ::hybridse::type::ColumnDef& column = schema_.Get(cnt_);
    if (column.type() != type) {
        LOG(WARNING) << "type mismatch required is "
                     << ::hybridse::type::Type_Name(type) << " but is "
                     << hybridse::type::Type_Name(column.type());
        return false;
    }
    if (column.type() != ::hybridse::type::kVarchar) {
        auto TYPE_SIZE_MAP = GetTypeSizeMap();
        auto iter = TYPE_SIZE_MAP.find(column.type());
        if (iter == TYPE_SIZE_MAP.end()) {
            LOG(WARNING) << ::hybridse::type::Type_Name(column.type())
                         << " is not supported";
            return false;
        }
    }
    return true;
}

void FillNullStringOffset(int8_t* buf, uint32_t start, uint32_t addr_length,
                          uint32_t str_idx, uint32_t str_offset) {
    if (FLAGS_enable_spark_unsaferow_format) {
        // Do not update row pointer for UnsafeRowOpt
    } else {
        auto ptr = buf + start + addr_length * str_idx;
        if (addr_length == 1) {
            *(reinterpret_cast<uint8_t*>(ptr)) = (uint8_t)str_offset;
        } else if (addr_length == 2) {
            *(reinterpret_cast<uint16_t*>(ptr)) = (uint16_t)str_offset;
        } else if (addr_length == 3) {
            *(reinterpret_cast<uint8_t*>(ptr)) = str_offset >> 16;
            *(reinterpret_cast<uint8_t*>(ptr + 1)) = (str_offset & 0xFF00) >> 8;
            *(reinterpret_cast<uint8_t*>(ptr + 2)) = str_offset & 0x00FF;
        } else {
            *(reinterpret_cast<uint32_t*>(ptr)) = str_offset;
        }
    }


}

bool RowBuilder::AppendNULL() {
    int8_t* ptr = buf_ + HEADER_LENGTH + (cnt_ >> 3);
    *(reinterpret_cast<uint8_t*>(ptr)) |= 1 << (cnt_ & 0x07);

    if (FLAGS_enable_spark_unsaferow_format) {
        // Do not fill null for UnsafeRowOpt
    } else {
        const ::hybridse::type::ColumnDef& column = schema_.Get(cnt_);
        if (column.type() == ::hybridse::type::kVarchar) {
            FillNullStringOffset(buf_, str_field_start_offset_, str_addr_length_,
                                 offset_vec_[cnt_], str_offset_);
        }
    }

    cnt_++;
    return true;
}

bool RowBuilder::AppendBool(bool val) {
    if (!Check(::hybridse::type::kBool)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<uint8_t*>(ptr)) = val ? 1 : 0;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt32(int32_t val) {
    if (!Check(::hybridse::type::kInt32)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt16(int16_t val) {
    if (!Check(::hybridse::type::kInt16)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int16_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendTimestamp(int64_t val) {
    if (!Check(::hybridse::type::kTimestamp)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}
bool RowBuilder::AppendDate(int32_t year, int32_t month, int32_t day) {
    if (year < 1900 || year > 9999) return false;
    if (month < 1 || month > 12) return false;
    if (day < 1 || day > 31) return false;
    if (!Check(::hybridse::type::kDate)) return false;

    int32_t data = (year - 1900) << 16;
    data = data | ((month - 1) << 8);
    data = data | day;
    return AppendDate(data);
}

bool RowBuilder::AppendDate(int32_t date) {
    if (!Check(::hybridse::type::kDate)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int32_t*>(ptr)) = date;
    cnt_++;
    return true;
}

bool RowBuilder::AppendInt64(int64_t val) {
    if (!Check(::hybridse::type::kInt64)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<int64_t*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendFloat(float val) {
    if (!Check(::hybridse::type::kFloat)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<float*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendDouble(double val) {
    if (!Check(::hybridse::type::kDouble)) return false;
    int8_t* ptr = buf_ + offset_vec_[cnt_];
    *(reinterpret_cast<double*>(ptr)) = val;
    cnt_++;
    return true;
}

bool RowBuilder::AppendString(const char* val, uint32_t length) {
    if (val == NULL || !Check(::hybridse::type::kVarchar)) return false;
    if (str_offset_ + length > size_) return false;

    if (FLAGS_enable_spark_unsaferow_format) {
        int8_t* ptr = buf_ + offset_vec_[cnt_];
        *(reinterpret_cast<uint32_t*>(ptr)) = length;
        *(reinterpret_cast<uint32_t*>(ptr + 4)) = str_offset_ - HEADER_LENGTH;
    } else {
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
    }

    if (length != 0) {
        memcpy(reinterpret_cast<char*>(buf_ + str_offset_), val, length);
    }

    str_offset_ += length;
    cnt_++;
    return true;
}

RowView::RowView()
    : str_addr_length_(0),
      is_valid_(false),
      string_field_cnt_(0),
      str_field_start_offset_(0),
      size_(0),
      row_(NULL),
      schema_(),
      offset_vec_() {
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
RowView::RowView(const RowView& copy)
    : str_addr_length_(copy.str_addr_length_),
      is_valid_(copy.is_valid_),
      string_field_cnt_(copy.string_field_cnt_),
      str_field_start_offset_(copy.str_field_start_offset_),
      size_(copy.size_),
      row_(copy.row_),
      schema_(copy.schema_),
      offset_vec_(copy.offset_vec_) {}
bool RowView::Init() {
    uint32_t offset = HEADER_LENGTH + BitMapSize(schema_.size());
    for (int idx = 0; idx < schema_.size(); idx++) {
        const ::hybridse::type::ColumnDef& column = schema_.Get(idx);
        if (column.type() == ::hybridse::type::kVarchar) {
            if (FLAGS_enable_spark_unsaferow_format) {
                offset_vec_.push_back(offset);
                offset += 8;
            } else {
                offset_vec_.push_back(string_field_cnt_);
            }
            string_field_cnt_++;
        } else {
            auto TYPE_SIZE_MAP = GetTypeSizeMap();
            auto iter = TYPE_SIZE_MAP.find(column.type());
            if (iter == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::hybridse::type::Type_Name(column.type())
                             << " is not supported";
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
    is_valid_ = true;
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
    is_valid_ = true;
    return true;
}

bool RowView::Reset(const hybridse::base::RawBuffer& buf) {
    return Reset(reinterpret_cast<int8_t*>(buf.addr), buf.size);
}

bool RowView::CheckValid(uint32_t idx, ::hybridse::type::Type type) {
    if (row_ == NULL || !is_valid_) {
        LOG(WARNING) << "row is invalid";
        return false;
    }
    if ((int32_t)idx >= schema_.size()) {
        LOG(WARNING) << "idx out of index";
        return false;
    }
    const ::hybridse::type::ColumnDef& column = schema_.Get(idx);
    if (column.type() != type) {
        LOG(WARNING) << "type mismatch required is "
                     << ::hybridse::type::Type_Name(type) << " but is "
                     << hybridse::type::Type_Name(column.type());
        return false;
    }
    return true;
}

bool RowView::GetBoolUnsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    int8_t v = v1::GetBoolFieldUnsafe(row_, offset);
    return v == 1 ? true : false;
}

int32_t RowView::GetInt32Unsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return v1::GetInt32FieldUnsafe(row_, offset);
}

int64_t RowView::GetInt64Unsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return v1::GetInt64FieldUnsafe(row_, offset);
}
int32_t RowView::GetDateUnsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return static_cast<int32_t>(v1::GetInt32FieldUnsafe(row_, offset));
}
int64_t RowView::GetTimestampUnsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return v1::GetInt64FieldUnsafe(row_, offset);
}

int16_t RowView::GetInt16Unsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return v1::GetInt16FieldUnsafe(row_, offset);
}

float RowView::GetFloatUnsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return v1::GetFloatFieldUnsafe(row_, offset);
}

double RowView::GetDoubleUnsafe(uint32_t idx) {
    uint32_t offset = offset_vec_.at(idx);
    return v1::GetDoubleFieldUnsafe(row_, offset);
}

std::string RowView::GetStringUnsafe(uint32_t idx) {
    uint32_t field_offset = offset_vec_.at(idx);
    uint32_t next_str_field_offset = 0;
    if (offset_vec_.at(idx) < string_field_cnt_ - 1) {
        next_str_field_offset = field_offset + 1;
    }
    const char* val;
    uint32_t length;

    v1::GetStrFieldUnsafe(row_, idx, field_offset, next_str_field_offset,
                          str_field_start_offset_, str_addr_length_, &val,
                          &length);
    return std::string(val, length);
}

int32_t RowView::GetBool(uint32_t idx, bool* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kBool)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetBoolUnsafe(idx);
    return 0;
}

int32_t RowView::GetInt32(uint32_t idx, int32_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kInt32)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetInt32Unsafe(idx);
    return 0;
}

int32_t RowView::GetTimestamp(uint32_t idx, int64_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kTimestamp)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetTimestampUnsafe(idx);
    return 0;
}

int32_t RowView::GetInt64(uint32_t idx, int64_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kInt64)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetInt64Unsafe(idx);
    return 0;
}

int32_t RowView::GetInt16(uint32_t idx, int16_t* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kInt16)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetInt16Unsafe(idx);
    return 0;
}

int32_t RowView::GetFloat(uint32_t idx, float* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kFloat)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetFloatUnsafe(idx);
    return 0;
}

int32_t RowView::GetDouble(uint32_t idx, double* val) {
    if (val == NULL) {
        LOG(WARNING) << "output val is null";
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kDouble)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *val = GetDoubleUnsafe(idx);
    return 0;
}

int32_t RowView::GetDate(uint32_t idx, int32_t* date) {
    if (date) {
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kDate)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    *date = GetDateUnsafe(idx);
    return 0;
}
int32_t RowView::GetYearUnsafe(int32_t days) { return 1900 + (days >> 16); }
int32_t RowView::GetMonthUnsafe(int32_t days) {
    days = days >> 8;
    return 1 + (days & 0x0000FF);
}
int32_t RowView::GetDayUnsafe(int32_t days) { return days & 0x0000000FF; }
int32_t RowView::GetDate(uint32_t idx, int32_t* year, int32_t* month,
                         int32_t* day) {
    if (year == NULL || month == NULL || day == NULL) {
        return -1;
    }
    if (!CheckValid(idx, ::hybridse::type::kDate)) {
        return -1;
    }
    if (IsNULL(row_, idx)) {
        return 1;
    }
    int32_t date = GetDateUnsafe(idx);
    openmldb::base::Date::Decode(date, year, month, day);
    return 0;
}
int32_t RowView::GetInteger(const int8_t* row, uint32_t idx,
                            ::hybridse::type::Type type, int64_t* val) {
    int32_t ret = 0;
    switch (type) {
        case ::hybridse::type::kInt16: {
            int16_t tmp_val = 0;
            ret = GetValue(row, idx, type, &tmp_val);
            if (ret == 0) *val = tmp_val;
            break;
        }
        case ::hybridse::type::kDate:
        case ::hybridse::type::kInt32: {
            int32_t tmp_val = 0;
            GetValue(row, idx, type, &tmp_val);
            if (ret == 0) *val = tmp_val;
            break;
        }
        case ::hybridse::type::kTimestamp:
        case ::hybridse::type::kInt64: {
            int64_t tmp_val = 0;
            GetValue(row, idx, type, &tmp_val);
            if (ret == 0) *val = tmp_val;
            break;
        }
        default:
            LOG(WARNING) << "type " << ::hybridse::type::Type_Name(type)
                         << " is not Integer";
            return -1;
    }
    return ret;
}

int32_t RowView::GetPrimaryFieldOffset(uint32_t idx) {
    return offset_vec_.at(idx);
}
int32_t RowView::GetValue(const int8_t* row, uint32_t idx,
                          ::hybridse::type::Type type, void* val) const {
    if (schema_.size() == 0 || row == NULL) {
        return -1;
    }
    if ((int32_t)idx >= schema_.size()) {
        LOG(WARNING) << "idx out of index";
        return -1;
    }
    const ::hybridse::type::ColumnDef& column = schema_.Get(idx);
    if (column.type() != type) {
        LOG(WARNING) << "type mismatch required is "
                     << ::hybridse::type::Type_Name(type) << " but is "
                     << hybridse::type::Type_Name(column.type());
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
        case ::hybridse::type::kBool: {
            int8_t v = v1::GetBoolFieldUnsafe(row, offset);
            if (v == 1) {
                *(reinterpret_cast<bool*>(val)) = true;
            } else {
                *(reinterpret_cast<bool*>(val)) = false;
            }
            break;
        }
        case ::hybridse::type::kInt16:
            *(reinterpret_cast<int16_t*>(val)) =
                v1::GetInt16FieldUnsafe(row, offset);
            break;
        case ::hybridse::type::kDate:
        case ::hybridse::type::kInt32:
            *(reinterpret_cast<int32_t*>(val)) =
                v1::GetInt32FieldUnsafe(row, offset);
            break;
        case ::hybridse::type::kTimestamp:
        case ::hybridse::type::kInt64:
            *(reinterpret_cast<int64_t*>(val)) =
                v1::GetInt64FieldUnsafe(row, offset);
            break;
        case ::hybridse::type::kFloat:
            *(reinterpret_cast<float*>(val)) =
                v1::GetFloatFieldUnsafe(row, offset);
            break;
        case ::hybridse::type::kDouble:
            *(reinterpret_cast<double*>(val)) =
                v1::GetDoubleFieldUnsafe(row, offset);
            break;
        default:
            return -1;
    }
    return 0;
}
std::string RowView::GetRowString() {
    if (schema_.size() == 0) {
        return "NA";
    }
    std::string row_str = "";

    for (int i = 0; i < schema_.size(); i++) {
        row_str.append(GetAsString(i));
        if (i != schema_.size() - 1) {
            row_str.append(", ");
        }
    }
    return row_str;
}
std::string RowView::GetAsString(uint32_t idx) {
    if (schema_.size() == 0) {
        return "NA";
    }

    if (row_ == nullptr || size_ == 0) {
        return "NA";
    }

    if ((int32_t)idx >= schema_.size()) {
        LOG(WARNING) << "idx out of index";
        return "NA";
    }

    if (IsNULL(idx)) {
        return "NULL";
    }
    const ::hybridse::type::ColumnDef& column = schema_.Get(idx);
    switch (column.type()) {
        case hybridse::type::kInt32: {
            int32_t value;
            if (0 == GetInt32(idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case hybridse::type::kInt64: {
            int64_t value;
            if (0 == GetInt64(idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case hybridse::type::kInt16: {
            int16_t value;
            if (0 == GetInt16(idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case hybridse::type::kFloat: {
            float value;
            if (0 == GetFloat(idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case hybridse::type::kDouble: {
            double value;
            if (0 == GetDouble(idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case hybridse::type::kBool: {
            bool value;
            if (0 == GetBool(idx, &value)) {
                return value ? "true" : "false";
            }
            break;
        }
        case hybridse::type::kVarchar: {
            const char* str = nullptr;
            uint32_t str_size;
            int32_t ret = GetString(idx, &str, &str_size);
            if (0 == ret) {
                if (str_size > 4096) {
                    LOG(ERROR) << "Invalid String: string size exceed max "
                                  "string size 4096, trunk string"
                               << "size_ = " << size_
                               << " *(reinterpret_cast<const uint32_t*>(row + "
                                  "VERSION_LENGTH)) = "
                               << *(reinterpret_cast<const uint32_t*>(
                                      row_ + VERSION_LENGTH));
                    return std::string(str, 4096);
                }
                return std::string(str, str_size);
            } else {
                LOG(ERROR) << "fail to get string: ret = " << ret
                           << "size_ = " << size_
                           << " *(reinterpret_cast<const uint32_t*>(row + "
                              "VERSION_LENGTH)) = "
                           << *(reinterpret_cast<const uint32_t*>(
                                  row_ + VERSION_LENGTH));
            }
            break;
        }
        case hybridse::type::kTimestamp: {
            int64_t value;
            if (0 == GetTimestamp(idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case hybridse::type::kDate: {
            int32_t year;
            int32_t month;
            int32_t day;
            if (0 == GetDate(idx, &year, &month, &day)) {
                char date[11];
                snprintf(date, 11u, "%4d-%.2d-%.2d", year, month, day);
                return std::string(date);
            }
            break;
        }
        default: {
            LOG(WARNING) << "fail to get string for "
                            "current row";
            break;
        }
    }

    return "NA";
}

int32_t RowView::GetValue(const int8_t* row, uint32_t idx, const char** val,
                          uint32_t* length) const {
    if (schema_.size() == 0 || row == NULL || length == NULL) {
        return -1;
    }
    if ((int32_t)idx >= schema_.size()) {
        LOG(WARNING) << "idx out of index";
        return -1;
    }
    const ::hybridse::type::ColumnDef& column = schema_.Get(idx);
    if (column.type() != ::hybridse::type::kVarchar) {
        LOG(WARNING) << "type mismatch required is "
                     << ::hybridse::type::Type_Name(::hybridse::type::kVarchar)
                     << " but is " << hybridse::type::Type_Name(column.type());
        return -1;
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

    return v1::GetStrFieldUnsafe(row, idx, field_offset, next_str_field_offset,
                                 str_field_start_offset_, GetAddrLength(size),
                                 val, length);
}

int32_t RowView::GetString(uint32_t idx, const char** val, uint32_t* length) {
    if (val == NULL || length == NULL) {
        LOG(WARNING) << "output val or length is null";
        return -1;
    }

    if (!CheckValid(idx, ::hybridse::type::kVarchar)) {
        return -1;
    }
    uint32_t size = GetSize(row_);
    if (size <= HEADER_LENGTH) {
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
    return v1::GetStrFieldUnsafe(row_, idx, field_offset, next_str_field_offset,
                                 str_field_start_offset_, str_addr_length_, val,
                                 length);
}

SliceFormat::SliceFormat(const hybridse::codec::Schema* schema)
    : schema_(schema), infos_(), next_str_pos_(), str_field_start_offset_(0) {
    if (nullptr == schema) {
        return;
    }
    uint32_t offset = codec::GetStartOffset(schema_->size());
    uint32_t string_field_cnt = 0;
    for (int32_t i = 0; i < schema_->size(); i++) {
        const ::hybridse::type::ColumnDef& column = schema_->Get(i);
        if (column.type() == ::hybridse::type::kVarchar) {
            if (FLAGS_enable_spark_unsaferow_format) {
                infos_.emplace_back(column.name(), column.type(), i, offset);
            } else {
                infos_.emplace_back(column.name(), column.type(), i, string_field_cnt);
            }

            infos_dict_[column.name()] = i;
            next_str_pos_.emplace(string_field_cnt, string_field_cnt);
            string_field_cnt += 1;

            if (FLAGS_enable_spark_unsaferow_format) {
                // For UnsafeRowOpt, the offset should be added for string and non-string columns
                offset += 8;
            }
        } else {
            auto TYPE_SIZE_MAP = codec::GetTypeSizeMap();
            auto it = TYPE_SIZE_MAP.find(column.type());
            if (it == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << "fail to find column type "
                             << ::hybridse::type::Type_Name(column.type());
            } else {
                infos_.emplace_back(column.name(), column.type(), i, offset);
                infos_dict_[column.name()] = i;
                offset += it->second;
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
}

const ColInfo* SliceFormat::GetColumnInfo(size_t idx) const {
    return idx < infos_.size() ? &infos_[idx] : nullptr;
}

bool SliceFormat::GetStringColumnInfo(size_t idx, StringColInfo* res) const {
    if (nullptr == res) {
        LOG(WARNING) << "input args have null";
        return false;
    }
    if (idx >= infos_.size()) {
        return false;
    }
    // TODO(wangtaize) support null check
    auto& base_col_info = infos_[idx];
    auto ty = base_col_info.type;
    uint32_t col_idx = base_col_info.idx;
    uint32_t offset = base_col_info.offset;
    uint32_t next_offset = -1;
    auto nit = next_str_pos_.find(offset);
    if (nit != next_str_pos_.end()) {
        next_offset = nit->second;
    } else {
        if (FLAGS_enable_spark_unsaferow_format) {
            // No need to get next offset for UnsafeRowOpt and ignore the warning
        } else {
            LOG(WARNING) << "fail to get string field next offset";
            return false;
        }
    }
    DLOG(INFO) << "get string with offset " << offset << " next offset "
               << next_offset << " str_field_start_offset "
               << str_field_start_offset_ << " for col " << base_col_info.name;

    *res = StringColInfo(base_col_info.name, ty, col_idx, offset, next_offset,
                        str_field_start_offset_);
    return true;
}

}  // namespace codec
}  // namespace hybridse
