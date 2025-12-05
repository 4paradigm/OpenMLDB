/*
 * Copyright 2021 4Paradigm
 * Licensed under the Apache License, Version 2.0 (the "License")
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
#include "sdk/codec_sdk.h"

#include "butil/iobuf.h"

namespace hybridse {
namespace sdk {

RowIOBufView::RowIOBufView(const hybridse::codec::Schema& schema)
    : row_(),
      str_addr_length_(0),
      is_valid_(true),
      string_field_cnt_(0),
      str_field_start_offset_(0),
      size_(0),
      schema_(schema),
      offset_vec_() {
    Init();
}

RowIOBufView::~RowIOBufView() {}

bool RowIOBufView::Init() {
    uint32_t offset = codec::HEADER_LENGTH + codec::BitMapSize(schema_.size());
    for (int idx = 0; idx < schema_.size(); idx++) {
        const ::hybridse::type::ColumnDef& column = schema_.Get(idx);
        if (column.type() == ::hybridse::type::kVarchar) {
            offset_vec_.push_back(string_field_cnt_);
            string_field_cnt_++;
        } else {
            auto TYPE_SIZE_MAP = codec::GetTypeSizeMap();
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

bool RowIOBufView::Reset(const butil::IOBuf& buf) {
    row_ = buf;
    if (schema_.size() == 0 || row_.size() <= codec::HEADER_LENGTH) {
        is_valid_ = false;
        return false;
    }
    size_ = row_.size();
    uint32_t tmp_size = 0;
    row_.copy_to(reinterpret_cast<void*>(&tmp_size), codec::SIZE_LENGTH,
                 codec::VERSION_LENGTH);
    if (tmp_size != size_) {
        is_valid_ = false;
        return false;
    }
    str_addr_length_ = codec::GetAddrLength(size_);
    DLOG(INFO) << "size " << size_ << " addr length " << (unsigned int)str_addr_length_;
    return true;
}

int32_t RowIOBufView::GetBool(uint32_t idx, bool* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetBoolField(row_, offset) == 1 ? true : false;
    return 0;
}

int32_t RowIOBufView::GetInt16(uint32_t idx, int16_t* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt16Field(row_, offset);
    return 0;
}

int32_t RowIOBufView::GetInt32(uint32_t idx, int32_t* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt32Field(row_, offset);
    return 0;
}

int32_t RowIOBufView::GetInt64(uint32_t idx, int64_t* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt64Field(row_, offset);
    return 0;
}

int32_t RowIOBufView::GetFloat(uint32_t idx, float* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetFloatField(row_, offset);
    return 0;
}

int32_t RowIOBufView::GetDouble(uint32_t idx, double* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetDoubleField(row_, offset);
    return 0;
}

int32_t RowIOBufView::GetTimestamp(uint32_t idx, int64_t* val) {
    if (val == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *val = v1::GetInt64Field(row_, offset);
    return 0;
}
int32_t RowIOBufView::GetDate(uint32_t idx, int32_t* date) {
    if (date == NULL) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    *date = static_cast<int32_t>(v1::GetInt32Field(row_, offset));
    return 0;
}
int32_t RowIOBufView::GetDate(uint32_t idx, int32_t* year, int32_t* month,
                              int32_t* day) {
    if (year == NULL || month == NULL || day == NULL) {
        return -1;
    }
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t offset = offset_vec_.at(idx);
    int32_t date = static_cast<int32_t>(v1::GetInt32Field(row_, offset));
    *day = date & 0x0000000FF;
    date = date >> 8;
    *month = 1 + (date & 0x0000FF);
    *year = 1900 + (date >> 8);
    return 0;
}
int32_t RowIOBufView::GetString(uint32_t idx, butil::IOBuf* buf) {
    if (buf == NULL) return -1;
    if (IsNULL(idx)) {
        return 1;
    }
    uint32_t field_offset = offset_vec_.at(idx);
    uint32_t next_str_field_offset = 0;
    if (offset_vec_.at(idx) < string_field_cnt_ - 1) {
        next_str_field_offset = field_offset + 1;
    }
    return v1::GetStrField(row_, field_offset, next_str_field_offset,
                           str_field_start_offset_, str_addr_length_, buf);
}

namespace v1 {

int32_t GetStrField(const butil::IOBuf& row, uint32_t field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, butil::IOBuf* output) {
    if (output == NULL) return -1;
    uint32_t str_offset = 0;
    uint32_t next_str_offset = 0;
    switch (addr_space) {
        case 1: {
            int32_t i8_str_pos = str_start_offset + field_offset;
            uint8_t i8_tmp_offset = 0;
            row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1, i8_str_pos);
            str_offset = i8_tmp_offset;
            if (next_str_field_offset > 0) {
                i8_str_pos = str_start_offset + next_str_field_offset;
                row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1,
                            i8_str_pos);
                next_str_offset = i8_tmp_offset;
            }
            break;
        }
        case 2: {
            int32_t i16_str_pos = str_start_offset + field_offset * 2;
            uint16_t i16_tmp_offset = 0;
            row.copy_to(reinterpret_cast<void*>(&i16_tmp_offset), 2,
                        i16_str_pos);
            str_offset = i16_tmp_offset;
            if (next_str_field_offset > 0) {
                i16_str_pos = str_start_offset + next_str_field_offset * 2;
                row.copy_to(reinterpret_cast<void*>(&i16_tmp_offset), 2,
                            i16_str_pos);
                next_str_offset = i16_tmp_offset;
            }
            break;
        }
        case 3: {
            uint8_t i8_tmp_offset = 0;
            int32_t i8_str_pos = str_start_offset + field_offset * 3;
            row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1, i8_str_pos);
            str_offset = i8_tmp_offset;
            row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1,
                        i8_str_pos + 1);
            str_offset = (str_offset << 8) + i8_tmp_offset;
            row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1,
                        i8_str_pos + 2);
            str_offset = (str_offset << 8) + i8_tmp_offset;
            if (next_str_field_offset > 0) {
                i8_str_pos = str_start_offset + next_str_field_offset * 3;
                row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1,
                            i8_str_pos);
                next_str_offset = i8_tmp_offset;
                row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1,
                            i8_str_pos + 1);
                next_str_offset = (next_str_offset << 8) + i8_tmp_offset;
                row.copy_to(reinterpret_cast<void*>(&i8_tmp_offset), 1,
                            i8_str_pos + 2);
                next_str_offset = (next_str_offset << 8) + i8_tmp_offset;
            }
            break;
        }
        case 4: {
            int32_t i32_str_pos = str_start_offset + field_offset * 4;
            row.copy_to(reinterpret_cast<void*>(&str_offset), 4, i32_str_pos);
            if (next_str_field_offset > 0) {
                i32_str_pos = str_start_offset + next_str_field_offset * 4;
                row.copy_to(reinterpret_cast<void*>(&next_str_offset), 4,
                            i32_str_pos);
            }
            break;
        }
        default: {
            return -2;
        }
    }
    if (next_str_field_offset <= 0) {
        uint32_t tmp_size = 0;
        row.copy_to(reinterpret_cast<void*>(&tmp_size), codec::SIZE_LENGTH,
                    codec::VERSION_LENGTH);
        uint32_t size = tmp_size - str_offset;
        row.append_to(output, size, str_offset);

    } else {
        uint32_t size = next_str_offset - str_offset;
        row.append_to(output, size, str_offset);
    }
    return 0;
}

}  // namespace v1

}  // namespace sdk
}  // namespace hybridse
