/*
 * fe_row_codec.h
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

#ifndef SRC_CODEC_FE_ROW_CODEC_H_
#define SRC_CODEC_FE_ROW_CODEC_H_

#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/raw_buffer.h"
#include "butil/iobuf.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codec {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))

typedef ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef> Schema;

static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
static constexpr uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static constexpr uint32_t UINT24_MAX = (1 << 24) - 1;
const std::string NONETOKEN = "!N@U#L$L%";  // NOLINT
const std::string EMPTY_STRING = "!@#$%";   // NOLINT

static const std::unordered_map<::fesql::type::Type, uint8_t> TYPE_SIZE_MAP = {
    {::fesql::type::kBool, sizeof(bool)},
    {::fesql::type::kInt16, sizeof(int16_t)},
    {::fesql::type::kInt32, sizeof(int32_t)},
    {::fesql::type::kFloat, sizeof(float)},
    {::fesql::type::kInt64, sizeof(int64_t)},
    {::fesql::type::kTimestamp, sizeof(int64_t)},
    {::fesql::type::kDate, sizeof(int32_t)},
    {::fesql::type::kDouble, sizeof(double)}};

inline uint8_t GetAddrLength(uint32_t size) {
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
inline uint32_t GetBitmapSize(uint32_t column_size) {
    return BitMapSize(column_size);
}
inline uint32_t GetStartOffset(int32_t column_count) {
    return HEADER_LENGTH + BitMapSize(column_count);
}

void FillNullStringOffset(int8_t* buf, uint32_t start, uint32_t addr_length,
                          uint32_t str_idx, uint32_t str_offset);

class RowBuilder {
 public:
    explicit RowBuilder(const fesql::codec::Schema& schema);
    ~RowBuilder() = default;
    uint32_t CalTotalLength(uint32_t string_length);
    bool SetBuffer(int8_t* buf, uint32_t size);
    bool SetBuffer(int64_t buf_handle, uint32_t size);
    bool SetBuffer(const fesql::base::RawBuffer& buf);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendDate(int32_t year, int32_t month, int32_t day);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const char* val, uint32_t length);
    bool AppendNULL();

 private:
    bool Check(::fesql::type::Type type);

 private:
    const Schema schema_;
    int8_t* buf_;
    uint32_t cnt_;
    uint32_t size_;
    uint32_t str_field_cnt_;
    uint32_t str_addr_length_;
    uint32_t str_field_start_offset_;
    uint32_t str_offset_;
    std::vector<uint32_t> offset_vec_;
};

class RowView {
 public:
    RowView(const fesql::codec::Schema& schema, const int8_t* row,
            uint32_t size);
    explicit RowView(const fesql::codec::Schema& schema);
    RowView(const RowView& row_view);
    ~RowView() = default;
    bool Reset(const int8_t* row, uint32_t size);
    bool Reset(const int8_t* row);
    bool Reset(const fesql::base::RawBuffer& buf);
    int32_t GetBool(uint32_t idx, bool* val);
    int32_t GetInt32(uint32_t idx, int32_t* val);
    int32_t GetInt64(uint32_t idx, int64_t* val);
    int32_t GetTimestamp(uint32_t idx, int64_t* val);
    int32_t GetInt16(uint32_t idx, int16_t* val);
    int32_t GetFloat(uint32_t idx, float* val);
    int32_t GetDouble(uint32_t idx, double* val);
    int32_t GetString(uint32_t idx, const char** val, uint32_t* length);
    int32_t GetDate(uint32_t idx, int32_t* year, int32_t* month, int32_t* day);
    int32_t GetDate(uint32_t idx, int32_t* date);

    bool GetBoolUnsafe(uint32_t idx);
    int32_t GetInt32Unsafe(uint32_t idx);
    int64_t GetInt64Unsafe(uint32_t idx);
    int64_t GetTimestampUnsafe(uint32_t idx);
    int32_t GetDateUnsafe(uint32_t idx);
    int32_t GetYearUnsafe(int32_t days);
    int32_t GetMonthUnsafe(int32_t days);
    int32_t GetDayUnsafe(int32_t idx);
    int16_t GetInt16Unsafe(uint32_t idx);
    float GetFloatUnsafe(uint32_t idx);
    double GetDoubleUnsafe(uint32_t idx);
    std::string GetStringUnsafe(uint32_t idx);

    bool IsNULL(uint32_t idx) { return IsNULL(row_, idx); }
    inline uint32_t GetSize() { return size_; }

    static inline uint32_t GetSize(const int8_t* row) {
        return *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH));
    }
    int32_t GetValue(const int8_t* row, uint32_t idx, ::fesql::type::Type type,
                     void* val) const;

    int32_t GetInteger(const int8_t* row, uint32_t idx,
                       ::fesql::type::Type type, int64_t* val);

    int32_t GetValue(const int8_t* row, uint32_t idx, const char** val,
                     uint32_t* length) const;

    std::string GetAsString(uint32_t idx);
    std::string GetRowString();
    int32_t GetPrimaryFieldOffset(uint32_t idx);
    const Schema* GetSchema() const { return &schema_; }

    inline bool IsNULL(const int8_t* row, uint32_t idx) const {
        const int8_t* ptr = row + HEADER_LENGTH + (idx >> 3);
        return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
    }

 private:
    bool Init();
    bool CheckValid(uint32_t idx, ::fesql::type::Type type);

 private:
    uint8_t str_addr_length_;
    bool is_valid_;
    uint32_t string_field_cnt_;
    uint32_t str_field_start_offset_;
    uint32_t size_;
    const int8_t* row_;
    const Schema schema_;
    std::vector<uint32_t> offset_vec_;
};

struct ColInfo {
    ::fesql::type::Type type;
    uint32_t idx;
    uint32_t offset;
    std::string name;

    ColInfo() {}
    ColInfo(const std::string& name, ::fesql::type::Type type, uint32_t idx,
            uint32_t offset)
        : type(type), idx(idx), offset(offset), name(name) {}
};

struct StringColInfo : public ColInfo {
    uint32_t str_next_offset;
    uint32_t str_start_offset;

    StringColInfo() {}
    StringColInfo(const std::string& name, ::fesql::type::Type type,
                  uint32_t idx, uint32_t offset, uint32_t str_next_offset,
                  uint32_t str_start_offset)
        : ColInfo(name, type, idx, offset),
          str_next_offset(str_next_offset),
          str_start_offset(str_start_offset) {}
};

class RowFormat {
 public:
    explicit RowFormat(const fesql::codec::Schema* schema);
    virtual ~RowFormat() {}

    bool GetStringColumnInfo(size_t idx, StringColInfo* res) const;

    const ColInfo* GetColumnInfo(size_t idx) const;

 private:
    const fesql::codec::Schema* schema_;
    std::vector<ColInfo> infos_;
    std::map<std::string, size_t> infos_dict_;
    std::map<uint32_t, uint32_t> next_str_pos_;
    uint32_t str_field_start_offset_;
};

}  // namespace codec
}  // namespace fesql
#endif  // SRC_CODEC_FE_ROW_CODEC_H_
