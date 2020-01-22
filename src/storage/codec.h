/*
 * codec.h
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

#ifndef SRC_STORAGE_CODEC_H_
#define SRC_STORAGE_CODEC_H_

#include <map>
#include <vector>
#include <unordered_map>
#include "proto/type.pb.h"
#include "catalog/catalog.h"


namespace fesql {
namespace storage {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))

using Schema = catalog::Schema;
static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
static constexpr uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static constexpr uint32_t UINT24_MAX = (1 << 24) - 1;


static const std::unordered_map<::fesql::type::Type, uint8_t> TYPE_SIZE_MAP = {
    {::fesql::type::kBool, sizeof(bool)},
    {::fesql::type::kInt16, sizeof(int16_t)},
    {::fesql::type::kInt32, sizeof(int32_t)},
    {::fesql::type::kFloat, sizeof(float)},
    {::fesql::type::kInt64, sizeof(int64_t)},
    {::fesql::type::kTimestamp, sizeof(int64_t)},
    {::fesql::type::kDouble, sizeof(double)}};

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

inline uint32_t GetStartOffset(int32_t column_count) {

    return HEADER_LENGTH + BitMapSize(column_count);
}

class RowBuilder {
 public:
    explicit RowBuilder(const Schema& schema);
    ~RowBuilder() = default;

    uint32_t CalTotalLength(uint32_t string_length);
    bool SetBuffer(int8_t* buf, uint32_t size);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const char* val, uint32_t length);
    bool AppendNULL();

 private:
    bool Check(::fesql::type::Type type);

 private:
    const Schema& schema_;
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
    RowView(const Schema& schema, const int8_t* row, uint32_t size);
    explicit RowView(const Schema& schema);
    ~RowView() = default;
    bool Reset(const int8_t* row, uint32_t size);
    bool Reset(const int8_t* row);

    int32_t GetBool(uint32_t idx, bool* val);
    int32_t GetInt32(uint32_t idx, int32_t* val);
    int32_t GetInt64(uint32_t idx, int64_t* val);
    int32_t GetTimestamp(uint32_t idx, int64_t* val);
    int32_t GetInt16(uint32_t idx, int16_t* val);
    int32_t GetFloat(uint32_t idx, float* val);
    int32_t GetDouble(uint32_t idx, double* val);
    int32_t GetString(uint32_t idx, char** val, uint32_t* length);
    bool IsNULL(uint32_t idx) { return IsNULL(row_, idx); }
    inline uint32_t GetSize() { return size_; }

    static inline uint32_t GetSize(const int8_t* row) {
        return *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH));
    }

    int32_t GetValue(const int8_t* row, uint32_t idx, ::fesql::type::Type type,
                     void* val);

    int32_t GetInteger(const int8_t* row, uint32_t idx,
                       ::fesql::type::Type type, int64_t* val);
    int32_t GetValue(const int8_t* row, uint32_t idx, char** val,
                     uint32_t* length);

 private:
    bool Init();
    bool CheckValid(uint32_t idx, ::fesql::type::Type type);

    inline bool IsNULL(const int8_t* row, uint32_t idx) {
        const int8_t* ptr = row + HEADER_LENGTH + (idx >> 3);
        return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
    }

 private:
    uint8_t str_addr_length_;
    bool is_valid_;
    uint32_t string_field_cnt_;
    uint32_t str_field_start_offset_;
    uint32_t size_;
    const int8_t* row_;
    const Schema& schema_;
    std::vector<uint32_t> offset_vec_;
};

}  // namespace storage
}  // namespace fesql
#endif  // SRC_STORAGE_CODEC_H_
