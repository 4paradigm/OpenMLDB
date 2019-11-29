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

#include <vector>
#include <map>
#include "proto/type.pb.h"

namespace fesql {
namespace storage {

using Schema = ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef>;

class RowBuilder {
 public:
    RowBuilder(const Schema& schema, int8_t* buf, uint32_t size);

    ~RowBuilder() = default;
    static uint32_t CalTotalLength(const Schema& schema,
                                   uint32_t string_length);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
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
    uint32_t offset_;
    uint32_t str_addr_length_;
    uint32_t str_start_offset_;
    uint32_t str_offset_;
};

class RowView {
 public:
    RowView(const Schema& schema, const int8_t* row, uint32_t size);
    void Reset(const int8_t* row, uint32_t size);
    ~RowView() = default;

    int32_t GetBool(uint32_t idx, bool* val);
    int32_t GetInt32(uint32_t idx, int32_t* val);
    int32_t GetInt64(uint32_t idx, int64_t* val);
    int32_t GetInt16(uint32_t idx, int16_t* val);
    int32_t GetFloat(uint32_t idx, float* val);
    int32_t GetDouble(uint32_t idx, double* val);
    int32_t GetString(uint32_t idx, char** val, uint32_t* length);
    bool IsNULL(uint32_t idx);

 private:
    bool CheckValid(uint32_t idx, ::fesql::type::Type type);

 private:
    uint8_t str_addr_length_;
    bool is_valid_;
    uint32_t str_field_start_offset_;
    uint32_t size_;
    const int8_t* row_;
    const Schema& schema_;
    std::vector<uint32_t> offset_vec_;
    std::map<uint32_t, uint32_t> next_str_pos_;
};

}  // namespace storage
}  // namespace fesql
#endif  // SRC_STORAGE_CODEC_H_
