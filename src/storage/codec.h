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

#include "proto/type.pb.h"

namespace fesql {
namespace storage {

typedef ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef> Schema;

class RowView;

class RowBuilder {

 public:

    RowBuilder(const Schema* schema, 
              int8_t* buf,
              uint32_t size);

    ~RowBuilder(); 

    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendFloat(float val);
    bool AppendDouble(double val);

 private:
    inline bool Check(uint32_t delta);
 private:
    const Schema* schema_;
    int8_t* buf_;
    uint32_t size_;
    uint32_t offset_;
};

class RowView {
 public:

    RowView(const Schema* schema,
            const int8_t* row,
            uint32_t size);

    ~RowView();

    bool GetInt32(uint32_t idx, int32_t* val);
    bool GetInt64(uint32_t idx, int64_t* val);
    bool GetInt16(uint32_t idx, int16_t* val);
    bool GetFloat(uint32_t idx, float* val);
    bool GetDouble(uint32_t idx, double* val);

 private:
    const Schema* schema_;
    const int8_t* row_;
    uint32_t size_;
    std::vector<uint32_t> offsets_;
};



}  // namespace storage
}  // namespace fesql
#endif  // SRC_STORAGE_CODEC_H_

