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

#ifndef HYBRIDSE_INCLUDE_SDK_CODEC_SDK_H_
#define HYBRIDSE_INCLUDE_SDK_CODEC_SDK_H_

#include <vector>
#include "butil/iobuf.h"
#include "codec/fe_row_codec.h"

namespace hybridse {
namespace sdk {

class RowBaseView {
 public:
    RowBaseView() {}
    virtual ~RowBaseView() {}
    virtual int32_t GetBool(uint32_t idx, bool* val) = 0;
    virtual int32_t GetInt16(uint32_t idx, int16_t* val) = 0;
    virtual int32_t GetInt32(uint32_t idx, int32_t* val) = 0;
    virtual int32_t GetInt64(uint32_t idx, int64_t* val) = 0;
    virtual int32_t GetFloat(uint32_t idx, float* val) = 0;
    virtual int32_t GetDouble(uint32_t idx, double* val) = 0;
    virtual int32_t GetTimestamp(uint32_t idx, int64_t* val) = 0;
    virtual int32_t GetString(uint32_t idx, butil::IOBuf* buf) = 0;
    virtual int32_t GetString(uint32_t idx, char** data, uint32_t* size) = 0;
    virtual bool IsNULL(uint32_t idx) = 0;
};

class RowIOBufView : public RowBaseView {
 public:
    explicit RowIOBufView(const hybridse::codec::Schema& schema);
    ~RowIOBufView();
    bool Reset(const butil::IOBuf& buf);
    int32_t GetInt16(uint32_t idx, int16_t* val);
    int32_t GetInt32(uint32_t idx, int32_t* val);
    int32_t GetInt64(uint32_t idx, int64_t* val);
    int32_t GetFloat(uint32_t idx, float* val);
    int32_t GetDouble(uint32_t idx, double* val);
    int32_t GetTimestamp(uint32_t, int64_t* val);
    int32_t GetDate(uint32_t, int32_t* year, int32_t* month, int32_t* day);
    int32_t GetDate(uint32_t, int32_t* date);
    int32_t GetString(uint32_t idx, butil::IOBuf* buf);
    int32_t GetString(uint32_t idx, char** val, uint32_t* length) { return -1; }
    int32_t GetBool(uint32_t idx, bool* val);

    inline bool IsNULL(uint32_t idx) {
        uint32_t offset = codec::HEADER_LENGTH + (idx >> 3);
        uint8_t val = 0;
        row_.copy_to(reinterpret_cast<void*>(&val), 1, offset);
        return val & (1 << (idx & 0x07));
    }

 private:
    bool Init();

 private:
    butil::IOBuf row_;
    uint8_t str_addr_length_;
    bool is_valid_;
    uint32_t string_field_cnt_;
    uint32_t str_field_start_offset_;
    uint32_t size_;
    const hybridse::codec::Schema schema_;
    std::vector<uint32_t> offset_vec_;
};

namespace v1 {

inline int8_t GetBoolField(const butil::IOBuf& row, uint32_t offset) {
    int8_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 1, offset);
    return value;
}

inline int16_t GetInt16Field(const butil::IOBuf& row, uint32_t offset) {
    int16_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 2, offset);
    return value;
}

inline int32_t GetInt32Field(const butil::IOBuf& row, uint32_t offset) {
    int32_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 4, offset);
    return value;
}

inline int64_t GetInt64Field(const butil::IOBuf& row, uint32_t offset) {
    int64_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 8, offset);
    return value;
}

inline float GetFloatField(const butil::IOBuf& row, uint32_t offset) {
    float value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 4, offset);
    return value;
}

inline double GetDoubleField(const butil::IOBuf& row, uint32_t offset) {
    double value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 8, offset);
    return value;
}

int32_t GetStrField(const butil::IOBuf& row, uint32_t str_field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, butil::IOBuf* output);

}  // namespace v1

}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_SDK_CODEC_SDK_H_
