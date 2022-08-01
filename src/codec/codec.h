/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CODEC_CODEC_H_
#define SRC_CODEC_CODEC_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/strings.h"
#include "proto/common.pb.h"

namespace openmldb {
namespace codec {

using ProjectList = ::google::protobuf::RepeatedField<uint32_t>;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;
static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
static constexpr uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static constexpr uint32_t UINT24_MAX = (1 << 24) - 1;

class RowBuilder;
class RowView;
class RowProject;

class RowProject {
 public:
    RowProject(const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema, const ProjectList& plist);

    ~RowProject();

    bool Init();

    bool Project(const int8_t* row_ptr, uint32_t row_size, int8_t** out_ptr, uint32_t* out_size);

    uint32_t GetMaxIdx() { return max_idx_; }

 private:
    const ProjectList& plist_;
    Schema output_schema_;
    // TODO(wangtaize) share the init overhead
    RowBuilder* row_builder_;
    std::shared_ptr<Schema> cur_schema_;
    std::shared_ptr<RowView> cur_rv_;
    uint32_t max_idx_;
    std::map<int32_t, std::shared_ptr<RowView>> vers_views_;
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema_;
    uint32_t cur_ver_;
};

class RowBuilder {
 public:
    explicit RowBuilder(const Schema& schema);

    uint32_t CalTotalLength(uint32_t string_length);
    bool InitBuffer(int8_t* buf, uint32_t size, bool need_clear);
    bool SetBuffer(int8_t* buf, uint32_t size);
    bool SetBuffer(int8_t* buf, uint32_t size, bool need_clear);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const char* val, uint32_t length);
    bool AppendNULL();
    bool SetNULL(uint32_t index);
    bool SetNULL(int8_t* buf, uint32_t size, uint32_t index);
    bool AppendDate(uint32_t year, uint32_t month, uint32_t day);
    // append the date that encoded
    bool AppendDate(int32_t date);
    bool AppendValue(const std::string& val);
    bool SetBool(uint32_t index, bool val);
    bool SetBool(int8_t* buf, uint32_t index, bool val);
    bool SetInt16(uint32_t index, int16_t val);
    bool SetInt16(int8_t* buf, uint32_t index, int16_t val);
    bool SetInt32(uint32_t index, int32_t val);
    bool SetInt32(int8_t* buf, uint32_t index, int32_t val);
    bool SetInt64(uint32_t index, int64_t val);
    bool SetInt64(int8_t* buf, uint32_t index, int64_t val);
    bool SetTimestamp(uint32_t index, int64_t val);
    bool SetTimestamp(int8_t* buf, uint32_t index, int64_t val);
    bool SetFloat(uint32_t index, float val);
    bool SetFloat(int8_t* buf, uint32_t index, float val);
    bool SetDouble(uint32_t index, double val);
    bool SetDouble(int8_t* buf, uint32_t index, double val);
    bool SetString(uint32_t index, const char* val, uint32_t length);
    bool SetString(int8_t* buf, uint32_t size, uint32_t index, const char* val, uint32_t length);
    bool SetDate(uint32_t index, uint32_t year, uint32_t month, uint32_t day);
    bool SetDate(int8_t* buf, uint32_t index, uint32_t year, uint32_t month, uint32_t day);
    // set the date that encoded
    bool SetDate(uint32_t index, int32_t date);
    bool SetDate(int8_t* buf, uint32_t index, int32_t date);

    void SetSchemaVersion(uint8_t version);
    inline bool IsComplete() { return cnt_ == (uint32_t)schema_.size(); }
    inline uint32_t GetAppendPos() { return cnt_; }

    static bool ConvertDate(uint32_t year, uint32_t month, uint32_t day, uint32_t* val);

 private:
    bool Check(uint32_t index, ::openmldb::type::DataType type);
    inline void SetField(uint32_t index);
    inline void SetField(int8_t* buf, uint32_t index);
    inline void SetStrOffset(uint32_t str_pos);
    void SetStrOffset(int8_t* buf, uint32_t size, uint32_t str_pos, uint32_t str_offset);
    bool GetStrOffset(int8_t* buf, uint32_t size, uint32_t str_pos, uint32_t* offset);

 private:
    const Schema& schema_;
    int8_t* buf_;
    uint32_t cnt_;
    uint32_t size_;
    uint32_t str_field_cnt_;
    uint32_t str_addr_length_;
    uint32_t str_field_start_offset_;
    uint32_t str_offset_;
    uint8_t schema_version_;
    std::vector<uint32_t> offset_vec_;
};

class RowView {
 public:
    RowView(const Schema& schema, const int8_t* row, uint32_t size);
    explicit RowView(const Schema& schema);
    ~RowView() = default;
    bool Reset(const int8_t* row, uint32_t size);
    bool Reset(const int8_t* row);

    static uint8_t GetSchemaVersion(const int8_t* row) { return *(reinterpret_cast<const uint8_t*>(row + 1)); }

    int32_t GetBool(uint32_t idx, bool* val) const;
    int32_t GetInt32(uint32_t idx, int32_t* val) const;
    int32_t GetInt64(uint32_t idx, int64_t* val) const;
    int32_t GetTimestamp(uint32_t idx, int64_t* val) const;
    int32_t GetInt16(uint32_t idx, int16_t* val) const;
    int32_t GetFloat(uint32_t idx, float* val) const;
    int32_t GetDouble(uint32_t idx, double* val) const;
    int32_t GetString(uint32_t idx, char** val, uint32_t* length) const;
    int32_t GetDate(uint32_t idx, uint32_t* year, uint32_t* month, uint32_t* day) const;
    int32_t GetDate(uint32_t idx, int32_t* date) const;
    bool IsNULL(uint32_t idx) const { return IsNULL(row_, idx); }
    inline bool IsNULL(const int8_t* row, uint32_t idx) const {
        const int8_t* ptr = row + HEADER_LENGTH + (idx >> 3);
        return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
    }
    inline uint32_t GetSize() const { return size_; }

    static inline uint32_t GetSize(const int8_t* row) {
        return *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH));
    }

    int32_t GetValue(const int8_t* row, uint32_t idx, ::openmldb::type::DataType type, void* val) const;

    int32_t GetInteger(const int8_t* row, uint32_t idx, ::openmldb::type::DataType type, int64_t* val) const;

    int32_t GetValue(const int8_t* row, uint32_t idx, char** val, uint32_t* length) const;

    int32_t GetStrValue(const int8_t* row, uint32_t idx, std::string* val) const;
    int32_t GetStrValue(uint32_t idx, std::string* val) const;

 private:
    bool Init();
    bool CheckValid(uint32_t idx, ::openmldb::type::DataType type) const;

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

namespace v1 {

inline int8_t GetAddrSpace(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= 1 << 24) {
        return 3;
    } else {
        return 4;
    }
}

inline int8_t GetBoolField(const int8_t* row, uint32_t offset) {
    int8_t value = *(row + offset);
    return value;
}

inline int16_t GetInt16Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int16_t*>(row + offset));
}

inline int32_t GetInt32Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int32_t*>(row + offset));
}

inline int64_t GetInt64Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int64_t*>(row + offset));
}

inline float GetFloatField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const float*>(row + offset));
}

inline double GetDoubleField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const double*>(row + offset));
}

// native get string field method
int32_t GetStrField(const int8_t* row, uint32_t str_field_offset, uint32_t next_str_field_offset,
                    uint32_t str_start_offset, uint32_t addr_space, int8_t** data, uint32_t* size);
int32_t GetCol(int8_t* input, int32_t offset, int32_t type_id, int8_t* data);
int32_t GetStrCol(int8_t* input, int32_t str_field_offset, int32_t next_str_field_offset, int32_t str_start_offset,
                  int32_t type_id, int8_t* data);
}  // namespace v1

}  // namespace codec
}  // namespace openmldb

#endif  // SRC_CODEC_CODEC_H_
