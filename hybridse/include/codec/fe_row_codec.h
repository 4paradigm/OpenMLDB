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

#ifndef HYBRIDSE_INCLUDE_CODEC_FE_ROW_CODEC_H_
#define HYBRIDSE_INCLUDE_CODEC_FE_ROW_CODEC_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/status/statusor.h"
#include "base/fe_status.h"
#include "base/fe_slice.h"
#include "base/raw_buffer.h"
#include "codec/row.h"
#include "proto/fe_type.pb.h"

namespace hybridse {

namespace node {
class ExprNode;
}
namespace codegen {
class InsertRowBuilder;
}
namespace vm {
class HybridSeJitWrapper;
}

namespace codec {

const uint32_t BitMapSize(uint32_t size);

typedef ::google::protobuf::RepeatedPtrField<::hybridse::type::ColumnDef>
    Schema;

static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
static constexpr uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static constexpr uint32_t UINT24_MAX = (1 << 24) - 1;
const std::string NONETOKEN = "!N@U#L$L%";  // NOLINT
const std::string EMPTY_STRING = "!@#$%";   // NOLINT

const std::unordered_map<::hybridse::type::Type, uint8_t>& GetTypeSizeMap();

// return true if the column considered base type in row codec.
// date & timestamp consider base type since they have single field in corresponding llvm struct,
// while string, map and array consider complex type.
//
// for base types, the column is written into row ptr by just writing the value of primitive type,
// for comple type, written is made by a string (or string-like) manner: str size + str data.
// map, array, or any other complex types, takes a extra encoding from their struct value into str data.
bool IsCodecBaseType(const type::ColumnSchema& sc);
bool IsCodecStrLikeType(const type::ColumnSchema& sc);

// returns the corresponding SQL string representation for input ColumnSchema
absl::StatusOr<std::string> ColumnSchemaStr(const type::ColumnSchema&);

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


// single slice builder from pure codegen
class SliceBuilder {
 public:
    SliceBuilder(vm::HybridSeJitWrapper*, const hybridse::codec::Schema* schema);
    virtual ~SliceBuilder() {}

    base::Status Build(const std::vector<node::ExprNode*>&, base::RefCountedSlice*);

    base::Status Build(absl::Span<node::ExprNode* const>, base::RefCountedSlice*);

 private:
    void EnsureInitialized() {
        assert(row_builder_ != nullptr && "must initialize the row builder before encoding");
    }

    const Schema* schema_;
    std::shared_ptr<codegen::InsertRowBuilder> row_builder_ = nullptr;
};

// new row builder from pure codegen
class RowBuilder2 {
 public:
    RowBuilder2(vm::HybridSeJitWrapper*, int sliceSize);
    RowBuilder2(vm::HybridSeJitWrapper*, const std::vector<codec::Schema>& schemas);
    RowBuilder2(vm::HybridSeJitWrapper*, const std::vector<std::vector<hybridse::type::ColumnDef>>& schemas);
    ~RowBuilder2() {}

    base::Status Init();

    base::Status InitSchema(int idx, const codec::Schema& sc);

    base::Status Build(const std::vector<node::ExprNode*>&, codec::Row*);

 private:
    void EnsureInitialized() {
        assert(initialized_ && "RowBuild not initialized");
    }

    vm::HybridSeJitWrapper* jit_ = nullptr;
    std::vector<codec::Schema> schemas_;
    std::vector<std::shared_ptr<SliceBuilder>> builders_;

    bool initialized_ = false;
};

// Old row builder in C
// limited data type support, no map, no array. U should upgrade to RowBuilder2
class RowBuilder {
 public:
    explicit RowBuilder(const hybridse::codec::Schema& schema);
    ~RowBuilder() = default;
    uint32_t CalTotalLength(uint32_t string_length);
    bool SetBuffer(int8_t* buf, uint32_t size);
    bool SetBuffer(int64_t buf_handle, uint32_t size);
    bool SetBuffer(const hybridse::base::RawBuffer& buf);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendDate(int32_t year, int32_t month, int32_t day);
    bool AppendDate(int32_t date);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const char* val, uint32_t length);
    bool AppendNULL();

 private:
    bool Check(::hybridse::type::Type type);

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
    RowView();
    RowView(const hybridse::codec::Schema& schema, const int8_t* row,
            uint32_t size);
    explicit RowView(const hybridse::codec::Schema& schema);
    RowView(const RowView& row_view);
    ~RowView() = default;
    bool Reset(const int8_t* row, uint32_t size);
    bool Reset(const int8_t* row);
    bool Reset(const hybridse::base::RawBuffer& buf);
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
    int32_t GetValue(const int8_t* row, uint32_t idx,
                     ::hybridse::type::Type type, void* val) const;

    int32_t GetInteger(const int8_t* row, uint32_t idx,
                       ::hybridse::type::Type type, int64_t* val);

    int32_t GetValue(const int8_t* row, uint32_t idx, const char** val,
                     uint32_t* length) const;

    std::string GetAsString(uint32_t idx);
    std::string GetRowString();
    int32_t GetPrimaryFieldOffset(uint32_t idx);
    const Schema* GetSchema() const { return &schema_; }

    inline bool IsNULL(const int8_t* row, uint32_t idx) const {
        if (row == nullptr) {
            return true;
        }
        const int8_t* ptr = row + HEADER_LENGTH + (idx >> 3);
        return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
    }

 private:
    bool Init();
    bool CheckValid(uint32_t idx, ::hybridse::type::Type type);

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
    // type is still used in same lagecy udf context,
    // cautious use for non-base types
    ::hybridse::type::Type type() const {
        if (!schema.has_base_type()) {
            return type::kNull;
        }
        return schema.base_type();
    }

    uint32_t idx;
    uint32_t offset;
    std::string name;
    type::ColumnSchema schema;

    ColInfo() {}
    ColInfo(const std::string& name, ::hybridse::type::Type type, uint32_t idx, uint32_t offset)
        : idx(idx), offset(offset), name(name) {
        schema.set_base_type(type);
    }

    ColInfo(const std::string& name, const type::ColumnSchema& sc, uint32_t idx, uint32_t offset)
        : idx(idx), offset(offset), name(name), schema(sc) {}
};

struct StringColInfo : public ColInfo {
    uint32_t str_next_offset;
    uint32_t str_start_offset;

    StringColInfo(const std::string& name, ::hybridse::type::ColumnSchema sc,
                  uint32_t idx, uint32_t offset, uint32_t str_next_offset,
                  uint32_t str_start_offset)
        : ColInfo(name, sc, idx, offset),
          str_next_offset(str_next_offset),
          str_start_offset(str_start_offset) {}
};

class SliceFormat {
 public:
    explicit SliceFormat(const hybridse::codec::Schema* schema);
    virtual ~SliceFormat() {}

    absl::StatusOr<StringColInfo> GetStringColumnInfo(size_t idx) const;

    const ColInfo* GetColumnInfo(size_t idx) const;

 private:
    const hybridse::codec::Schema* schema_;
    std::vector<ColInfo> infos_;
    std::map<std::string, size_t> infos_dict_;
    std::map<uint32_t, uint32_t> next_str_pos_;
    uint32_t str_field_start_offset_;
};

class RowFormat {
 public:
    virtual ~RowFormat() {}
    virtual absl::StatusOr<StringColInfo> GetStringColumnInfo(size_t schema_idx, size_t idx) const = 0;
    virtual const ColInfo* GetColumnInfo(size_t schema_idx, size_t idx) const = 0;
    virtual size_t GetSliceId(size_t schema_idx) const = 0;
};

class MultiSlicesRowFormat : public RowFormat {
 public:
    explicit MultiSlicesRowFormat(const Schema* schema) {
        slice_formats_.emplace_back(schema);
    }

    explicit MultiSlicesRowFormat(const std::vector<const Schema*>& schemas) {
        for (auto schema : schemas) {
            slice_formats_.emplace_back(schema);
        }
    }

    ~MultiSlicesRowFormat() override {
        slice_formats_.clear();
    }

    absl::StatusOr<StringColInfo> GetStringColumnInfo(size_t schema_idx, size_t idx) const override {
        return slice_formats_[schema_idx].GetStringColumnInfo(idx);
    }

    const ColInfo* GetColumnInfo(size_t schema_idx, size_t idx) const override {
        return slice_formats_[schema_idx].GetColumnInfo(idx);
    }

    size_t GetSliceId(size_t schema_idx) const override {
        return schema_idx;
    }

 private:
    std::vector<SliceFormat> slice_formats_;
};

class SingleSliceRowFormat : public RowFormat {
 public:
    explicit SingleSliceRowFormat(const Schema* schema) {
        slice_format_ = new SliceFormat(schema);
        offsets_.emplace_back(0);
    }

    explicit SingleSliceRowFormat(const std::vector<const Schema*>& schemas) {
        int offset = 0;
        for (auto schema : schemas) {
            offsets_.emplace_back(offset);
            offset += schema->size();
            // Merge schema and make sure it is appended
            merged_schema_.MergeFrom(*schema);
        }

        slice_format_ = new SliceFormat(&merged_schema_);
    }

    ~SingleSliceRowFormat() override {
        offsets_.clear();
        if (slice_format_) {
            delete slice_format_;
        }
    }

    absl::StatusOr<StringColInfo> GetStringColumnInfo(size_t schema_idx, size_t idx) const override {
        return slice_format_->GetStringColumnInfo(offsets_[schema_idx] + idx);
    }

    const ColInfo* GetColumnInfo(size_t schema_idx, size_t idx) const override {
        return slice_format_->GetColumnInfo(offsets_[schema_idx] + idx);
    }

    size_t GetSliceId(size_t schema_idx) const override {
        return 0;
    }

 private:
    std::vector<size_t> offsets_;
    SliceFormat* slice_format_;
    Schema merged_schema_;
};

}  // namespace codec
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_CODEC_FE_ROW_CODEC_H_
