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

#ifndef SRC_SDK_SQL_REQUEST_ROW_H_
#define SRC_SDK_SQL_REQUEST_ROW_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/fe_slice.h"
#include "codec/fe_row_codec.h"
#include "codec/fe_row_selector.h"
#include "sdk/base.h"

namespace openmldb {
namespace sdk {
class SQLRequestRow {
 public:
    SQLRequestRow() {}
    SQLRequestRow(std::shared_ptr<hybridse::sdk::Schema> schema, const std::set<std::string>& record_cols);
    ~SQLRequestRow() {}
    bool Init(int32_t str_length);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendDate(int32_t val);
    bool AppendDate(int32_t year, int32_t month, int32_t day);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const std::string& val);
    bool AppendString(const char* string_buffer_var_name, uint32_t length);
    bool AppendNULL();
    bool Build();
    inline bool OK() { return is_ok_; }
    inline const std::string& GetRow() { return val_; }
    inline const std::shared_ptr<hybridse::sdk::Schema> GetSchema() { return schema_; }
    bool GetRecordVal(const std::string& col, std::string* val);

    static std::shared_ptr<openmldb::sdk::SQLRequestRow> CreateSQLRequestRowFromColumnTypes(
        std::shared_ptr<hybridse::sdk::ColumnTypes> types);

 private:
    bool Check(hybridse::sdk::DataType type);

 private:
    std::shared_ptr<hybridse::sdk::Schema> schema_;
    uint32_t cnt_;
    uint32_t size_;
    uint32_t str_field_cnt_;
    uint32_t str_addr_length_;
    uint32_t str_field_start_offset_;
    uint32_t str_offset_;
    std::vector<uint32_t> offset_vec_;
    std::string val_;
    int8_t* buf_;
    uint32_t str_length_expect_;
    uint32_t str_length_current_;
    bool has_error_;
    bool is_ok_;
    std::set<uint32_t> record_cols_;
    std::map<std::string, std::string> record_value_;
};

class ColumnIndicesSet;

/**
 * SDK input interface for batch request, with common column encoding optimization.
 */
class SQLRequestRowBatch {
 public:
    SQLRequestRowBatch(std::shared_ptr<hybridse::sdk::Schema> schema, std::shared_ptr<ColumnIndicesSet> indices);
    bool AddRow(std::shared_ptr<SQLRequestRow> row);
    int Size() const { return non_common_slices_.size(); }

    const std::set<size_t>& common_column_indices() const { return common_column_indices_; }

    const std::string* GetCommonSlice() const { return &common_slice_; }

    const std::string* GetNonCommonSlice(uint32_t idx) const {
        if (idx >= non_common_slices_.size()) {
            return nullptr;
        }
        return &non_common_slices_[idx];
    }

    void Clear() {
        common_slice_.clear();
        non_common_slices_.clear();
    }

 private:
    ::hybridse::codec::Schema request_schema_;
    std::set<size_t> common_column_indices_;

    std::unique_ptr<::hybridse::codec::RowSelector> common_selector_;
    std::unique_ptr<::hybridse::codec::RowSelector> non_common_selector_;

    std::string common_slice_;
    std::vector<std::string> non_common_slices_;
};

class ColumnIndicesSet {
 public:
    ColumnIndicesSet() {}
    explicit ColumnIndicesSet(std::shared_ptr<hybridse::sdk::Schema> schema) : bound_(schema->GetColumnCnt()) {}

    bool Empty() const { return common_column_indices_.empty(); }

    void AddCommonColumnIdx(size_t idx);

 private:
    friend class SQLRequestRowBatch;
    size_t bound_ = 0;
    std::set<size_t> common_column_indices_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_SQL_REQUEST_ROW_H_
