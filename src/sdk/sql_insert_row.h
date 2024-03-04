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

#ifndef SRC_SDK_SQL_INSERT_ROW_H_
#define SRC_SDK_SQL_INSERT_ROW_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/hash.h"
#include "codec/codec.h"
#include "codec/fe_row_codec.h"
#include "node/sql_node.h"
#include "proto/name_server.pb.h"
#include "schema/schema_adapter.h"
#include "sdk/base.h"

namespace openmldb::sdk {

typedef std::shared_ptr<std::map<uint32_t, std::shared_ptr<::hybridse::node::ConstNode>>> DefaultValueMap;

// used in java to build InsertPreparedStatementCache
class DefaultValueContainer {
 public:
    explicit DefaultValueContainer(const DefaultValueMap& default_map) : default_map_(default_map) {}

    std::vector<uint32_t> GetAllPosition() {
        std::vector<uint32_t> vec;
        for (const auto& kv : *default_map_) {
            vec.push_back(kv.first);
        }
        return vec;
    }

    bool IsValid(int idx) {
        return idx >= 0 && idx < Size();
    }

    int Size() {
        return default_map_->size();
    }

    bool IsNull(int idx) {
        return default_map_->at(idx)->IsNull();
    }

    bool GetBool(int idx) {
        return default_map_->at(idx)->GetBool();
    }

    int16_t GetSmallInt(int idx) {
        return default_map_->at(idx)->GetSmallInt();
    }

    int32_t GetInt(int idx) {
        return default_map_->at(idx)->GetInt();
    }

    int64_t GetBigInt(int idx) {
        return default_map_->at(idx)->GetLong();
    }

    float GetFloat(int idx) {
        return default_map_->at(idx)->GetFloat();
    }

    double GetDouble(int idx) {
        return default_map_->at(idx)->GetDouble();
    }

    int32_t GetDate(int idx) {
        return default_map_->at(idx)->GetInt();
    }

    int64_t GetTimeStamp(int idx) {
        return default_map_->at(idx)->GetLong();
    }

    std::string GetString(int idx) {
        return default_map_->at(idx)->GetStr();
    }

 private:
    DefaultValueMap default_map_;
};

class SQLInsertRow {
 public:
    // for raw insert sql(no hole)
    SQLInsertRow(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                 std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map,
                 uint32_t default_str_length, bool put_if_absent);
    SQLInsertRow(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                 std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map,
                 uint32_t default_str_length, std::vector<uint32_t> hole_idx_arr, bool put_if_absent);
    SQLInsertRow(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                 std::shared_ptr<hybridse::sdk::Schema> schema, std::shared_ptr<int8_t> codegen_row, bool put_if_absent)
        : table_info_(table_info),
          schema_(schema),
          rb_(table_info->column_desc()),
          put_if_absent_(put_if_absent),
          is_codegen_row_(true) {
        auto size = hybridse::codec::RowView::GetSize(codegen_row.get());
        val_ = std::string(reinterpret_cast<char*>(codegen_row.get()), size);
        std::map<std::string, uint32_t> column_name_map;
        for (int idx = 0; idx < table_info_->column_desc_size(); idx++) {
            column_name_map.emplace(table_info_->column_desc(idx).name(), idx);
        }
        if (table_info_->column_key_size() > 0) {
            index_map_.clear();
            raw_dimensions_.clear();
            for (int idx = 0; idx < table_info_->column_key_size(); ++idx) {
                const auto& index = table_info_->column_key(idx);
                if (index.flag()) {
                    continue;
                }
                for (const auto& column : index.col_name()) {
                    index_map_[idx].push_back(column_name_map[column]);
                    raw_dimensions_[column_name_map[column]] = hybridse::codec::NONETOKEN;
                }
                if (!index.ts_name().empty()) {
                    ts_set_.insert(column_name_map[index.ts_name()]);
                }
            }
        }
    }

    ~SQLInsertRow() = default;
    bool Init(int str_length);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const std::string& val);
    bool AppendDate(uint32_t year, uint32_t month, uint32_t day);
    bool AppendDate(int32_t date);

    // If you don't have the value of the column, append null, this method will help you to set:
    // 1. if the column can be null, set null
    // 2. if not, it will set to default value(from DefaultValueMap)
    bool AppendNULL();
    bool IsComplete();
    bool Build() const;
    const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& GetDimensions();
    inline const std::string& GetRow() { return val_; }
    inline const std::shared_ptr<hybridse::sdk::Schema> GetSchema() { return schema_; }

    std::vector<uint32_t> GetHoleIdx() { return hole_idx_arr_; }

    bool AppendString(const char* string_buffer_var_name, uint32_t length);

    // If the insert sql has columns names, the hole idx array is consistent with insert sql, not the table schema.
    // e.g. table t1(c1,c2,c3)
    // insert into t1 (c3,c2,c1) values(?,2,?);
    // the hole idx array is {2,0}
    //
    // If empty(insert stmt has no column names, e.g. insert into t1 values(?,?,?)), the array should be in schema
    // order.
    static std::vector<uint32_t> GetHoleIdxArr(const DefaultValueMap& default_map,
                                               const std::vector<uint32_t>& stmt_column_idx_in_table,
                                               const std::shared_ptr<::hybridse::sdk::Schema>& schema);

    std::shared_ptr<DefaultValueContainer> GetDefaultValue() {
        return std::make_shared<DefaultValueContainer>(default_map_);
    }

    ::openmldb::nameserver::TableInfo GetTableInfo() {
        return *table_info_;
    }

    bool IsPutIfAbsent() const {
        return put_if_absent_;
    }

 private:
    bool MakeDefault();
    void PackDimension(const std::string& val);
    inline bool IsDimension() { return raw_dimensions_.find(rb_.GetAppendPos()) != raw_dimensions_.end(); }
    inline bool IsTsCol() { return ts_set_.find(rb_.GetAppendPos()) != ts_set_.end(); }

 private:
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info_;
    std::shared_ptr<hybridse::sdk::Schema> schema_;
    DefaultValueMap default_map_;
    uint32_t default_string_length_;
    std::vector<uint32_t> hole_idx_arr_;

    std::map<uint32_t, std::vector<uint32_t>> index_map_;
    std::set<uint32_t> ts_set_;
    std::map<uint32_t, std::string> raw_dimensions_;
    std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions_;
    ::openmldb::codec::RowBuilder rb_;
    std::string val_;
    uint32_t str_size_;
    bool put_if_absent_;

    bool is_codegen_row_ = false;
};

class SQLInsertRows {
 public:
    SQLInsertRows(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                  std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map, uint32_t str_size,
                  const std::vector<uint32_t>& hole_idx_arr, bool put_if_absent);
    ~SQLInsertRows() = default;
    std::shared_ptr<SQLInsertRow> NewRow();
    inline uint32_t GetCnt() { return rows_.size(); }
    inline std::shared_ptr<SQLInsertRow> GetRow(uint32_t i) {
        if (i >= rows_.size()) {
            return {};
        }
        return rows_[i];
    }
    inline const std::shared_ptr<hybridse::sdk::Schema> GetSchema() { return schema_; }
    std::vector<uint32_t> GetHoleIdx() { return hole_idx_arr_; }

 private:
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info_;
    std::shared_ptr<hybridse::sdk::Schema> schema_;
    DefaultValueMap default_map_;
    uint32_t default_str_length_;
    std::vector<uint32_t> hole_idx_arr_;
    bool put_if_absent_;

    std::vector<std::shared_ptr<SQLInsertRow>> rows_;
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_SQL_INSERT_ROW_H_
