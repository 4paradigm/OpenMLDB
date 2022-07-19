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
#include "sdk/base.h"

namespace openmldb::sdk {

typedef std::shared_ptr<std::map<uint32_t, std::shared_ptr<::hybridse::node::ConstNode>>> DefaultValueMap;

class SQLInsertRow {
 public:
    SQLInsertRow(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                 std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map,
                 uint32_t default_str_length);
    SQLInsertRow(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                 std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map,
                 uint32_t default_str_length, std::vector<uint32_t> hole_idx_arr);
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

 private:
    bool MakeDefault();
    void PackDimension(const std::string& val);
    inline bool IsDimension() { return raw_dimensions_.find(rb_.GetAppendPos()) != raw_dimensions_.end(); }

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
};

class SQLInsertRows {
 public:
    SQLInsertRows(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                  std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map, uint32_t str_size,
                  const std::vector<uint32_t>& hole_idx_arr);
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

    std::vector<std::shared_ptr<SQLInsertRow>> rows_;
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_SQL_INSERT_ROW_H_
