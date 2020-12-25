/*
 * sql_insert_row.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_SDK_SQL_INSERT_ROW_H_
#define SRC_SDK_SQL_INSERT_ROW_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/hash.h"
#include "boost/lexical_cast.hpp"
#include "codec/codec.h"
#include "codec/fe_row_codec.h"
#include "node/sql_node.h"
#include "proto/name_server.pb.h"
#include "sdk/base.h"

namespace rtidb {
namespace sdk {

typedef std::shared_ptr<std::map<uint32_t, std::shared_ptr<::fesql::node::ConstNode>>> DefaultValueMap;

static inline ::fesql::sdk::DataType ConvertType(::rtidb::type::DataType type) {
    switch (type) {
        case rtidb::type::kBool:
            return ::fesql::sdk::kTypeBool;
        case rtidb::type::kSmallInt:
            return ::fesql::sdk::kTypeInt16;
        case rtidb::type::kInt:
            return ::fesql::sdk::kTypeInt32;
        case rtidb::type::kBigInt:
            return ::fesql::sdk::kTypeInt64;
        case rtidb::type::kFloat:
            return ::fesql::sdk::kTypeFloat;
        case rtidb::type::kDouble:
            return ::fesql::sdk::kTypeDouble;
        case rtidb::type::kTimestamp:
            return ::fesql::sdk::kTypeTimestamp;
        case rtidb::type::kString:
        case rtidb::type::kVarchar:
            return ::fesql::sdk::kTypeString;
        default:
            return ::fesql::sdk::kTypeUnknow;
    }
}

class SQLInsertRow {
 public:
    explicit SQLInsertRow(
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
        std::shared_ptr<fesql::sdk::Schema> schema, DefaultValueMap default_map,
        uint32_t default_str_length);
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
    bool AppendNULL();
    bool IsComplete();
    bool Build();
    const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& GetDimensions();
    inline const std::vector<uint64_t>& GetTs() { return ts_; }
    inline const std::string& GetRow() { return val_; }
    inline const std::shared_ptr<fesql::sdk::Schema> GetSchema() {
        return schema_;
    }

    const std::vector<uint32_t> GetHoleIdx() {
        std::vector<uint32_t> result;
        for (uint32_t i = 0; i < (int64_t)schema_->GetColumnCnt(); ++i) {
            if (default_map_->count(i) == 0) {
                result.push_back(i);
            }
        }
        return std::move(result);
    }

    bool AppendStringByteBufferVarName(char* string_buffer_var_name, uint32_t length);

 private:
    bool DateToString(uint32_t year, uint32_t month, uint32_t day,
                      std::string* date);
    bool AppendString(const char* val, uint32_t length);
    bool MakeDefault();
    bool PackTs(uint64_t ts);
    void PackDimension(const std::string& val);
    inline bool IsDimension() {
        return raw_dimensions_.find(rb_.GetAppendPos()) !=
               raw_dimensions_.end();
    }

 private:
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info_;
    std::shared_ptr<fesql::sdk::Schema> schema_;
    DefaultValueMap default_map_;
    uint32_t default_string_length_;
    std::map<uint32_t, std::vector<uint32_t>> index_map_;
    std::set<uint32_t> ts_set_;
    std::map<uint32_t, std::string> raw_dimensions_;
    std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions_;
    std::vector<uint64_t> ts_;
    ::rtidb::codec::RowBuilder rb_;
    std::string val_;
    uint32_t str_size_;
};

class SQLInsertRows {
 public:
    SQLInsertRows(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
                  std::shared_ptr<fesql::sdk::Schema> schema,
                  DefaultValueMap default_map, uint32_t str_size);
    ~SQLInsertRows() = default;
    std::shared_ptr<SQLInsertRow> NewRow();
    inline uint32_t GetCnt() { return rows_.size(); }
    inline std::shared_ptr<SQLInsertRow> GetRow(uint32_t i) {
        if (i >= rows_.size()) {
            return std::shared_ptr<SQLInsertRow>();
        }
        return rows_[i];
    }

 private:
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info_;
    std::shared_ptr<fesql::sdk::Schema> schema_;
    DefaultValueMap default_map_;
    uint32_t default_str_length_;
    std::vector<std::shared_ptr<SQLInsertRow>> rows_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_SQL_INSERT_ROW_H_
