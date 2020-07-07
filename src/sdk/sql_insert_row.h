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

#include "boost/lexical_cast.hpp"
#include "proto/name_server.pb.h"
#include "sdk/base.h"

namespace rtidb {
namespace sdk {

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

struct DefaultValue {
    ::fesql::sdk::DataType type;
    std::string value;
};

class SQLInsertRow {
 public:
    SQLInsertRow() {}
    explicit SQLInsertRow(
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
        std::shared_ptr<std::map<uint32_t, DefaultValue>> default_map,
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
    bool AppendNULL();
    bool Build();
    bool IsComplete();
    inline const std::string& GetRow() { return val_; }
    inline const std::vector<std::pair<std::string, uint32_t>>&
    GetDimensions() {
        return dimensions_;
    }
    inline const std::vector<uint64_t>& GetTs() { return ts_; }
    inline const std::shared_ptr<::rtidb::nameserver::TableInfo>
    GetTableInfo() {
        return table_info_;
    }

 private:
    bool Check(fesql::sdk::DataType type);
    bool MakeDefault();
    bool GetIndex(const std::string& val);
    bool GetTs(uint64_t ts);
    bool AppendBool(const std::string& val);
    bool AppendInt32(const std::string& val);
    bool AppendInt16(const std::string& val);
    bool AppendInt64(const std::string& val);
    bool AppendTimestamp(const std::string& val);
    bool AppendFloat(const std::string& val);
    bool AppendDouble(const std::string& val);

 private:
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info_;
    std::shared_ptr<std::map<uint32_t, DefaultValue>> default_map_;
    std::set<uint32_t> index_set_;
    std::set<uint32_t> ts_set_;
    std::vector<std::pair<std::string, uint32_t>> dimensions_;
    std::vector<uint64_t> ts_;
    uint32_t cnt_;
    uint32_t size_;
    uint32_t str_field_cnt_;
    uint32_t str_addr_length_;
    uint32_t str_field_start_offset_;
    uint32_t str_offset_;
    std::vector<uint32_t> offset_vec_;
    std::string val_;
    int8_t* buf_;
};

class SQLInsertRows {
 public:
    SQLInsertRows(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
                  std::shared_ptr<std::map<uint32_t, DefaultValue>> default_map,
                  uint32_t str_size);
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
    std::shared_ptr<std::map<uint32_t, DefaultValue>> default_map_;
    uint32_t default_str_length_;
    std::vector<std::shared_ptr<SQLInsertRow>> rows_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_SQL_INSERT_ROW_H_
