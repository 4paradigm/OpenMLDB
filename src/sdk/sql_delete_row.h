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

#ifndef SRC_SDK_SQL_DELETE_ROW_H_
#define SRC_SDK_SQL_DELETE_ROW_H_

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "node/node_manager.h"
#include "proto/type.pb.h"

namespace openmldb::sdk {

struct Condition {
    Condition(const std::string& name, hybridse::node::FnOperator con,
            const std::optional<std::string>& v, openmldb::type::DataType type)
        : col_name(name), op(con), val(v), data_type(type) {}
    std::string col_name;
    hybridse::node::FnOperator op;
    std::optional<std::string> val;
    openmldb::type::DataType data_type;
};

class SQLDeleteRow {
 public:
    SQLDeleteRow(const std::string& db, const std::string& table_name,
            const std::vector<Condition>& condition_vec,
            const std::vector<Condition>& parameter_vec);

    void Reset();

    bool SetString(int pos, const std::string& val);
    bool SetBool(int pos, bool val);
    bool SetInt(int pos, int64_t val);
    bool SetTimestamp(int pos, int64_t val);
    bool SetDate(int pos, int32_t date);
    bool SetDate(int pos, uint32_t year, uint32_t month, uint32_t day);
    bool SetNULL(int pos);
    bool Build();

    const std::string& GetDatabase() const { return db_; }
    const std::string& GetTableName() const { return table_name_; }
    const std::vector<Condition>& GetValue() const { return value_; }

 private:
    const std::string db_;
    const std::string table_name_;
    std::vector<Condition> condition_vec_;
    std::vector<Condition> parameter_vec_;
    std::vector<Condition> result_;
    std::vector<Condition> value_;
    std::map<int, size_t> pos_map_;
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_SQL_DELETE_ROW_H_
