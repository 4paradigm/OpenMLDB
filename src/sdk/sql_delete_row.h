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
#include <string>
#include <vector>

namespace openmldb::sdk {

class SQLDeleteRow {
 public:
    SQLDeleteRow(const std::string& db, const std::string& table_name,
            const std::string& index, const std::vector<std::string>& col_names,
            const std::map<std::string, std::string>& default_value,
            const std::map<int, std::string> hole_column_map) :
        db_(db), table_name_(table_name), index_(index), col_names_(col_names) {}

    void Reset();

    bool SetString(int pos, const std::string& val);
    bool SetBool(int pos, bool val);
    bool SetInt(int pos, int64_t val);
    bool SetTimestamp(int pos, int64_t val);
    bool SetDate(int pos, int32_t date);
    bool SetDate(int pos, uint32_t year, uint32_t month, uint32_t day);
    bool SetNULL(int pos);
    bool Build();

    const std::string& GetValue() const { return val_; }
    const std::string& GetDatabase() const { return db_; }
    const std::string& GetTableName() const { return table_name_; }
    const std::string& GetIndexName() const { return index_; }

 private:
    const std::string db_;
    const std::string table_name_;
    const std::string index_;
    const std::vector<std::string> col_names_;
    const std::map<std::string, std::string> default_value_;
    const std::map<int, std::string> hole_column_map_;
    std::string val_;
    std::map<std::string, std::string> col_values_;
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_SQL_DELETE_ROW_H_
