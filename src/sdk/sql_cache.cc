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

#include "sdk/sql_cache.h"
namespace openmldb {
namespace sdk {

bool RouterSQLCache::IsCompatibleCache(const std::shared_ptr<::hybridse::sdk::Schema>& other_parameter_schema) const {
    if (!parameter_schema_ && !other_parameter_schema) {
        return true;
    }
    if (!parameter_schema_ || !other_parameter_schema) {
        return false;
    }
    if (parameter_schema_->GetColumnCnt() != other_parameter_schema->GetColumnCnt()) {
        return false;
    }
    for (int i = 0; i < parameter_schema_->GetColumnCnt(); i++) {
        if (parameter_schema_->GetColumnType(i) != other_parameter_schema->GetColumnType(i)) {
            return false;
        }
    }
    return true;
}

DeleteSQLCache::DeleteSQLCache(const std::string& db, uint32_t tid, const std::string& table_name,
        const openmldb::common::ColumnKey& column_key,
        const std::map<std::string, std::string>& default_value,
        const std::map<std::string, int>& parameter_map)
    : SQLCache(db, tid, table_name),
    index_name_(column_key.index_name()), default_value_(default_value) {
    for (const auto& col : column_key.col_name()) {
        col_names_.push_back(col);
    }
    for (const auto& kv : parameter_map) {
        hole_column_map_.emplace(kv.second, kv.first);
    }
}

}  // namespace sdk
}  // namespace openmldb
