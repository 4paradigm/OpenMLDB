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

#include "sdk/sql_delete_row.h"

namespace openmldb::sdk {

void SQLDeleteRow::Reset() {
    val_.clear();
    col_values_.clear();
}

bool SQLDeleteRow::SetString(int pos, const std::string& val) {
    if (pos > static_cast<int>(col_names_.size())) {
        return false;
    }
    if (col_names_.size() == 1) {
        val_ = val;
    } else {
        auto iter = hole_column_map_.find(pos);
        if (iter == hole_column_map_.end()) {
            return false;
        }
        col_values_.emplace(iter->second, val);
    }
    return true;
}

bool SQLDeleteRow::Build() {
    if (col_names_.size() == 1) {
        return !val_.empty();
    }
    if (col_values_.size() != hole_column_map_.size()) {
        return false;
    }
    val_.clear();
    for (const auto& name : col_names_) {
        if (!val_.empty()) {
            val_.append("|");
        }
        auto iter = default_value_.find(name);
        if (iter != default_value_.end()) {
            val_.append(iter->second);
            continue;
        }
        iter = col_values_.find(name);
        if (iter == col_values_.end()) {
            return false;
        }
        val_.append(iter->second);
    }
    return true;
}

}  // namespace openmldb::sdk
