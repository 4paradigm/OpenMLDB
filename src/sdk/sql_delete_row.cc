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
#include "codec/codec.h"
#include "codec/fe_row_codec.h"

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
        if (val.empty()) {
            val_ = hybridse::codec::EMPTY_STRING;
        } else {
            val_ = val;
        }
    } else {
        auto iter = hole_column_map_.find(pos);
        if (iter == hole_column_map_.end()) {
            return false;
        }
        if (val.empty()) {
            col_values_.emplace(iter->second, hybridse::codec::EMPTY_STRING);
        } else {
            col_values_.emplace(iter->second, val);
        }
    }
    return true;
}

bool SQLDeleteRow::SetBool(int pos, bool val) {
    return SetString(pos, val ? "true" : "false");
}

bool SQLDeleteRow::SetInt(int pos, int64_t val) {
    return SetString(pos, std::to_string(val));
}

bool SQLDeleteRow::SetTimestamp(int pos, int64_t val) {
    return SetString(pos, std::to_string(val));
}

bool SQLDeleteRow::SetDate(int pos, int32_t val) {
    return SetString(pos, std::to_string(val));
}

bool SQLDeleteRow::SetDate(int pos, uint32_t year, uint32_t month, uint32_t day) {
    uint32_t date = 0;
    if (!openmldb::codec::RowBuilder::ConvertDate(year, month, day, &date)) {
        return false;
    }
    return SetString(pos, std::to_string(date));
}

bool SQLDeleteRow::SetNULL(int pos) {
    return SetString(pos, hybridse::codec::NONETOKEN);
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
