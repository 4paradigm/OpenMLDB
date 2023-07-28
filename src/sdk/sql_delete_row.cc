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

#include <utility>
#include "absl/strings/numbers.h"
#include "codec/codec.h"
#include "codec/fe_row_codec.h"

namespace openmldb::sdk {

SQLDeleteRow::SQLDeleteRow(const std::string& db, const std::string& table_name,
            const std::vector<Condition>& condition_vec,
            const std::vector<Condition>& parameter_vec) :
        db_(db), table_name_(table_name), condition_vec_(condition_vec),
        parameter_vec_(parameter_vec), result_(condition_vec_), value_(), pos_map_() {
    for (size_t idx = 0; idx < parameter_vec_.size(); ++idx) {
        int pos = 0;
        if (absl::SimpleAtoi(parameter_vec.at(idx).val.value(), &pos)) {
            pos_map_.emplace(pos, idx);
        }
    }
}

void SQLDeleteRow::Reset() {
    result_.clear();
    result_.insert(result_.end(), condition_vec_.begin(), condition_vec_.end());
}

bool SQLDeleteRow::SetString(int pos, const std::string& val) {
    auto iter = pos_map_.find(pos);
    if (iter == pos_map_.end()) {
        return false;
    }
    auto con = parameter_vec_.at(iter->second);
    if (con.data_type != ::openmldb::type::kVarchar && con.data_type != ::openmldb::type::kString) {
        return false;
    }
    con.val = val;
    if (con.val.value().empty()) {
        con.val = hybridse::codec::EMPTY_STRING;
    }
    result_.emplace_back(std::move(con));
    return true;
}

bool SQLDeleteRow::SetBool(int pos, bool val) {
    auto iter = pos_map_.find(pos);
    if (iter == pos_map_.end()) {
        return false;
    }
    auto con = parameter_vec_.at(iter->second);
    if (con.data_type != ::openmldb::type::kBool) {
        return false;
    }
    con.val = val ? "true" : "false";
    result_.emplace_back(std::move(con));
    return true;
}

bool SQLDeleteRow::SetInt(int pos, int64_t val) {
    auto iter = pos_map_.find(pos);
    if (iter == pos_map_.end()) {
        return false;
    }
    auto con = parameter_vec_.at(iter->second);
    if (con.data_type != ::openmldb::type::kSmallInt && con.data_type != ::openmldb::type::kInt &&
        con.data_type != ::openmldb::type::kBigInt) {
        return false;
    }
    con.val = std::to_string(val);
    result_.emplace_back(std::move(con));
    return true;
}

bool SQLDeleteRow::SetTimestamp(int pos, int64_t val) {
    auto iter = pos_map_.find(pos);
    if (iter == pos_map_.end()) {
        return false;
    }
    auto con = parameter_vec_.at(iter->second);
    if (con.data_type != ::openmldb::type::kTimestamp) {
        return false;
    }
    con.val = std::to_string(val);
    result_.emplace_back(std::move(con));
    return true;
}

bool SQLDeleteRow::SetDate(int pos, int32_t val) {
    auto iter = pos_map_.find(pos);
    if (iter == pos_map_.end()) {
        return false;
    }
    auto con = parameter_vec_.at(iter->second);
    if (con.data_type != ::openmldb::type::kDate) {
        return false;
    }
    con.val = std::to_string(val);
    result_.emplace_back(std::move(con));
    return true;
}

bool SQLDeleteRow::SetDate(int pos, uint32_t year, uint32_t month, uint32_t day) {
    uint32_t date = 0;
    if (!openmldb::codec::RowBuilder::ConvertDate(year, month, day, &date)) {
        return false;
    }
    return SetDate(pos, date);
}

bool SQLDeleteRow::SetNULL(int pos) {
    auto iter = pos_map_.find(pos);
    if (iter == pos_map_.end()) {
        return false;
    }
    auto con = parameter_vec_.at(iter->second);
    con.val = hybridse::codec::NONETOKEN;
    result_.emplace_back(std::move(con));
    return true;
}

bool SQLDeleteRow::Build() {
    value_.swap(result_);
    result_.clear();
    return true;
}

}  // namespace openmldb::sdk
