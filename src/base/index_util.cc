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
#include "base/index_util.h"

#include <map>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "base/glog_wrapper.h"
#include "storage/schema.h"

namespace openmldb::base {
// <pkey_col_name, (idx_in_row, type)>, error if empty
std::map<std::string, std::pair<uint32_t, type::DataType>> MakePkeysHint(const codec::Schema& schema,
                                                                         const common::ColumnKey& cidx_ck) {
    if (cidx_ck.col_name().empty()) {
        LOG(WARNING) << "empty cidx column key";
        return {};
    }
    // pkey col idx in row
    std::set<std::string> pkey_set;
    for (int i = 0; i < cidx_ck.col_name().size(); i++) {
        pkey_set.insert(cidx_ck.col_name().Get(i));
    }
    if (pkey_set.empty()) {
        LOG(WARNING) << "empty pkey set";
        return {};
    }
    if (pkey_set.size() != static_cast<std::set<std::string>::size_type>(cidx_ck.col_name().size())) {
        LOG(WARNING) << "pkey set size not equal to cidx pkeys size";
        return {};
    }
    std::map<std::string, std::pair<uint32_t, type::DataType>> col_idx;
    for (int i = 0; i < schema.size(); i++) {
        if (pkey_set.find(schema.Get(i).name()) != pkey_set.end()) {
            col_idx[schema.Get(i).name()] = {i, schema.Get(i).data_type()};
        }
    }
    if (col_idx.size() != pkey_set.size()) {
        LOG(WARNING) << "col idx size not equal to cidx pkeys size";
        return {};
    }
    return col_idx;
}

// error if empty
std::string MakeDeleteSQL(const std::string& db, const std::string& name, const common::ColumnKey& cidx_ck,
                          const int8_t* values, uint64_t ts, const codec::RowView& row_view,
                          const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx) {
    auto sql_prefix = absl::StrCat("delete from ", db, ".", name, " where ");
    std::string cond;
    for (int i = 0; i < cidx_ck.col_name().size(); i++) {
        // append primary keys, pkeys in dimension are encoded, so we should get them from raw value
        // split can't work if string has `|`
        auto& col_name = cidx_ck.col_name().Get(i);
        auto col = col_idx.find(col_name);
        if (col == col_idx.end()) {
            LOG(WARNING) << "col " << col_name << " not found in col idx";
            return "";
        }
        std::string val;
        row_view.GetStrValue(values, col->second.first, &val);
        if (!cond.empty()) {
            absl::StrAppend(&cond, " and ");
        }
        // TODO(hw): string should add quotes how about timestamp?
        // check existence before, so here we skip
        absl::StrAppend(&cond, col_name);
        if (auto t = col->second.second; t == type::kVarchar || t == type::kString) {
            absl::StrAppend(&cond, "=\"", val, "\"");
        } else {
            absl::StrAppend(&cond, "=", val);
        }
    }
    // ts must be integer, won't be string
    if (!cidx_ck.ts_name().empty() && cidx_ck.ts_name() != storage::DEFAULT_TS_COL_NAME) {
        if (!cond.empty()) {
            absl::StrAppend(&cond, " and ");
        }
        absl::StrAppend(&cond, cidx_ck.ts_name(), "=", std::to_string(ts));
    }
    auto sql = absl::StrCat(sql_prefix, cond, ";");
    // TODO(hw): if delete failed, we can't revert. And if sidx skeys+sts doesn't change, no need to delete and
    // then insert
    DLOG(INFO) << "delete sql " << sql;
    return sql;
}

// error if empty
std::string ExtractPkeys(const common::ColumnKey& cidx_ck, const int8_t* values, const codec::RowView& row_view,
                         const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx) {
    // join with |
    std::vector<std::string> pkeys;
    for (int i = 0; i < cidx_ck.col_name().size(); i++) {
        auto& col_name = cidx_ck.col_name().Get(i);
        auto col = col_idx.find(col_name);
        if (col == col_idx.end()) {
            LOG(WARNING) << "col " << col_name << " not found in col idx";
            return "";
        }
        std::string val;
        row_view.GetStrValue(values, col->second.first, &val);
        pkeys.push_back(val);
    }
    return absl::StrJoin(pkeys, "|");
}

}  // namespace openmldb::base
