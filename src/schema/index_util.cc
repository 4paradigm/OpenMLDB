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

#include "schema/index_util.h"
#include <map>
#include <string>
#include <set>
#include <utility>
#include "common/timer.h"
#include "gflags/gflags.h"
#include "vm/catalog.h"

DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(latest_ttl_max);

namespace openmldb {
namespace schema {

static const std::map<::openmldb::type::TTLType, ::hybridse::type::TTLType> TTL_TYPE_MAP = {
    {::openmldb::type::kAbsoluteTime, ::hybridse::type::kTTLTimeLive},
    {::openmldb::type::kLatestTime, ::hybridse::type::kTTLCountLive},
    {::openmldb::type::kAbsAndLat, ::hybridse::type::kTTLTimeLiveAndCountLive},
    {::openmldb::type::kAbsOrLat, ::hybridse::type::kTTLTimeLiveOrCountLive}};

base::Status IndexUtil::CheckIndex(const std::map<std::string, ::openmldb::common::ColumnDesc>& column_map,
        const PBIndex& index) {
    if (index.size() == 0) {
        return {base::ReturnCode::kError, "no index"};
    }
    std::set<std::string> index_set;
    for (const auto& column_key : index) {
        bool has_iter = false;
        std::set<std::string> col_set;
        for (const auto& column_name : column_key.col_name()) {
            if (col_set.count(column_name) > 0) {
                return {base::ReturnCode::kError, "duplicated col " + column_name};
            }
            col_set.insert(column_name);
            has_iter = true;
            auto iter = column_map.find(column_name);
            if (iter != column_map.end()) {
                if (iter->second.data_type() == ::openmldb::type::kFloat
                        || iter->second.data_type() == ::openmldb::type::kDouble) {
                    return {base::ReturnCode::kError,
                        "float or double type column can not be index, column is: " + column_key.index_name()};
                }
            } else {
                return {base::ReturnCode::kError, "can not find col in schema. col: " + column_name};
            }
        }
        if (!has_iter) {
            auto iter = column_map.find(column_key.index_name());
            if (iter == column_map.end()) {
                return {base::ReturnCode::kError, "index must member of columns when column key col name is empty"};
            }
            if (iter->second.data_type() == ::openmldb::type::kFloat
                    || iter->second.data_type() == ::openmldb::type::kDouble) {
                return {base::ReturnCode::kError, "float or double column can not be index"};
            }
        }
        if (column_key.has_ts_name()) {
            if (auto iter = column_map.find(column_key.ts_name()); iter == column_map.end()) {
                return {base::ReturnCode::kError, "can not find col in schema. col: " + column_key.ts_name()};
            } else if (iter->second.data_type() != ::openmldb::type::kBigInt
                  && iter->second.data_type() != ::openmldb::type::kTimestamp) {
                return {base::ReturnCode::kError,
                    "ts column type should be bigint or timestamp, column is: " + column_key.ts_name()};
            }
        }
        if (column_key.has_ttl()) {
            if (auto status = CheckTTL(column_key.ttl()); !status.OK()) {
                return status;
            }
        }
    }
    return CheckUnique(index);
}

base::Status IndexUtil::CheckTTL(const ::openmldb::common::TTLSt& ttl) {
    if (ttl.abs_ttl() > FLAGS_absolute_ttl_max) {
        return {base::ReturnCode::kError, absl::StrCat("absolute ttl cannot be greater than ", FLAGS_absolute_ttl_max)};
    } else if (ttl.lat_ttl() > FLAGS_latest_ttl_max) {
        return {base::ReturnCode::kError, absl::StrCat("latest ttl cannot be greater than ", FLAGS_latest_ttl_max)};
    }
    return {};
}

bool IndexUtil::AddDefaultIndex(openmldb::nameserver::TableInfo* table_info) {
    if (table_info == nullptr) {
        return false;
    }
    for (const auto& column : table_info->column_desc()) {
        if (column.data_type() != type::kFloat && column.data_type() != type::kDouble) {
            ::openmldb::common::ColumnKey* index = nullptr;
            if (table_info->column_key_size() == 0) {
                index = table_info->add_column_key();
            } else {
                index = table_info->mutable_column_key(0);
            }
            index->add_col_name(column.name());
            // Ref hybridse::plan::PlanAPI::GenerateName
            index->set_index_name("INDEX_0_" + std::to_string(::baidu::common::timer::now_time()));
            // use the default ttl
            index->mutable_ttl();
            break;
        }
    }
    return true;
}

bool IndexUtil::FillColumnKey(openmldb::nameserver::TableInfo* table_info) {
    if (table_info == nullptr) {
        return false;
    }
    for (int idx = 0; idx < table_info->column_key_size(); idx++) {
        const auto& cur_index = table_info->column_key(idx);
        if (cur_index.col_name_size() == 0) {
            auto column_key = table_info->mutable_column_key(idx);
            column_key->add_col_name(cur_index.index_name());
        }
    }
    return true;
}

base::Status IndexUtil::CheckUnique(const PBIndex& index) {
    std::set<std::string> id_set;
    std::set<std::string> name_set;
    for (int32_t index_pos = 0; index_pos < index.size(); index_pos++) {
        if (name_set.count(index.Get(index_pos).index_name()) > 0) {
            return {base::ReturnCode::kError, "duplicate index " + index.Get(index_pos).index_name()};
        }
        name_set.insert(index.Get(index_pos).index_name());
        auto id_str = GetIDStr(index.Get(index_pos));
        if (id_set.count(id_str) > 0) {
            return {base::ReturnCode::kError, "duplicate index " + index.Get(index_pos).index_name()};
        }
        id_set.insert(id_str);
    }
    return {};
}

bool IndexUtil::IsExist(const ::openmldb::common::ColumnKey& column_key, const PBIndex& index) {
    std::string id_str = GetIDStr(column_key);
    for (int32_t index_pos = 0; index_pos < index.size(); index_pos++) {
        if (index.Get(index_pos).flag() == 0) {
            if (index.Get(index_pos).index_name() == column_key.index_name() ||
                    id_str == GetIDStr(index.Get(index_pos))) {
                return true;
            }
        }
    }
    return false;
}

int IndexUtil::GetPosition(const ::openmldb::common::ColumnKey& column_key, const PBIndex& index) {
    std::string id_str = GetIDStr(column_key);
    for (int32_t index_pos = 0; index_pos < index.size(); index_pos++) {
        if (id_str == GetIDStr(index.Get(index_pos))) {
            return index_pos;
        }
    }
    return -1;
}

std::vector<::openmldb::common::ColumnKey> IndexUtil::Convert2Vector(const PBIndex& index) {
    std::vector<::openmldb::common::ColumnKey> vec;
    for (const auto& column_key : index) {
        vec.push_back(column_key);
    }
    return vec;
}

PBIndex IndexUtil::Convert2PB(const std::vector<::openmldb::common::ColumnKey>& index) {
    PBIndex pb_index;
    for (const auto& column_key : index) {
        pb_index.Add()->CopyFrom(column_key);
    }
    return pb_index;
}

std::string IndexUtil::GetIDStr(const ::openmldb::common::ColumnKey& column_key) {
    std::string id_str;
    for (const auto& cur_col : column_key.col_name()) {
        id_str.append(cur_col + "|");
    }
    if (column_key.has_ts_name() && !column_key.ts_name().empty()) {
        id_str.append(column_key.ts_name());
    }
    return id_str;
}

bool TableIndexInfo::Init() {
    for (int32_t i = 0; i < table_meta_.column_desc_size(); i++) {
        column_idx_map_.emplace(table_meta_.column_desc(i).name(), i);
    }
    uint32_t base_size = table_meta_.column_desc_size();
    for (int32_t i = 0; i < table_meta_.added_column_desc_size(); ++i) {
        column_idx_map_.emplace(table_meta_.added_column_desc(i).name(), i + base_size);
    }
    std::set<uint32_t> index_col_set;
    for (int32_t i = 0; i < table_meta_.column_key_size(); i++) {
        const auto& ck = table_meta_.column_key(i);
        if (ck.flag()) {
            continue;
        }
        for (const auto& add_ck : add_indexs_) {
            if (ck.index_name() == add_ck.index_name()) {
                add_index_idx_vec_.push_back(i);
                break;
            }
        }
        std::vector<uint32_t> cols;
        for (const auto& name : ck.col_name()) {
            auto iter = column_idx_map_.find(name);
            if (iter != column_idx_map_.end()) {
                cols.push_back(iter->second);
                index_col_set.insert(iter->second);
            } else {
                return false;
            }
        }
        index_cols_map_.emplace(i, std::move(cols));
    }
    if (add_index_idx_vec_.size() != add_indexs_.size()) {
        return false;
    }
    std::map<uint32_t, uint32_t> col_idx_map;
    for (auto idx : index_col_set) {
        col_idx_map.emplace(idx, all_index_cols_.size());
        all_index_cols_.push_back(idx);
    }
    for (const auto& kv : index_cols_map_) {
        std::vector<uint32_t> vec;
        for (auto idx : kv.second) {
            vec.push_back(col_idx_map[idx]);
        }
        real_index_cols_map_.emplace(kv.first, std::move(vec));
    }
    return true;
}

bool TableIndexInfo::HasIndex(uint32_t idx) const {
    return index_cols_map_.find(idx) != index_cols_map_.end();
}

const std::vector<uint32_t>& TableIndexInfo::GetIndexCols(uint32_t idx) {
    return index_cols_map_[idx];
}

const std::vector<uint32_t>& TableIndexInfo::GetRealIndexCols(uint32_t idx) {
    return real_index_cols_map_[idx];
}

}  // namespace schema
}  // namespace openmldb
