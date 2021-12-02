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
#include <string>
#include <set>
#include "common/timer.h"
#include "gflags/gflags.h"
#include "vm/catalog.h"

DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(latest_ttl_max);

namespace openmldb {
namespace schema {

static const std::unordered_map<::openmldb::type::TTLType, ::hybridse::type::TTLType> TTL_TYPE_MAP = {
    {::openmldb::type::kAbsoluteTime, ::hybridse::type::kTTLTimeLive},
    {::openmldb::type::kLatestTime, ::hybridse::type::kTTLCountLive},
    {::openmldb::type::kAbsAndLat, ::hybridse::type::kTTLTimeLiveAndCountLive},
    {::openmldb::type::kAbsOrLat, ::hybridse::type::kTTLTimeLiveOrCountLive}};

bool IndexUtil::ConvertIndex(const PBIndex& index, ::hybridse::vm::IndexList* output) {
    if (output == nullptr) {
        LOG(WARNING) << "output ptr is null";
        return false;
    }
    for (int32_t i = 0; i < index.size(); i++) {
        const ::openmldb::common::ColumnKey& key = index.Get(i);
        ::hybridse::type::IndexDef* index_def = output->Add();
        index_def->set_name(key.index_name());
        index_def->mutable_first_keys()->CopyFrom(key.col_name());
        if (key.has_ts_name() && !key.ts_name().empty()) {
            index_def->set_second_key(key.ts_name());
            index_def->set_ts_offset(0);
        }
        if (key.has_ttl()) {
            auto ttl_type = key.ttl().ttl_type();
            auto it = TTL_TYPE_MAP.find(ttl_type);
            if (it == TTL_TYPE_MAP.end()) {
                LOG(WARNING) << "not found " << ::openmldb::type::TTLType_Name(ttl_type);
                return false;
            }
            index_def->set_ttl_type(it->second);
            if (ttl_type == ::openmldb::type::kAbsAndLat || ttl_type == ::openmldb::type::kAbsOrLat) {
                index_def->add_ttl(key.ttl().abs_ttl());
                index_def->add_ttl(key.ttl().lat_ttl());
            } else if (ttl_type == ::openmldb::type::kAbsoluteTime) {
                index_def->add_ttl(key.ttl().abs_ttl());
            } else {
                index_def->add_ttl(key.ttl().lat_ttl());
            }
        }
    }
    return true;
}

base::Status IndexUtil::CheckIndex(const std::map<std::string, ::openmldb::type::DataType>& column_map,
        const PBIndex& index) {
    if (index.size() == 0) {
        return {base::ReturnCode::kError, "no index"};
    }
    std::set<std::string> index_set;
    for (const auto& column_key : index) {
        bool has_iter = false;
        for (const auto& column_name : column_key.col_name()) {
            has_iter = true;
            auto iter = column_map.find(column_name);
            if ((iter != column_map.end() &&
                 ((iter->second == ::openmldb::type::kFloat) || (iter->second == ::openmldb::type::kDouble)))) {
                return {base::ReturnCode::kError,
                    "float or double type column can not be index, column is: " + column_key.index_name()};
            }
        }
        if (!has_iter) {
            auto iter = column_map.find(column_key.index_name());
            if (iter == column_map.end()) {
                return {base::ReturnCode::kError, "index must member of columns when column key col name is empty"};
            }
            if (iter->second == ::openmldb::type::kFloat || iter->second == ::openmldb::type::kDouble) {
                return {base::ReturnCode::kError, "float or double column can not be index"};
            }
        }
        if (column_key.has_ttl()) {
            if (!CheckTTL(column_key.ttl())) {
                return {base::ReturnCode::kError, "ttl check failed"};
            }
        }
    }
    return {};
}

bool IndexUtil::CheckTTL(const ::openmldb::common::TTLSt& ttl) {
    if (ttl.abs_ttl() > FLAGS_absolute_ttl_max || ttl.lat_ttl() > FLAGS_latest_ttl_max) {
        return false;
    }
    return true;
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

}  // namespace schema
}  // namespace openmldb
