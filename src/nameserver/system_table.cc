/*
 * Copyright 2022 4Paradigm
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

#include "nameserver/system_table.h"

#include "absl/container/flat_hash_map.h"

namespace openmldb {
namespace nameserver {

static absl::flat_hash_map<SystemTableType, SystemTableInfo const> CreateSystemTableMap() {
    absl::flat_hash_map<SystemTableType, SystemTableInfo const> map = {
        {SystemTableType::kJobInfo, {INTERNAL_DB, JOB_INFO_NAME}},
        {SystemTableType::kPreAggMetaInfo, {INTERNAL_DB, PRE_AGG_META_NAME}},
        {SystemTableType::kGlobalVariable, {INFORMATION_SCHEMA_DB, GLOBAL_VARIABLES}},
        {SystemTableType::kDeployResponseTime, {INFORMATION_SCHEMA_DB, DEPLOY_RESPONSE_TIME}},
        {SystemTableType::kUser, {INTERNAL_DB, USER_INFO_NAME}},
        {SystemTableType::kObject, {INTERNAL_DB, OBJECT_INFO_NAME}},
    };
    return map;
}

static const absl::flat_hash_map<SystemTableType, SystemTableInfo const>& GetSystemTableMap() {
    static const absl::flat_hash_map<SystemTableType, SystemTableInfo const>& map = *new auto(CreateSystemTableMap());
    return map;
}

const SystemTableInfo* GetSystemTableInfo(SystemTableType type) {
    const auto& map = GetSystemTableMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return &it->second;
    }
    return nullptr;
}

absl::string_view GetSystemTableName(SystemTableType type) {
    const auto& map = GetSystemTableMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return it->second.name_;
    }
    return "UnkownSystemTable";
}

}  // namespace nameserver
}  // namespace openmldb
