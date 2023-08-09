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

#ifndef SRC_SCHEMA_INDEX_UTIL_H_
#define SRC_SCHEMA_INDEX_UTIL_H_

#include <map>
#include <string>
#include <vector>
#include "base/status.h"
#include "proto/common.pb.h"
#include "proto/name_server.pb.h"
#include "vm/catalog.h"

namespace openmldb {
namespace schema {

using PBSchema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;
using PBIndex = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>;

class IndexUtil {
 public:
    static base::Status CheckIndex(const std::map<std::string, ::openmldb::common::ColumnDesc>& column_map,
            const PBIndex& index);

    static bool IsExist(const ::openmldb::common::ColumnKey& column_key, const PBIndex& index);

    static int GetPosition(const ::openmldb::common::ColumnKey& column_key, const PBIndex& index);

    static std::vector<::openmldb::common::ColumnKey> Convert2Vector(const PBIndex& index);

    static base::Status CheckUnique(const PBIndex& index);

    static base::Status CheckTTL(const ::openmldb::common::TTLSt& ttl);

    static bool AddDefaultIndex(openmldb::nameserver::TableInfo* table_info);

    static bool FillColumnKey(openmldb::nameserver::TableInfo* table_info);

    static std::string GetIDStr(const ::openmldb::common::ColumnKey& column_key);
};

}  // namespace schema
}  // namespace openmldb
#endif  // SRC_SCHEMA_INDEX_UTIL_H_
