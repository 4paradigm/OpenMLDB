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

#include <sstream>
#include "sdk/sdk_util.h"
#include "codec/schema_codec.h"

namespace openmldb {
namespace sdk {

std::string SDKUtil::GenCreateTableSQL(const ::openmldb::nameserver::TableInfo& table_info) {
    std::stringstream ss;
    ss << "CREATE TABLE `" << table_info.name() << "` (\n";
    for (const auto& column : table_info.column_desc()) {
        auto it = openmldb::codec::DATA_TYPE_STR_MAP.find(column.data_type());
        if (it != openmldb::codec::DATA_TYPE_STR_MAP.end()) {
            ss << "`" << column.name() << "` " << it->second;
            if (column.not_null()) {
                ss << " NOT NULL ";
            }
            if (column.has_default_value()) {
                ss << " DEFAULT \"" << column.default_value() << "\"";
            }
            ss << ",\n";
        }
    }
    int index_cnt = 0;
    for (const auto& index : table_info.column_key()) {
        if (index.flag() != 0) {
            continue;
        }
        if (index_cnt > 0) {
            ss << ",\n";
        }
        ss << "INDEX (";
        if (index.col_name_size() == 1) {
            ss << "KEY=`" << index.col_name(0) << "`";
        } else {
            ss << "KEY=(";
            for (int idx = 0; idx < index.col_name_size(); idx++) {
                if (idx > 0) {
                    ss << ",";
                }
                ss << "`" << index.col_name(idx) << "`";
            }
            ss << ")";
        }
        if (index.has_ts_name() && !index.ts_name().empty()) {
            ss << ", TS=`" << index.ts_name() << "`";
        }
        if (index.has_ttl()) {
            ss << ", TTL_TYPE=";
            if (index.ttl().ttl_type() == openmldb::type::TTLType::kAbsoluteTime) {
                ss << "ABSOLUTE, TTL=" << index.ttl().abs_ttl() << "m";
            } else if (index.ttl().ttl_type() == openmldb::type::TTLType::kLatestTime) {
                ss << "LATEST, TTL=" << index.ttl().lat_ttl();
            } else if (index.ttl().ttl_type() == openmldb::type::TTLType::kAbsAndLat) {
                ss << "ABSANDLAT, TTL=(" << index.ttl().abs_ttl() << "m, " << index.ttl().lat_ttl() << ")";
            } else if (index.ttl().ttl_type() == openmldb::type::TTLType::kAbsOrLat) {
                ss << "ABSORLAT, TTL=(" << index.ttl().abs_ttl() << "m, " << index.ttl().lat_ttl() << ")";
            }
        }
        index_cnt++;
        ss << ")";
    }
    ss << "\n) ";
    ss << "OPTIONS (";
    ss << "PARTITIONNUM=" << table_info.partition_num();
    ss << ", REPLICANUM=" << table_info.replica_num();
    if (table_info.storage_mode() == openmldb::common::StorageMode::kSSD) {
        ss << ", STORAGE_MODE='SSD'";
    } else if (table_info.storage_mode() == openmldb::common::StorageMode::kHDD) {
        ss << ", STORAGE_MODE='HDD'";
    } else {
        ss << ", STORAGE_MODE='Memory'";
    }
    ss << ");";
    return ss.str();
}

}  // namespace sdk
}  // namespace openmldb
