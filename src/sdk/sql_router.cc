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

#include "sdk/sql_router.h"
#include <map>
#include "base/ddl_parser.h"
#include "glog/logging.h"
#include "schema/schema_adapter.h"
#include "sdk/sql_cluster_router.h"

namespace openmldb::sdk {

std::shared_ptr<SQLRouter> NewClusterSQLRouter(const SQLRouterOptions& options) {
    auto router = std::make_shared<SQLClusterRouter>(options);
    if (!router->Init()) {
        LOG(WARNING) << "Fail to init sql cluster router";
        return {};
    }
    return router;
}

std::shared_ptr<SQLRouter> NewStandaloneSQLRouter(const StandaloneOptions& options) {
    auto router = std::make_shared<SQLClusterRouter>(options);
    if (!router->Init()) {
        LOG(WARNING) << "Fail to init standalone sql router";
        return {};
    }
    return router;
}

// To avoid using protobuf message in swig, we use normal type in GenDDL/OutputSchema parameters,
// so we should convert schema format here
std::map<std::string, std::vector<openmldb::common::ColumnDesc>> convertSchema(
    const std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>& schemas) {
    std::map<std::string, std::vector<openmldb::common::ColumnDesc>> table_desc_map;
    for (auto& table_item : schemas) {
        auto table_name = table_item.first;
        auto column_list = table_item.second;
        std::vector<openmldb::common::ColumnDesc> column_desc_list;
        for (auto& column_map : column_list) {
            openmldb::common::ColumnDesc column_desc;
            std::string column_name = column_map.first;
            hybridse::sdk::DataType column_type = column_map.second;
            column_desc.set_name(column_name);
            openmldb::type::DataType data_type;
            if (!openmldb::schema::SchemaAdapter::ConvertType(column_type, &data_type)) {
                return {};
            }
            column_desc.set_data_type(data_type);
            column_desc_list.push_back(column_desc);
        }
        table_desc_map.insert(std::make_pair(table_name, column_desc_list));
    }
    return table_desc_map;
}

bool ToTTLTypeString(openmldb::type::TTLType ttl_type, std::string* ttl_type_str) {
    switch (ttl_type) {
        case openmldb::type::TTLType::kAbsoluteTime:
            *ttl_type_str = "absolute";
            return true;
        case openmldb::type::TTLType::kLatestTime:
            *ttl_type_str = "latest";
            return true;
        case openmldb::type::TTLType::kAbsAndLat:
            *ttl_type_str = "absandlat";
            return true;
        case openmldb::type::TTLType::kAbsOrLat:
            *ttl_type_str = "absorlat";
            return true;
        default:
            DLOG(ERROR) << "Can Not Found This TTL Type: " + openmldb::type::TTLType_Name(ttl_type);
            return false;
    }
}

std::string ToIndexString(const std::string& ts, const std::string& key_name, openmldb::type::TTLType ttl_type,
                          const std::string& expire) {
    std::string index;
    std::string ttl_type_str;
    ToTTLTypeString(ttl_type, &ttl_type_str);
    index = "\tindex(key=(";
    index.append(key_name);
    index.append("), ttl=");
    index.append(expire);
    index.append(", ttl_type=");
    index.append(ttl_type_str);
    if (ts.empty()) {
        index.append(")");
    } else {
        index.append(", ts=`");
        index.append(ts);
        index.append("`)");
    }
    return index;
}

bool GetTTL(openmldb::type::TTLType ttl_type, ::google::protobuf::uint64 abs_ttl, ::google::protobuf::uint64 lat_ttl,
            std::string* ttl) {
    switch (ttl_type) {
        case openmldb::type::TTLType::kAbsoluteTime:
            *ttl = std::to_string(abs_ttl).append("m");
            return true;
        case openmldb::type::TTLType::kAbsAndLat:
        case openmldb::type::TTLType::kAbsOrLat:
            *ttl = "(" + std::to_string(abs_ttl) + "m, " + std::to_string(lat_ttl) + ")";
            return true;
        case openmldb::type::TTLType::kLatestTime:
            *ttl = std::to_string(lat_ttl);
            return true;
        default:
            return false;
    }
}

std::vector<std::string> GenDDL(
    const std::string& sql,
    const std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>& schemas) {
    auto table_desc_map = convertSchema(schemas);
    if (table_desc_map.empty()) {
        LOG_IF(WARNING, !schemas.empty()) << "input schemas is not emtpy, but conversion failed";
        return {};
    }
    auto index_map = openmldb::base::DDLParser::ExtractIndexes(sql, table_desc_map);
    std::vector<std::string> ddl_vector;
    for (auto& table_item : table_desc_map) {
        std::string ddl = "CREATE TABLE IF NOT EXISTS ";
        std::string table_name = table_item.first;
        ddl = ddl.append(table_name);
        ddl = ddl.append("(\n");
        auto column_desc_list = table_item.second;
        for (auto& column_desc : column_desc_list) {
            const auto& column_name = column_desc.name();
            auto data_type = column_desc.data_type();
            std::string column_type = openmldb::codec::DATA_TYPE_STR_MAP.find(data_type)->second;
            ddl = ddl.append("\t");
            ddl = ddl.append(column_name);
            ddl = ddl.append(" ");
            ddl = ddl.append(column_type);
            ddl = ddl.append(",\n");
        }
        auto iter = index_map.find(table_name);
        if (iter != index_map.end()) {
            auto column_key_list = iter->second;
            if (!column_key_list.empty()) {
                for (const auto& column_key : column_key_list) {
                    auto col_name_list = column_key.col_name();
                    std::string key_name;
                    for (const auto& col_name : col_name_list) {
                        key_name = col_name;
                        key_name = key_name.append(",");
                    }
                    key_name = key_name.substr(0, key_name.find_last_of(','));
                    const auto& ttl = column_key.ttl();
                    auto ttl_type = ttl.ttl_type();
                    auto abs_ttl = ttl.abs_ttl();
                    auto lat_ttl = ttl.lat_ttl();
                    std::string expire;
                    GetTTL(ttl_type, abs_ttl, lat_ttl, &expire);
                    const auto& ts = column_key.ts_name();
                    ddl = ddl.append(ToIndexString(ts, key_name, ttl_type, expire));
                    ddl = ddl.append(",\n");
                }
            }
        }

        ddl = ddl.substr(0, ddl.find_last_of(','));
        ddl = ddl.append("\n);");
        DLOG(INFO) << "ddl is " + ddl;
        ddl_vector.push_back(ddl);
    }
    return ddl_vector;
}

std::shared_ptr<hybridse::sdk::Schema> GenOutputSchema(
    const std::string& sql,
    const std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>& schemas) {
    auto table_desc_map = convertSchema(schemas);
    if (table_desc_map.empty()) {
        LOG_IF(WARNING, !schemas.empty()) << "input schemas is not emtpy, but conversion failed";
        return {};
    }
    return openmldb::base::DDLParser::GetOutputSchema(sql, table_desc_map);
}

}  // namespace openmldb::sdk
