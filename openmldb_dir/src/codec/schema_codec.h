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

#ifndef SRC_CODEC_SCHEMA_CODEC_H_
#define SRC_CODEC_SCHEMA_CODEC_H_

#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

#include "base/status.h"
#include "codec/codec.h"
#include "codec/fe_row_codec.h"
#include "codec/field_codec.h"
#include "proto/name_server.pb.h"
#include <boost/algorithm/string.hpp>

namespace openmldb {
namespace codec {

constexpr uint32_t MAX_ROW_BYTE_SIZE = 1024 * 1024;  // 1M
constexpr uint32_t HEADER_BYTE_SIZE = 3;

const std::string NONETOKEN = "!N@U#L$L%";  // NOLINT
const std::string EMPTY_STRING = "!@#$%";   // NOLINT
const std::string DEFAULT_LONG = "1";       // NOLINT

static const std::unordered_map<std::string, ::openmldb::type::DataType> DATA_TYPE_MAP = {
    {"bool", ::openmldb::type::kBool},       {"smallint", ::openmldb::type::kSmallInt},
    {"uint16", ::openmldb::type::kSmallInt}, {"int16", ::openmldb::type::kSmallInt},
    {"int", ::openmldb::type::kInt},         {"int32", ::openmldb::type::kInt},
    {"uint32", ::openmldb::type::kInt},      {"bigint", ::openmldb::type::kBigInt},
    {"int64", ::openmldb::type::kBigInt},    {"uint64", ::openmldb::type::kBigInt},
    {"float", ::openmldb::type::kFloat},     {"double", ::openmldb::type::kDouble},
    {"varchar", ::openmldb::type::kVarchar}, {"string", openmldb::type::DataType::kString},
    {"date", ::openmldb::type::kDate},       {"timestamp", ::openmldb::type::kTimestamp}};

static const std::unordered_map<::openmldb::type::DataType, std::string> DATA_TYPE_STR_MAP = {
    {::openmldb::type::kBool, "bool"},
    {::openmldb::type::kSmallInt, "smallInt"},
    {::openmldb::type::kInt, "int"},
    {::openmldb::type::kBigInt, "bigInt"},
    {::openmldb::type::kFloat, "float"},
    {::openmldb::type::kDouble, "double"},
    {::openmldb::type::kTimestamp, "timestamp"},
    {::openmldb::type::kDate, "date"},
    {::openmldb::type::kVarchar, "varchar"},
    {::openmldb::type::kString, "string"}};

enum ColType {
    kString = 0,
    kFloat = 1,
    kInt32 = 2,
    kInt64 = 3,
    kDouble = 4,
    kNull = 5,
    kUInt32 = 6,
    kUInt64 = 7,
    kTimestamp = 8,
    kDate = 9,
    kInt16 = 10,
    kUInt16 = 11,
    kBool = 12,
    kEmptyString = 100,
    kUnknown = 200
};

struct Column {
    ColType type;
    std::string buffer;
};

struct ColumnDesc {
    ColType type;
    std::string name;
};

class SchemaCodec {
 public:
    static void SetColumnDesc(::openmldb::common::ColumnDesc* desc, const std::string& name,
                              ::openmldb::type::DataType type) {
        desc->set_name(name);
        desc->set_data_type(type);
    }

    static void SetIndex(::openmldb::common::ColumnKey* index, const std::string& name, const std::string& col_name,
                         const std::string& ts_name, ::openmldb::type::TTLType ttl_type, uint64_t abs_ttl,
                         uint64_t lat_ttl) {
        index->set_index_name(name);
        std::vector<std::string> parts;
        boost::split(parts, col_name, boost::is_any_of("|"));
        for (const auto& col : parts) {
            index->add_col_name(col);
        }
        if (!ts_name.empty()) {
            index->set_ts_name(ts_name);
        }
        auto ttl = index->mutable_ttl();
        ttl->set_ttl_type(ttl_type);
        ttl->set_abs_ttl(abs_ttl);
        ttl->set_lat_ttl(lat_ttl);
    }

    static ::openmldb::codec::ColType ConvertType(const std::string& raw_type) {
        ::openmldb::codec::ColType type;
        if (raw_type == "int32") {
            type = ::openmldb::codec::ColType::kInt32;
        } else if (raw_type == "int64") {
            type = ::openmldb::codec::ColType::kInt64;
        } else if (raw_type == "uint32") {
            type = ::openmldb::codec::ColType::kUInt32;
        } else if (raw_type == "uint64") {
            type = ::openmldb::codec::ColType::kUInt64;
        } else if (raw_type == "float") {
            type = ::openmldb::codec::ColType::kFloat;
        } else if (raw_type == "double") {
            type = ::openmldb::codec::ColType::kDouble;
        } else if (raw_type == "string") {
            type = ::openmldb::codec::ColType::kString;
        } else if (raw_type == "timestamp") {
            type = ::openmldb::codec::ColType::kTimestamp;
        } else if (raw_type == "int16") {
            type = ::openmldb::codec::ColType::kInt16;
        } else if (raw_type == "uint16") {
            type = ::openmldb::codec::ColType::kUInt16;
        } else if (raw_type == "bool") {
            type = ::openmldb::codec::ColType::kBool;
        } else if (raw_type == "date") {
            type = ::openmldb::codec::ColType::kDate;
        } else {
            type = ::openmldb::codec::ColType::kUnknown;
        }
        return type;
    }

    static hybridse::type::Type ConvertType(openmldb::type::DataType type) {
        switch (type) {
            case openmldb::type::kBool:
                return hybridse::type::kBool;
            case openmldb::type::kSmallInt:
                return hybridse::type::kInt16;
            case openmldb::type::kInt:
                return hybridse::type::kInt32;
            case openmldb::type::kBigInt:
                return hybridse::type::kInt64;
            case openmldb::type::kFloat:
                return hybridse::type::kFloat;
            case openmldb::type::kDouble:
                return hybridse::type::kDouble;
            case openmldb::type::kDate:
                return hybridse::type::kDate;
            case openmldb::type::kTimestamp:
                return hybridse::type::kTimestamp;
            case openmldb::type::kVarchar:
                return hybridse::type::kVarchar;
            case openmldb::type::kString:
                return hybridse::type::kVarchar;
            default:
                return hybridse::type::kNull;
        }
    }

    static void GetSchemaData(const std::map<std::string, std::string>& columns_map, const Schema& schema,
                              Schema& new_schema) {  // NOLINT
        for (int i = 0; i < schema.size(); i++) {
            const ::openmldb::common::ColumnDesc& col = schema.Get(i);
            const std::string& col_name = col.name();
            auto iter = columns_map.find(col_name);
            if (iter != columns_map.end()) {
                ::openmldb::common::ColumnDesc* tmp = new_schema.Add();
                tmp->CopyFrom(col);
            }
        }
    }

    static openmldb::base::ResultMsg GetCdColumns(
        const Schema& schema, const std::map<std::string, std::string>& cd_columns_map,
        ::google::protobuf::RepeatedPtrField<::openmldb::api::Columns>* cd_columns) {
        openmldb::base::ResultMsg rm;
        std::map<std::string, ::openmldb::type::DataType> name_type_map;
        for (const auto& col_desc : schema) {
            name_type_map.insert(std::make_pair(col_desc.name(), col_desc.data_type()));
        }
        for (const auto& kv : cd_columns_map) {
            auto iter = name_type_map.find(kv.first);
            if (iter == name_type_map.end()) {
                rm.code = -1;
                rm.msg = "query failed! col_name " + kv.first + " not exist";
                return rm;
            }
            ::openmldb::api::Columns* index = cd_columns->Add();
            index->add_name(kv.first);
            if (kv.second == NONETOKEN || kv.second == "null") {
                continue;
            }
        }
        rm.code = 0;
        rm.msg = "ok";
        return rm;
    }

 private:
    // calc the total size of schema
    uint32_t GetSize(const std::vector<ColumnDesc>& columns) {
        uint32_t byte_size = 0;
        for (uint32_t i = 0; i < columns.size(); i++) {
            byte_size += (HEADER_BYTE_SIZE + columns[i].name.size());
        }
        return byte_size;
    }
};

}  // namespace codec
}  // namespace openmldb

#endif  // SRC_CODEC_SCHEMA_CODEC_H_
