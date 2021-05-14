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
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <boost/algorithm/string.hpp>

#include "base/status.h"
#include "codec/codec.h"
#include "proto/name_server.pb.h"
#include "codec/fe_row_codec.h"
#include "codec/field_codec.h"

namespace fedb {
namespace codec {

constexpr uint32_t MAX_ROW_BYTE_SIZE = 1024 * 1024;  // 1M
constexpr uint32_t HEADER_BYTE_SIZE = 3;

const std::string NONETOKEN = "!N@U#L$L%"; // NOLINT
const std::string EMPTY_STRING = "!@#$%";  // NOLINT
const std::string DEFAULT_LONG = "1";      // NOLINT

static const std::unordered_map<std::string, ::fedb::type::DataType>
    DATA_TYPE_MAP = {{"bool", ::fedb::type::kBool},
                     {"smallint", ::fedb::type::kSmallInt},
                     {"uint16", ::fedb::type::kSmallInt},
                     {"int16", ::fedb::type::kSmallInt},
                     {"int", ::fedb::type::kInt},
                     {"int32", ::fedb::type::kInt},
                     {"uint32", ::fedb::type::kInt},
                     {"bigint", ::fedb::type::kBigInt},
                     {"int64", ::fedb::type::kBigInt},
                     {"uint64", ::fedb::type::kBigInt},
                     {"float", ::fedb::type::kFloat},
                     {"double", ::fedb::type::kDouble},
                     {"varchar", ::fedb::type::kVarchar},
                     {"string", fedb::type::DataType::kString},
                     {"date", ::fedb::type::kDate},
                     {"timestamp", ::fedb::type::kTimestamp}};

static const std::unordered_map<::fedb::type::DataType, std::string>
    DATA_TYPE_STR_MAP = {{::fedb::type::kBool, "bool"},
                         {::fedb::type::kSmallInt, "smallInt"},
                         {::fedb::type::kInt, "int"},
                         {::fedb::type::kBigInt, "bigInt"},
                         {::fedb::type::kFloat, "float"},
                         {::fedb::type::kDouble, "double"},
                         {::fedb::type::kTimestamp, "timestamp"},
                         {::fedb::type::kDate, "date"},
                         {::fedb::type::kVarchar, "varchar"},
                         {::fedb::type::kString, "string"}};

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
    static void SetColumnDesc(::fedb::common::ColumnDesc* desc, const std::string& name, ::fedb::type::DataType type) {
        desc->set_name(name);
        desc->set_data_type(type);
    }

    static void SetIndex(::fedb::common::ColumnKey* index, const std::string& name, const std::string& col_name,
            const std::string& ts_name, ::fedb::type::TTLType ttl_type, uint64_t abs_ttl, uint64_t lat_ttl) {
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

    static ::fedb::codec::ColType ConvertType(const std::string& raw_type) {
        ::fedb::codec::ColType type;
        if (raw_type == "int32") {
            type = ::fedb::codec::ColType::kInt32;
        } else if (raw_type == "int64") {
            type = ::fedb::codec::ColType::kInt64;
        } else if (raw_type == "uint32") {
            type = ::fedb::codec::ColType::kUInt32;
        } else if (raw_type == "uint64") {
            type = ::fedb::codec::ColType::kUInt64;
        } else if (raw_type == "float") {
            type = ::fedb::codec::ColType::kFloat;
        } else if (raw_type == "double") {
            type = ::fedb::codec::ColType::kDouble;
        } else if (raw_type == "string") {
            type = ::fedb::codec::ColType::kString;
        } else if (raw_type == "timestamp") {
            type = ::fedb::codec::ColType::kTimestamp;
        } else if (raw_type == "int16") {
            type = ::fedb::codec::ColType::kInt16;
        } else if (raw_type == "uint16") {
            type = ::fedb::codec::ColType::kUInt16;
        } else if (raw_type == "bool") {
            type = ::fedb::codec::ColType::kBool;
        } else if (raw_type == "date") {
            type = ::fedb::codec::ColType::kDate;
        } else {
            type = ::fedb::codec::ColType::kUnknown;
        }
        return type;
    }

    static fedb::type::DataType ConvertStrType(const std::string& type) {
        auto it = DATA_TYPE_MAP.find(type);
        if (it != DATA_TYPE_MAP.end()) {
            return it->second;
        }
        return fedb::type::kString;
    }

    static hybridse::type::Type ConvertType(fedb::type::DataType type) {
        switch (type) {
            case fedb::type::kBool: return hybridse::type::kBool;
            case fedb::type::kSmallInt: return hybridse::type::kInt16;
            case fedb::type::kInt: return hybridse::type::kInt32;
            case fedb::type::kBigInt: return hybridse::type::kInt64;
            case fedb::type::kFloat: return hybridse::type::kFloat;
            case fedb::type::kDouble: return hybridse::type::kDouble;
            case fedb::type::kDate: return hybridse::type::kDate;
            case fedb::type::kTimestamp: return hybridse::type::kTimestamp;
            case fedb::type::kVarchar: return hybridse::type::kVarchar;
            case fedb::type::kString: return hybridse::type::kVarchar;
            default: return hybridse::type::kNull;
        }
    }

    static int ConvertColumnDesc(
        const ::fedb::nameserver::TableInfo& table_info,
        std::vector<ColumnDesc>& columns) {  // NOLINT
        return ConvertColumnDesc(table_info, columns, 0);
    }

    static int ConvertColumnDesc(
        const ::fedb::nameserver::TableInfo& table_info,
        std::vector<ColumnDesc>& columns, int modify_index) {  // NOLINT
        columns.clear();
        if (modify_index > 0) {
            return ConvertColumnDesc(table_info.column_desc(), columns,
                                     table_info.added_column_desc());
        }
        return ConvertColumnDesc(table_info.column_desc(), columns);
    }

    static int ConvertColumnDesc(const Schema& column_desc_field,
                                 std::vector<ColumnDesc>& columns,  // NOLINT
                                 const Schema& added_column_field) {
        columns.clear();
        for (const auto& cur_column_desc : column_desc_field) {
            ::fedb::codec::ColType type = ConvertType(cur_column_desc.type());
            if (type == ::fedb::codec::ColType::kUnknown) {
                return -1;
            }
            ColumnDesc column_desc;
            column_desc.type = type;
            column_desc.name = cur_column_desc.name();
            columns.push_back(column_desc);
        }
        if (!added_column_field.empty()) {
            for (int idx = 0; idx < added_column_field.size(); idx++) {
                ::fedb::codec::ColType type =
                    ConvertType(added_column_field.Get(idx).type());
                if (type == ::fedb::codec::ColType::kUnknown) {
                    return -1;
                }
                ColumnDesc column_desc;
                column_desc.type = type;
                column_desc.name = added_column_field.Get(idx).name();
                columns.push_back(column_desc);
            }
        }
        return 0;
    }

    static int ConvertColumnDesc(const Schema& column_desc_field,
                                 std::vector<ColumnDesc>& columns) {  // NOLINT
        Schema added_column_field;
        return ConvertColumnDesc(column_desc_field, columns,
                                 added_column_field);
    }

    static int ConvertColumnDesc(const Schema& column_desc_field,
                                 Schema& columns,  // NOLINT
                                 const Schema& added_column_field) {
        columns.Clear();
        for (const auto& cur_column_desc : column_desc_field) {
            fedb::common::ColumnDesc* column_desc = columns.Add();
            column_desc->CopyFrom(cur_column_desc);
            if (!cur_column_desc.has_data_type()) {
                auto iter = DATA_TYPE_MAP.find(cur_column_desc.type());
                if (iter == DATA_TYPE_MAP.end()) {
                    return -1;
                } else {
                    column_desc->set_data_type(iter->second);
                }
            }
        }
        for (const auto& cur_column_desc : added_column_field) {
            fedb::common::ColumnDesc* column_desc = columns.Add();
            column_desc->CopyFrom(cur_column_desc);
            if (!cur_column_desc.has_data_type()) {
                auto iter = DATA_TYPE_MAP.find(cur_column_desc.type());
                if (iter == DATA_TYPE_MAP.end()) {
                    return -1;
                } else {
                    column_desc->set_data_type(iter->second);
                }
            }
        }
        return 0;
    }

    static void GetSchemaData(
        const std::map<std::string, std::string>& columns_map,
        const Schema& schema, Schema& new_schema) {  // NOLINT
        for (int i = 0; i < schema.size(); i++) {
            const ::fedb::common::ColumnDesc& col = schema.Get(i);
            const std::string& col_name = col.name();
            auto iter = columns_map.find(col_name);
            if (iter != columns_map.end()) {
                ::fedb::common::ColumnDesc* tmp = new_schema.Add();
                tmp->CopyFrom(col);
            }
        }
    }

    static fedb::base::ResultMsg GetCdColumns(const Schema& schema,
            const std::map<std::string, std::string>& cd_columns_map,
            ::google::protobuf::RepeatedPtrField<::fedb::api::Columns>*
            cd_columns) {
        fedb::base::ResultMsg rm;
        std::map<std::string, ::fedb::type::DataType> name_type_map;
        for (const auto& col_desc : schema) {
            name_type_map.insert(std::make_pair(
                        col_desc.name(), col_desc.data_type()));
        }
        for (const auto& kv : cd_columns_map) {
            auto iter = name_type_map.find(kv.first);
            if (iter == name_type_map.end()) {
                rm.code = -1;
                rm.msg = "query failed! col_name " + kv.first + " not exist";
                return rm;
            }
            ::fedb::api::Columns* index = cd_columns->Add();
            index->add_name(kv.first);
            if (kv.second == NONETOKEN || kv.second == "null") {
                continue;
            }
        }
        rm.code = 0;
        rm.msg = "ok";
        return rm;
    }

    static bool AddTypeToColumnDesc(
            std::shared_ptr<::fedb::nameserver::TableInfo> table_info) {
        for (int i = 0; i < table_info->column_desc_size(); i++) {
            ::fedb::common::ColumnDesc* col_desc =
                table_info->mutable_column_desc(i);
            ::fedb::type::DataType data_type = col_desc->data_type();
            switch (data_type) {
                case fedb::type::kBool: {
                    col_desc->set_type("bool");
                    break;
                }
                case fedb::type::kSmallInt: {
                    col_desc->set_type("int16");
                    break;
                }
                case fedb::type::kInt: {
                    col_desc->set_type("int32");
                    break;
                }
                case fedb::type::kBigInt: {
                    col_desc->set_type("int64");
                    break;
                }
                case fedb::type::kDate: {
                    col_desc->set_type("date");
                    break;
                }
                case fedb::type::kTimestamp: {
                    col_desc->set_type("timestamp");
                    break;
                }
                case fedb::type::kFloat: {
                    col_desc->set_type("float");
                    break;
                }
                case fedb::type::kDouble: {
                    col_desc->set_type("double");
                    break;
                }
                case fedb::type::kVarchar:
                case fedb::type::kString: {
                    col_desc->set_type("string");
                    break;
                }
                default: {
                    return false;
                }
            }
        }
        return true;
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
}  // namespace fedb

#endif  // SRC_CODEC_SCHEMA_CODEC_H_
