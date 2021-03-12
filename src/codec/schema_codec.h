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

#include "base/status.h"
#include "boost/lexical_cast.hpp"
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

static const std::unordered_map<std::string, ::fedb::type::IndexType>
    INDEX_TYPE_MAP = {{"unique", ::fedb::type::kUnique},
                      {"nounique", ::fedb::type::kNoUnique},
                      {"primarykey", ::fedb::type::kPrimaryKey},
                      {"autogen", ::fedb::type::kAutoGen},
                      {"increment", ::fedb::type::kIncrement}};

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
    bool add_ts_idx = false;
    bool is_ts_col = false;
};

class SchemaCodec {
 public:
    bool Encode(const std::vector<ColumnDesc>& columns,
                std::string& buffer) {  // NOLINT
        uint32_t byte_size = GetSize(columns);
        if (byte_size > MAX_ROW_BYTE_SIZE) {
            return false;
        }
        buffer.resize(byte_size);
        char* cbuffer = reinterpret_cast<char*>(&(buffer[0]));
        for (uint32_t i = 0; i < columns.size(); i++) {
            uint8_t type = (uint8_t)columns[i].type;
            memcpy(cbuffer, static_cast<const void*>(&type), 1);
            cbuffer += 1;
            uint8_t add_ts_idx = 0;
            if (columns[i].add_ts_idx) {
                add_ts_idx = 1;
            }
            memcpy(cbuffer, static_cast<const void*>(&add_ts_idx), 1);
            cbuffer += 1;
            const std::string& name = columns[i].name;
            if (name.size() >= 128) {
                return false;
            }
            uint8_t name_size = (uint8_t)name.size();
            memcpy(cbuffer, static_cast<const void*>(&name_size), 1);
            cbuffer += 1;
            memcpy(cbuffer, static_cast<const void*>(name.c_str()), name_size);
            cbuffer += name_size;
        }
        return true;
    }

    void Decode(const std::string& schema,
                std::vector<ColumnDesc>& columns) {  // NOLINT
        const char* buffer = schema.c_str();
        uint32_t read_size = 0;
        while (read_size < schema.size()) {
            if (schema.size() - read_size < HEADER_BYTE_SIZE) {
                return;
            }
            uint8_t type = 0;
            memcpy(static_cast<void*>(&type), buffer, 1);
            buffer += 1;
            uint8_t add_ts_idx = 0;
            memcpy(static_cast<void*>(&add_ts_idx), buffer, 1);
            buffer += 1;
            uint8_t name_size = 0;
            memcpy(static_cast<void*>(&name_size), buffer, 1);
            buffer += 1;
            uint32_t total_size = HEADER_BYTE_SIZE + name_size;
            if (schema.size() - read_size < total_size) {
                return;
            }
            std::string name(buffer, name_size);
            buffer += name_size;
            read_size += total_size;
            ColumnDesc desc;
            desc.name = name;
            desc.type = static_cast<ColType>(type);
            desc.add_ts_idx = add_ts_idx;
            columns.push_back(desc);
        }
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

    static fesql::type::Type ConvertType(fedb::type::DataType type) {
        switch (type) {
            case fedb::type::kBool: return fesql::type::kBool;
            case fedb::type::kSmallInt: return fesql::type::kInt16;
            case fedb::type::kInt: return fesql::type::kInt32;
            case fedb::type::kBigInt: return fesql::type::kInt64;
            case fedb::type::kFloat: return fesql::type::kFloat;
            case fedb::type::kDouble: return fesql::type::kDouble;
            case fedb::type::kDate: return fesql::type::kDate;
            case fedb::type::kTimestamp: return fesql::type::kTimestamp;
            case fedb::type::kVarchar: return fesql::type::kVarchar;
            case fedb::type::kString: return fesql::type::kVarchar;
            default: return fesql::type::kNull;
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
        if (table_info.column_desc_v1_size() > 0) {
            if (modify_index > 0) {
                return ConvertColumnDesc(table_info.column_desc_v1(), columns,
                                         table_info.added_column_desc());
            }
            return ConvertColumnDesc(table_info.column_desc_v1(), columns);
        }
        for (int idx = 0; idx < table_info.column_desc_size(); idx++) {
            ::fedb::codec::ColType type =
                ConvertType(table_info.column_desc(idx).type());
            if (type == ::fedb::codec::ColType::kUnknown) {
                return -1;
            }
            ColumnDesc column_desc;
            column_desc.type = type;
            column_desc.name = table_info.column_desc(idx).name();
            column_desc.add_ts_idx = table_info.column_desc(idx).add_ts_idx();
            column_desc.is_ts_col = false;
            columns.push_back(column_desc);
        }
        if (modify_index > 0) {
            for (int idx = 0; idx < modify_index; idx++) {
                ::fedb::codec::ColType type =
                    ConvertType(table_info.added_column_desc(idx).type());
                if (type == ::fedb::codec::ColType::kUnknown) {
                    return -1;
                }
                ColumnDesc column_desc;
                column_desc.type = type;
                column_desc.name = table_info.added_column_desc(idx).name();
                column_desc.add_ts_idx = false;
                column_desc.is_ts_col = false;
                columns.push_back(column_desc);
            }
        }
        return 0;
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
            column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
            column_desc.is_ts_col = cur_column_desc.is_ts_col();
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
                column_desc.add_ts_idx = false;
                column_desc.is_ts_col = false;
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

    static bool HasTSCol(const std::vector<ColumnDesc>& columns) {
        for (const auto& column_desc : columns) {
            if (column_desc.is_ts_col) {
                return true;
            }
        }
        return false;
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
            ::fedb::type::DataType type = iter->second;
            std::string* val = index->mutable_value();
            if (!::fedb::codec::Convert(kv.second, type, val)) {
                rm.code = -1;
                rm.msg = "convert str " + kv.second + "  failed!";
                return rm;
            }
        }
        rm.code = 0;
        rm.msg = "ok";
        return rm;
    }

    static bool AddTypeToColumnDesc(
            std::shared_ptr<::fedb::nameserver::TableInfo> table_info) {
        for (int i = 0; i < table_info->column_desc_v1_size(); i++) {
            ::fedb::common::ColumnDesc* col_desc =
                table_info->mutable_column_desc_v1(i);
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
