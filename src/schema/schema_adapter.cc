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

#include "schema/schema_adapter.h"
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include "glog/logging.h"

namespace openmldb {
namespace schema {

bool SchemaAdapter::ConvertSchemaAndIndex(const ::hybridse::vm::Schema& sql_schema,
        const ::hybridse::vm::IndexList& index,
        PBSchema* schema_output, PBIndex* index_output) {
    if (nullptr == schema_output || nullptr == index_output) {
        LOG(WARNING) << "schema or index output ptr is null";
        return false;
    }

    std::set<std::string> ts_cols;
    // Conver Index
    for (int32_t i = 0; i < index.size(); i++) {
        auto& sql_key = index.Get(i);
        auto index = index_output->Add();
        index->set_index_name(sql_key.name());
        for (int32_t k = 0; k < sql_key.first_keys_size(); k++) {
            index->add_col_name(sql_key.first_keys(k));
        }
        index->set_ts_name(sql_key.second_key());
        ts_cols.insert(sql_key.second_key());
    }

    for (int32_t i = 0; i < sql_schema.size(); i++) {
        auto& sql_column = sql_schema.Get(i);
        auto column = schema_output->Add();
        if (!ConvertColumn(sql_column, column)) {
            return false;
        }
    }
    return true;
}

bool SchemaAdapter::SubSchema(const ::hybridse::vm::Schema* schema,
                      const ::google::protobuf::RepeatedField<uint32_t>& projection, hybridse::vm::Schema* output) {
    if (output == nullptr) {
        LOG(WARNING) << "output ptr is null";
        return false;
    }
    auto it = projection.begin();
    for (; it != projection.end(); ++it) {
        const hybridse::type::ColumnDef& col = schema->Get(*it);
        output->Add()->CopyFrom(col);
    }
    return true;
}
std::shared_ptr<::hybridse::sdk::Schema> SchemaAdapter::ConvertSchema(const PBSchema& schema) {
    ::hybridse::vm::Schema vm_schema;
    ConvertSchema(schema, &vm_schema);
    return std::make_shared<::hybridse::sdk::SchemaImpl>(vm_schema);
}

bool SchemaAdapter::ConvertSchema(const PBSchema& schema, ::hybridse::vm::Schema* output) {
    if (output == nullptr) {
        LOG(WARNING) << "output ptr is null";
        return false;
    }
    if (schema.empty()) {
        LOG(WARNING) << "schema is empty";
        return false;
    }
    for (int32_t i = 0; i < schema.size(); i++) {
        const common::ColumnDesc& column = schema.Get(i);
        ::hybridse::type::ColumnDef* new_column = output->Add();
        new_column->set_name(column.name());
        new_column->set_is_not_null(column.not_null());
        new_column->set_is_constant(column.is_constant());
        ::hybridse::type::Type type;
        if (!ConvertType(column.data_type(), &type)) {
            LOG(WARNING) << "type " << ::openmldb::type::DataType_Name(column.data_type())
                         << " is not supported";
            return false;
        }
        new_column->set_type(type);
    }
    return true;
}

bool SchemaAdapter::ConvertSchema(const ::hybridse::vm::Schema& hybridse_schema, PBSchema* schema) {
    if (schema == nullptr) {
        LOG(WARNING) << "schema is null";
        return false;
    }
    for (int32_t i = 0; i < hybridse_schema.size(); i++) {
        const hybridse::type::ColumnDef& sql_column = hybridse_schema.Get(i);
        openmldb::common::ColumnDesc* column = schema->Add();
        if (!ConvertColumn(sql_column, column)) {
            return false;
        }
    }
    return true;
}

bool SchemaAdapter::ConvertType(hybridse::node::DataType hybridse_type, openmldb::type::DataType* type) {
    if (type == nullptr) {
        return false;
    }
    switch (hybridse_type) {
        case hybridse::node::kBool:
            *type = openmldb::type::kBool;
            break;
        case hybridse::node::kInt16:
            *type = openmldb::type::kSmallInt;
            break;
        case hybridse::node::kInt32:
            *type = openmldb::type::kInt;
            break;
        case hybridse::node::kInt64:
            *type = openmldb::type::kBigInt;
            break;
        case hybridse::node::kFloat:
            *type = openmldb::type::kFloat;
            break;
        case hybridse::node::kDouble:
            *type = openmldb::type::kDouble;
            break;
        case hybridse::node::kDate:
            *type = openmldb::type::kDate;
            break;
        case hybridse::node::kTimestamp:
            *type = openmldb::type::kTimestamp;
            break;
        case hybridse::node::kVarchar:
            *type = openmldb::type::kVarchar;
            break;
        default:
            LOG(WARNING) << "unsupported type" << hybridse_type;
            return false;
    }
    return true;
}

bool SchemaAdapter::ConvertType(openmldb::type::DataType type, hybridse::node::DataType* hybridse_type) {
    if (hybridse_type == nullptr) {
        return false;
    }
    switch (type) {
        case openmldb::type::kBool:
            *hybridse_type = hybridse::node::kBool;
            break;
        case openmldb::type::kSmallInt:
            *hybridse_type = hybridse::node::kInt16;
            break;
        case openmldb::type::kInt:
            *hybridse_type = hybridse::node::kInt32;
            break;
        case openmldb::type::kBigInt:
            *hybridse_type = hybridse::node::kInt64;
            break;
        case openmldb::type::kFloat:
            *hybridse_type = hybridse::node::kFloat;
            break;
        case openmldb::type::kDouble:
            *hybridse_type = hybridse::node::kDouble;
            break;
        case openmldb::type::kDate:
            *hybridse_type = hybridse::node::kDate;
            break;
        case openmldb::type::kTimestamp:
            *hybridse_type = hybridse::node::kTimestamp;
            break;
        case openmldb::type::kString:
        case openmldb::type::kVarchar:
            *hybridse_type = hybridse::node::kVarchar;
            break;
        default:
            LOG(WARNING) << "unsupported type" << openmldb::type::DataType_Name(type);
            return false;
    }
    return true;
}

bool SchemaAdapter::ConvertType(hybridse::type::Type hybridse_type, openmldb::type::DataType* openmldb_type) {
    if (openmldb_type == nullptr) {
        return false;
    }
    switch (hybridse_type) {
        case hybridse::type::kBool:
            *openmldb_type = openmldb::type::kBool;
            break;
        case hybridse::type::kInt16:
            *openmldb_type = openmldb::type::kSmallInt;
            break;
        case hybridse::type::kInt32:
            *openmldb_type = openmldb::type::kInt;
            break;
        case hybridse::type::kInt64:
            *openmldb_type = openmldb::type::kBigInt;
            break;
        case hybridse::type::kFloat:
            *openmldb_type = openmldb::type::kFloat;
            break;
        case hybridse::type::kDouble:
            *openmldb_type = openmldb::type::kDouble;
            break;
        case hybridse::type::kDate:
            *openmldb_type = openmldb::type::kDate;
            break;
        case hybridse::type::kTimestamp:
            *openmldb_type = openmldb::type::kTimestamp;
            break;
        case hybridse::type::kVarchar:
            *openmldb_type = openmldb::type::kVarchar;
            break;
        default:
            LOG(WARNING) << "unsupported type" << hybridse_type;
            return false;
    }
    return true;
}

bool SchemaAdapter::ConvertType(openmldb::type::DataType openmldb_type, hybridse::type::Type* hybridse_type) {
    if (hybridse_type == nullptr) {
        return false;
    }
    switch (openmldb_type) {
        case openmldb::type::kBool:
            *hybridse_type = hybridse::type::kBool;
            break;
        case openmldb::type::kSmallInt:
            *hybridse_type = hybridse::type::kInt16;
            break;
        case openmldb::type::kInt:
            *hybridse_type = hybridse::type::kInt32;
            break;
        case openmldb::type::kBigInt:
            *hybridse_type = hybridse::type::kInt64;
            break;
        case openmldb::type::kFloat:
            *hybridse_type = hybridse::type::kFloat;
            break;
        case openmldb::type::kDouble:
            *hybridse_type = hybridse::type::kDouble;
            break;
        case openmldb::type::kDate:
            *hybridse_type = hybridse::type::kDate;
            break;
        case openmldb::type::kTimestamp:
            *hybridse_type = hybridse::type::kTimestamp;
            break;
        case openmldb::type::kVarchar:
        case openmldb::type::kString:
            *hybridse_type = hybridse::type::kVarchar;
            break;
        default:
            LOG(WARNING) << "unsupported type: " << openmldb::type::DataType_Name(openmldb_type);
            return false;
    }
    return true;
}

bool SchemaAdapter::ConvertType(hybridse::sdk::DataType type, hybridse::type::Type *cased_type) {
    switch (type) {
        case hybridse::sdk::DataType::kTypeBool:
            *cased_type = hybridse::type::kBool;
            return true;
        case hybridse::sdk::DataType::kTypeInt16:
            *cased_type = hybridse::type::kInt16;
            return true;
        case hybridse::sdk::DataType::kTypeInt32:
            *cased_type = hybridse::type::kInt32;
            return true;
        case hybridse::sdk::DataType::kTypeInt64:
            *cased_type = hybridse::type::kInt64;
            return true;
        case hybridse::sdk::DataType::kTypeFloat:
            *cased_type = hybridse::type::kFloat;
            return true;
        case hybridse::sdk::DataType::kTypeDouble:
            *cased_type = hybridse::type::kDouble;
            return true;
        case hybridse::sdk::DataType::kTypeDate:
            *cased_type = hybridse::type::kDate;
            return true;
        case hybridse::sdk::DataType::kTypeTimestamp:
            *cased_type = hybridse::type::kTimestamp;
            return true;
        case hybridse::sdk::DataType::kTypeString:
            *cased_type = hybridse::type::kVarchar;
            return true;
        default:
            return false;
    }
}
bool SchemaAdapter::ConvertType(hybridse::sdk::DataType type, openmldb::type::DataType *cased_type) {
    switch (type) {
        case hybridse::sdk::DataType::kTypeBool:
            *cased_type = openmldb::type::kBool;
            return true;
        case hybridse::sdk::DataType::kTypeInt16:
            *cased_type = openmldb::type::kSmallInt;
            return true;
        case hybridse::sdk::DataType::kTypeInt32:
            *cased_type = openmldb::type::kInt;
            return true;
        case hybridse::sdk::DataType::kTypeInt64:
            *cased_type = openmldb::type::kBigInt;
            return true;
        case hybridse::sdk::DataType::kTypeFloat:
            *cased_type = openmldb::type::kFloat;
            return true;
        case hybridse::sdk::DataType::kTypeDouble:
            *cased_type = openmldb::type::kDouble;
            return true;
        case hybridse::sdk::DataType::kTypeDate:
            *cased_type = openmldb::type::kDate;
            return true;
        case hybridse::sdk::DataType::kTypeTimestamp:
            *cased_type = openmldb::type::kTimestamp;
            return true;
        case hybridse::sdk::DataType::kTypeString:
            *cased_type = openmldb::type::kString;
            return true;
        default:
            return false;
    }
}

bool SchemaAdapter::ConvertColumn(const hybridse::type::ColumnDef& sql_column, openmldb::common::ColumnDesc* column) {
    if (column == nullptr) {
        LOG(WARNING) << "column is null";
        return false;
    }
    column->set_name(sql_column.name());
    column->set_not_null(sql_column.is_not_null());
    column->set_is_constant(sql_column.is_constant());
    openmldb::type::DataType openmldb_type;
    if (!ConvertType(sql_column.type(), &openmldb_type)) {
        LOG(WARNING) << "type " << hybridse::type::Type_Name(sql_column.type()) << " is not supported";
        return false;
    }
    column->set_data_type(openmldb_type);
    return true;
}

std::map<std::string, openmldb::type::DataType> SchemaAdapter::GetColMap(const nameserver::TableInfo& table_info) {
    std::map<std::string, openmldb::type::DataType> col_map;
    for (const auto& col : table_info.column_desc()) {
        col_map.emplace(col.name(), col.data_type());
    }
    return col_map;
}

base::Status SchemaAdapter::CheckTableMeta(const openmldb::api::TableMeta& table_meta) {
    if (table_meta.name().empty()) {
        return {base::ReturnCode::kError, "table name is empty"};
    }
    if (table_meta.tid() <= 0) {
        return {base::ReturnCode::kError, "tid <= 0, invalid tid"};
    }
    if (table_meta.storage_mode() == common::kUnknown) {
        return {base::ReturnCode::kError, "storage_mode is unknown"};
    }
    if (table_meta.column_desc_size() == 0) {
        return {base::ReturnCode::kError, "no column"};
    }
    std::map<std::string, ::openmldb::common::ColumnDesc> column_map;
    for (const auto& column_desc : table_meta.column_desc()) {
        if (!column_map.emplace(column_desc.name(), column_desc).second) {
            return {base::ReturnCode::kError, "duplicated column: " + column_desc.name()};
        }
    }
    auto status = IndexUtil::CheckIndex(column_map, table_meta.column_key());
    if (!status.OK()) {
        return status;
    }
    return {};
}

base::Status SchemaAdapter::CheckTableMeta(const ::openmldb::nameserver::TableInfo& table_info) {
    if (table_info.name().empty()) {
        return {base::ReturnCode::kError, "table name is empty"};
    }
    if (table_info.column_desc_size() == 0) {
        return {base::ReturnCode::kError, "no column"};
    }
    std::map<std::string, ::openmldb::common::ColumnDesc> column_map;
    for (const auto& column_desc : table_info.column_desc()) {
        if (!column_map.emplace(column_desc.name(), column_desc).second) {
            return {base::ReturnCode::kError, "duplicated column: " + column_desc.name()};
        }
    }
    auto status = IndexUtil::CheckIndex(column_map, table_info.column_key());
    if (!status.OK()) {
        return status;
    }
    std::set<std::string> partition_keys;
    for (int idx = 0; idx < table_info.partition_key_size(); idx++) {
        const std::string& partition_column = table_info.partition_key(idx);
        if (column_map.find(partition_column) == column_map.end()) {
            return {base::ReturnCode::kError, "not found column " + partition_column};
        }
        if (partition_keys.find(partition_column) != partition_keys.end()) {
            return {base::ReturnCode::kError, "duplicated partition_key: " + partition_column};
        }
        partition_keys.insert(partition_column);
    }
    return {};
}

PBSchema SchemaAdapter::BuildSchema(const std::vector<std::string>& fields) {
    PBSchema schema;
    for (const auto& field : fields) {
        auto column = schema.Add();
        column->set_name(field);
        column->set_data_type(openmldb::type::kString);
    }
    return schema;
}

}  // namespace schema
}  // namespace openmldb
