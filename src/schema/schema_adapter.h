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

#ifndef SRC_SCHEMA_SCHEMA_ADAPTER_H_
#define SRC_SCHEMA_SCHEMA_ADAPTER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>
#include "base/status.h"
#include "catalog/base.h"
#include "node/node_enum.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "schema/index_util.h"
#include "vm/catalog.h"
#include "node/sql_node.h"

namespace openmldb {
namespace schema {

class SchemaAdapter {
 public:
    static bool ConvertSchemaAndIndex(const ::hybridse::codec::Schema& sql_schema,
            const ::hybridse::vm::IndexList& index,
            PBSchema* schema_output, PBIndex* index_output);

    static bool SubSchema(const ::hybridse::codec::Schema* schema,
            const ::google::protobuf::RepeatedField<uint32_t>& projection,
            hybridse::codec::Schema* output);

    static bool ConvertSchema(const PBSchema& schema, ::hybridse::codec::Schema* output);

    static std::shared_ptr<::hybridse::sdk::Schema> ConvertSchema(const PBSchema& schema);

    static bool ConvertSchema(const ::hybridse::codec::Schema& hybridse_schema, PBSchema* schema);

    static bool ConvertType(hybridse::node::DataType hybridse_type, openmldb::type::DataType* type);

    static absl::Status ConvertType(const hybridse::node::ColumnSchemaNode* sc, common::TableColumnSchema* tbs);

    static bool ConvertType(openmldb::type::DataType type, hybridse::node::DataType* hybridse_type);

    static bool ConvertType(hybridse::type::Type hybridse_type, openmldb::type::DataType* openmldb_type);

    static bool ConvertType(openmldb::type::DataType openmldb_type, hybridse::type::Type* hybridse_type);

    static bool ConvertType(hybridse::sdk::DataType type, hybridse::type::Type *cased_type);

    static bool ConvertType(hybridse::sdk::DataType type, openmldb::type::DataType *cased_type);

    static base::Status CheckTableMeta(const ::openmldb::nameserver::TableInfo& table_info);

    static base::Status CheckTableMeta(const openmldb::api::TableMeta& table_meta);

    static PBSchema BuildSchema(const std::vector<std::string>& fields);

    static std::map<std::string, openmldb::type::DataType> GetColMap(const nameserver::TableInfo& table_info);

 private:
    static bool ConvertColumn(const hybridse::type::ColumnDef& sql_column, openmldb::common::ColumnDesc* column);

    // table column definition to SQL type.
    //
    // NOTE NOT ALL fields from table column are convertable to SQL type, be aware the difference between
    // 'table_column_definition' and 'type' from parser.
    // For example common::ColumnDesc::default_value does not have corresponding field in hybridse::type::ColumnDef.
    static absl::Status ConvertColumn(const openmldb::common::ColumnDesc& column, hybridse::type::ColumnDef* sql_column)
        ABSL_ATTRIBUTE_NONNULL();
    static absl::Status ConvertSchema(const openmldb::common::TableColumnSchema&, hybridse::type::ColumnSchema*)
        ABSL_ATTRIBUTE_NONNULL();
};

}  // namespace schema
}  // namespace openmldb
#endif  // SRC_SCHEMA_SCHEMA_ADAPTER_H_
