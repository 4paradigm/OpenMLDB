/*
 * schema_adapter.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CATALOG_SCHEMA_ADAPTER_H_
#define SRC_CATALOG_SCHEMA_ADAPTER_H_

#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "vm/catalog.h"
#include "proto/tablet.pb.h"
#include "catalog/base.h"

namespace rtidb {
namespace catalog {
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;


static const std::unordered_map<::rtidb::api::TTLType, ::fesql::type::TTLType>
    TTL_TYPE_MAP = {{::rtidb::api::kAbsoluteTime, ::fesql::type::kTTLTimeLive},
                    {::rtidb::api::kLatestTime, ::fesql::type::kTTLCountLive},
                    {::rtidb::api::kAbsAndLat, ::fesql::type::kTTLTimeLiveAndCountLive},
                    {::rtidb::api::kAbsOrLat, ::fesql::type::kTTLTimeLiveOrCountLive}};

class SchemaAdapter {
 public:
    SchemaAdapter() {}
    ~SchemaAdapter() {}

    static bool ConvertSchemaAndIndex(const ::fesql::vm::Schema& sql_schema,
                                      const ::fesql::vm::IndexList& index,
                                      RtiDBSchema* schema_output,
                                      RtiDBIndex* index_output) {
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
            index->add_ts_name(sql_key.second_key());
            ts_cols.insert(sql_key.second_key());
        }

        for (int32_t i = 0; i < sql_schema.size(); i++) {
            auto& sql_column = sql_schema.Get(i);
            auto rtidb_column = schema_output->Add();
            if (!ConvertType(sql_column, rtidb_column)) {
                return false;
            }
            if (ts_cols.find(sql_column.name()) != ts_cols.cend()) {
                rtidb_column->set_is_ts_col(true);
            }
        }
        return true;
    }

    static bool ConvertIndex(const RtiDBIndex& index,
                             ::fesql::vm::IndexList* output) {
        if (output == nullptr) {
            LOG(WARNING) << "output ptr is null";
            return false;
        }

        for (int32_t i = 0; i < index.size(); i++) {
            const ::rtidb::common::ColumnKey& key = index.Get(i);
            if (key.ts_name_size() > 0) {
                for (int32_t k = 0; k < key.ts_name_size(); k++) {
                    ::fesql::type::IndexDef* index = output->Add();
                    if (k > 0) {
                        index->set_name(key.index_name() + std::to_string(k));
                    } else {
                        index->set_name(key.index_name());
                    }
                    auto keys = index->mutable_first_keys();
                    keys->CopyFrom(key.col_name());
                    index->set_second_key(key.ts_name(k));
                    index->set_ts_offset(k);
                }
            } else {
                ::fesql::type::IndexDef* index = output->Add();
                index->set_name(key.index_name());
                index->mutable_first_keys()->CopyFrom(key.col_name());
            }
        }
        return true;
    }

    static bool ConvertSchema(const RtiDBSchema& rtidb_schema,
                              ::fesql::vm::Schema* output) {
        if (output == nullptr) {
            LOG(WARNING) << "output ptr is null";
            return false;
        }
        if (rtidb_schema.empty()) {
            LOG(WARNING) << "rtidb_schema is empty";
            return false;
        }
        for (int32_t i = 0; i < rtidb_schema.size(); i++) {
            const common::ColumnDesc& column = rtidb_schema.Get(i);
            ::fesql::type::ColumnDef* new_column = output->Add();
            new_column->set_name(column.name());
            new_column->set_is_not_null(column.not_null());
            new_column->set_is_constant(column.is_constant());
            switch (column.data_type()) {
                case rtidb::type::kBool:
                    new_column->set_type(::fesql::type::kBool);
                    break;
                case rtidb::type::kSmallInt:
                    new_column->set_type(::fesql::type::kInt16);
                    break;
                case rtidb::type::kInt:
                    new_column->set_type(::fesql::type::kInt32);
                    break;
                case rtidb::type::kBigInt:
                    new_column->set_type(::fesql::type::kInt64);
                    break;
                case rtidb::type::kFloat:
                    new_column->set_type(::fesql::type::kFloat);
                    break;
                case rtidb::type::kDouble:
                    new_column->set_type(::fesql::type::kDouble);
                    break;
                case rtidb::type::kDate:
                    new_column->set_type(::fesql::type::kDate);
                    break;
                case rtidb::type::kTimestamp:
                    new_column->set_type(::fesql::type::kTimestamp);
                    break;
                case rtidb::type::kString:
                case rtidb::type::kVarchar:
                    new_column->set_type(::fesql::type::kVarchar);
                    break;
                default:
                    LOG(WARNING)
                        << "type "
                        << ::rtidb::type::DataType_Name(column.data_type())
                        << " is not supported";
                    return false;
            }
        }
        return true;
    }

    static bool ConvertSchema(const ::fesql::vm::Schema& fesql_schema,
            RtiDBSchema* rtidb_schema) {
        if (rtidb_schema == nullptr) {
            LOG(WARNING) << "rtidb_schema is null";
            return false;
        }
        for (int32_t i = 0; i < fesql_schema.size(); i++) {
            const fesql::type::ColumnDef& sql_column = fesql_schema.Get(i);
            rtidb::common::ColumnDesc* rtidb_column = rtidb_schema->Add();
            if (!ConvertType(sql_column, rtidb_column)) {
                return false;
            }
        }
        return true;
    }

    static bool ConvertType(fesql::node::DataType fesql_type,
            rtidb::type::DataType* rtidb_type) {
        if (rtidb_type == nullptr) {
            return false;
        }
        switch (fesql_type) {
            case fesql::node::kBool:
                *rtidb_type = rtidb::type::kBool;
                break;
            case fesql::node::kInt16:
                *rtidb_type = rtidb::type::kSmallInt;
                break;
            case fesql::node::kInt32:
                *rtidb_type = rtidb::type::kInt;
                break;
            case fesql::node::kInt64:
                *rtidb_type = rtidb::type::kBigInt;
                break;
            case fesql::node::kFloat:
                *rtidb_type = rtidb::type::kFloat;
                break;
            case fesql::node::kDouble:
                *rtidb_type = rtidb::type::kDouble;
                break;
            case fesql::node::kDate:
                *rtidb_type = rtidb::type::kDate;
                break;
            case fesql::node::kTimestamp:
                *rtidb_type = rtidb::type::kTimestamp;
                break;
            case fesql::node::kVarchar:
                *rtidb_type = rtidb::type::kVarchar;
                break;
            default:
                LOG(WARNING) << "unsupported type" << fesql_type;
                return false;
        }
        return true;
    }

    static bool ConvertType(const fesql::type::ColumnDef& sql_column,
            rtidb::common::ColumnDesc* rtidb_column) {
        if (rtidb_column == nullptr) {
            LOG(WARNING) << "rtidb_column is null";
            return false;
        }
        rtidb_column->set_name(sql_column.name());
        rtidb_column->set_not_null(sql_column.is_not_null());
        rtidb_column->set_is_constant(sql_column.is_constant());
        switch (sql_column.type()) {
            case fesql::type::kBool:
                rtidb_column->set_data_type(rtidb::type::kBool);
                break;
            case fesql::type::kInt16:
                rtidb_column->set_data_type(rtidb::type::kSmallInt);
                break;
            case fesql::type::kInt32:
                rtidb_column->set_data_type(rtidb::type::kInt);
                break;
            case fesql::type::kInt64:
                rtidb_column->set_data_type(rtidb::type::kBigInt);
                break;
            case fesql::type::kFloat:
                rtidb_column->set_data_type(rtidb::type::kFloat);
                break;
            case fesql::type::kDouble:
                rtidb_column->set_data_type(rtidb::type::kDouble);
                break;
            case fesql::type::kDate:
                rtidb_column->set_data_type(rtidb::type::kDate);
                break;
            case fesql::type::kTimestamp:
                rtidb_column->set_data_type(rtidb::type::kTimestamp);
                break;
            case fesql::type::kVarchar:
                rtidb_column->set_data_type(rtidb::type::kVarchar);
                break;
            default:
                LOG(WARNING) << "type "
                    << fesql::type::Type_Name(sql_column.type())
                    << " is not supported";
                return false;
        }
        return true;
    }

    static std::shared_ptr<fesql::sdk::ProcedureInfo> ConvertProcedureInfo(
            const rtidb::api::ProcedureInfo& sp_info) {
        ::fesql::vm::Schema fesql_in_schema;
        if (!rtidb::catalog::SchemaAdapter::ConvertSchema(sp_info.input_schema(), &fesql_in_schema)) {
            LOG(WARNING) << "fail to convert input schema";
            return nullptr;
        }
        ::fesql::vm::Schema fesql_out_schema;
        if (!rtidb::catalog::SchemaAdapter::ConvertSchema(sp_info.output_schema(), &fesql_out_schema)) {
            LOG(WARNING) << "fail to convert output schema";
            return nullptr;
        }
        ::fesql::sdk::SchemaImpl input_schema(fesql_in_schema);
        ::fesql::sdk::SchemaImpl output_schema(fesql_out_schema);
        std::vector<std::string> table_vec;
        auto& tables = sp_info.tables();
        for (const auto& table : tables) {
            table_vec.push_back(table);
        }
        std::shared_ptr<rtidb::catalog::ProcedureInfoImpl> sp_info_impl =
            std::make_shared<rtidb::catalog::ProcedureInfoImpl>(
                    sp_info.db_name(), sp_info.sp_name(), sp_info.sql(), input_schema, output_schema,
                    table_vec, sp_info.main_table());
        return sp_info_impl;
    }
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_SCHEMA_ADAPTER_H_
