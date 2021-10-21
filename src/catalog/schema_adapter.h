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

#ifndef SRC_CATALOG_SCHEMA_ADAPTER_H_
#define SRC_CATALOG_SCHEMA_ADAPTER_H_

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/base.h"
#include "glog/logging.h"
#include "node/node_enum.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "vm/catalog.h"

namespace openmldb {
namespace catalog {
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;

static const std::unordered_map<::openmldb::type::TTLType, ::hybridse::type::TTLType> TTL_TYPE_MAP = {
    {::openmldb::type::kAbsoluteTime, ::hybridse::type::kTTLTimeLive},
    {::openmldb::type::kLatestTime, ::hybridse::type::kTTLCountLive},
    {::openmldb::type::kAbsAndLat, ::hybridse::type::kTTLTimeLiveAndCountLive},
    {::openmldb::type::kAbsOrLat, ::hybridse::type::kTTLTimeLiveOrCountLive}};

class SchemaAdapter {
 public:
    SchemaAdapter() {}
    ~SchemaAdapter() {}

    static bool ConvertSchemaAndIndex(const ::hybridse::vm::Schema& sql_schema, const ::hybridse::vm::IndexList& index,
                                      RtiDBSchema* schema_output, RtiDBIndex* index_output) {
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
            auto fedb_column = schema_output->Add();
            if (!ConvertType(sql_column, fedb_column)) {
                return false;
            }
        }
        return true;
    }

    static bool ConvertIndex(const RtiDBIndex& index, ::hybridse::vm::IndexList* output) {
        if (output == nullptr) {
            LOG(WARNING) << "output ptr is null";
            return false;
        }
        for (int32_t i = 0; i < index.size(); i++) {
            const ::openmldb::common::ColumnKey& key = index.Get(i);
            ::hybridse::type::IndexDef* index_def = output->Add();
            index_def->set_name(key.index_name());
            index_def->mutable_first_keys()->CopyFrom(key.col_name());
            if (key.has_ts_name() && !key.ts_name().empty()) {
                index_def->set_second_key(key.ts_name());
                index_def->set_ts_offset(0);
            }
            if (key.has_ttl()) {
                auto ttl_type = key.ttl().ttl_type();
                auto it = TTL_TYPE_MAP.find(ttl_type);
                if (it == TTL_TYPE_MAP.end()) {
                    LOG(WARNING) << "not found " << ::openmldb::type::TTLType_Name(ttl_type);
                    return false;
                }
                index_def->set_ttl_type(it->second);
                if (ttl_type == ::openmldb::type::kAbsAndLat || ttl_type == ::openmldb::type::kAbsOrLat) {
                    index_def->add_ttl(key.ttl().abs_ttl());
                    index_def->add_ttl(key.ttl().lat_ttl());
                } else if (ttl_type == ::openmldb::type::kAbsoluteTime) {
                    index_def->add_ttl(key.ttl().abs_ttl());
                } else {
                    index_def->add_ttl(key.ttl().lat_ttl());
                }
            }
        }
        return true;
    }

    static bool SubSchema(const ::hybridse::vm::Schema* schema,
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

    static bool ConvertSchema(const RtiDBSchema& fedb_schema, ::hybridse::vm::Schema* output) {
        if (output == nullptr) {
            LOG(WARNING) << "output ptr is null";
            return false;
        }
        if (fedb_schema.empty()) {
            LOG(WARNING) << "fedb_schema is empty";
            return false;
        }
        for (int32_t i = 0; i < fedb_schema.size(); i++) {
            const common::ColumnDesc& column = fedb_schema.Get(i);
            ::hybridse::type::ColumnDef* new_column = output->Add();
            new_column->set_name(column.name());
            new_column->set_is_not_null(column.not_null());
            new_column->set_is_constant(column.is_constant());
            switch (column.data_type()) {
                case openmldb::type::kBool:
                    new_column->set_type(::hybridse::type::kBool);
                    break;
                case openmldb::type::kSmallInt:
                    new_column->set_type(::hybridse::type::kInt16);
                    break;
                case openmldb::type::kInt:
                    new_column->set_type(::hybridse::type::kInt32);
                    break;
                case openmldb::type::kBigInt:
                    new_column->set_type(::hybridse::type::kInt64);
                    break;
                case openmldb::type::kFloat:
                    new_column->set_type(::hybridse::type::kFloat);
                    break;
                case openmldb::type::kDouble:
                    new_column->set_type(::hybridse::type::kDouble);
                    break;
                case openmldb::type::kDate:
                    new_column->set_type(::hybridse::type::kDate);
                    break;
                case openmldb::type::kTimestamp:
                    new_column->set_type(::hybridse::type::kTimestamp);
                    break;
                case openmldb::type::kString:
                case openmldb::type::kVarchar:
                    new_column->set_type(::hybridse::type::kVarchar);
                    break;
                default:
                    LOG(WARNING) << "type " << ::openmldb::type::DataType_Name(column.data_type())
                                 << " is not supported";
                    return false;
            }
        }
        return true;
    }

    static bool ConvertSchema(const ::hybridse::vm::Schema& hybridse_schema, RtiDBSchema* fedb_schema) {
        if (fedb_schema == nullptr) {
            LOG(WARNING) << "fedb_schema is null";
            return false;
        }
        for (int32_t i = 0; i < hybridse_schema.size(); i++) {
            const hybridse::type::ColumnDef& sql_column = hybridse_schema.Get(i);
            openmldb::common::ColumnDesc* fedb_column = fedb_schema->Add();
            if (!ConvertType(sql_column, fedb_column)) {
                return false;
            }
        }
        return true;
    }

    static bool ConvertType(hybridse::node::DataType hybridse_type, openmldb::type::DataType* fedb_type) {
        if (fedb_type == nullptr) {
            return false;
        }
        switch (hybridse_type) {
            case hybridse::node::kBool:
                *fedb_type = openmldb::type::kBool;
                break;
            case hybridse::node::kInt16:
                *fedb_type = openmldb::type::kSmallInt;
                break;
            case hybridse::node::kInt32:
                *fedb_type = openmldb::type::kInt;
                break;
            case hybridse::node::kInt64:
                *fedb_type = openmldb::type::kBigInt;
                break;
            case hybridse::node::kFloat:
                *fedb_type = openmldb::type::kFloat;
                break;
            case hybridse::node::kDouble:
                *fedb_type = openmldb::type::kDouble;
                break;
            case hybridse::node::kDate:
                *fedb_type = openmldb::type::kDate;
                break;
            case hybridse::node::kTimestamp:
                *fedb_type = openmldb::type::kTimestamp;
                break;
            case hybridse::node::kVarchar:
                *fedb_type = openmldb::type::kVarchar;
                break;
            default:
                LOG(WARNING) << "unsupported type" << hybridse_type;
                return false;
        }
        return true;
    }

    static bool ConvertType(hybridse::type::Type hybridse_type, openmldb::type::DataType* oepnmldb_type) {
        if (oepnmldb_type == nullptr) {
            return false;
        }
        switch (hybridse_type) {
            case hybridse::type::kBool:
                *oepnmldb_type = openmldb::type::kBool;
                break;
            case hybridse::type::kInt16:
                *oepnmldb_type = openmldb::type::kSmallInt;
                break;
            case hybridse::type::kInt32:
                *oepnmldb_type = openmldb::type::kInt;
                break;
            case hybridse::type::kInt64:
                *oepnmldb_type = openmldb::type::kBigInt;
                break;
            case hybridse::type::kFloat:
                *oepnmldb_type = openmldb::type::kFloat;
                break;
            case hybridse::type::kDouble:
                *oepnmldb_type = openmldb::type::kDouble;
                break;
            case hybridse::type::kDate:
                *oepnmldb_type = openmldb::type::kDate;
                break;
            case hybridse::type::kTimestamp:
                *oepnmldb_type = openmldb::type::kTimestamp;
                break;
            case hybridse::type::kVarchar:
                *oepnmldb_type = openmldb::type::kVarchar;
                break;
            default:
                LOG(WARNING) << "unsupported type" << hybridse_type;
                return false;
        }
        return true;
    }

    static bool ConvertType(openmldb::type::DataType oepnmldb_type, hybridse::type::Type* hybridse_type) {
        if (hybridse_type == nullptr) {
            return false;
        }
        switch (oepnmldb_type) {
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
                LOG(WARNING) << "unsupported type: " << openmldb::type::DataType_Name(oepnmldb_type);
                return false;
        }
        return true;
    }

    static bool ConvertType(hybridse::sdk::DataType type, hybridse::type::Type *cased_type) {
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
        return false;
    }
    static bool ConvertType(hybridse::sdk::DataType type, openmldb::type::DataType *cased_type) {
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
        return false;
    }

    static bool ConvertType(const hybridse::type::ColumnDef& sql_column, openmldb::common::ColumnDesc* fedb_column) {
        if (fedb_column == nullptr) {
            LOG(WARNING) << "fedb_column is null";
            return false;
        }
        fedb_column->set_name(sql_column.name());
        fedb_column->set_not_null(sql_column.is_not_null());
        fedb_column->set_is_constant(sql_column.is_constant());
        openmldb::type::DataType openmldb_type;
        if (!ConvertType(sql_column.type(), &openmldb_type)) {
            LOG(WARNING) << "type " << hybridse::type::Type_Name(sql_column.type()) << " is not supported";
            return false;
        }
        fedb_column->set_data_type(openmldb_type);
        return true;
    }
};

}  // namespace catalog
}  // namespace openmldb
#endif  // SRC_CATALOG_SCHEMA_ADAPTER_H_
