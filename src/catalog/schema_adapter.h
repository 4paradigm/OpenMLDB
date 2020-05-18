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

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;

class SchemaAdapter {
 public:
    SchemaAdapter() {}
    ~SchemaAdapter() {}

    static bool ConvertSchema(const ::fesql::vm::Schema& sql_schema,
                              RtiDBSchema* output) {
        return false;
    }

    static bool ConvertIndex(const RtiDBIndex& index,
                             ::fesql::vm::IndexList* output) {
        if (output == NULL) {
            LOG(WARNING) << "output ptr is null";
            return false;
        }

        for (int32_t i = 0; i < index.size(); i++) {
            const ::rtidb::common::ColumnKey& key = index.Get(i);
            for (int32_t k = 0; k < key.ts_name_size(); k++) {
                ::fesql::type::IndexDef* index = output->Add();
                index->set_name(key.index_name() + std::to_string(k));
                auto keys = index->mutable_first_keys();
                keys->CopyFrom(key.col_name());
                index->set_second_key(key.ts_name(k));
                index->set_ts_offset(k);
            }
        }
        return true;
    }

    static bool ConvertSchema(const RtiDBSchema& rtidb_schema,
                              ::fesql::vm::Schema* output) {
        if (output == NULL) {
            LOG(WARNING) << "output ptr is null";
            return false;
        }
        for (int32_t i = 0; i < rtidb_schema.size(); i++) {
            const common::ColumnDesc& column = rtidb_schema.Get(i);
            ::fesql::type::ColumnDef* new_column = output->Add();
            new_column->set_name(column.name());
            new_column->set_is_not_null(column.not_null());
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
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_SCHEMA_ADAPTER_H_
