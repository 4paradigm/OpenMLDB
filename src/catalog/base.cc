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

#include "catalog/base.h"
#include "schema/schema_adapter.h"
namespace openmldb {
namespace catalog {

ProcedureInfoImpl::ProcedureInfoImpl(const ::openmldb::api::ProcedureInfo& procedure) :
    db_name_(procedure.db_name()), sp_name_(procedure.sp_name()), sql_(procedure.sql()),
    main_table_(procedure.main_table()), main_db_(procedure.main_db()),
    type_(::hybridse::sdk::ProcedureType::kReqProcedure) {
    if (procedure.input_schema_size() > 0) {
        ::hybridse::vm::Schema hybridse_in_schema;
        openmldb::schema::SchemaAdapter::ConvertSchema(procedure.input_schema(), &hybridse_in_schema);
        input_schema_.SetSchema(hybridse_in_schema);
    }
    if (procedure.output_schema_size() > 0) {
        ::hybridse::vm::Schema hybridse_out_schema;
        openmldb::schema::SchemaAdapter::ConvertSchema(procedure.output_schema(), &hybridse_out_schema);
        output_schema_.SetSchema(hybridse_out_schema);
    }

    for (const auto& namePair : procedure.tables()) {
        dbs_.push_back(namePair.db_name());
        tables_.push_back(namePair.table_name());
    }
    if (procedure.type() == ::openmldb::type::ProcedureType::kReqDeployment) {
        type_ = ::hybridse::sdk::ProcedureType::kReqDeployment;
    }

    for (const auto& op : procedure.options()) {
        options_[op.name()] = op.value().value();
    }
    if (procedure.router_col_size() > 0) {
        router_col_ = procedure.router_col(0);
    }
}

}  // namespace catalog
}  // namespace openmldb
