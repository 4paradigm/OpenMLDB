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


#pragma once

#include <string>
#include <vector>
#include "sdk/base.h"
#include "sdk/base_impl.h"

namespace fedb {
namespace catalog {

class ProcedureInfoImpl : public hybridse::sdk::ProcedureInfo {
 public:
     ProcedureInfoImpl(const std::string& db_name, const std::string& sp_name,
             const std::string& sql,
             const ::hybridse::sdk::SchemaImpl& input_schema,
             const ::hybridse::sdk::SchemaImpl& output_schema,
             const std::vector<std::string>& tables,
             const std::string& main_table)
        : db_name_(db_name),
          sp_name_(sp_name),
          sql_(sql),
          input_schema_(input_schema),
          output_schema_(output_schema),
          tables_(tables),
          main_table_(main_table) {}

    ~ProcedureInfoImpl() {}

    const ::hybridse::sdk::Schema& GetInputSchema() const override { return input_schema_; }

    const ::hybridse::sdk::Schema& GetOutputSchema() const override { return output_schema_; }

    const std::string& GetDbName() const override { return db_name_; }

    const std::string& GetSpName() const override { return sp_name_; }

    const std::string& GetSql() const override { return sql_; }

    const std::vector<std::string>& GetTables() const override { return tables_; }

    const std::string& GetMainTable() const override { return main_table_; }

 private:
    std::string db_name_;
    std::string sp_name_;
    std::string sql_;
    ::hybridse::sdk::SchemaImpl input_schema_;
    ::hybridse::sdk::SchemaImpl output_schema_;
    std::vector<std::string> tables_;
    std::string main_table_;
};

}  // namespace catalog
}  // namespace fedb
