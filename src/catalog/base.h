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
#include <unordered_map>

#include "proto/sql_procedure.pb.h"
#include "sdk/base.h"
#include "sdk/base_impl.h"

namespace openmldb {
namespace catalog {

class ProcedureInfoImpl : public hybridse::sdk::ProcedureInfo {
 public:
    explicit ProcedureInfoImpl(const ::openmldb::api::ProcedureInfo& procedure);

    ~ProcedureInfoImpl() = default;

    const ::hybridse::sdk::Schema& GetInputSchema() const override { return input_schema_; }

    const ::hybridse::sdk::Schema& GetOutputSchema() const override { return output_schema_; }

    const std::string& GetDbName() const override { return db_name_; }

    const std::string& GetSpName() const override { return sp_name_; }

    const std::string& GetSql() const override { return sql_; }

    const std::vector<std::string>& GetTables() const override { return tables_; }
    const std::vector<std::string>& GetDbs() const override { return dbs_; }

    const std::string& GetMainTable() const override { return main_table_; }
    const std::string& GetMainDb() const override { return main_db_; }

    ::hybridse::sdk::ProcedureType GetType() const override { return type_; }

    const std::string* GetOption(const std::string& key) const override {
        if (options_.count(key)) {
            return &options_.at(key);
        } else {
            return nullptr;
        }
    }

    const std::unordered_map<std::string, std::string>* GetOption() const override {
        return &options_;
    }

    int GetRouterCol() const override {
        return router_col_;
    }

 private:
    std::string db_name_;
    std::string sp_name_;
    std::string sql_;
    ::hybridse::sdk::SchemaImpl input_schema_;
    ::hybridse::sdk::SchemaImpl output_schema_;
    std::vector<std::string> tables_;
    std::vector<std::string> dbs_;
    std::string main_table_;
    std::string main_db_;
    ::hybridse::sdk::ProcedureType type_;
    std::unordered_map<std::string, std::string> options_;
    int router_col_ = -1;
};

}  // namespace catalog
}  // namespace openmldb
