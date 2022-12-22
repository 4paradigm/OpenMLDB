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

#ifndef HYBRIDSE_INCLUDE_CASE_SQL_CASE_H_
#define HYBRIDSE_INCLUDE_CASE_SQL_CASE_H_

#include <set>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "codec/fe_row_codec.h"
#include "proto/fe_type.pb.h"
#include "vm/catalog.h"
#include "yaml-cpp/node/node.h"
#include "yaml-cpp/yaml.h"

namespace hybridse {
namespace sqlcase {
class SqlCase {
 public:
    struct TableInfo {
        std::string db_;
        std::string name_;
        std::string schema_;
        std::string index_;
        std::string data_;
        std::string order_;
        std::vector<std::string> indexs_;
        std::vector<std::string> columns_;
        std::vector<std::vector<std::string>> rows_;
        std::string create_;
        std::string insert_;
        std::vector<std::string> inserts_;
        std::set<size_t> common_column_indices_;
        std::string repeat_tag_;
        int64_t repeat_ = 1;
    };
    struct ExpectInfo {
        int64_t count_ = -1;
        std::vector<std::string> columns_;
        std::vector<std::vector<std::string>> rows_;
        std::string schema_;
        std::string data_;
        std::string order_;
        std::set<size_t> common_column_indices_;
        // SqlNode's TreeString output
        std::string node_tree_str_;
        // PlanNode TreeString
        std::string plan_tree_str_;
        bool success_ = true;
        int code_ = -1;
        std::string msg_;
    };
    SqlCase() {}
    virtual ~SqlCase() {}

    const std::string id() const { return id_; }
    const std::string& desc() const { return desc_; }
    const std::string case_name() const;
    const std::string& mode() const { return mode_; }
    int level() const { return level_; }
    const std::string& cluster_request_plan() const {
        return cluster_request_plan_;
    }
    const std::string& request_plan() const { return request_plan_; }
    const std::string& batch_plan() const { return batch_plan_; }
    const std::string& sql_str() const { return sql_str_; }
    const bool standard_sql() const { return standard_sql_; }
    const bool standard_sql_compatible() const {
        return standard_sql_compatible_;
    }
    const bool debug() const { return debug_; }
    const std::string& db() const { return db_; }
    const std::vector<TableInfo>& inputs() const { return inputs_; }
    const TableInfo& parameters() const { return parameters_; }
    const TableInfo& batch_request() const { return batch_request_; }
    const ExpectInfo& expect() const { return expect_; }
    void set_expect(const ExpectInfo& data) { expect_ = data; }
    void set_input_name(const std::string name, int32_t idx) {
        if (idx < static_cast<int32_t>(inputs_.size())) {
            inputs_[idx].name_ = name;
        }
    }

    const int32_t CountInputs() const { return inputs_.size(); }
    // extract schema from schema string
    // name:type|name:type|name:type|
    bool ExtractInputTableDef(type::TableDef& table,  // NOLINT
                              int32_t input_idx = 0) const;
    bool ExtractInputTableDef(const TableInfo& info,
                              type::TableDef& table) const;  // NOLINT
    bool BuildCreateSqlFromInput(int32_t input_idx, std::string* sql,
                                 int partition_num = 1) const;
    bool BuildInsertSqlFromInput(int32_t input_idx, std::string* sql) const;
    bool BuildInsertSqlListFromInput(int32_t input_idx,
                                     std::vector<std::string>* sql_list) const;
    bool ExtractOutputSchema(type::TableDef& table) const;          // NOLINT
    const codec::Schema ExtractParameterTypes() const;

    bool ExtractInputData(std::vector<hybridse::codec::Row>& rows,  // NOLINT
                          int32_t input_idx = 0) const;
    bool ExtractInputData(
        const TableInfo& info,
        std::vector<hybridse::codec::Row>& rows) const;  // NOLINT
    bool ExtractOutputData(
        std::vector<hybridse::codec::Row>& rows) const;  // NOLINT

    bool AddInput(const TableInfo& table_data);
    static bool TypeParse(const std::string& row_str,
                          hybridse::type::Type* type);
    static bool TTLTypeParse(const std::string& type_str,
                             ::hybridse::type::TTLType* type);
    static bool TTLParse(const std::string& type_str,
                         std::vector<int64_t>& ttls);  // NOLINT

    static const std::string TypeString(hybridse::type::Type type);
    static bool ExtractSchema(const std::vector<std::string>& columns,
                              type::TableDef& table);  // NOLINT
    static bool ExtractSchema(const std::string& schema_str,
                              type::TableDef& table);  // NOLINT
    static bool BuildCreateSqlFromSchema(const type::TableDef& table,
                                         std::string* create_sql,
                                         bool isGenerateIndex = true,
                                         int partition_num = 1);  // NOLINT
    static bool ExtractIndex(const std::string& index_str,
                             type::TableDef& table);  // NOLINT
    static bool ExtractIndex(const std::vector<std::string>& indexs,
                             type::TableDef& table);  // NOLINT
    static bool ExtractTableDef(const std::vector<std::string>& columns,
                                const std::vector<std::string>& indexs,
                                type::TableDef& table);  // NOLINT
    static bool ExtractTableDef(const std::string& schema_str,
                                const std::string& index_str,
                                type::TableDef& table);  // NOLINT
    static bool ExtractRows(const codec::Schema& schema,
                            const std::string& data_str,
                            std::vector<hybridse::codec::Row>& rows);  // NOLINT
    static bool ExtractRows(
        const codec::Schema& schema,
        const std::vector<std::vector<std::string>>& row_vec,
        std::vector<hybridse::codec::Row>& rows);  // NOLINT
    static bool BuildInsertSqlFromData(const type::TableDef& table,
                                       std::string data,
                                       std::string* insert_sql);
    static bool BuildInsertValueStringFromRow(
        const type::TableDef& table, const std::vector<std::string>& item_vec,
        std::string* create_sql);
    static bool BuildInsertSqlFromRows(
        const type::TableDef& table,
        const std::vector<std::vector<std::string>>& rows,
        std::string* insert_sql);
    static bool ExtractRow(const codec::Schema& schema, const std::string& row_str,
                           int8_t** out_ptr, int32_t* out_size);
    static bool ExtractRow(const codec::Schema& schema,
                           const std::vector<std::string>& item_vec,
                           int8_t** out_ptr, int32_t* out_size);
    static bool CreateTableInfoFromYamlNode(const YAML::Node& node,
                                            SqlCase::TableInfo* output);
    static bool CreateExpectFromYamlNode(const YAML::Node& schema_data,
                                         const YAML::Node& expect_provider,
                                         SqlCase::ExpectInfo* table);
    static bool LoadSchemaAndRowsFromYaml(
        const std::string& cases_dir, const std::string& resource_path,
        type::TableDef& table,                     // NOLINT
        std::vector<hybridse::codec::Row>& rows);  // NOLINT
    static bool CreateSqlCasesFromYaml(
        const std::string& cases_dir, const std::string& yaml_path,
        std::vector<SqlCase>& sql_case_ptr,  // NOLINT
        const std::string filter_mode = "");
    static bool CreateSqlCasesFromYaml(
        const std::string& cases_dir, const std::string& yaml_path,
        std::vector<SqlCase>& sql_case_ptr,  // NOLINT
        const std::vector<std::string>& filter_modes);
    static bool CreateTableInfoFromYaml(const std::string& cases_dir,
                                        const std::string& yaml_path,
                                        TableInfo* table_info);
    static bool CreateStringListFromYamlNode(
        const YAML::Node& node,
        std::vector<std::string>& rows);  // NOLINT
    static bool CreateRowsFromYamlNode(
        const YAML::Node& node,
        std::vector<std::vector<std::string>>& rows);  // NOLINT
    static std::string GenRand(const std::string& prefix) {
        return prefix + std::to_string(rand() % 10000000 + 1);  // NOLINT
    }
    absl::StatusOr<std::string> BuildCreateSpSqlFromInput(int32_t input_idx, absl::string_view sql,
                                                          const std::set<size_t>& common_idx);
    absl::StatusOr<std::string> BuildCreateSpSqlFromSchema(const type::TableDef& table, absl::string_view select_sql,
                                                           const std::set<size_t>& common_idx);

    friend std::ostream& operator<<(std::ostream& output, const SqlCase& thiz);
    static bool IS_PERF() {
        const char* env_name = "HYBRIDSE_PERF";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }
    static std::set<std::string> HYBRIDSE_LEVEL();

    static std::string SqlCaseBaseDir();

    static bool IsDebug() {
        const char* env_name = "HYBRIDSE_DEV";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        value = getenv("HYBRIDSE_DEBUG");
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }
    static bool IsCluster() {
        const char* env_name = "HYBRIDSE_CLUSTER";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }

    static bool IsDisableExprOpt() {
        const char* env_name = "HYBRIDSE_DISABLE_EXPR_OPT";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }

    static bool IsBatchRequestOpt() {
        const char* env_name = "HYBRIDSE_BATCH_REQUEST_OPT";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }

    static bool IsDisableLocalTablet() {
        const char* env_name = "HYBRIDSE_DISTABLE_LOCALTABLET";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }

    static bool IsProcedure() {
        const char* env_name = "HYBRIDSE_PROCEDURE";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }

    static hybridse::sqlcase::SqlCase LoadSqlCaseWithID(
        const std::string& dir_path, const std::string& yaml_path,
        const std::string& case_id);
    void SqlCaseRepeatConfig(const std::string& tag, const int value) {
        for (size_t idx = 0; idx < inputs_.size(); idx++) {
            if (inputs_[idx].repeat_tag_ == tag) {
                LOG(INFO) << "config input " << idx << " " << tag << " "
                          << value;
                inputs_[idx].repeat_ = value;
            }
        }
        if (batch_request_.repeat_tag_ == tag) {
            LOG(INFO) << "config batch request " << tag << " " << value;
            batch_request_.repeat_ = value;
        }
    }

    const YAML::Node raw_node() const { return raw_node_; }
    std::string id_;
    std::string mode_;
    std::string desc_;
    std::vector<std::string> tags_;
    std::string db_;
    std::string sql_str_;
    std::vector<std::string> sql_strs_;
    bool debug_ = false;
    bool standard_sql_;
    bool standard_sql_compatible_;
    bool batch_request_optimized_;
    std::string batch_plan_;
    std::string request_plan_;
    std::string cluster_request_plan_;
    std::vector<TableInfo> inputs_;
    TableInfo batch_request_;
    TableInfo parameters_;
    ExpectInfo expect_;
    YAML::Node raw_node_;
    std::string sp_name_;
    int level_ = 0;

    // also generate deployment test for the query
    bool deployable_ = false;
};
std::string FindSqlCaseBaseDirPath();

std::vector<SqlCase> InitCases(std::string yaml_path);
std::vector<SqlCase> InitCases(std::string yaml_path, std::vector<std::string> filters);
void InitCases(std::string yaml_path, std::vector<SqlCase>& cases);  // NOLINT
void InitCases(std::string yaml_path, std::vector<SqlCase>& cases, // NOLINT
               const std::vector<std::string>& filters);
}  // namespace sqlcase
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_CASE_SQL_CASE_H_
