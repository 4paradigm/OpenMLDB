/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * case.h
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CASE_SQL_CASE_H_
#define SRC_CASE_SQL_CASE_H_
#include <vm/catalog.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h>
#include <set>
#include <string>
#include <vector>
#include "codec/fe_row_codec.h"
#include "proto/fe_type.pb.h"
namespace fesql {
namespace sqlcase {
class SQLCaseBuilder;
class SQLCase {
 public:
    struct TableInfo {
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
        std::set<size_t> common_column_indices_;
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
        bool success_ = true;
    };
    SQLCase() {}
    virtual ~SQLCase() {}

    const std::string id() const { return id_; }
    const std::string& desc() const { return desc_; }
    const std::string case_name() const;
    const std::string& mode() const { return mode_; }
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
    bool BuildCreateSQLFromInput(int32_t input_idx, std::string* sql,
                                 int partition_num = 1) const;
    bool BuildInsertSQLFromInput(int32_t input_idx, std::string* sql) const;
    bool BuildInsertSQLListFromInput(int32_t input_idx,
                                     std::vector<std::string>* sql_list) const;
    bool ExtractOutputSchema(type::TableDef& table) const;       // NOLINT
    bool ExtractInputData(std::vector<fesql::codec::Row>& rows,  // NOLINT
                          int32_t input_idx = 0) const;
    bool ExtractInputData(
        const TableInfo& info,
        std::vector<fesql::codec::Row>& rows) const;  // NOLINT
    bool ExtractOutputData(
        std::vector<fesql::codec::Row>& rows) const;  // NOLINT

    bool AddInput(const TableInfo& table_data);
    static bool TypeParse(const std::string& row_str, fesql::type::Type* type);
    static const std::string TypeString(fesql::type::Type type);
    static bool ExtractSchema(const std::vector<std::string>& columns,
                              type::TableDef& table);  // NOLINT
    static bool ExtractSchema(const std::string& schema_str,
                              type::TableDef& table);  // NOLINT
    static bool BuildCreateSQLFromSchema(const type::TableDef& table,
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
    static bool ExtractRows(const vm::Schema& schema,
                            const std::string& data_str,
                            std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool ExtractRows(
        const vm::Schema& schema,
        const std::vector<std::vector<std::string>>& row_vec,
        std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool BuildInsertSQLFromData(const type::TableDef& table,
                                       std::string data,
                                       std::string* insert_sql);
    static bool BuildInsertValueStringFromRow(
        const type::TableDef& table, const std::vector<std::string>& item_vec,
        std::string* create_sql);
    static bool BuildInsertSQLFromRows(
        const type::TableDef& table,
        const std::vector<std::vector<std::string>>& rows,
        std::string* insert_sql);
    static bool ExtractRow(const vm::Schema& schema, const std::string& row_str,
                           int8_t** out_ptr, int32_t* out_size);
    static bool ExtractRow(const vm::Schema& schema,
                           const std::vector<std::string>& item_vec,
                           int8_t** out_ptr, int32_t* out_size);
    static bool CreateTableInfoFromYamlNode(const YAML::Node& node,
                                            SQLCase::TableInfo* output);
    static bool CreateExpectFromYamlNode(const YAML::Node& schema_data,
                                         SQLCase::ExpectInfo* table);
    static bool LoadSchemaAndRowsFromYaml(
        const std::string& cases_dir, const std::string& resource_path,
        type::TableDef& table,                  // NOLINT
        std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool CreateSQLCasesFromYaml(
        const std::string& cases_dir, const std::string& yaml_path,
        std::vector<SQLCase>& sql_case_ptr,  // NOLINT
        const std::string filter_mode = "");
    static bool CreateSQLCasesFromYaml(
        const std::string& cases_dir, const std::string& yaml_path,
        std::vector<SQLCase>& sql_case_ptr,  // NOLINT
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
    bool BuildCreateSpSQLFromInput(int32_t input_idx,
                                   const std::string& select_sql,
                                   const std::set<size_t>& common_idx,
                                   std::string* create_sp_sql);
    bool BuildCreateSpSQLFromSchema(const type::TableDef& table,
                                    const std::string& select_sql,
                                    const std::set<size_t>& common_idx,
                                    std::string* create_sql);

    friend SQLCaseBuilder;
    friend std::ostream& operator<<(std::ostream& output, const SQLCase& thiz);
    static bool IS_PERF() {
        const char* env_name = "FESQL_PERF";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }
    static bool IS_DEBUG() {
        const char* env_name = "FESQL_DEV";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }
    static bool IS_CLUSTER() {
        const char* env_name = "FESQL_CLUSTER";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }
    static bool IS_DISABLE_LOCALTABLET() {
        const char* env_name = "FESQL_DISTABLE_LOCALTABLET";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }
    static bool IS_PROCEDURE() {
        const char* env_name = "FESQL_PROCEDURE";
        char* value = getenv(env_name);
        if (value != nullptr && strcmp(value, "true") == 0) {
            return true;
        }
        return false;
    }

    const YAML::Node raw_node() const { return raw_node_; }
    std::string id_;
    std::string mode_;
    std::string desc_;
    std::vector<std::string> tags_;
    std::string db_;
    std::string sql_str_;
    std::vector<std::string> sql_strs_;
    bool debug_;
    bool standard_sql_;
    bool standard_sql_compatible_;
    std::string batch_plan_;
    std::string request_plan_;
    std::string cluster_request_plan_;
    std::vector<TableInfo> inputs_;
    TableInfo batch_request_;
    ExpectInfo expect_;
    YAML::Node raw_node_;
    std::string sp_name_;
};
std::string FindFesqlDirPath();

}  // namespace sqlcase
}  // namespace fesql
#endif  // SRC_CASE_SQL_CASE_H_
