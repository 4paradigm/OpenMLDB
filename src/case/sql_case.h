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
    };
    struct ExpectInfo {
        int64_t count_ = -1;
        std::vector<std::string> columns_;
        std::vector<std::vector<std::string>> rows_;
        std::string schema_;
        std::string data_;
        std::string order_;
        bool success_ = true;
    };
    SQLCase() {}
    virtual ~SQLCase() {}

    const std::string id() const { return id_; }
    const std::string& desc() const { return desc_; }
    const std::string case_name() const;
    const std::string& mode() const { return mode_; }
    const std::string& request_plan() const { return request_plan_; }
    const std::string& batch_plan() const { return batch_plan_; }
    const std::string& sql_str() const { return sql_str_; }
    const bool standard_sql() const { return standard_sql_; }
    const bool standard_sql_compatible() const {
        return standard_sql_compatible_;
    }
    const std::string& db() const { return db_; }
    const std::vector<TableInfo>& inputs() const { return inputs_; }
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
                              int32_t input_idx = 0);
    bool BuildCreateSQLFromInput(int32_t input_idx, std::string* sql);
    bool BuildInsertSQLFromInput(int32_t input_idx, std::string* sql);
    bool BuildInsertSQLListFromInput(int32_t input_idx,
                                     std::vector<std::string>* sql_list);
    bool ExtractOutputSchema(type::TableDef& table);             // NOLINT
    bool ExtractInputData(std::vector<fesql::codec::Row>& rows,  // NOLINT
                          int32_t input_idx = 0);
    bool ExtractOutputData(std::vector<fesql::codec::Row>& rows);  // NOLINT

    bool AddInput(const TableInfo& table_data);
    static bool TypeParse(const std::string& row_str, fesql::type::Type* type);
    static const std::string TypeString(fesql::type::Type type);
    static bool ExtractSchema(const std::vector<std::string>& columns,
                              type::TableDef& table);  // NOLINT
    static bool ExtractSchema(const std::string& schema_str,
                              type::TableDef& table);  // NOLINT
    static bool BuildCreateSQLFromSchema(const type::TableDef& table,
                                         std::string* create_sql,
                                         bool isGenerateIndex = true);  // NOLINT
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
    friend SQLCaseBuilder;
    friend std::ostream& operator<<(std::ostream& output, const SQLCase& thiz);

 private:
    std::string id_;
    std::string mode_;
    std::string desc_;
    std::vector<std::string> tags_;
    std::string db_;
    std::string sql_str_;
    std::vector<std::string> sql_strs_;
    bool standard_sql_;
    bool standard_sql_compatible_;
    std::string batch_plan_;
    std::string request_plan_;
    std::vector<TableInfo> inputs_;
    ExpectInfo expect_;
};
std::string FindFesqlDirPath();

}  // namespace sqlcase
}  // namespace fesql
#endif  // SRC_CASE_SQL_CASE_H_
