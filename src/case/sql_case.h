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
#include <parser/parser.h>
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
    };
    SQLCase() {}
    virtual ~SQLCase() {}

    const int32_t id() const { return id_; }
    const std::string& desc() const { return desc_; }
    const std::string& mode() const { return mode_; }
    const std::string& request_plan() const { return request_plan_; }
    const std::string& batch_plan() const { return batch_plan_; }
    const std::string& sql_str() const { return sql_str_; }
    const std::string& db() const { return db_; }
    const std::vector<TableInfo>& inputs() const { return inputs_; }
    const TableInfo& output() const { return output_; }
    void set_output(const TableInfo& data) { output_ = data; }
    const int32_t CountInputs() const { return inputs_.size(); }
    // extract schema from schema string
    // name:type|name:type|name:type|
    bool ExtractInputTableDef(type::TableDef& table,  // NOLINT
                              int32_t input_idx = 0);
    bool ExtractOutputSchema(type::TableDef& table);             // NOLINT
    bool ExtractInputData(std::vector<fesql::codec::Row>& rows,  // NOLINT
                          int32_t input_idx = 0);
    bool ExtractOutputData(std::vector<fesql::codec::Row>& rows);  // NOLINT

    bool AddInput(const TableInfo& table_data);
    static bool TypeParse(const std::string& row_str, fesql::type::Type* type);
    static bool ExtractSchema(const std::string& schema_str,
                              type::TableDef& table);  // NOLINT
    static bool ExtractIndex(const std::string& index_str,
                             type::TableDef& table);  // NOLINT
    static bool ExtractTableDef(const std::string& schema_str,
                                const std::string& index_str,
                                type::TableDef& table);  // NOLINT
    static bool ExtractRows(const vm::Schema& schema,
                            const std::string& data_str,
                            std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool ExtractRow(const vm::Schema& schema, const std::string& row_str,
                           int8_t** out_ptr, int32_t* out_size);
    static bool CreateTableInfoFromYamlNode(const YAML::Node& node,
                                            SQLCase::TableInfo* output);
    static bool LoadSchemaAndRowsFromYaml(
        const std::string& resource_path,
        type::TableDef& table,                  // NOLINT
        std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool CreateSQLCasesFromYaml(
        const std::string& yaml_path,
        std::vector<SQLCase>& sql_case_ptr,  // NOLINT
        const std::string filter_mode = "");
    static bool CreateSQLCasesFromYaml(
        const std::string& yaml_path,
        std::vector<SQLCase>& sql_case_ptr,  // NOLINT
        const std::vector<std::string>& filter_modes);
    static bool CreateTableInfoFromYaml(const std::string& yaml_path,
                                        TableInfo* table_info);
    friend SQLCaseBuilder;
    friend std::ostream& operator<<(std::ostream& output, const SQLCase& thiz);

 private:
    int32_t id_;
    std::string mode_;
    std::string desc_;
    std::string db_;
    std::string sql_str_;
    std::string batch_plan_;
    std::string request_plan_;
    std::vector<TableInfo> inputs_;
    TableInfo output_;
};
std::string FindFesqlDirPath();

}  // namespace sqlcase
}  // namespace fesql
#endif  // SRC_CASE_SQL_CASE_H_
