/*
 * engine_test.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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
#ifndef SRC_VM_ENGINE_TEST_H_
#define SRC_VM_ENGINE_TEST_H_

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/fe_row_selector.h"
#include "codec/list_iterator_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "sys/time.h"
#include "vm/engine.h"
#include "vm/test_base.h"
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

static const int ENGINE_TEST_RET_SUCCESS = 0;
static const int ENGINE_TEST_RET_INVALID_CASE = 1;
static const int ENGINE_TEST_RET_COMPILE_ERROR = 2;
static const int ENGINE_TEST_RET_EXECUTION_ERROR = 3;

namespace fesql {
namespace vm {
using fesql::base::Status;
using fesql::codec::ArrayListV;
using fesql::codec::Row;
using fesql::common::kSQLError;
using fesql::sqlcase::SQLCase;

enum EngineRunMode { RUNBATCH, RUNONE };

std::vector<SQLCase> InitCases(std::string yaml_path);
void InitCases(std::string yaml_path, std::vector<SQLCase>& cases);  // NOLINT

void InitCases(std::string yaml_path, std::vector<SQLCase>& cases) {  // NOLINT
    if (!SQLCase::CreateSQLCasesFromYaml(fesql::sqlcase::FindFesqlDirPath(),
                                         yaml_path, cases)) {
        FAIL();
    }
}
std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}

int GenerateSqliteTestStringCallback(void* s, int argc, char** argv,
                                     char** azColName) {
    std::string& sqliteStr = *static_cast<std::string*>(s);
    int i;
    for (i = 0; i < argc; i++) {
        sqliteStr += NULL == argv[i] ? "NULL" : argv[i];
        sqliteStr += ", ";
    }
    sqliteStr = sqliteStr.substr(0, sqliteStr.length() - 2);
    sqliteStr += "\n";
    return 0;
}

bool IsNaN(float x) { return x != x; }
bool IsNaN(double x) { return x != x; }

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema);
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows);
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows);
void StoreData(::fesql::storage::Table* table, const std::vector<Row>& rows);

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema) {
    ASSERT_EQ(schema.size(), exp_schema.size());
    for (int i = 0; i < schema.size(); i++) {
        ASSERT_EQ(schema.Get(i).DebugString(), exp_schema.Get(i).DebugString())
            << "Fail column type at " << i;
    }
}
std::string YamlTypeName(type::Type type) {
    return fesql::sqlcase::SQLCase::TypeString(type);
}

// 打印符合yaml测试框架格式的预期结果
void PrintYamlResult(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    oss << "print schema\n";
    for (int i = 0; i < schema.size(); i++) {
        auto col = schema.Get(i);
        oss << col.name() << ":" << YamlTypeName(col.type());
        if (i + 1 != schema.size()) {
            oss << ", ";
        }
    }
    oss << "\nprint rows\n";
    RowView row_view(schema);
    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            oss << str;
            if (idx + 1 != schema.size()) {
                oss << ", ";
            }
        }
    }
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void PrintYamlV1Result(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    oss << "print schema\n[";
    for (int i = 0; i < schema.size(); i++) {
        auto col = schema.Get(i);
        oss << "\"" << col.name() << " " << YamlTypeName(col.type()) << "\"";
        if (i + 1 != schema.size()) {
            oss << ", ";
        }
    }
    oss << "]\nprint rows\n";
    RowView row_view(schema);
    for (auto row : rows) {
        row_view.Reset(row.buf());
        oss << "- [";
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            auto col = schema.Get(idx);
            if (YamlTypeName(col.type()) == "string" ||
                YamlTypeName(col.type()) == "date") {
                oss << "\"" << str << "\"";
            } else {
                oss << str;
            }
            if (idx + 1 != schema.size()) {
                oss << ", ";
            }
        }
        oss << "]\n";
    }
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
    if (rows.empty()) {
        t.add("Empty set");
        t.endOfRow();
        return;
    }

    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            t.add(str);
            if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
                t.add("...");
                break;
            }
        }
        t.endOfRow();
        if (t.rows().size() >= MAX_DEBUG_LINES_CNT) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

const std::vector<Row> SortRows(const vm::Schema& schema,
                                const std::vector<Row>& rows,
                                const std::string& order_col) {
    DLOG(INFO) << "sort rows start";
    RowView row_view(schema);
    int idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            idx = i;
            break;
        }
    }
    if (-1 == idx) {
        return rows;
    }

    if (schema.Get(idx).type() == fesql::type::kVarchar) {
        std::vector<std::pair<std::string, Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(row_view.GetAsString(idx), row));
        }
        std::sort(
            sort_rows.begin(), sort_rows.end(),
            [](std::pair<std::string, Row>& a, std::pair<std::string, Row>& b) {
                return a.first < b.first;
            });
        std::vector<Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    } else {
        std::vector<std::pair<int64_t, Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(
                boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<int64_t, Row>& a, std::pair<int64_t, Row>& b) {
                      return a.first < b.first;
                  });
        std::vector<Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    }
}
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows) {
    ASSERT_EQ(rows.size(), exp_rows.size());
    RowView row_view(schema);
    RowView row_view_exp(schema);
    for (size_t row_index = 0; row_index < rows.size(); row_index++) {
        ASSERT_TRUE(nullptr != rows[row_index].buf());
        row_view.Reset(rows[row_index].buf());
        row_view_exp.Reset(exp_rows[row_index].buf());
        for (int i = 0; i < schema.size(); i++) {
            if (row_view_exp.IsNULL(i)) {
                ASSERT_TRUE(row_view.IsNULL(i)) << " At " << i;
                continue;
            }
            ASSERT_FALSE(row_view.IsNULL(i)) << " At " << i;
            switch (schema.Get(i).type()) {
                case fesql::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i),
                              row_view_exp.GetInt32Unsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case fesql::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i),
                              row_view_exp.GetInt64Unsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case fesql::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i),
                              row_view_exp.GetInt16Unsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case fesql::type::kFloat: {
                    float act = row_view.GetFloatUnsafe(i);
                    float exp = row_view_exp.GetFloatUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act))
                            << " At " << i << " " << schema.Get(i).name();
                    } else {
                        ASSERT_FLOAT_EQ(act, exp)
                            << " At " << i << " " << schema.Get(i).name();
                    }
                    break;
                }
                case fesql::type::kDouble: {
                    double act = row_view.GetDoubleUnsafe(i);
                    double exp = row_view_exp.GetDoubleUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act))
                            << " At " << i << " " << schema.Get(i).name();
                    } else {
                        ASSERT_DOUBLE_EQ(act, exp)
                            << " At " << i << " " << schema.Get(i).name();
                    }
                    break;
                }
                case fesql::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i),
                              row_view_exp.GetStringUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case fesql::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i),
                              row_view_exp.GetDateUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case fesql::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i),
                              row_view_exp.GetTimestampUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case fesql::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i),
                              row_view_exp.GetBoolUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                default: {
                    FAIL() << "Invalid Column Type";
                    break;
                }
            }
        }
    }
}

void StoreData(::fesql::storage::Table* table, const std::vector<Row>& rows) {
    ASSERT_TRUE(table->Init());
    for (auto row : rows) {
        ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));
    }
}

const std::string GenerateTableName(int32_t id) {
    return "auto_t" + std::to_string(id);
}

void InitEngineCatalog(
    const SQLCase& sql_case, const EngineOptions& engine_options,
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>&  // NOLINT
        name_table_map,                                               // NOLINT
    std::map<size_t, std::string>& idx_table_name_map,                // NOLINT
    std::shared_ptr<vm::Engine> engine,
    std::shared_ptr<tablet::TabletCatalog> catalog) {
    for (int32_t i = 0; i < sql_case.CountInputs(); i++) {
        std::string actual_name = sql_case.inputs()[i].name_;
        if (actual_name.empty()) {
            actual_name = GenerateTableName(i);
        }
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        table_def.set_name(actual_name);

        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        if (engine_options.is_cluster_optimzied()) {
            // add table with local tablet
            ASSERT_TRUE(AddTable(catalog, table_def, table, engine.get()));
        } else {
            ASSERT_TRUE(AddTable(catalog, table_def, table));
        }
        idx_table_name_map[i] = actual_name;
    }
}

void DoEngineCheckExpect(const SQLCase& sql_case,
                         std::shared_ptr<RunSession> session,
                         const std::vector<Row>& output) {
    if (sql_case.expect().count_ >= 0) {
        ASSERT_EQ(static_cast<size_t>(sql_case.expect().count_), output.size());
    }
    const Schema& schema = session->GetSchema();
    std::vector<Row> sorted_output;

    bool is_batch_request = session->engine_mode() == kBatchRequestMode;
    if (is_batch_request) {
        const auto& sql_ctx = session->GetCompileInfo()->get_sql_context();
        const auto& output_common_column_indices =
            sql_ctx.batch_request_info.output_common_column_indices;
        if (!output_common_column_indices.empty() &&
            output_common_column_indices.size() !=
                static_cast<size_t>(schema.size()) &&
            sql_ctx.is_batch_request_optimized) {
            LOG(INFO) << "Reorder batch request outputs for non-trival common "
                         "columns";

            auto& expect_common_column_indices =
                sql_case.expect().common_column_indices_;
            if (!expect_common_column_indices.empty()) {
                ASSERT_EQ(expect_common_column_indices,
                          output_common_column_indices);
            }

            std::vector<Row> reordered;
            std::vector<std::pair<size_t, size_t>> select_indices;
            size_t common_col_idx = 0;
            size_t non_common_col_idx = 0;
            auto plan = sql_ctx.physical_plan;
            for (size_t i = 0; i < plan->GetOutputSchemaSize(); ++i) {
                if (output_common_column_indices.find(i) !=
                    output_common_column_indices.end()) {
                    select_indices.push_back(std::make_pair(0, common_col_idx));
                    common_col_idx += 1;
                } else {
                    select_indices.push_back(
                        std::make_pair(1, non_common_col_idx));
                    non_common_col_idx += 1;
                }
            }
            codec::RowSelector selector(
                {plan->GetOutputSchemaSource(0)->GetSchema(),
                 plan->GetOutputSchemaSource(1)->GetSchema()},
                select_indices);
            for (const auto& row : output) {
                int8_t* reordered_buf = nullptr;
                size_t reordered_size;
                ASSERT_TRUE(
                    selector.Select(row, &reordered_buf, &reordered_size));
                reordered.push_back(Row(codec::RefCountedSlice::Create(
                    reordered_buf, reordered_size)));
            }
            sorted_output = reordered;
        } else {
            sorted_output = SortRows(schema, output, sql_case.expect().order_);
        }
    } else {
        sorted_output = SortRows(schema, output, sql_case.expect().order_);
    }
    if (sql_case.expect().schema_.empty() &&
        sql_case.expect().columns_.empty()) {
        LOG(INFO) << "Expect result columns empty, Real result:\n";
        PrintRows(schema, sorted_output);
        PrintYamlResult(schema, sorted_output);
    } else {
        // Check Output Schema
        type::TableDef case_output_table;
        ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
        ASSERT_NO_FATAL_FAILURE(
            CheckSchema(schema, case_output_table.columns()));

        LOG(INFO) << "Real result:\n";
        PrintRows(schema, sorted_output);

        std::vector<Row> case_output_data;
        ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
        // for batch request mode, trivally compare last result
        if (is_batch_request && sql_case.batch_request().columns_.empty()) {
            if (!case_output_data.empty()) {
                case_output_data = {case_output_data.back()};
            }
        }

        LOG(INFO) << "Expect result:\n";
        PrintRows(schema, case_output_data);

        ASSERT_NO_FATAL_FAILURE(
            CheckRows(schema, sorted_output, case_output_data));
    }
}

void CheckSQLiteCompatible(const SQLCase& sql_case, const vm::Schema& schema,
                           const std::vector<Row>& output) {
    // Use SQLite to get output
    sqlite3* db;
    char* zErrMsg = 0;
    int rc;

    // Create database in the memory
    rc = sqlite3_open(":memory:", &db);
    if (rc) {
        LOG(ERROR) << "Can't open database: %s\n" << sqlite3_errmsg(db);
        exit(0);
    } else {
        LOG(INFO) << "Database Create successfully\n";
    }

    // Create SQL statement to create a table schema
    type::TableDef output_table;
    sql_case.ExtractInputTableDef(output_table);
    std::string create_table_sql;
    SQLCase::BuildCreateSQLFromSchema(output_table, &create_table_sql, false);
    LOG(INFO) << create_table_sql;

    // Create a table schema
    const char* create_table_sql_ch = create_table_sql.c_str();
    rc = sqlite3_exec(db, create_table_sql_ch, 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "SQL error: %s\n" << zErrMsg;
        sqlite3_free(zErrMsg);
    } else {
        LOG(INFO) << "Table schema created successfully\n";
    }

    // Create SQL statements to insert data to the table (One insert)
    std::string create_insert_sql = "";
    std::vector<std::string> data_line;
    sql_case.BuildInsertSQLFromInput(0, &create_insert_sql);

    // Insert data into the table
    const char* create_insert_sql_ch = create_insert_sql.c_str();
    std::cout << create_insert_sql_ch << std::endl;
    rc = sqlite3_exec(db, create_insert_sql_ch, 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "SQL error: %s\n" << zErrMsg;
        sqlite3_free(zErrMsg);
    } else {
        LOG(INFO) << "Records created successfully\n";
    }

    // Execute SQL statement
    const char* create_execute_sql_ch = sql_case.sql_str().c_str();
    std::string sqliteStr = "";
    rc = sqlite3_exec(db, create_execute_sql_ch,
                      GenerateSqliteTestStringCallback,
                      static_cast<void*>(&sqliteStr), &zErrMsg);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "SQL error: " << zErrMsg
                   << "\nsql: " << create_execute_sql_ch;
        sqlite3_free(zErrMsg);
    } else {
        LOG(INFO) << "Operation done successfully\n";
    }
    sqlite3_close(db);
    sqliteStr.pop_back();

    // Transfer Sqlite outcome to Fesql row
    std::vector<fesql::codec::Row> sqliteRows;
    SQLCase::ExtractRows(schema, sqliteStr, sqliteRows);

    // Compare Fesql output with SQLite output.
    ASSERT_NO_FATAL_FAILURE(CheckRows(
        schema, SortRows(schema, sqliteRows, sql_case.expect().order_),
        SortRows(schema, output, sql_case.expect().order_)));
}

class EngineTestRunner {
 public:
    explicit EngineTestRunner(const SQLCase& sql_case,
                              const EngineOptions options)
        : sql_case_(sql_case), options_(options) {
        catalog_ = BuildCommonCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        InitSQLCase();
        InitEngineCatalog(sql_case_, options_, name_table_map_,
                          idx_table_name_map_, engine_, catalog_);
    }
    virtual ~EngineTestRunner() {}

    void SetSession(std::shared_ptr<RunSession> session) { session_ = session; }

    std::shared_ptr<RunSession> GetSession() const { return session_; }

    static Status ExtractTableInfoFromCreateString(
        const std::string& create, SQLCase::TableInfo* table_info);
    Status Compile();
    virtual void InitSQLCase();
    virtual Status PrepareData() = 0;
    virtual Status Compute(std::vector<codec::Row>*) = 0;

    int return_code() const { return return_code_; }
    const SQLCase& sql_case() const { return sql_case_; }

    void RunCheck();
    void RunBenchmark(size_t iters);

 protected:
    SQLCase sql_case_;
    EngineOptions options_;

    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map_;
    std::map<size_t, std::string> idx_table_name_map_;
    std::shared_ptr<tablet::TabletCatalog> catalog_;

    std::shared_ptr<Engine> engine_ = nullptr;

    std::shared_ptr<RunSession> session_ = nullptr;

    int return_code_ = ENGINE_TEST_RET_INVALID_CASE;
};
Status EngineTestRunner::ExtractTableInfoFromCreateString(
    const std::string& create, SQLCase::TableInfo* table_info) {
    CHECK_TRUE(table_info != nullptr, common::kNullPointer,
               "Fail extract with null table info");
    CHECK_TRUE(!create.empty(), common::kSQLError,
               "Fail extract with empty create string");

    node::NodeManager manager;
    parser::FeSQLParser parser;
    fesql::plan::NodePointVector trees;
    base::Status status;
    int ret = parser.parse(create, trees, &manager, status);

    if (0 != status.code) {
        std::cout << status << std::endl;
    }
    CHECK_TRUE(0 == ret, common::kSQLError, "Fail to parser SQL");
    //    ASSERT_EQ(1, trees.size());
    //    std::cout << *(trees.front()) << std::endl;
    plan::SimplePlanner planner_ptr(&manager);
    node::PlanNodeList plan_trees;
    CHECK_TRUE(0 == planner_ptr.CreatePlanTree(trees, plan_trees, status),
               common::kPlanError, "Fail to resolve logical plan");
    CHECK_TRUE(1u == plan_trees.size(), common::kPlanError,
               "Fail to extract table info with multi logical plan tree");
    CHECK_TRUE(
        nullptr != plan_trees[0] &&
            node::kPlanTypeCreate == plan_trees[0]->type_,
        common::kPlanError,
        "Fail to extract table info with invalid SQL, CREATE SQL is required");
    node::CreatePlanNode* create_plan =
        dynamic_cast<node::CreatePlanNode*>(plan_trees[0]);
    table_info->name_ = create_plan->GetTableName();
    CHECK_TRUE(create_plan->ExtractColumnsAndIndexs(table_info->columns_,
                                                    table_info->indexs_),
               common::kPlanError, "Invalid Create Plan Node");
    std::ostringstream oss;
    oss << "name: " << table_info->name_ << "\n";
    oss << "columns: [";
    for (auto column : table_info->columns_) {
        oss << column << ",";
    }
    oss << "]\n";
    oss << "indexs: [";
    for (auto index : table_info->indexs_) {
        oss << index << ",";
    }
    oss << "]\n";
    LOG(INFO) << oss.str();
    return Status::OK();
}
void EngineTestRunner::InitSQLCase() {
    for (size_t idx = 0; idx < sql_case_.inputs_.size(); idx++) {
        if (!sql_case_.inputs_[idx].create_.empty()) {
            auto status = ExtractTableInfoFromCreateString(
                sql_case_.inputs_[idx].create_, &sql_case_.inputs_[idx]);
            ASSERT_TRUE(status.isOK()) << status;
        }
    }

    if (!sql_case_.batch_request_.create_.empty()) {
        auto status = ExtractTableInfoFromCreateString(
            sql_case_.batch_request_.create_, &sql_case_.batch_request_);
        ASSERT_TRUE(status.isOK()) << status;
    }
}
Status EngineTestRunner::Compile() {
    std::string sql_str = sql_case_.sql_str();
    for (int j = 0; j < sql_case_.CountInputs(); ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        boost::replace_all(sql_str, placeholder, idx_table_name_map_[j]);
    }
    LOG(INFO) << "Compile SQL:\n" << sql_str;
    CHECK_TRUE(session_ != nullptr, common::kSQLError, "Session is not set");
    if (fesql::sqlcase::SQLCase::IS_DEBUG() || sql_case_.debug()) {
        session_->EnableDebug();
    }
    struct timeval st;
    struct timeval et;
    gettimeofday(&st, nullptr);
    Status status;
    bool ok = engine_->Get(sql_str, sql_case_.db(), *session_, status);
    gettimeofday(&et, nullptr);
    double mill =
        (et.tv_sec - st.tv_sec) * 1000 + (et.tv_usec - st.tv_usec) / 1000.0;
    LOG(INFO) << "SQL Compile take " << mill << " milliseconds";

    if (!ok || !status.isOK()) {
        LOG(INFO) << status.str();
        return_code_ = ENGINE_TEST_RET_COMPILE_ERROR;
    } else {
        LOG(INFO) << "SQL output schema:";
        PrintSchema(session_->GetSchema());

        std::ostringstream oss;
        session_->GetPhysicalPlan()->Print(oss, "");
        LOG(INFO) << "Physical plan:";
        std::cerr << oss.str() << std::endl;

        std::ostringstream runner_oss;
        session_->GetClusterJob().Print(runner_oss, "");
        LOG(INFO) << "Runner plan:";
        std::cerr << runner_oss.str() << std::endl;
    }
    return status;
}

void EngineTestRunner::RunCheck() {
    auto engine_mode = session_->engine_mode();
    Status status = Compile();
    ASSERT_EQ(sql_case_.expect().success_, status.isOK());
    if (!status.isOK()) {
        return_code_ = ENGINE_TEST_RET_COMPILE_ERROR;
        return;
    }
    std::ostringstream oss;
    session_->GetPhysicalPlan()->Print(oss, "");
    if (!sql_case_.batch_plan().empty() && engine_mode == kBatchMode) {
        ASSERT_EQ(oss.str(), sql_case_.batch_plan());
    } else if (!sql_case_.cluster_request_plan().empty() &&
               engine_mode == kRequestMode && options_.is_cluster_optimzied()) {
        ASSERT_EQ(oss.str(), sql_case_.cluster_request_plan());
    } else if (!sql_case_.request_plan().empty() &&
               engine_mode == kRequestMode) {
        ASSERT_EQ(oss.str(), sql_case_.request_plan());
    }
    status = PrepareData();
    ASSERT_TRUE(status.isOK()) << "Prepare data error: " << status;
    if (!status.isOK()) {
        return;
    }
    std::vector<Row> output_rows;
    status = Compute(&output_rows);
    ASSERT_TRUE(status.isOK()) << "Session run error: " << status;
    if (!status.isOK()) {
        return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
        return;
    }
    ASSERT_NO_FATAL_FAILURE(
        DoEngineCheckExpect(sql_case_, session_, output_rows));
    return_code_ = ENGINE_TEST_RET_SUCCESS;
}

void EngineTestRunner::RunBenchmark(size_t iters) {
    auto engine_mode = session_->engine_mode();
    if (engine_mode == kRequestMode) {
        LOG(WARNING) << "Request mode case can not properly run many times";
        return;
    }

    Status status = Compile();
    if (!status.isOK()) {
        LOG(WARNING) << "Compile error: " << status;
        return;
    }
    status = PrepareData();
    if (!status.isOK()) {
        LOG(WARNING) << "Prepare data error: " << status;
        return;
    }

    std::vector<Row> output_rows;
    status = Compute(&output_rows);
    if (!status.isOK()) {
        LOG(WARNING) << "Run error: " << status;
        return;
    }
    PrintRows(session_->GetSchema(), output_rows);

    struct timeval st;
    struct timeval et;
    gettimeofday(&st, nullptr);
    for (size_t i = 0; i < iters; ++i) {
        output_rows.clear();
        status = Compute(&output_rows);
        if (!status.isOK()) {
            LOG(WARNING) << "Run error at " << i << "th iter: " << status;
            return;
        }
    }
    gettimeofday(&et, nullptr);
    if (iters != 0) {
        double mill =
            (et.tv_sec - st.tv_sec) * 1000 + (et.tv_usec - st.tv_usec) / 1000.0;
        printf("Engine run take approximately %.5f ms per run\n", mill / iters);
    }
}

class BatchEngineTestRunner : public EngineTestRunner {
 public:
    explicit BatchEngineTestRunner(const SQLCase& sql_case,
                                   const EngineOptions options)
        : EngineTestRunner(sql_case, options) {
        session_ = std::make_shared<BatchRunSession>();
    }
    Status PrepareData() override {
        for (int32_t i = 0; i < sql_case_.CountInputs(); i++) {
            auto input = sql_case_.inputs()[i];
            std::vector<Row> rows;
            sql_case_.ExtractInputData(rows, i);
            size_t repeat = sql_case_.inputs()[i].repeat_;
            if (repeat > 1) {
                size_t row_num = rows.size();
                rows.resize(row_num * repeat);
                size_t offset = row_num;
                for (size_t i = 0; i < repeat - 1; ++i) {
                    std::copy(rows.begin(), rows.begin() + row_num,
                              rows.begin() + offset);
                    offset += row_num;
                }
            }
            if (!rows.empty()) {
                std::string table_name = idx_table_name_map_[i];
                StoreData(name_table_map_[table_name].get(), rows);
            }
        }
        return Status::OK();
    }

    Status Compute(std::vector<Row>* outputs) override {
        auto batch_session =
            std::dynamic_pointer_cast<BatchRunSession>(session_);
        CHECK_TRUE(batch_session != nullptr, common::kSQLError);
        int run_ret = batch_session->Run(*outputs);
        if (run_ret != 0) {
            return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
        }
        CHECK_TRUE(run_ret == 0, common::kSQLError, "Run batch session failed");
        return Status::OK();
    }

    void RunSQLiteCheck() {
        // Determine whether to compare with SQLite
        if (sql_case_.standard_sql() && sql_case_.standard_sql_compatible()) {
            std::vector<Row> output_rows;
            ASSERT_TRUE(Compute(&output_rows).isOK());
            CheckSQLiteCompatible(sql_case_, GetSession()->GetSchema(),
                                  output_rows);
        }
    }
};

class RequestEngineTestRunner : public EngineTestRunner {
 public:
    explicit RequestEngineTestRunner(const SQLCase& sql_case,
                                     const EngineOptions options)
        : EngineTestRunner(sql_case, options) {
        session_ = std::make_shared<RequestRunSession>();
    }

    Status PrepareData() override {
        request_rows_.clear();
        const bool has_batch_request =
            !sql_case_.batch_request_.columns_.empty();
        auto request_session =
            std::dynamic_pointer_cast<RequestRunSession>(session_);
        CHECK_TRUE(request_session != nullptr, common::kSQLError);
        std::string request_name = request_session->GetRequestName();

        if (has_batch_request) {
            CHECK_TRUE(1 == sql_case_.batch_request_.rows_.size(), kSQLError,
                       "RequestEngine can't handler multi rows batch requests");
            CHECK_TRUE(sql_case_.ExtractInputData(sql_case_.batch_request_,
                                                  request_rows_),
                       kSQLError, "Extract case request rows failed");
        }
        for (int32_t i = 0; i < sql_case_.CountInputs(); i++) {
            std::string input_name = idx_table_name_map_[i];

            if (input_name == request_name && !has_batch_request) {
                CHECK_TRUE(sql_case_.ExtractInputData(request_rows_, i),
                           kSQLError, "Extract case request rows failed");
                auto request_table = name_table_map_[request_name];
                CHECK_TRUE(request_table->Init(), kSQLError,
                           "Init request table failed");
                continue;
            } else {
                std::vector<Row> rows;
                if (!sql_case_.inputs_[i].rows_.empty() ||
                    !sql_case_.inputs_[i].data_.empty()) {
                    CHECK_TRUE(sql_case_.ExtractInputData(rows, i), kSQLError,
                               "Extract case request rows failed");
                }

                if (sql_case_.inputs()[i].repeat_ > 1) {
                    std::vector<Row> store_rows;
                    for (int64_t j = 0; j < sql_case_.inputs()[i].repeat_;
                         j++) {
                        for (auto row : rows) {
                            store_rows.push_back(row);
                        }
                    }
                    StoreData(name_table_map_[input_name].get(), store_rows);
                } else {
                    StoreData(name_table_map_[input_name].get(), rows);
                }
            }
        }
        return Status::OK();
    }

    Status Compute(std::vector<Row>* outputs) override {
        const bool has_batch_request =
            !sql_case_.batch_request_.columns_.empty() &&
            !sql_case_.batch_request_.schema_.empty();
        auto request_session =
            std::dynamic_pointer_cast<RequestRunSession>(session_);
        std::string request_name = request_session->GetRequestName();
        for (auto in_row : request_rows_) {
            Row out_row;
            int run_ret = request_session->Run(in_row, &out_row);
            if (run_ret != 0) {
                return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
                return Status(kSQLError, "Run request session failed");
            }
            if (!has_batch_request) {
                CHECK_TRUE(name_table_map_[request_name]->Put(
                               reinterpret_cast<const char*>(in_row.buf()),
                               in_row.size()),
                           kSQLError);
            }
            outputs->push_back(out_row);
        }
        return Status::OK();
    }

 private:
    std::vector<Row> request_rows_;
};

class BatchRequestEngineTestRunner : public EngineTestRunner {
 public:
    BatchRequestEngineTestRunner(const SQLCase& sql_case,
                                 const EngineOptions options,
                                 const std::set<size_t>& common_column_indices)
        : EngineTestRunner(sql_case, options) {
        auto request_session = std::make_shared<BatchRequestRunSession>();
        for (size_t idx : common_column_indices) {
            request_session->AddCommonColumnIdx(idx);
        }
        session_ = request_session;
    }

    Status PrepareData() override {
        request_rows_.clear();
        auto request_session =
            std::dynamic_pointer_cast<BatchRequestRunSession>(session_);
        CHECK_TRUE(request_session != nullptr, common::kSQLError);

        bool has_batch_request = !sql_case_.batch_request().columns_.empty();
        if (!has_batch_request) {
            LOG(WARNING) << "No batch request field in case, "
                         << "try use last row from primary input";
        }

        std::vector<Row> original_request_data;
        std::string request_name = request_session->GetRequestName();
        auto& request_schema = request_session->GetRequestSchema();
        for (int32_t i = 0; i < sql_case_.CountInputs(); i++) {
            auto input = sql_case_.inputs()[i];
            std::vector<Row> rows;
            sql_case_.ExtractInputData(rows, i);
            if (!rows.empty()) {
                if (idx_table_name_map_[i] == request_name &&
                    !has_batch_request) {
                    original_request_data.push_back(rows.back());
                    rows.pop_back();
                }
                std::string table_name = idx_table_name_map_[i];
                size_t repeat = sql_case_.inputs()[i].repeat_;
                if (repeat > 1) {
                    size_t row_num = rows.size();
                    rows.resize(row_num * repeat);
                    size_t offset = row_num;
                    for (size_t i = 0; i < repeat - 1; ++i) {
                        std::copy(rows.begin(), rows.begin() + row_num,
                                  rows.begin() + offset);
                        offset += row_num;
                    }
                }
                StoreData(name_table_map_[table_name].get(), rows);
            }
        }

        type::TableDef request_table;
        if (has_batch_request) {
            sql_case_.ExtractTableDef(sql_case_.batch_request().columns_,
                                      sql_case_.batch_request().indexs_,
                                      request_table);
            sql_case_.ExtractRows(request_table.columns(),
                                  sql_case_.batch_request().rows_,
                                  original_request_data);
        } else {
            sql_case_.ExtractInputTableDef(request_table, 0);
        }

        std::vector<size_t> common_column_indices;
        for (size_t idx : request_session->common_column_indices()) {
            common_column_indices.push_back(idx);
        }
        size_t request_schema_size = static_cast<size_t>(request_schema.size());
        if (common_column_indices.empty() ||
            common_column_indices.size() == request_schema_size ||
            !options_.is_batch_request_optimized()) {
            request_rows_ = original_request_data;
        } else {
            std::vector<size_t> non_common_column_indices;
            for (size_t i = 0; i < request_schema_size; ++i) {
                if (std::find(common_column_indices.begin(),
                              common_column_indices.end(),
                              i) == common_column_indices.end()) {
                    non_common_column_indices.push_back(i);
                }
            }
            codec::RowSelector left_selector(&request_table.columns(),
                                             common_column_indices);
            codec::RowSelector right_selector(&request_table.columns(),
                                              non_common_column_indices);

            bool left_selected = false;
            codec::RefCountedSlice left_slice;
            for (auto& original_row : original_request_data) {
                if (!left_selected) {
                    int8_t* left_buf;
                    size_t left_size;
                    left_selector.Select(original_row.buf(0),
                                         original_row.size(0), &left_buf,
                                         &left_size);
                    left_slice = codec::RefCountedSlice::CreateManaged(
                        left_buf, left_size);
                    left_selected = true;
                }
                int8_t* right_buf = nullptr;
                size_t right_size;
                right_selector.Select(original_row.buf(0), original_row.size(0),
                                      &right_buf, &right_size);
                codec::RefCountedSlice right_slice =
                    codec::RefCountedSlice::CreateManaged(right_buf,
                                                          right_size);
                request_rows_.emplace_back(codec::Row(
                    1, codec::Row(left_slice), 1, codec::Row(right_slice)));
            }
        }
        size_t repeat = sql_case_.batch_request().repeat_;
        if (repeat > 1) {
            size_t row_num = request_rows_.size();
            request_rows_.resize(row_num * repeat);
            size_t offset = row_num;
            for (size_t i = 0; i < repeat - 1; ++i) {
                std::copy(request_rows_.begin(),
                          request_rows_.begin() + row_num,
                          request_rows_.begin() + offset);
                offset += row_num;
            }
        }
        return Status::OK();
    }

    Status Compute(std::vector<Row>* outputs) override {
        auto request_session =
            std::dynamic_pointer_cast<BatchRequestRunSession>(session_);
        CHECK_TRUE(request_session != nullptr, common::kSQLError);

        int run_ret = request_session->Run(request_rows_, *outputs);
        if (run_ret != 0) {
            return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
            return Status(kSQLError, "Run batch request session failed");
        }
        return Status::OK();
    }

 private:
    std::vector<Row> request_rows_;
};

void BatchRequestEngineCheckWithCommonColumnIndices(
    const SQLCase& sql_case, const EngineOptions options,
    const std::set<size_t>& common_column_indices) {
    std::ostringstream oss;
    for (size_t index : common_column_indices) {
        oss << index << ",";
    }
    LOG(INFO) << "BatchRequestEngineCheckWithCommonColumnIndices: "
                 "common_column_indices = ["
              << oss.str() << "]";
    BatchRequestEngineTestRunner engine_test(sql_case, options,
                                             common_column_indices);
    engine_test.RunCheck();
}

void BatchRequestEngineCheck(const SQLCase& sql_case,
                             const EngineOptions options) {
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    if (has_batch_request) {
        BatchRequestEngineCheckWithCommonColumnIndices(
            sql_case, options, sql_case.batch_request().common_column_indices_);
    } else if (!sql_case.inputs().empty()) {
        // set different common column conf
        size_t schema_size = sql_case.inputs()[0].columns_.size();
        std::set<size_t> common_column_indices;

        // empty
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);

        // full
        for (size_t i = 0; i < schema_size; ++i) {
            common_column_indices.insert(i);
        }
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);
        common_column_indices.clear();

        // partial
        // 0, 2, 4, ...
        for (size_t i = 0; i < schema_size; i += 2) {
            common_column_indices.insert(i);
        }
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);
        common_column_indices.clear();
        return;
        // 1, 3, 5, ...
        for (size_t i = 1; i < schema_size; i += 2) {
            common_column_indices.insert(i);
        }
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);
    }
}

void EngineCheck(const SQLCase& sql_case, const EngineOptions& options,
                 EngineMode engine_mode) {
    if (engine_mode == kBatchMode) {
        BatchEngineTestRunner engine_test(sql_case, options);
        engine_test.RunCheck();
        engine_test.RunSQLiteCheck();
    } else if (engine_mode == kRequestMode) {
        RequestEngineTestRunner engine_test(sql_case, options);
        engine_test.RunCheck();
    } else {
        BatchRequestEngineCheck(sql_case, options);
    }
}

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_ENGINE_TEST_H_
