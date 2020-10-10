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
#include <string>
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
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
#include "vm/engine.h"
#include "vm/test_base.h"
#define MAX_DEBUG_LINES_CNT 100 * 100 * 100
#define MAX_DEBUG_COLUMN_CNT 100 * 100 * 100

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

static const int ENGINE_TEST_RET_SUCCESS = 0;
static const int ENGINE_TEST_RET_INVALID_CASE = 1;
static const int ENGINE_TEST_RET_COMPILE_ERROR = 2;
static const int ENGINE_TEST_RET_EXECUTION_ERROR = 3;

namespace fesql {
namespace vm {
using fesql::codec::ArrayListV;
using fesql::codec::Row;
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
        row_view.Reset(rows[row_index].buf());
        row_view_exp.Reset(exp_rows[row_index].buf());
        for (int i = 0; i < schema.size(); i++) {
            if (row_view_exp.IsNULL(i)) {
                ASSERT_TRUE(row_view.IsNULL(i)) << " At " << i;
                continue;
            }
            switch (schema.Get(i).type()) {
                case fesql::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i),
                              row_view_exp.GetInt32Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i),
                              row_view_exp.GetInt64Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i),
                              row_view_exp.GetInt16Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kFloat: {
                    float act = row_view.GetFloatUnsafe(i);
                    float exp = row_view_exp.GetFloatUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i;
                    } else {
                        ASSERT_FLOAT_EQ(act, exp) << " At " << i;
                    }
                    break;
                }
                case fesql::type::kDouble: {
                    double act = row_view.GetDoubleUnsafe(i);
                    double exp = row_view_exp.GetDoubleUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i;
                    } else {
                        ASSERT_DOUBLE_EQ(act, exp) << " At " << i;
                    }
                    break;
                }
                case fesql::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i),
                              row_view_exp.GetStringUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i),
                              row_view_exp.GetDateUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i),
                              row_view_exp.GetTimestampUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i),
                              row_view_exp.GetBoolUnsafe(i))
                        << " At " << i;
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

void EngineCheck(SQLCase& sql_case, EngineMode engine_mode,  // NOLINT
                 bool check_compatible, int* return_status) {
    *return_status = ENGINE_TEST_RET_INVALID_CASE;
    int32_t input_cnt = sql_case.CountInputs();

    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(GenerateTableName(i), i);
        }
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < input_cnt; ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        boost::replace_all(sql_str, placeholder, sql_case.inputs()[j].name_);
    }
    std::cout << sql_str << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    std::unique_ptr<RunSession> session;
    if (engine_mode == kBatchMode) {
        session = std::unique_ptr<RunSession>(new BatchRunSession);
    } else if (engine_mode == kRequestMode) {
        session = std::unique_ptr<RunSession>(new RequestRunSession);
    } else {
        session = std::unique_ptr<RunSession>(new BatchRequestRunSession);
    }
    if (fesql::sqlcase::SQLCase::IS_DEBUG() || sql_case.debug()) {
        session->EnableDebug();
    }

    bool ok = engine.Get(sql_str, sql_case.db(), *(session.get()), get_status);
    if (!ok) {
        *return_status = ENGINE_TEST_RET_COMPILE_ERROR;
    }
    ASSERT_EQ(sql_case.expect().success_, ok);
    if (!sql_case.expect().success_) {
        return;
    }
    std::vector<Row> request_data;
    std::string request_name = "";

    bool is_batch = engine_mode == kBatchMode;
    if (!is_batch) {
        if (engine_mode == kRequestMode) {
            request_name = dynamic_cast<RequestRunSession*>(session.get())
                               ->GetRequestName();
        } else {
            request_name = dynamic_cast<BatchRequestRunSession*>(session.get())
                               ->GetRequestName();
        }
    }
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        if (!is_batch && input.name_ == request_name) {
            ASSERT_TRUE(sql_case.ExtractInputData(request_data, i));
        } else {
            std::vector<Row> rows;
            sql_case.ExtractInputData(rows, i);
            if (!rows.empty()) {
                ASSERT_NO_FATAL_FAILURE(
                    StoreData(name_table_map[input.name_].get(), rows));
            }
        }
    }

    vm::Schema schema;
    schema = session->GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session->GetPhysicalPlan()->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;

    if (is_batch && !sql_case.batch_plan().empty()) {
        ASSERT_EQ(oss.str(), sql_case.batch_plan());
    } else if (!is_batch && !sql_case.request_plan().empty()) {
        ASSERT_EQ(oss.str(), sql_case.request_plan());
    }

    std::ostringstream runner_oss;
    session->GetMainTask()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Data
    std::vector<Row> output;
    if (engine_mode == kBatchMode) {
        auto batch_session = dynamic_cast<BatchRunSession*>(session.get());
        int run_ret = batch_session->Run(output);
        if (run_ret != 0) {
            *return_status = ENGINE_TEST_RET_EXECUTION_ERROR;
        }
        ASSERT_EQ(0, run_ret);
    } else if (engine_mode == kRequestMode) {
        auto request_table = name_table_map[request_name];
        ASSERT_TRUE(request_table->Init());
        auto request_session = dynamic_cast<RequestRunSession*>(session.get());
        for (auto in_row : request_data) {
            Row out_row;
            int run_ret = request_session->Run(in_row, &out_row);
            if (run_ret != 0) {
                *return_status = ENGINE_TEST_RET_EXECUTION_ERROR;
            }
            ASSERT_EQ(0, run_ret);
            ASSERT_TRUE(request_table->Put(
                reinterpret_cast<const char*>(in_row.buf()), in_row.size()));
            output.push_back(out_row);
        }
    } else {
        auto request_table = name_table_map[request_name];
        ASSERT_TRUE(request_table->Init());
        auto request_session =
            dynamic_cast<BatchRequestRunSession*>(session.get());
        RunnerContext runner_context(false);
        for (auto in_row : request_data) {
            Row out_row;
            int run_ret =
                request_session->RunSingle(runner_context, in_row, &out_row);
            if (run_ret != 0) {
                *return_status = ENGINE_TEST_RET_EXECUTION_ERROR;
            }
            ASSERT_EQ(0, run_ret);
            ASSERT_TRUE(request_table->Put(
                reinterpret_cast<const char*>(in_row.buf()), in_row.size()));
            output.push_back(out_row);
        }
    }
    auto sorted_output = SortRows(schema, output, sql_case.expect().order_);

    if (sql_case.expect().count_ >= 0) {
        ASSERT_EQ(sql_case.expect().count_, output.size());
    }

    // LOG(INFO) << "Expect result:\n";
    // PrintRows(schema, case_output_data);

    LOG(INFO) << "Real result:\n";
    PrintRows(schema, sorted_output);

    // if (!sql_case.expect().schema_.empty() ||
    //     !sql_case.expect().columns_.empty()) {
    //     // Check Output Schema
    //     type::TableDef case_output_table;
    //     ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    //     std::vector<Row> case_output_data;
    //     ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    //     ASSERT_NO_FATAL_FAILURE(
    //         CheckSchema(schema, case_output_table.columns()));
    //     ASSERT_NO_FATAL_FAILURE(
    //         CheckRows(schema, sorted_output, case_output_data));
    // } else {
    //     LOG(INFO) << "Real result:\n";
    //     PrintRows(schema, sorted_output);
    // }

    *return_status = ENGINE_TEST_RET_SUCCESS;

    // Determine whether to compare with SQLite
    if (is_batch && check_compatible && sql_case.standard_sql() &&
        sql_case.standard_sql_compatible()) {
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
        SQLCase::BuildCreateSQLFromSchema(output_table, &create_table_sql,
                                          false);
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
}

void RequestModeCheck(SQLCase& sql_case) {  // NOLINT
    int return_status;
    EngineCheck(sql_case, kRequestMode, false, &return_status);
}

void BatchModeCheck(SQLCase& sql_case) {  // NOLINT
    int return_status;
    EngineCheck(sql_case, kBatchMode, true, &return_status);
}

void BatchRequestModeCheck(SQLCase& sql_case) {  // NOLINT
    int return_status;
    EngineCheck(sql_case, kBatchRequestMode, false, &return_status);
}

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_ENGINE_TEST_H_
