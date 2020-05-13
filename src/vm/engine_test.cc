/*
 * engine_test.cc
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

#include "vm/engine.h"
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "codec/list_iterator_codec.h"
#include "codec/row_codec.h"
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
#include "vm/test_base.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {
using fesql::codec::ArrayListV;
using fesql::codec::Row;
using fesql::sqlcase::SQLCase;
enum EngineRunMode { RUNBATCH, RUNONE };

const bool IS_DEBUG = true;
std::vector<SQLCase> InitCases(std::string yaml_path);
void InitCases(std::string yaml_path, std::vector<SQLCase>& cases);  // NOLINT

void InitCases(std::string yaml_path, std::vector<SQLCase>& cases) {  // NOLINT
    if (!SQLCase::CreateSQLCasesFromYaml(
            fesql::sqlcase::FindFesqlDirPath() + "/" + yaml_path, cases)) {
        FAIL();
    }
}
std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema);
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows);
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows);
void StoreData(::fesql::storage::Table* table, const std::vector<Row>& rows);

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema) {
    ASSERT_EQ(schema.size(), exp_schema.size());
    for (int i = 0; i < schema.size(); i++) {
        ASSERT_EQ(schema.Get(i).DebugString(), exp_schema.Get(i).DebugString());
    }
}
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
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
        }
        t.endOfRow();
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
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
                    ASSERT_FLOAT_EQ(row_view.GetFloatUnsafe(i),
                                    row_view_exp.GetFloatUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kDouble: {
                    ASSERT_DOUBLE_EQ(row_view.GetDoubleUnsafe(i),
                                     row_view_exp.GetDoubleUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i),
                              row_view_exp.GetStringUnsafe(i))
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

class EngineTest : public ::testing::TestWithParam<SQLCase> {
 public:
    EngineTest() {}
    virtual ~EngineTest() {}
};
void RequestModeCheck(SQLCase& sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();
    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::cout << sql_case.sql_str() << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    RequestRunSession session;
    if (IS_DEBUG) {
        session.EnableDebug();
    }

    bool ok =
        engine.Get(sql_case.sql_str(), sql_case.db(), session, get_status);
    ASSERT_TRUE(ok);

    const std::string& request_name = session.GetRequestName();
    const vm::Schema& request_schema = session.GetRequestSchema();

    std::vector<Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        if (input.name_ == request_name) {
            ASSERT_TRUE(sql_case.ExtractInputData(request_data, i));
            continue;
        }
        std::vector<Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            StoreData(name_table_map[input.name_].get(), rows);
        }
    }

    int32_t ret = -1;
    DLOG(INFO) << "RUN IN MODE REQUEST";
    std::vector<Row> output;
    vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    std::cout << "physical plan:\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    std::cout << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Schema
    std::vector<Row> case_output_data;
    type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    auto request_table = name_table_map[request_name];
    ASSERT_TRUE(request_table->Init());
    for (auto in_row : request_data) {
        Row out_row;
        ret = session.Run(in_row, &out_row);
        ASSERT_EQ(0, ret);
        ASSERT_TRUE(request_table->Put(in_row.data(), in_row.size()));
        output.push_back(out_row);
    }
    LOG(INFO) << "expect result:\n";
    PrintRows(case_output_table.columns(), case_output_data);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, output);
    CheckRows(schema, output, case_output_data);
}
void BatchModeCheck(SQLCase& sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();

    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::cout << sql_case.sql_str() << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    BatchRunSession session;
    if (IS_DEBUG) {
        session.EnableDebug();
    }

    bool ok =
        engine.Get(sql_case.sql_str(), sql_case.db(), session, get_status);
    ASSERT_TRUE(ok);
    std::vector<Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        std::vector<Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            StoreData(name_table_map[input.name_].get(), rows);
        }
    }

    int32_t ret = -1;
    DLOG(INFO) << "RUN IN MODE BATCH";
    vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    std::cout << "physical plan:\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    std::cout << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Schema
    std::vector<Row> case_output_data;
    type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    std::vector<Row> output;
    std::vector<int8_t*> output_ptr;
    ASSERT_EQ(0, session.Run(output_ptr));
    for (auto ptr : output_ptr) {
        output.push_back(Row(ptr, RowView::GetSize(ptr)));
    }
    LOG(INFO) << "expect result:\n";
    PrintRows(case_output_table.columns(), case_output_data);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, output);
    CheckRows(schema, output, case_output_data);
}
INSTANTIATE_TEST_CASE_P(
    EngineRequstSimpleQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineBatchSimpleQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/simple_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchLastJoinQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestLastJoinQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(EngineBatchtLastJoinWindowQuery, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/batch_query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestLastJoinWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchWindowWithUnionQuery, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/batch_query/window_with_union_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestWindowWithUnionQuery, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/query/window_with_union_query.yaml")));


INSTANTIATE_TEST_CASE_P(
    EngineBatchGroupQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/group_query.yaml")));

TEST_P(EngineTest, test_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << sql_case.desc();
    if (sql_case.mode() == "request") {
        RequestModeCheck(sql_case);
    } else if (sql_case.mode() == "batch") {
        BatchModeCheck(sql_case);
    } else {
        LOG(WARNING) << "Invalid Check Mode " << sql_case.mode();
        FAIL();
    }
}

TEST_F(EngineTest, EngineCompileOnlyTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");

    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;

    BuildTableDef(table_def);
    BuildTableDef(table_def2);

    table_def.set_name("t1");
    table_def2.set_name("t2");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 last join t2 on "
            "t1.col1 = t2.col2;"};
        EngineOptions options;
        options.set_compile_only(true);
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_TRUE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;"};
        EngineOptions options;
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_FALSE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }
}
}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
