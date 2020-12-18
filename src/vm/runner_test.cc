/*
 * runner_test.cc
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

#include <memory>
#include <utility>
#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "gtest/gtest.h"
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
#include "tablet/tablet_catalog.h"
#include "vm/sql_compiler.h"
#include "vm/test_base.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {
using fesql::sqlcase::SQLCase;
Runner* GetFirstRunnerOfType(Runner* root, const RunnerType type);

std::vector<SQLCase> InitCases(std::string yaml_path);
void InitCases(std::string yaml_path, std::vector<SQLCase>& cases);  // NOLINT

void InitCases(std::string yaml_path, std::vector<SQLCase>& cases) {  // NOLINT
    if (!SQLCase::CreateSQLCasesFromYaml(
            fesql::sqlcase::FindFesqlDirPath(), yaml_path, cases,
            std::vector<std::string>({"runner-unsupport",
                                      "physical-plan-unsupport",
                                      "logical-plan-unsupport"}))) {
        FAIL();
    }
}
std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}

class RunnerTest : public ::testing::TestWithParam<SQLCase> {};
INSTANTIATE_TEST_CASE_P(
    SqlSimpleQueryParse, RunnerTest,
    testing::ValuesIn(InitCases("cases/plan/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    SqlWindowQueryParse, RunnerTest,
    testing::ValuesIn(InitCases("cases/plan/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlWherePlan, RunnerTest,
    testing::ValuesIn(InitCases("cases/plan/where_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlGroupPlan, RunnerTest,
    testing::ValuesIn(InitCases("cases/plan/group_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, RunnerTest,
    testing::ValuesIn(InitCases("cases/plan/join_query.yaml")));

void RunnerCheck(std::shared_ptr<Catalog> catalog, const std::string sql,
                 EngineMode engine_mode) {
    SQLCompiler sql_compiler(catalog);
    SQLContext sql_context;
    sql_context.sql = sql;
    sql_context.db = "db";
    sql_context.engine_mode = engine_mode;
    sql_context.is_performance_sensitive = false;
    sql_context.is_cluster_optimized = false;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(sql_compiler.BuildClusterJob(sql_context, compile_status));
    ASSERT_TRUE(nullptr != sql_context.physical_plan);
    ASSERT_TRUE(sql_context.cluster_job.IsValid());
    std::ostringstream oss;
    sql_context.physical_plan->Print(oss, "");
    std::cout << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    sql_context.cluster_job.Print(runner_oss, "");
    std::cout << "runner: \n" << runner_oss.str() << std::endl;

    std::ostringstream oss_schema;
    PrintSchema(oss_schema, sql_context.schema);
    std::cout << "schema:\n" << oss_schema.str();
}

TEST_P(RunnerTest, request_mode_test) {
    if (boost::contains(GetParam().mode(), "request-unsupport")) {
        LOG(INFO) << "Skip sql case: request unsupport";
        return;
    }
    std::string sqlstr = GetParam().sql_str();
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;

    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;
    fesql::type::TableDef table_def3;
    fesql::type::TableDef table_def4;
    fesql::type::TableDef table_def5;
    fesql::type::TableDef table_def6;

    BuildTableDef(table_def);
    BuildTableDef(table_def2);
    BuildTableDef(table_def3);
    BuildTableDef(table_def4);
    BuildTableDef(table_def5);
    BuildTableDef(table_def6);

    table_def.set_name("t1");
    table_def2.set_name("t2");
    table_def3.set_name("t3");
    table_def4.set_name("t4");
    table_def5.set_name("t5");
    table_def6.set_name("t6");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    std::shared_ptr<::fesql::storage::Table> table3(
        new ::fesql::storage::Table(3, 1, table_def3));
    std::shared_ptr<::fesql::storage::Table> table4(
        new ::fesql::storage::Table(4, 1, table_def4));
    std::shared_ptr<::fesql::storage::Table> table5(
        new ::fesql::storage::Table(5, 1, table_def5));
    std::shared_ptr<::fesql::storage::Table> table6(
        new ::fesql::storage::Table(6, 1, table_def6));

    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);
    AddTable(catalog, table_def3, table3);
    AddTable(catalog, table_def4, table4);
    AddTable(catalog, table_def5, table5);
    AddTable(catalog, table_def6, table6);
    {
        fesql::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tb");
        std::shared_ptr<::fesql::storage::Table> table(
            new fesql::storage::Table(1, 1, table_def));
        AddTable(catalog, table_def, table);
    }
    {
        fesql::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tc");
        std::shared_ptr<::fesql::storage::Table> table(
            new fesql::storage::Table(1, 1, table_def));
        AddTable(catalog, table_def, table);
    }
    RunnerCheck(catalog, sqlstr, kRequestMode);
}

TEST_P(RunnerTest, batch_mode_test) {
    if (boost::contains(GetParam().mode(), "batch-unsupport")) {
        LOG(INFO) << "Skip sql case: batch unsupport";
        return;
    }
    std::string sqlstr = GetParam().sql_str();
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;

    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;
    fesql::type::TableDef table_def3;
    fesql::type::TableDef table_def4;
    fesql::type::TableDef table_def5;
    fesql::type::TableDef table_def6;

    BuildTableDef(table_def);
    BuildTableDef(table_def2);
    BuildTableDef(table_def3);
    BuildTableDef(table_def4);
    BuildTableDef(table_def5);
    BuildTableDef(table_def6);

    table_def.set_name("t1");
    table_def2.set_name("t2");
    table_def3.set_name("t3");
    table_def4.set_name("t4");
    table_def5.set_name("t5");
    table_def6.set_name("t6");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    std::shared_ptr<::fesql::storage::Table> table3(
        new ::fesql::storage::Table(3, 1, table_def3));
    std::shared_ptr<::fesql::storage::Table> table4(
        new ::fesql::storage::Table(4, 1, table_def4));
    std::shared_ptr<::fesql::storage::Table> table5(
        new ::fesql::storage::Table(5, 1, table_def5));
    std::shared_ptr<::fesql::storage::Table> table6(
        new ::fesql::storage::Table(6, 1, table_def6));

    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);
    AddTable(catalog, table_def3, table3);
    AddTable(catalog, table_def4, table4);
    AddTable(catalog, table_def5, table5);
    AddTable(catalog, table_def6, table6);
    {
        fesql::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tb");
        std::shared_ptr<::fesql::storage::Table> table(
            new fesql::storage::Table(1, 1, table_def));
        AddTable(catalog, table_def, table);
    }
    {
        fesql::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tc");
        std::shared_ptr<::fesql::storage::Table> table(
            new fesql::storage::Table(1, 1, table_def));
        AddTable(catalog, table_def, table);
    }
    RunnerCheck(catalog, sqlstr, kBatchMode);
}

Runner* GetFirstRunnerOfType(Runner* root, const RunnerType type) {
    if (nullptr == root) {
        return nullptr;
    }

    if (type == root->type_) {
        return root;
    } else {
        for (auto runner : root->GetProducers()) {
            auto res = GetFirstRunnerOfType(runner, type);
            if (nullptr != res) {
                return res;
            }
        }
        return nullptr;
    }
}
TEST_F(RunnerTest, KeyGeneratorTest) {
    std::string sqlstr =
        "select avg(col1), avg(col2) from t1 group by col1, col2 limit 1;";
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col3");
    index->add_first_keys("col4");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    RunnerCheck(catalog, sqlstr, kBatchMode);

    SQLCompiler sql_compiler(catalog);
    SQLContext sql_context;
    sql_context.sql = sqlstr;
    sql_context.db = "db";
    sql_context.engine_mode = kBatchMode;
    sql_context.is_performance_sensitive = false;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(sql_compiler.BuildClusterJob(sql_context, compile_status));
    ASSERT_TRUE(sql_context.physical_plan != nullptr);

    auto root = GetFirstRunnerOfType(
        sql_context.cluster_job.GetTask(0).GetRoot(), kRunnerGroup);
    auto group_runner = dynamic_cast<GroupRunner*>(root);
    std::vector<Row> rows;
    fesql::type::TableDef temp_table;
    BuildRows(temp_table, rows);
    ASSERT_EQ("1|5", group_runner->partition_gen_.GetKey(rows[0]));
    ASSERT_EQ("2|5", group_runner->partition_gen_.GetKey(rows[1]));
    ASSERT_EQ("3|55", group_runner->partition_gen_.GetKey(rows[2]));
    ASSERT_EQ("4|55", group_runner->partition_gen_.GetKey(rows[3]));
    ASSERT_EQ("5|55", group_runner->partition_gen_.GetKey(rows[4]));
}

TEST_F(RunnerTest, RunnerPrintDataTest) {
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col3");
    index->add_first_keys("col4");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    std::vector<Row> rows;
    fesql::type::TableDef temp_table;
    BuildRows(temp_table, rows);

    SchemasContext schemas_ctx;
    auto source = schemas_ctx.AddSource();
    source->SetSourceName("t1");
    source->SetSchema(&table_def.columns());

    // Print Empty Set
    std::shared_ptr<MemTableHandler> table_handler =
        std::shared_ptr<MemTableHandler>(new MemTableHandler());
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }

    // Print Table
    for (auto row : rows) {
        table_handler->AddRow(row);
    }
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }

    // Print Table
    int i = 0;
    while (i++ < 10) {
        for (auto row : rows) {
            table_handler->AddRow(row);
        }
    }
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }

    // Print Row
    std::shared_ptr<MemRowHandler> row_handler =
        std::shared_ptr<MemRowHandler>(new MemRowHandler(rows[0]));
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }
}
TEST_F(RunnerTest, RunnerPrintDataMemTimeTableTest) {
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col3");
    index->add_first_keys("col4");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    std::vector<Row> rows;
    fesql::type::TableDef temp_table;
    BuildRows(temp_table, rows);

    SchemasContext schemas_ctx;
    auto source = schemas_ctx.AddSource();
    source->SetSourceName("t1");
    source->SetSchema(&table_def.columns());

    // Print Empty Set
    std::shared_ptr<MemTimeTableHandler> table_handler =
        std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }

    // Print Table
    uint64_t ts = 1000;
    for (auto row : rows) {
        table_handler->AddRow(ts++, row);
    }
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }

    // Print Table
    int i = 0;
    while (i++ < 10) {
        for (auto row : rows) {
            table_handler->AddRow(ts++, row);
        }
    }
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }

    // Print Row
    std::shared_ptr<MemRowHandler> row_handler =
        std::shared_ptr<MemRowHandler>(new MemRowHandler(rows[0]));
    {
        std::ostringstream oss;
        Runner::PrintData(oss, &schemas_ctx, table_handler);
        LOG(INFO) << oss.str();
    }
}
}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
