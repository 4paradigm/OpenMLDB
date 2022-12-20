/*
 * Copyright 2021 4Paradigm
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

#include "absl/strings/match.h"
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
#include "plan/plan_api.h"
#include "testing/test_base.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace hybridse {
namespace vm {
using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"runner-unsupport", "physical-plan-unsupport",
                                        "zetasql-unsupport", "logical-plan-unsupport"});
Runner* GetFirstRunnerOfType(Runner* root, const RunnerType type);


class RunnerTest : public ::testing::TestWithParam<SqlCase> {};
INSTANTIATE_TEST_SUITE_P(
    SqlSimpleQueryParse, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlWindowQueryParse, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlTableUdafQueryPlan, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/table_aggregation_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlWherePlan, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlGroupPlan, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/group_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlHavingPlan, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/having_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlJoinPlan, RunnerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

void RunnerCheck(std::shared_ptr<Catalog> catalog, const std::string sql,
                 const vm::Schema& parameter_types,
                 EngineMode engine_mode) {
    SqlCompiler sql_compiler(catalog);
    SqlContext sql_context;
    sql_context.sql = sql;
    sql_context.db = "db";
    sql_context.engine_mode = engine_mode;
    sql_context.is_cluster_optimized = false;
    sql_context.parameter_types = parameter_types;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok) << compile_status;
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

TEST_P(RunnerTest, RequestModeTest) {
    if (absl::StrContains(GetParam().mode(), "request-unsupport")) {
        LOG(INFO) << "Skip sql case: request unsupport";
        return;
    }
    auto& sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;

    hybridse::type::TableDef table_def;
    hybridse::type::TableDef table_def2;
    hybridse::type::TableDef table_def3;
    hybridse::type::TableDef table_def4;
    hybridse::type::TableDef table_def5;
    hybridse::type::TableDef table_def6;

    BuildTableDef(table_def);
    table_def.set_name("t1");
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index0");
        index->add_first_keys("col0");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index2");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }

    BuildTableDef(table_def2);
    BuildTableDef(table_def3);
    BuildTableDef(table_def4);
    BuildTableDef(table_def5);
    BuildTableDef(table_def6);
    table_def2.set_name("t2");
    {
        ::hybridse::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index2_t2");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    table_def3.set_name("t3");
    table_def2.set_name("t2");
    {
        ::hybridse::type::IndexDef* index = table_def3.add_indexes();
        index->set_name("index1_t3");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    table_def4.set_name("t4");
    table_def5.set_name("t5");
    table_def6.set_name("t6");



    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    AddTable(db, table_def2);
    AddTable(db, table_def3);
    AddTable(db, table_def4);
    AddTable(db, table_def5);
    AddTable(db, table_def6);

    {
        hybridse::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tb");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index1_tb");
            index->add_first_keys("c1");
            index->set_second_key("c5");
        }
        AddTable(db, table_def);
    }
    {
        hybridse::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tc");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index1_tc");
            index->add_first_keys("c1");
            index->set_second_key("c5");
        }
        AddTable(db, table_def);
    }
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index2_table2");
            index->add_first_keys("col2");
            index->set_second_key("col5");
        }
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    RunnerCheck(catalog, sqlstr, sql_case.ExtractParameterTypes(), kRequestMode);
}

TEST_P(RunnerTest, BatchModeTest) {
    if (absl::StrContains(GetParam().mode(), "batch-unsupport")) {
        LOG(INFO) << "Skip sql case: batch unsupport";
        return;
    }
    auto& sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;

    hybridse::type::TableDef table_def;
    hybridse::type::TableDef table_def2;
    hybridse::type::TableDef table_def3;
    hybridse::type::TableDef table_def4;
    hybridse::type::TableDef table_def5;
    hybridse::type::TableDef table_def6;

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
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    AddTable(db, table_def2);
    AddTable(db, table_def3);
    AddTable(db, table_def4);
    AddTable(db, table_def5);
    AddTable(db, table_def6);
    {
        hybridse::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tb");
        AddTable(db, table_def);
    }
    {
        hybridse::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tc");
        AddTable(db, table_def);
    }
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    RunnerCheck(catalog, sqlstr, sql_case.ExtractParameterTypes(), kBatchMode);
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
    std::string sqlstr = "select avg(col1), avg(col2) from t1 group by col1, col2 limit 1;";
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col3");
    index->add_first_keys("col4");
    index->set_second_key("col5");
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    codec::Schema empty_schema;
    RunnerCheck(catalog, sqlstr, empty_schema, kBatchMode);

    SqlCompiler sql_compiler(catalog);
    SqlContext sql_context;
    sql_context.sql = sqlstr;
    sql_context.db = "db";
    sql_context.engine_mode = kBatchMode;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(sql_compiler.BuildClusterJob(sql_context, compile_status));
    ASSERT_TRUE(sql_context.physical_plan != nullptr);

    auto root = GetFirstRunnerOfType(
        sql_context.cluster_job.GetTask(0).GetRoot(), kRunnerGroup);
    auto group_runner = dynamic_cast<GroupRunner*>(root);
    std::vector<Row> rows;
    hybridse::type::TableDef temp_table;
    Row empty_parameter;
    BuildRows(temp_table, rows);
    ASSERT_EQ("1|5", group_runner->partition_gen_.GetKey(rows[0], empty_parameter));
    ASSERT_EQ("2|5", group_runner->partition_gen_.GetKey(rows[1], empty_parameter));
    ASSERT_EQ("3|55", group_runner->partition_gen_.GetKey(rows[2], empty_parameter));
    ASSERT_EQ("4|55", group_runner->partition_gen_.GetKey(rows[3], empty_parameter));
    ASSERT_EQ("5|55", group_runner->partition_gen_.GetKey(rows[4], empty_parameter));
}

TEST_F(RunnerTest, RunnerPrintDataTest) {
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col3");
    index->add_first_keys("col4");
    index->set_second_key("col5");
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    std::vector<Row> rows;
    hybridse::type::TableDef temp_table;
    BuildRows(temp_table, rows);

    SchemasContext schemas_ctx;
    auto source = schemas_ctx.AddSource();
    source->SetSourceDBAndTableName("", "t1");
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
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col3");
    index->add_first_keys("col4");
    index->set_second_key("col5");
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    std::vector<Row> rows;
    hybridse::type::TableDef temp_table;
    BuildRows(temp_table, rows);

    SchemasContext schemas_ctx;
    auto source = schemas_ctx.AddSource();
    source->SetSourceDBAndTableName("", "t1");
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
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
