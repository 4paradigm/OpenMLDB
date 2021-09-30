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

#include "vm/sql_compiler.h"
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
#include "vm/simple_catalog.h"
#include "testing/test_base.h"
#include "testing/engine_test_base.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace hybridse {
namespace vm {

using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"physical-plan-unsupport",  "zetasql-unsupport",
                                        "plan-unsupport", "parser-unsupport"});

class SqlCompilerTest : public ::testing::TestWithParam<SqlCase> {};
INSTANTIATE_TEST_SUITE_P(
    SqlSimpleQueryParse, SqlCompilerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlWindowQueryParse, SqlCompilerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlTableUdafQueryPlan, SqlCompilerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/table_aggregation_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlWherePlan, SqlCompilerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlGroupPlan, SqlCompilerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/group_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlJoinPlan, SqlCompilerTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

void CompilerCheck(std::shared_ptr<Catalog> catalog, const std::string sql,
                   const Schema& paramter_types, const EngineMode engine_mode,
                   const bool enable_batch_window_paralled) {
    SqlCompiler sql_compiler(catalog, false, true, false);
    SqlContext sql_context;
    sql_context.sql = sql;
    sql_context.db = "db";
    sql_context.engine_mode = engine_mode;
    sql_context.is_performance_sensitive = false;
    sql_context.enable_batch_window_parallelization = enable_batch_window_paralled;
    sql_context.parameter_types = paramter_types;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok) << compile_status;
    ASSERT_TRUE(nullptr != sql_context.physical_plan);
    std::ostringstream oss;
    sql_context.physical_plan->Print(oss, "");
    std::cout << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;

    std::ostringstream oss_schema;
    PrintSchema(oss_schema, sql_context.schema);
    std::cout << "schema:\n" << oss_schema.str();
}
void CompilerCheck(std::shared_ptr<Catalog> catalog, const std::string sql,
                   const Schema& paramter_types, EngineMode engine_mode) {
    CompilerCheck(catalog, sql, paramter_types, engine_mode, false);
}
void RequestSchemaCheck(std::shared_ptr<Catalog> catalog, const std::string sql,
                        const vm::Schema& paramter_types, const type::TableDef& exp_table_def) {
    SqlCompiler sql_compiler(catalog);
    SqlContext sql_context;
    sql_context.sql = sql;
    sql_context.db = "db";
    sql_context.engine_mode = kRequestMode;
    sql_context.is_performance_sensitive = false;
    sql_context.parameter_types = paramter_types;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok && compile_status.isOK()) << compile_status;
    ASSERT_TRUE(nullptr != sql_context.physical_plan);
    std::ostringstream oss;
    sql_context.physical_plan->Print(oss, "");
    std::cout << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;

    std::ostringstream oss_schema;
    PrintSchema(oss_schema, sql_context.schema);
    std::cout << "schema:\n" << oss_schema.str();

    std::ostringstream oss_request_schema;
    PrintSchema(oss_schema, sql_context.request_schema);
    std::cout << "request schema:\n" << oss_request_schema.str();

    ASSERT_EQ(sql_context.request_name, exp_table_def.name());
    ASSERT_EQ(sql_context.request_schema.size(), exp_table_def.columns().size());
    for (int i = 0; i < sql_context.request_schema.size(); i++) {
        ASSERT_EQ(sql_context.request_schema.Get(i).DebugString(), exp_table_def.columns().Get(i).DebugString());
    }
}

TEST_P(SqlCompilerTest, compile_request_mode_test) {
    if (boost::contains(GetParam().mode(), "request-unsupport")) {
        LOG(INFO) << "Skip sql case: request unsupport";
        return;
    }
    auto& sql_case = GetParam();
    std::string sqlstr = GetParam().sql_str();
    LOG(INFO) << sqlstr;

    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

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

    CompilerCheck(catalog, sqlstr, sql_case.ExtractParameterTypes(), kRequestMode);
    RequestSchemaCheck(catalog, sqlstr, sql_case.ExtractParameterTypes(), table_def);
}

TEST_P(SqlCompilerTest, compile_batch_mode_test) {
    if (boost::contains(GetParam().mode(), "batch-unsupport")) {
        LOG(INFO) << "Skip sql case: batch unsupport";
        return;
    }
    auto& sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    LOG(INFO) << sqlstr;

    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

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
    CompilerCheck(catalog, sqlstr, sql_case.ExtractParameterTypes(), kBatchMode, false);
    {
        // Check for work with simple catalog
        auto simple_catalog = std::make_shared<SimpleCatalog>();
        hybridse::type::Database db;
        db.set_name("db");
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def2;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def3;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def4;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def5;
        }
        {
            hybridse::type::TableDef table_def;
            BuildTableA(table_def);
            table_def.set_name("ta");
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }
        {
            hybridse::type::TableDef table_def;
            BuildTableA(table_def);
            table_def.set_name("tb");
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }
        {
            hybridse::type::TableDef table_def;
            BuildTableA(table_def);
            table_def.set_name("tc");
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }

        simple_catalog->AddDatabase(db);
        CompilerCheck(simple_catalog, sqlstr, sql_case.ExtractParameterTypes(), kBatchMode, false);
    }
}
TEST_F(SqlCompilerTest, TestEnableWindowParalled) {
    hybridse::type::TableDef t1;
    hybridse::type::TableDef t2;
    SqlCase::ExtractTableDef(
        {"col0 string", "col1 int", "col2 int"}, {}, t1);
    t1.set_name("t1");
    SqlCase::ExtractTableDef(
        {"str0 string", "str1 string", "col0 int", "col1 int"}, {}, t2);
    t2.set_name("t2");
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, t1);
    AddTable(db, t2);
    auto simple_catalog = BuildSimpleCatalogIndexUnsupport(db);
    std::string sqlstr = " SELECT sum(t1.col1) over w1 as sum_t1_col1, t2.str1 as t2_str1\n"
                         " FROM t1\n"
                         " last join t2 order by t2.col1\n"
                         " on t1.col1 = t2.col1 and t1.col2 = t2.col0\n"
                         " WINDOW w1 AS (\n"
                         "  PARTITION BY t1.col2 ORDER BY t1.col1\n"
                         "  ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW\n"
                         " ) limit 10;";
    CompilerCheck(simple_catalog, sqlstr, {}, kBatchMode, true);
}
TEST_P(SqlCompilerTest, compile_batch_mode_enable_window_paralled_test) {
    if (boost::contains(GetParam().mode(), "batch-unsupport")) {
        LOG(INFO) << "Skip sql case: batch unsupport";
        return;
    }
    auto& sql_case = GetParam();
    std::string sqlstr = GetParam().sql_str();
    LOG(INFO) << sqlstr;

    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

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
    auto catalog = BuildSimpleCatalogIndexUnsupport(db);
    CompilerCheck(catalog, sqlstr, sql_case.ExtractParameterTypes(), kBatchMode, true);

    {
        // Check for work with simple catalog
        auto simple_catalog = std::make_shared<SimpleCatalog>();
        hybridse::type::Database db;
        db.set_name("db");
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def2;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def3;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def4;
        }
        {
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def5;
        }
        {
            hybridse::type::TableDef table_def;
            BuildTableA(table_def);
            table_def.set_name("ta");
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }
        {
            hybridse::type::TableDef table_def;
            BuildTableA(table_def);
            table_def.set_name("tb");
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }
        {
            hybridse::type::TableDef table_def;
            BuildTableA(table_def);
            table_def.set_name("tc");
            ::hybridse::type::TableDef* p_table = db.add_tables();
            *p_table = table_def;
        }

        simple_catalog->AddDatabase(db);
        CompilerCheck(simple_catalog, sqlstr, sql_case.ExtractParameterTypes(), kBatchMode, true);
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
