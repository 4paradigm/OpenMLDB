/*
 * sql_compiler_test.cc
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

#include "vm/sql_compiler.h"
#include <memory>
#include <utility>
#include "boost/algorithm/string.hpp"
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
#include "vm/simple_catalog.h"
#include "vm/test_base.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {

class SQLCompilerTest : public ::testing::TestWithParam<std::string> {};

INSTANTIATE_TEST_CASE_P(
    SqlSimpleProject, SQLCompilerTest,
    testing::Values(
        "SELECT COL1 as c1 FROM t1;", "SELECT t1.COL1 c1 FROM t1 limit 10;",
        "SELECT COL1 as c1, col2  FROM t1;",
        "SELECT col1-col2 as col1_2, *, col1+col2 as col12 FROM t1 limit 10;",
        "SELECT *, col1+col2 as col12 FROM t1 limit 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlWindowProjectPlanner, SQLCompilerTest,
    testing::Values(
        "SELECT COL1, COL2, `COL5`, AVG(COL3) OVER w, SUM(COL3) OVER w FROM "
        "t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `COL5` ROWS BETWEEN UNBOUNDED PRECEDING AND "
        "CURRENT ROW);",
        "SELECT COL1, SUM(col4) OVER w as w_amt_sum FROM t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `col5` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col5 ORDER BY `col5` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlWherePlan, SQLCompilerTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL1+COL2;",
        "SELECT COL1 FROM t1 where COL1;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 where COL1 > 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlGroupPlan, SQLCompilerTest,
    testing::Values(
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
        "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
        "SELECT sum(COL1) FROM t1 group by COL1+COL2;",
        "SELECT sum(COL1) FROM t1 group by COL1;",
        "SELECT sum(COL1) FROM t1 group by COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT sum(COL1) FROM t1 group by COL1, COL2;",
        "SELECT sum(COL1) FROM t1 group by COL1;"));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, SQLCompilerTest,
    testing::Values(
        "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 on "
        "t1.col1 = t2.col2;",
        "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 on "
        "t1.col1 = t2.col2 and t2.col5 >= t1.col5;"));
INSTANTIATE_TEST_CASE_P(
    WindowSqlTest, SQLCompilerTest,
    testing::Values(
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 10;",
        "SELECT COL1, COL2, `COL5`, AVG(COL3) OVER w, SUM(COL3) OVER w FROM "
        "t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `COL5` ROWS BETWEEN UNBOUNDED PRECEDING AND "
        "CURRENT ROW);",
        "SELECT COL1, SUM(col4) OVER w as w_amt_sum FROM t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `col5` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col5 ORDER BY `col5` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;"));

void CompilerRunnerCheck(std::shared_ptr<Catalog> catalog,
                           const std::string sql, const bool is_batch) {
    node::NodeManager nm;
    SQLCompiler sql_compiler(catalog, &nm);
    SQLContext sql_context;
    sql_context.sql = sql;
    sql_context.db = "db";
    sql_context.is_batch_mode = is_batch;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(nullptr != sql_context.plan);
    std::ostringstream oss;
    sql_context.plan->Print(oss, "");
    std::cout << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;

    ASSERT_TRUE(nullptr != sql_context.runner);
    std::ostringstream runner_oss;
    sql_context.runner->Print(runner_oss, "");
    std::cout << "runner: \n" << oss.str() << std::endl;

    std::ostringstream oss_schema;
    PrintSchema(oss_schema, sql_context.schema);
    std::cout << "schema:\n" << oss_schema.str();
}

void RequestSchemaCheck(std::shared_ptr<Catalog> catalog,
                          const std::string sql,
                          const type::TableDef& exp_table_def) {
    node::NodeManager nm;
    SQLCompiler sql_compiler(catalog, &nm);
    SQLContext sql_context;
    sql_context.sql = sql;
    sql_context.db = "db";
    sql_context.is_batch_mode = false;
    base::Status compile_status;
    bool ok = sql_compiler.Compile(sql_context, compile_status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(nullptr != sql_context.plan);
    std::ostringstream oss;
    sql_context.plan->Print(oss, "");
    std::cout << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;

    ASSERT_TRUE(nullptr != sql_context.runner);
    std::ostringstream runner_oss;
    sql_context.runner->Print(runner_oss, "");
    std::cout << "runner: \n" << oss.str() << std::endl;

    std::ostringstream oss_schema;
    PrintSchema(oss_schema, sql_context.schema);
    std::cout << "schema:\n" << oss_schema.str();

    std::ostringstream oss_request_schema;
    PrintSchema(oss_schema, sql_context.request_schema);
    std::cout << "request schema:\n" << oss_request_schema.str();

    ASSERT_EQ(sql_context.request_name, exp_table_def.name());
    ASSERT_EQ(sql_context.request_schema.size(),
              exp_table_def.columns().size());
    for (int i = 0; i < sql_context.request_schema.size(); i++) {
        ASSERT_EQ(sql_context.request_schema.Get(i).DebugString(),
                  exp_table_def.columns().Get(i).DebugString());
    }
}

TEST_P(SQLCompilerTest, compile_request_mode_test) {
    std::string sqlstr = GetParam();
    LOG(INFO) << sqlstr;

    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

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
    CompilerRunnerCheck(catalog, sqlstr, false);
    RequestSchemaCheck(catalog, sqlstr, table_def);
}

TEST_P(SQLCompilerTest, compile_batch_mode_test) {
    std::string sqlstr = GetParam();
    LOG(INFO) << sqlstr;

    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

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

    CompilerRunnerCheck(catalog, sqlstr, true);

    // Check for work with simple catalog
    auto simple_catalog = std::make_shared<SimpleCatalog>();
    fesql::type::Database db;
    db.set_name("db");
    {
        ::fesql::type::TableDef* p_table = db.add_tables();
        *p_table = table_def;
    }
    {
        ::fesql::type::TableDef* p_table = db.add_tables();
        *p_table = table_def2;
    }
    {
        ::fesql::type::TableDef* p_table = db.add_tables();
        *p_table = table_def3;
    }
    {
        ::fesql::type::TableDef* p_table = db.add_tables();
        *p_table = table_def4;
    }
    {
        ::fesql::type::TableDef* p_table = db.add_tables();
        *p_table = table_def5;
    }

    simple_catalog->AddDatabase(db);
    CompilerRunnerCheck(simple_catalog, sqlstr, true);
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
