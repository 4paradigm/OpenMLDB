/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/transform.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "node/node_manager.h"
#include "passes/physical/condition_optimized.h"
#include "plan/plan_api.h"
#include "testing/test_base.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/simple_catalog.h"
#include "vm/sql_compiler.h"
using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace hybridse {
namespace vm {

using hybridse::passes::ConditionOptimized;
using hybridse::passes::ExprPair;
using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"physical-plan-unsupport",
                                        "zetasql-unsupport",
                                        "logical-plan-unsupport",
                                        "batch-unsupport",
                                        "parser-unsupport"});

class TransformTest : public ::testing::TestWithParam<SqlCase> {
 public:
    TransformTest() {}
    ~TransformTest() {}
    node::NodeManager manager;
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(TransformTest);
INSTANTIATE_TEST_SUITE_P(
    SqlSimpleQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlUdafQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/table_aggregation_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlReanmeQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/rename_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(
    SqlWindowQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlWherePlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlGroupPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/group_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlJoinPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlDistinctPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/distinct_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(
    SqlSubQueryPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/sub_query.yaml", FILTERS)));

TEST_P(TransformTest, TransformPhysicalPlan) {
    auto sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    std::cout << sqlstr << std::endl;

    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
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
        table_def.set_name("ta");
        AddTable(db, table_def);
    }
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
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        if (plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, &manager, base_status)) {
            LOG(INFO) << "\n" << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
            EXPECT_EQ(false, sql_case.expect().success_);
            return;
        }
        ASSERT_EQ(0, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    auto parameter_types = sql_case.ExtractParameterTypes();
    BatchModeTransformer transform(&manager, "db", catalog, &parameter_types, m.get(), lib);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    base::Status status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    EXPECT_EQ(sql_case.expect().success_, status.isOK()) << status;
    std::ostringstream oss;
    physical_plan->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;
}

TEST_P(TransformTest, TransformPhysicalPlanEnableWindowParalled) {
    auto& sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    std::cout << sqlstr << std::endl;

    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
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
        table_def.set_name("ta");
        AddTable(db, table_def);
    }
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
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        if (plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, &manager, base_status)) {
            LOG(INFO) << "\n" << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
            EXPECT_EQ(false, sql_case.expect().success_);
            return;
        }
        ASSERT_EQ(common::kOk, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    auto parameter_types = sql_case.ExtractParameterTypes();
    BatchModeTransformer transform(&manager, "db", catalog, &parameter_types, m.get(), lib, false, false, true);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    auto status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    EXPECT_EQ(sql_case.expect().success_, status.isOK()) << status;
}

void PhysicalPlanCheck(const std::shared_ptr<Catalog>& catalog, std::string sql,
                       std::string exp) {
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");

    boost::to_lower(sql);
    std::cout << sql << std::endl;

    ::hybridse::node::NodeManager manager;
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        if (plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &manager, base_status)) {
            LOG(INFO) << "\n" << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
        }
        ASSERT_EQ(0, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    base::Status status;
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    BatchModeTransformer transform(&manager, "db", catalog, nullptr, m.get(), lib);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;

    base_status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    std::cout << base_status.str() << std::endl;
    ASSERT_TRUE(base_status.isOK());

    std::ostringstream oos;
    physical_plan->Print(oos, "");
    LOG(INFO) << "physical plan:\n" << oos.str() << std::endl;

    std::ostringstream ss;
    PrintSchema(ss, *physical_plan->GetOutputSchema());
    LOG(INFO) << "schema:\n" << ss.str() << std::endl;

    ASSERT_EQ(oos.str(), exp);
}

void PhysicalPlanFailCheck(const std::shared_ptr<Catalog>& catalog,
                           const std::string& sql,
                           const vm::EngineMode runner_mode,
                           int err_code,
                           const std::string& err_msg) {
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");

    ::hybridse::node::NodeManager manager;
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status status;

    bool is_batch_mode = runner_mode == kBatchMode;

    // logical plan consider pass
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &manager, status, is_batch_mode)) << status;
    ASSERT_EQ(common::kOk, status.code);

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    std::unique_ptr<BatchModeTransformer> transform;
    switch (runner_mode) {
        case kBatchMode: {
            transform.reset(new BatchModeTransformer(&manager, "db", catalog, nullptr, m.get(), lib));
            break;
        }
        case kRequestMode: {
            transform.reset(new RequestModeTransformer(&manager, "db",
                                                       catalog, nullptr, m.get(), lib, {}, false, false, false));
            break;
        }
        default: {
            ASSERT_TRUE(false) << "unsupported runner mode: " << runner_mode;
            return;
        }
    }

    transform->AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;

    status = transform->TransformPhysicalPlan(plan_trees, &physical_plan);
    EXPECT_EQ(err_code, status.code) << status;
    EXPECT_EQ(err_msg.c_str(), status.msg);
}

// physical plan transform will fail if the partition key of a window is not in the supported type list
//  which is [bool, intxx, data, timestamp, string]
TEST_F(TransformTest, PhysicalPlanFailOnWindowPartitionType) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);

    const std::string sql = R"sql(SELECT sum(col1) OVER w1 as w1_c4_sum FROM t1
                                  WINDOW w1 AS (PARTITION BY col3 ORDER BY col5 ROWS_RANGE BETWEEN 2s PRECEDING AND CURRENT ROW);)sql";

    PhysicalPlanFailCheck(
        catalog, sql, kBatchMode, common::kPhysicalPlanError,
        "unsupported partition key: 'col3', type is float, should be bool, intxx, string, date or timestamp");
    PhysicalPlanFailCheck(
        catalog, sql, kRequestMode, common::kPhysicalPlanError,
        "unsupported partition key: 'col3', type is float, should be bool, intxx, string, date or timestamp");
}

// physical plan transform will fail if the group key is not in the supported type list
//  which is [bool, intxx, data, timestamp, string]
TEST_F(TransformTest, PhysicalPlanFailOnGroupType) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);

    const std::string sql = "SELECT sum(col1), col3 FROM t1 GROUP BY col3";

    PhysicalPlanFailCheck(
        catalog, sql, kBatchMode, common::kPhysicalPlanError,
        "unsupported partition key: 'col3', type is float, should be bool, intxx, string, date or timestamp");
}


// physical plan transform will fail if the right key of a join op is not in the supported type list
//  which is [bool, intxx, data, timestamp, string]
TEST_F(TransformTest, PhysicalPlanFailOnRightKeyTypeOfJoin) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    hybridse::type::TableDef table_def2;
    BuildTableT2Def(table_def2);
    AddTable(db, table_def2);

    auto catalog = BuildSimpleCatalog(db);

    const std::string sql =
        R"sql(SELECT t1.col1 as id, t1.col0 as t1_col0, t1.col1 + t2.col1 + 1 as test_col1, t1.col2 as t1_col2, str1 FROM t1
              last join t2 order by t2.col5 on t1.col4 = t2.col4 AND t1.col5 = t2.col5;)sql";

    PhysicalPlanFailCheck(
        catalog, sql, kBatchMode, common::kPhysicalPlanError,
        "unsupported partition key: 't2.col4', type is double, should be bool, intxx, string, date or timestamp");
    PhysicalPlanFailCheck(
        catalog, sql, kRequestMode, common::kPhysicalPlanError,
        "unsupported partition key: 't2.col4', type is double, should be bool, intxx, string, date or timestamp");
}

TEST_F(TransformTest, PhysicalPlanFailOnLastJoinWindow) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    hybridse::type::TableDef table_def2;
    BuildTableT2Def(table_def2);
    AddTable(db, table_def2);

    auto catalog = BuildSimpleCatalog(db);

    const std::string last_join_window = R"sql(
      SELECT t1.col1 as id, t1.col2 as t1_col2, t1.col5 as t1_col5,
      sum(t1.col1) OVER w1 as w1_col1_sum, sum(t1.col3) OVER w1 as w1_col3_sum,
      sum(t2.col4) OVER w1 as w1_t2_col4_sum, sum(t2.col2) OVER w1 as w1_t2_col2_sum,
      sum(t1.col5) OVER w1 as w1_col5_sum,
      str1 as t2_str1 FROM t1
      last join t2 order by t2.col5 on t1.col3=t2.col3 and t1.col5 = t2.col5
      WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;
    )sql";
    PhysicalPlanFailCheck(catalog, last_join_window, kBatchMode, common::kPhysicalPlanError,
        "unsupported partition key: 't2.col3', type is float, should be bool, intxx, string, date or timestamp");
    PhysicalPlanFailCheck(catalog, last_join_window, kRequestMode, common::kPhysicalPlanError,
        "unsupported partition key: 't2.col3', type is float, should be bool, intxx, string, date or timestamp");
}

// OpenMLDB only support ONLY_FULL_GROUP_BY mode
TEST_F(TransformTest, PhysicalPlanFailOnOnlyFullGroupBy) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);
    // ONLY_FULL_GROUP_BY Rule #1:
    PhysicalPlanFailCheck(
        catalog, "SELECT col1, SUM(col3) from t1;", kBatchMode, common::kPlanError,
        "In aggregated query without GROUP BY, expression #0 of SELECT list contains nonaggregated "
        "project 'col1'; this is incompatible with sql_mode=only_full_group_by");
    // ONLY_FULL_GROUP_BY Rule #1:
    PhysicalPlanFailCheck(catalog, "SELECT col1, SUM(col2) from t1 HAVING SUM(col2) > 0;", kBatchMode,
                          common::kPlanError,
                          "In aggregated query without GROUP BY, expression #0 of SELECT list contains nonaggregated "
                          "project 'col1'; this is incompatible with sql_mode=only_full_group_by");

    // ONLY_FULL_GROUP_BY Rule #2:
    PhysicalPlanFailCheck(catalog,
                          "SELECT col1 from t1 HAVING SUM(col2) > 0;", kBatchMode, common::kPlanError,
                          "Having clause can only be used in conjunction with aggregated query");
    // ONLY_FULL_GROUP_BY Rule #3:
    PhysicalPlanFailCheck(catalog, "SELECT SUM(col1) from t1 HAVING col2 > 0;", kBatchMode, common::kPlanError,
                          "In aggregated query without GROUP BY, Having clause contains nonaggregated project 'col2 > "
                          "0'; this is incompatible with sql_mode=only_full_group_by");
    // ONLY_FULL_GROUP_BY Rule #4:
    PhysicalPlanFailCheck(
        catalog, "SELECT col1, SUM(col3) from t1 GROUP BY col2;", kBatchMode, common::kPlanError,
        "Expression #0 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'db.t1.col1' which "
        "is not "
        "functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by");

    // ONLY_FULL_GROUP_BY Rule #5:
    PhysicalPlanFailCheck(
        catalog, "SELECT col1, SUM(col3) from t1 GROUP BY col1 HAVING col2 > 0;", kBatchMode, common::kPlanError,
        "Having clause is not in GROUP BY clause and contains nonaggregated column 'db.t1.col2' which is not "
        "functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by");
}

// OpenMLDB column resolve failure check
TEST_F(TransformTest, PhysicalColumnResolveFailTest) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);
    PhysicalPlanFailCheck(
        catalog,
        "select c1 from (select col1 as c1, col1 as c1, col3 from t1) as tt;",
        kBatchMode, common::kOk,
        "ok");
    PhysicalPlanFailCheck(
        catalog,
        "select c1 from (select col1 as c1, col1 as c1, col3 from t1) as tt;",
        kRequestMode, common::kOk,
        "ok");

    PhysicalPlanFailCheck(
        catalog,
        "select c1 from t1;",
        kBatchMode, common::kColumnNotFound,
        "Fail to find column c1");
    PhysicalPlanFailCheck(
        catalog,
        "select c1 from t1;",
        kRequestMode, common::kColumnNotFound,
        "Fail to find column c1");

    PhysicalPlanFailCheck(
        catalog,
        "select c1 from (select col1 as c1, col2 as c1, col3 from t1) as tt;",
        kBatchMode, common::kColumnAmbiguous,
        "Ambiguous column name c1");
    PhysicalPlanFailCheck(
        catalog,
        "select c1 from (select col1 as c1, col2 as c1, col3 from t1) as tt;",
        kRequestMode, common::kColumnAmbiguous,
        "Ambiguous column name c1");

    PhysicalPlanFailCheck(
        catalog,
        R"sql(select * from
              (
                SELECT col1 as id, col1 as id, sum(col2) OVER w1 as w1_col2_sum,
                FROM t1
                WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 10 OPEN PRECEDING AND CURRENT ROW)
              ) as out0
              LAST JOIN
              (
                SELECT col1 as id, sum(col2) OVER w2 as w2_col2_sum
                FROM t1
                WINDOW w2 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 1d OPEN PRECEDING AND CURRENT ROW)
              ) as out1
              ON out0.id = out1.id;)sql",
        kBatchMode, common::kOk, "ok");

    PhysicalPlanFailCheck(
        catalog,
        R"sql(select * from
              (
                SELECT col1 as id, col2 as id, sum(col2) OVER w1 as w1_col2_sum,
                FROM t1
                WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 10 OPEN PRECEDING AND CURRENT ROW)
              ) as out0
              LAST JOIN
              (
                SELECT col1 as id, sum(col2) OVER w2 as w2_col2_sum
                FROM t1
                WINDOW w2 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 1d OPEN PRECEDING AND CURRENT ROW)
              ) as out1 ON out0.id = out1.id;)sql",
        kBatchMode, common::kColumnAmbiguous,
        "Ambiguous column name .out0.id");
}

TEST_F(TransformTest, TransfromConditionsTest) {
    std::vector<std::pair<std::string, std::vector<std::string>>> sql_exp;

    sql_exp.push_back(std::make_pair(
        "select t1.col1=t2.col1 or t1.col2 = t2.col2 and t1.col3 = t2.col3 "
        "from t1,t2;",
        std::vector<std::string>({"t1.col1 = t2.col1 OR t1.col2 = t2.col2 AND "
                                  "t1.col3 = t2.col3"})));  // expr1

    sql_exp.push_back(std::make_pair(
        "select t1.col1=t2.col1 or t1.col2 = t2.col2 from t1,t2;",
        std::vector<std::string>(
            {"t1.col1 = t2.col1 OR t1.col2 = t2.col2"})));  // expr1

    sql_exp.push_back(std::make_pair(
        "select t1.col1=t2.col1 and t1.col2 = t2.col2 from t1,t2;",
        std::vector<std::string>({"t1.col1 = t2.col1",      // expr1
                                  "t1.col2 = t2.col2"})));  // expr2

    sql_exp.push_back(std::make_pair(
        "select (t1.col1=t2.col1 and t1.col2 = t2.col2) from t1,t2;",
        std::vector<std::string>({"t1.col1 = t2.col1",      // expr1
                                  "t1.col2 = t2.col2"})));  // expr2

    sql_exp.push_back(std::make_pair(
        "select t1.col1=t2.col1 and t1.col2 = t2.col2 and "
        "t1.col3+t1.col4=t2.col3 from t1,t2;",
        std::vector<std::string>({"t1.col1 = t2.col1",                // expr1
                                  "t1.col2 = t2.col2",                // expr2
                                  "t1.col3 + t1.col4 = t2.col3"})));  // expr3

    sql_exp.push_back(std::make_pair(
        "select t1.col1 = t2.col2 and t2.col5 >= t1.col5 from t1,t2;",
        std::vector<std::string>({"t1.col1 = t2.col2",       // expr1
                                  "t2.col5 >= t1.col5"})));  // expr2
    for (size_t i = 0; i < sql_exp.size(); i++) {
        std::string sql = sql_exp[i].first;
        std::vector<std::string>& exp_list = sql_exp[i].second;
        node::ExprNode* condition;
        boost::to_lower(sql);
        ExtractExprFromSimpleSql(&manager, sql, &condition);
        LOG(INFO) << "TEST condition [" << i
                  << "]: " << node::ExprString(condition);
        node::ExprListNode and_condition_list;
        ConditionOptimized::TransfromAndConditionList(condition,
                                                      &and_condition_list);
        LOG(INFO) << "and condition list: "
                  << node::ExprString(&and_condition_list);
        ASSERT_EQ(exp_list.size(), and_condition_list.children_.size());
        for (size_t i = 0; i < exp_list.size(); i++) {
            ASSERT_EQ(exp_list[i],
                      node::ExprString(and_condition_list.children_[i]));
        }
    }
}

TEST_F(TransformTest, TransformEqualExprPairTest) {
    vm::SchemasContext left_ctx;
    vm::SchemasContext right_ctx;

    type::TableDef t1;
    type::TableDef t2;
    {
        BuildTableDef(t1);
        t1.set_name("t1");
        left_ctx.BuildTrivial(t1.catalog(), {&t1});
    }
    {
        BuildTableT2Def(t2);
        t2.set_name("t2");
        right_ctx.BuildTrivial(t2.catalog(), {&t2});
    }

    std::vector<std::pair<std::string, std::pair<std::string, std::string>>>
        sql_exp;

    sql_exp.push_back(std::make_pair("select t1.col1=t2.col1 from t1,t2;",
                                     std::make_pair("t1.col1", "t2.col1")));

//    sql_exp.push_back(std::make_pair("select t2.col1=t1.col1 from t1,t2;",
//                                     std::make_pair("t1.col1", "t2.col1")));
//
//    // Fail Extract Equal Pair
//    sql_exp.push_back(std::make_pair(
//        "select t2.col1+t1.col1=t2.col3 from t1,t2;", std::make_pair("", "")));
//    sql_exp.push_back(std::make_pair("select t1.col1=t1.col2 from t1,t2;",
//                                     std::make_pair("", "")));
//    sql_exp.push_back(std::make_pair("select t1.col1=t3.col2 from t1,t2;",
//                                     std::make_pair("", "")));
//    sql_exp.push_back(std::make_pair("select t2.col1>t1.col1 from t1,t2;",
//                                     std::make_pair("", "")));

    for (size_t i = 0; i < sql_exp.size(); i++) {
        std::string sql = sql_exp[i].first;
        std::pair<std::string, std::string>& exp_list = sql_exp[i].second;
        node::ExprNode* condition;
        boost::to_lower(sql);
        ExtractExprFromSimpleSql(&manager, sql, &condition);
        LOG(INFO) << "TEST condition [" << i
                  << "]: " << node::ExprString(condition);
        node::ExprListNode mock_condition_list;
        mock_condition_list.AddChild(condition);

        node::ExprListNode out_condition_list;
        std::vector<ExprPair> mock_expr_pairs;

        ConditionOptimized::TransformJoinEqualExprPair(
            &left_ctx, &right_ctx, &mock_condition_list, &out_condition_list,
            mock_expr_pairs);

        ExprPair mock_pair;
        ExprPair expr_pair =
            mock_expr_pairs.empty() ? mock_pair : mock_expr_pairs[0];
        LOG(INFO) << "REST CONDITION: "
                  << node::ExprString(&out_condition_list);
        ASSERT_EQ(exp_list.first, node::ExprString(expr_pair.left_expr_));
        ASSERT_EQ(exp_list.second, node::ExprString(expr_pair.right_expr_));
        ASSERT_EQ(mock_condition_list.children_.size(),
                  out_condition_list.children_.size() + mock_expr_pairs.size());
    }
}

TEST_P(TransformTest, WindowMergeOptTest) {
    auto& sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    std::cout << sqlstr << std::endl;

    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
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
        table_def.set_name("ta");
        AddTable(db, table_def);
    }
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
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        if (plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, &manager, base_status)) {
            LOG(INFO) << "\n" << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
            EXPECT_EQ(false, sql_case.expect().success_);
            return;
        }

        ASSERT_EQ(0, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    base::Status status;
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    auto parameter_types = sql_case.ExtractParameterTypes();
    BatchModeTransformer transform(&manager, "db", catalog, &parameter_types, m.get(), lib);
    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    ASSERT_TRUE(status.isOK()) << status;
    std::ostringstream oss;

    physical_plan->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << "\n";
    //    m->print(::llvm::errs(), NULL);
}

class KeyGenTest : public ::testing::TestWithParam<std::string> {
 public:
    KeyGenTest() {}
    ~KeyGenTest() {}
    node::NodeManager nm;
};

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(KeyGenTest);
INSTANTIATE_TEST_SUITE_P(KeyGen, KeyGenTest,
                        testing::Values("select col1 from t1;",
                                        "select col1, col2 from t1;"));

TEST_P(KeyGenTest, GenTest) {
    base::Status status;

    hybridse::type::TableDef table_def;
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
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    auto catalog = BuildSimpleCatalog(db);

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    codec::Schema parameter_types;
    BatchModeTransformer transformer(&nm, "db", catalog, &parameter_types, m.get(), lib);

    auto groups = nm.MakeExprList();
    ExtractExprListFromSimpleSql(&nm, GetParam(), groups);

    PhysicalTableProviderNode* table_provider;
    transformer.GetPlanContext()->CreateOp<PhysicalTableProviderNode>(&table_provider, catalog->GetTable("db", "t1"));

    Key group(groups);

    ASSERT_TRUE(transformer.GenKey(&group, table_provider->schemas_ctx()).isOK());
    m->print(::llvm::errs(), NULL);
    ASSERT_FALSE(group.fn_info().fn_name().empty());
}

class FilterGenTest : public ::testing::TestWithParam<std::string> {
 public:
    FilterGenTest() {}
    ~FilterGenTest() {}
    node::NodeManager nm;
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(FilterGenTest);
INSTANTIATE_TEST_SUITE_P(FilterGen, FilterGenTest,
                        testing::Values("select t1.col1=t2.col1 from t1,t2;",
                                        "select t1.col1!=t2.col2 from t1,t2;",
                                        "select t1.col1>t2.col2 from t1,t2;",
                                        "select t1.col1<t2.col2 from t1,t2;"));
TEST_P(FilterGenTest, GenFilter) {
    base::Status status;

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");

    hybridse::type::TableDef table_def2;
    BuildTableDef(table_def2);
    table_def2.set_name("t2");

    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    AddTable(db, table_def2);
    auto catalog = BuildSimpleCatalog(db);
    node::ExprNode* condition;
    ExtractExprFromSimpleSql(&nm, GetParam(), &condition);

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    vm::Schema parameter_types;
    BatchModeTransformer transformer(&nm, "db", catalog, &parameter_types, m.get(), lib);

    auto plan_ctx = transformer.GetPlanContext();
    PhysicalTableProviderNode* table_provider1;
    plan_ctx->CreateOp<PhysicalTableProviderNode>(
        &table_provider1, catalog->GetTable("db", "t1"));

    PhysicalTableProviderNode* table_provider2;
    plan_ctx->CreateOp<PhysicalTableProviderNode>(
        &table_provider2, catalog->GetTable("db", "t2"));

    PhysicalJoinNode* join_node = nullptr;
    plan_ctx->CreateOp<PhysicalJoinNode>(
        &join_node, table_provider1, table_provider2, node::kJoinTypeConcat);

    ConditionFilter filter(condition);
    ASSERT_TRUE(
        transformer.GenConditionFilter(&filter, join_node->schemas_ctx())
            .isOK());
    m->print(::llvm::errs(), NULL);
    ASSERT_FALSE(filter.fn_info().fn_name().empty());
}

class TransformPassOptimizedTest
    : public ::testing::TestWithParam<std::pair<std::string, std::string>> {
 public:
    TransformPassOptimizedTest() {}
    ~TransformPassOptimizedTest() {}
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(TransformPassOptimizedTest);
INSTANTIATE_TEST_SUITE_P(
    GroupHavingOptimized, TransformPassOptimizedTest,
    testing::Values(
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1 HAVING sum(col2) > 100;",
            "PROJECT(type=GroupAggregation, group_keys=(col1), having_condition=sum(col2) > 100)\n"
            "  DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2 HAVING sum(col2) > 100;",
            "PROJECT(type=GroupAggregation, group_keys=(col1,col2), having_condition=sum(col2) > 100)\n"
            "  DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col6 HAVING sum(col2) > 100;",
            "PROJECT(type=GroupAggregation, group_keys=(col1,col2,col6), having_condition=sum(col2) > 100)\n"
            "  GROUP_BY(group_keys=(col6))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col6, col2, col1 HAVING col1 > 0 AND col2 > 0;",
            "PROJECT(type=GroupAggregation, group_keys=(col6,col2,col1), having_condition=col1 > 0 AND col2 > 0)\n"
            "  GROUP_BY(group_keys=(col6))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM (select c1 as col1, c2 as col2 , "
            "c6 as col6 from tc) group by col6, col2, col1 HAVING col1 > 0;",
            "PROJECT(type=GroupAggregation, group_keys=(col6,col2,col1), having_condition=col1 > 0)\n"
            "  GROUP_BY(group_keys=(col6))\n"
            "    SIMPLE_PROJECT(sources=(c1 -> col1, c2 -> col2, c6 -> col6))\n"
            "      DATA_PROVIDER(type=Partition, table=tc, "
            "index=index12_tc)")));
INSTANTIATE_TEST_SUITE_P(
    GroupOptimized, TransformPassOptimizedTest,
    testing::Values(
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1;",
            "PROJECT(type=GroupAggregation, group_keys=(col1))\n"
            "  DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
            "PROJECT(type=GroupAggregation, group_keys=(col1,col2))\n"
            "  DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col6;",
            "PROJECT(type=GroupAggregation, group_keys=(col1,col2,col6))\n"
            "  GROUP_BY(group_keys=(col6))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col6, col2, col1;",
            "PROJECT(type=GroupAggregation, group_keys=(col6,col2,col1))\n"
            "  GROUP_BY(group_keys=(col6))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM (select c1 as col1, c2 as col2 , "
            "c6 as col6 from tc) group by col6, col2, col1;",
            "PROJECT(type=GroupAggregation, group_keys=(col6,col2,col1))\n"
            "  GROUP_BY(group_keys=(col6))\n"
            "    SIMPLE_PROJECT(sources=(c1 -> col1, c2 -> col2, c6 -> col6))\n"
            "      DATA_PROVIDER(type=Partition, table=tc, "
            "index=index12_tc)")));
INSTANTIATE_TEST_SUITE_P(
    WhereOptimized, TransformPassOptimizedTest,
    testing::Values(
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10;",
                       "PROJECT(type=Aggregation)\n"
                       "  FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(10))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 = 20;",
                       "PROJECT(type=Aggregation)\n"
                       "  FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(10,20))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 < 20;",
                       "PROJECT(type=Aggregation)\n"
                       "  FILTER_BY(condition=col2 < 20, left_keys=(), right_keys=(), index_keys=(10))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col0 = \"test\";",
                       "PROJECT(type=Aggregation)\n"
                       "  FILTER_BY(condition=test = col0, left_keys=(), right_keys=(), index_keys=(10))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM (select c1 as col1, c2 as col2 , "
                       "c3 as col3 from tc) where col1 = 10 and col2 = 20;",
                       "PROJECT(type=Aggregation)\n"
                       "  FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(10,20))\n"
                       "    SIMPLE_PROJECT(sources=(c1 -> col1, c2 -> col2, c3 -> col3))\n"
                       "      DATA_PROVIDER(type=Partition, table=tc, index=index12_tc)")));
INSTANTIATE_TEST_SUITE_P(
    WhereGroupOptimized, TransformPassOptimizedTest,
    testing::Values(
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 group by col1;",
                       "PROJECT(type=GroupAggregation, group_keys=(col1))\n"
                       "  FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(10))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 = 20 group by col1;",
                       "PROJECT(type=GroupAggregation, group_keys=(col1))\n"
                       "  FILTER_BY(condition=20 = col2, left_keys=(), right_keys=(), index_keys=(10))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 = 20 group by col1, col2;",
                       "PROJECT(type=GroupAggregation, group_keys=(col1,col2))\n"
                       "  FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(10,20))\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col2 = 20 group by col1;",
                       "PROJECT(type=GroupAggregation, group_keys=(col1))\n"
                       "  FILTER_BY(condition=20 = col2, left_keys=(), right_keys=(), index_keys=)\n"
                       "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 = 20 group by col1, col2, col6;",
                       "PROJECT(type=GroupAggregation, group_keys=(col1,col2,col6))\n"
                       "  FILTER_BY(condition=10 = col1 AND 20 = col2, left_keys=(), right_keys=(), index_keys=)\n"
                       "    GROUP_BY(group_keys=(col6))\n"
                       "      DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM (select c1 as col1, c2 as col2 , "
                       "c3 as col3 from tc) where col1 = 10 and col2 = 20 group by col2, col1;",
                       "PROJECT(type=GroupAggregation, group_keys=(col2,col1))\n"
                       "  FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(10,20))\n"
                       "    SIMPLE_PROJECT(sources=(c1 -> col1, c2 -> col2, c3 -> col3))\n"
                       "      DATA_PROVIDER(type=Partition, table=tc, index=index12_tc)")));
INSTANTIATE_TEST_SUITE_P(
    SortOptimized, TransformPassOptimizedTest,
    testing::Values(
        std::make_pair(
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair(
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2, col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), "
            "range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair("SELECT "
                       "col1, "
                       "sum(col3) OVER w1 as w1_col3_sum, "
                       "sum(col2) OVER w1 as w1_col2_sum "
                       "FROM t1 WINDOW w1 AS (PARTITION BY col6 ORDER BY col5 "
                       "ROWS_RANGE BETWEEN 3 "
                       "PRECEDING AND CURRENT ROW) limit 10;",
                       "LIMIT(limit=10, optimized)\n"
                       "  PROJECT(type=WindowAggregation, limit=10)\n"
                       "    +-WINDOW(partition_keys=(col6), orders=(col5 ASC), "
                       "range=(col5, 3 PRECEDING, 0 CURRENT))\n"
                       "    DATA_PROVIDER(table=t1)")));

INSTANTIATE_TEST_SUITE_P(
    JoinFilterOptimized, TransformPassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col2 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=t2.col5 "
            ">= t1.col5, "
            "left_keys=(t1.col1), right_keys=(t2.col2), index_keys=)\n"
            "    DATA_PROVIDER(table=t1)\n"
            "    DATA_PROVIDER(table=t2)"),
        // 1
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col1 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  JOIN(type=LastJoin, right_sort=(ASC), condition=t2.col5 >= "
            "t1.col5, left_keys=(), "
            "right_keys=(), index_keys=(t1.col1))\n"
            "    DATA_PROVIDER(table=t1)\n"
            "    DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)"),
        // 2
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col1 "
            "WINDOW w1 AS (PARTITION BY t1.col0 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(t1.col0), orders=(t1.col5 ASC), "
            "range=(t1.col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-JOIN(type=LastJoin, right_sort=(ASC), condition=, "
            "left_keys=(), "
            "right_keys=(), index_keys=(t1.col1))\n"
            "        DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)\n"
            "    DATA_PROVIDER(table=t1)"),
        // 3
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col1 "
            "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(t1.col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-JOIN(type=LastJoin, right_sort=(ASC), condition=, "
            "left_keys=(), "
            "right_keys=(), index_keys=(t1.col1))\n"
            "        DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        // 4
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col0 = t2.col0 last "
            "join t3 order by t3.col5 on "
            "t2.col0=t3.col0 "
            "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(t1.col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=, "
            "left_keys=(t1.col0), "
            "right_keys=(t2.col0), index_keys=)\n"
            "        DATA_PROVIDER(table=t2)\n"
            "    +-JOIN(type=LastJoin, right_sort=(t3.col5 ASC), condition=, "
            "left_keys=(t2.col0), "
            "right_keys=(t3.col0), index_keys=)\n"
            "        DATA_PROVIDER(table=t3)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        // 5
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col2 = t2.col2 last "
            "join t3 order by t3.col5 on "
            "t2.col2=t3.col2 "
            "WINDOW w1 AS (PARTITION BY t1.col0 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(t1.col0), orders=(t1.col5 ASC), "
            "range=(t1.col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=, "
            "left_keys=(t1.col2), "
            "right_keys=(t2.col2), index_keys=)\n"
            "        DATA_PROVIDER(table=t2)\n"
            "    +-JOIN(type=LastJoin, right_sort=(ASC), condition=, "
            "left_keys=(), "
            "right_keys=(), index_keys=(t2.col2))\n"
            "        DATA_PROVIDER(type=Partition, table=t3, index=index2_t3)\n"
            "    DATA_PROVIDER(table=t1)"),
        // 6 window partition keys resolved from secondary table.
        // Join optimized doesn't work
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col2 = t2.col2 last "
            "join t3 order by t3.col5 on "
            "t2.col2=t3.col2 "
            "WINDOW w1 AS (PARTITION BY t3.col0 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(t3.col0), orders=(t1.col5 ASC), "
            "range=(t1.col5, 3 PRECEDING, 0 CURRENT))\n"
            "    JOIN(type=LastJoin, right_sort=(ASC), condition=, "
            "left_keys=(), right_keys=(), "
            "index_keys=(t2.col2))\n"
            "      JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=, "
            "left_keys=(t1.col2), "
            "right_keys=(t2.col2), index_keys=)\n"
            "        DATA_PROVIDER(table=t1)\n"
            "        DATA_PROVIDER(table=t2)\n"
            "      DATA_PROVIDER(type=Partition, table=t3, index=index2_t3)")));

INSTANTIATE_TEST_SUITE_P(
    WindowUnionOptimized, TransformPassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-UNION(partition_keys=(col1), orders=(col5 ASC), "
            "range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "        RENAME(name=t1)\n"
            "          DATA_PROVIDER(table=t3)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        // 1
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1,col2 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-UNION(partition_keys=(col1), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "        RENAME(name=t1)\n"
            "          DATA_PROVIDER(type=Partition, table=t3, index=index2_t3)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)")));

INSTANTIATE_TEST_SUITE_P(
    SimpleProjectOptimized, TransformPassOptimizedTest,
    testing::Values(
        // SIMPLE SELECT COLUMNS
        std::make_pair("SELECT COL0, COL1, COL2, COL6 FROM t1 LIMIT 10;",
                       "LIMIT(limit=10)\n"
                       "  SIMPLE_PROJECT(sources=(col0, col1, col2, col6))\n"
                       "    DATA_PROVIDER(table=t1)"),
        // SIMPLE SELECT COLUMNS and CONST VALUES
        std::make_pair(
            "SELECT c0 as col0, c1 as col1, c2 as col2, 0.0f as col3, 0.0 as "
            "col4, c5 as col5, c6 as col6 from tb LIMIT 10;\n",
            "LIMIT(limit=10)\n"
            "  SIMPLE_PROJECT(sources=(c0 -> col0, c1 -> col1, c2 -> col2, "
            "0.000000 -> col3, 0.000000 -> col4, c5 -> col5, c6 -> col6))\n"
            "    DATA_PROVIDER(table=tb)"),
        // SIMPLE SELECT FROM SIMPLE SELECT FROM SIMPLE SELECT
        std::make_pair(
            "SELECT x, y , z, 1, 1.0 from (select col0 as x, col1 as y, col2 "
            "as z from (select c0 as col0, c1 as col1, c2 "
            "as col2, 0.0f as col3, 0.0 as "
            "col4, c5 as col5, c6 as col6 from tb)) LIMIT 10;\n",
            "LIMIT(limit=10)\n"
            "  SIMPLE_PROJECT(sources=(c0 -> x, c1 -> y, c2 -> z, 1, "
            "1.000000))\n"
            "    DATA_PROVIDER(table=tb)"),
        // SIMPLE SELECT COLUMNS and CONST VALUES
        std::make_pair(
            "SELECT col3+col4 as col01 from (select c0 as col0, c1 as col1, c2 "
            "as col2, 0.0f as col3, 0.0 as "
            "col4, c5 as col5, c6 as col6 from tb) LIMIT 10;\n",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=TableProject, limit=10)\n"
            "    SIMPLE_PROJECT(sources=(c0 -> col0, c1 -> col1, c2 -> col2, "
            "0.000000 -> col3, 0.000000 -> col4, c5 -> col5, c6 -> col6))\n"
            "      DATA_PROVIDER(table=tb)"),
        // SIMPLE SELECT COLUMNS and CONST VALUES
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION (select c0 as col0, c1 as col1, c2 as "
            "col2, 0.0f as col3, 0.0 as col4, c5 as col5, c6 as col6 from tb) "
            "PARTITION BY col1,col2 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-UNION(partition_keys=(col1,col2), orders=(col5 ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "        RENAME(name=t1)\n"
            "          SIMPLE_PROJECT(sources=(c0 -> col0, c1 -> col1, c2 -> "
            "col2, 0.000000 -> col3, 0.000000 -> col4, c5 -> col5, c6 -> "
            "col6))\n"
            "            DATA_PROVIDER(table=tb)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        // SIMPLE SELECT COLUMNS and CONST VALUES
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION (select c0 as col0, c1 as col1, c2 as "
            "col2, 0.0f as col3, 0.0 as col4, c5 as col5, c6 as col6 from tc) "
            "PARTITION BY col1,col2 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "    +-UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT))\n"
            "        RENAME(name=t1)\n"
            "          SIMPLE_PROJECT(sources=(c0 -> col0, c1 -> col1, c2 -> "
            "col2, 0.000000 -> col3, 0.000000 -> col4, c5 -> col5, c6 -> "
            "col6))\n"
            "            DATA_PROVIDER(type=Partition, table=tc, "
            "index=index12_tc)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)")));
TEST_P(TransformPassOptimizedTest, PassOptimizedTest) {
    hybridse::type::TableDef table_def;
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
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    {
        hybridse::type::TableDef table_def2;
        BuildTableDef(table_def2);
        table_def2.set_name("t2");
        ::hybridse::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
        AddTable(db, table_def2);
    }
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_name("t3");
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index2_t3");
        index->add_first_keys("col2");
        index->set_second_key("col5");
        AddTable(db, table_def);
    }

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
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index12_tc");
            index->add_first_keys("c1");
            index->add_first_keys("c2");
            index->set_second_key("c5");
        }
        AddTable(db, table_def);
    }
    auto catalog = BuildSimpleCatalog(db);
    auto in_out = GetParam();
    PhysicalPlanCheck(catalog, in_out.first, in_out.second);
}

class SimpleCataLogTransformPassOptimizedTest
    : public ::testing::TestWithParam<std::pair<std::string, std::string>> {
 public:
    SimpleCataLogTransformPassOptimizedTest() {}
    ~SimpleCataLogTransformPassOptimizedTest() {}
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(SimpleCataLogTransformPassOptimizedTest);
// LeftJoinPass dosen't work in simple catalog
INSTANTIATE_TEST_SUITE_P(
    JoinFilterOptimized, SimpleCataLogTransformPassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col2 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=t2.col5 "
            ">= t1.col5, "
            "left_keys=(t1.col1), right_keys=(t2.col2), index_keys=)\n"
            "    DATA_PROVIDER(table=t1)\n"
            "    DATA_PROVIDER(table=t2)"),
        // 1
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col1 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=t2.col5 "
            ">= t1.col5, "
            "left_keys=(t1.col1), right_keys=(t2.col1), index_keys=)\n"
            "    DATA_PROVIDER(table=t1)\n"
            "    DATA_PROVIDER(table=t2)"),
        // 2
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col1 "
            "WINDOW w1 AS (PARTITION BY t1.col0 ORDER BY t1.col5 ROWS_RANGE "
            "BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(t1.col0), orders=(t1.col5 ASC), "
            "range=(t1.col5, 3 PRECEDING, 0 CURRENT))\n"
            "    JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=, "
            "left_keys=(t1.col1), "
            "right_keys=(t2.col1), index_keys=)\n"
            "      DATA_PROVIDER(table=t1)\n"
            "      DATA_PROVIDER(table=t2)")));

INSTANTIATE_TEST_SUITE_P(
    WhereOptimized, SimpleCataLogTransformPassOptimizedTest,
    testing::Values(
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10;",
                       "PROJECT(type=Aggregation)\n"
                                   "  FILTER_BY(condition=, left_keys=(10), right_keys=(col1), index_keys=)\n"
                                   "    DATA_PROVIDER(table=t1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 = 20;",
                       "PROJECT(type=Aggregation)\n"
                                   "  FILTER_BY(condition=, left_keys=(10,20), right_keys=(col1,col2), index_keys=)\n"
                                   "    DATA_PROVIDER(table=t1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col2 < 20;",
                       "PROJECT(type=Aggregation)\n"
                                   "  FILTER_BY(condition=col2 < 20, left_keys=(10), right_keys=(col1), index_keys=)\n"
                                   "    DATA_PROVIDER(table=t1)"),
        std::make_pair("SELECT sum(col1) as col1sum FROM t1 where col1 = 10 and col0 = \"test\";",
                       "PROJECT(type=Aggregation)\n"
                                   "  FILTER_BY(condition=, left_keys=(10,test), right_keys=(col1,col0), index_keys=)\n"
                                   "    DATA_PROVIDER(table=t1)")));
TEST_P(SimpleCataLogTransformPassOptimizedTest, PassOptimizedTest) {
    // Check for work with simple catalog
    auto simple_catalog = std::make_shared<vm::SimpleCatalog>();
    hybridse::type::Database db;
    db.set_name("db");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_name("t1");
        ::hybridse::type::TableDef* p_table = db.add_tables();
        *p_table = table_def;
    }
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_name("t2");
        ::hybridse::type::TableDef* p_table = db.add_tables();
        *p_table = table_def;
    }
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_name("t3");
        ::hybridse::type::TableDef* p_table = db.add_tables();
        *p_table = table_def;
    }
    simple_catalog->AddDatabase(db);
    auto in_out = GetParam();
    PhysicalPlanCheck(simple_catalog, in_out.first, in_out.second);
}

TEST_F(TransformTest, DeleteStmt) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);

    PhysicalPlanCheck(catalog, "delete job 12", R"r(DELETE(target=JOB, job_id=12))r");
}

TEST_F(TransformTest, AlterTableStmt) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);

    PhysicalPlanCheck(catalog, "alter table t1 add path 'foo'", R"d(ALTER_TABLE(db.t1))d");
}

TEST_F(TransformTest, InsertStmt) {
    hybridse::type::Database db;
    db.set_name("db");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);

    PhysicalPlanCheck(catalog, "insert into t1 values ('abc', 1, 1, 1.0, 2.0, 100, 'val');", R"r(INSERT(db=, table=t1, is_all=true))r");
}

TEST_F(TransformTest, GetDatabaseName) {
    hybridse::type::Database db;
    db.set_name("db1");

    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_catalog("db1");
    AddTable(db, table_def);

    auto catalog = BuildSimpleCatalog(db);

    auto sql = "select col0 from t1;";

    ::hybridse::node::NodeManager manager;
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status status;
    plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &manager, status);
    ASSERT_EQ(common::kOk, status.code);

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    BatchModeTransformer transform(&manager, "db1", catalog, nullptr, m.get(), lib);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;

    status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    ASSERT_TRUE(status.isOK()) << status;

    ASSERT_EQ(1, physical_plan->GetProducerCnt());
    ASSERT_TRUE(physical_plan->GetProducer(0)->GetOpType() == kPhysicalOpDataProvider);
    auto provider = dynamic_cast<const PhysicalTableProviderNode*>(physical_plan->GetProducer(0));
    ASSERT_TRUE(provider != nullptr);
    ASSERT_STREQ("db1", provider->GetDb().c_str());
}

}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
