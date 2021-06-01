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
#include "parser/parser.h"
#include "passes/physical/condition_optimized.h"
#include "plan/planner.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/simple_catalog.h"
#include "vm/sql_compiler.h"
#include "testing/test_base.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace hybridse {
namespace vm {

using hybridse::passes::ConditionOptimized;
using hybridse::passes::ExprPair;
using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"physical-plan-unsupport",
                                        "logical-plan-unsupport",
                                        "parser-unsupport"});

class TransformTest : public ::testing::TestWithParam<SqlCase> {
 public:
    TransformTest() {}
    ~TransformTest() {}
    node::NodeManager manager;
};
INSTANTIATE_TEST_CASE_P(
    SqlSimpleQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));
INSTANTIATE_TEST_CASE_P(
    SqlReanmeQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/rename_query.yaml", FILTERS)));
INSTANTIATE_TEST_CASE_P(
    SqlWindowQueryParse, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlWherePlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlGroupPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/group_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlHavingPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/having_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlOrderPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/order_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlDistinctPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/distinct_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryPlan, TransformTest,
    testing::ValuesIn(sqlcase::InitCases("cases/plan/sub_query.yaml", FILTERS)));

TEST_P(TransformTest, transform_physical_plan) {
    std::string sqlstr = GetParam().sql_str();
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
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        ::hybridse::plan::SimplePlanner planner(&manager);
        ::hybridse::parser::HybridSeParser parser;
        ::hybridse::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, &manager, base_status);
        LOG(INFO) << *(parser_trees[0]) << std::endl;
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            LOG(INFO) << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
        }

        ASSERT_EQ(0, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    BatchModeTransformer transform(&manager, "db", catalog, m.get(), lib);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(
        transform.TransformPhysicalPlan(plan_trees, &physical_plan).isOK());
    std::ostringstream oss;

    physical_plan->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << "\n";
    //    m->print(::llvm::errs(), NULL);
}

TEST_P(TransformTest, transform_physical_plan_enable_window_paralled) {
    std::string sqlstr = GetParam().sql_str();
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
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        ::hybridse::plan::SimplePlanner planner(&manager);
        ::hybridse::parser::HybridSeParser parser;
        ::hybridse::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, &manager, base_status);
        LOG(INFO) << *(parser_trees[0]) << std::endl;
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            LOG(INFO) << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
        }

        ASSERT_EQ(0, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    BatchModeTransformer transform(&manager, "db", catalog, m.get(), lib, false,
                                   false, false, true);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(
        transform.TransformPhysicalPlan(plan_trees, &physical_plan).isOK());
    std::ostringstream oss;

    physical_plan->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << "\n";
    //    m->print(::llvm::errs(), NULL);
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
        ::hybridse::plan::SimplePlanner planner(&manager);
        ::hybridse::parser::HybridSeParser parser;
        ::hybridse::node::NodePointVector parser_trees;
        parser.parse(sql, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::cout << base_status.str();
            LOG(INFO) << *(plan_trees[0]) << std::endl;
        } else {
            std::cout << base_status.str();
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    base::Status status;
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    BatchModeTransformer transform(&manager, "db", catalog, m.get(), lib);

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
        left_ctx.BuildTrivial({&t1});
    }
    {
        BuildTableT2Def(t2);
        right_ctx.BuildTrivial({&t2});
    }

    std::vector<std::pair<std::string, std::pair<std::string, std::string>>>
        sql_exp;

    sql_exp.push_back(std::make_pair("select t1.col1=t2.col1 from t1,t2;",
                                     std::make_pair("t1.col1", "t2.col1")));

    sql_exp.push_back(std::make_pair("select t2.col1=t1.col1 from t1,t2;",
                                     std::make_pair("t1.col1", "t2.col1")));

    // Fail Extract Equal Pair
    sql_exp.push_back(std::make_pair(
        "select t2.col1+t1.col1=t2.col3 from t1,t2;", std::make_pair("", "")));
    sql_exp.push_back(std::make_pair("select t1.col1=t1.col2 from t1,t2;",
                                     std::make_pair("", "")));
    sql_exp.push_back(std::make_pair("select t1.col1=t3.col2 from t1,t2;",
                                     std::make_pair("", "")));
    sql_exp.push_back(std::make_pair("select t2.col1>t1.col1 from t1,t2;",
                                     std::make_pair("", "")));

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

TEST_P(TransformTest, window_merge_opt_test) {
    std::string sqlstr = GetParam().sql_str();
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
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        ::hybridse::plan::SimplePlanner planner(&manager);
        ::hybridse::parser::HybridSeParser parser;
        ::hybridse::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, &manager, base_status);
        LOG(INFO) << *(parser_trees[0]) << std::endl;
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
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
    BatchModeTransformer transform(&manager, "db", catalog, m.get(), lib);
    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(
        transform.TransformPhysicalPlan(plan_trees, &physical_plan).isOK());
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
INSTANTIATE_TEST_CASE_P(KeyGen, KeyGenTest,
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
    BatchModeTransformer transformer(&nm, "db", catalog, m.get(), lib);

    auto groups = nm.MakeExprList();
    ExtractExprListFromSimpleSql(&nm, GetParam(), groups);

    PhysicalTableProviderNode* table_provider;
    transformer.GetPlanContext()->CreateOp<PhysicalTableProviderNode>(
        &table_provider, catalog->GetTable("db", "t1"));

    Key group(groups);

    ASSERT_TRUE(
        transformer.GenKey(&group, table_provider->schemas_ctx()).isOK());
    m->print(::llvm::errs(), NULL);
    ASSERT_FALSE(group.fn_info().fn_name().empty());
}

class FilterGenTest : public ::testing::TestWithParam<std::string> {
 public:
    FilterGenTest() {}
    ~FilterGenTest() {}
    node::NodeManager nm;
};
INSTANTIATE_TEST_CASE_P(FilterGen, FilterGenTest,
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
    BatchModeTransformer transformer(&nm, "db", catalog, m.get(), lib);

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

INSTANTIATE_TEST_CASE_P(
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
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
            "PROJECT(type=GroupAggregation, group_keys=(col1,col2,col3))\n"
            "  GROUP_BY(group_keys=(col3))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
            "PROJECT(type=GroupAggregation, group_keys=(col3,col2,col1))\n"
            "  GROUP_BY(group_keys=(col3))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM (select c1 as col1, c2 as col2 , "
            "c3 as col3 from tc) group by col3, col2, col1;",
            "PROJECT(type=GroupAggregation, group_keys=(col3,col2,col1))\n"
            "  GROUP_BY(group_keys=(col3))\n"
            "    SIMPLE_PROJECT(sources=(c1 -> col1, c2 -> col2, c3 -> col3))\n"
            "      DATA_PROVIDER(type=Partition, table=tc, "
            "index=index12_tc)")));

INSTANTIATE_TEST_CASE_P(
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(col5, -3, "
            "0))\n"
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, "
            "range=(col5, -3, 0))\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair("SELECT "
                       "col1, "
                       "sum(col3) OVER w1 as w1_col3_sum, "
                       "sum(col2) OVER w1 as w1_col2_sum "
                       "FROM t1 WINDOW w1 AS (PARTITION BY col3 ORDER BY col5 "
                       "ROWS_RANGE BETWEEN 3 "
                       "PRECEDING AND CURRENT ROW) limit 10;",
                       "LIMIT(limit=10, optimized)\n"
                       "  PROJECT(type=WindowAggregation, limit=10)\n"
                       "    +-WINDOW(partition_keys=(col3), orders=(col5) ASC, "
                       "range=(col5, "
                       "-3, 0))\n"
                       "    DATA_PROVIDER(table=t1)")));

INSTANTIATE_TEST_CASE_P(
    JoinFilterOptimized, TransformPassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col2 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=t2.col5 "
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
            "  JOIN(type=LastJoin, right_sort=() ASC, condition=t2.col5 >= "
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
            "    +-WINDOW(partition_keys=(t1.col0), orders=(t1.col5) ASC, "
            "range=(t1.col5, -3, 0))\n"
            "    +-JOIN(type=LastJoin, right_sort=() ASC, condition=, "
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(t1.col5, "
            "-3, 0))\n"
            "    +-JOIN(type=LastJoin, right_sort=() ASC, condition=, "
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(t1.col5, "
            "-3, 0))\n"
            "    +-JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=, "
            "left_keys=(t1.col0), "
            "right_keys=(t2.col0), index_keys=)\n"
            "        DATA_PROVIDER(table=t2)\n"
            "    +-JOIN(type=LastJoin, right_sort=(t3.col5) ASC, condition=, "
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
            "    +-WINDOW(partition_keys=(t1.col0), orders=(t1.col5) ASC, "
            "range=(t1.col5, -3, 0))\n"
            "    +-JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=, "
            "left_keys=(t1.col2), "
            "right_keys=(t2.col2), index_keys=)\n"
            "        DATA_PROVIDER(table=t2)\n"
            "    +-JOIN(type=LastJoin, right_sort=() ASC, condition=, "
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
            "    +-WINDOW(partition_keys=(t3.col0), orders=(t1.col5) ASC, "
            "range=(t1.col5, -3, 0))\n"
            "    JOIN(type=LastJoin, right_sort=() ASC, condition=, "
            "left_keys=(), right_keys=(), "
            "index_keys=(t2.col2))\n"
            "      JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=, "
            "left_keys=(t1.col2), "
            "right_keys=(t2.col2), index_keys=)\n"
            "        DATA_PROVIDER(table=t1)\n"
            "        DATA_PROVIDER(table=t2)\n"
            "      DATA_PROVIDER(type=Partition, table=t3, index=index2_t3)")));

INSTANTIATE_TEST_CASE_P(
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(col5, -3, "
            "0))\n"
            "    +-UNION(partition_keys=(col1), orders=(col5) ASC, "
            "range=(col5, -3, 0))\n"
            "        DATA_PROVIDER(table=t3)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        // 1
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1,col2 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=WindowAggregation, limit=10)\n"
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(col5, -3, "
            "0))\n"
            "    +-UNION(partition_keys=(col1), orders=() ASC, range=(col5, "
            "-3, 0))\n"
            "        DATA_PROVIDER(type=Partition, table=t3, index=index2_t3)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)")));

INSTANTIATE_TEST_CASE_P(
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(col5, -3, "
            "0))\n"
            "    +-UNION(partition_keys=(col1,col2), orders=(col5) ASC, "
            "range=(col5, -3, 0))\n"
            "        SIMPLE_PROJECT(sources=(c0 -> col0, c1 -> col1, c2 -> "
            "col2, 0.000000 -> col3, 0.000000 -> col4, c5 -> col5, c6 -> "
            "col6))\n"
            "          DATA_PROVIDER(table=tb)\n"
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
            "    +-WINDOW(partition_keys=(), orders=() ASC, range=(col5, -3, "
            "0))\n"
            "    +-UNION(partition_keys=(), orders=() ASC, range=(col5, -3, "
            "0))\n"
            "        SIMPLE_PROJECT(sources=(c0 -> col0, c1 -> col1, c2 -> "
            "col2, 0.000000 -> col3, 0.000000 -> col4, c5 -> col5, c6 -> "
            "col6))\n"
            "          DATA_PROVIDER(type=Partition, table=tc, "
            "index=index12_tc)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)")));
TEST_P(TransformPassOptimizedTest, pass_optimized_test) {
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

// LeftJoinPass dosen't work in simple catalog
INSTANTIATE_TEST_CASE_P(
    JoinFilterOptimized, SimpleCataLogTransformPassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col2 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=t2.col5 "
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
            "  JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=t2.col5 "
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
            "    +-WINDOW(partition_keys=(t1.col0), orders=(t1.col5) ASC, "
            "range=(t1.col5, -3, 0))\n"
            "    JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=, "
            "left_keys=(t1.col1), "
            "right_keys=(t2.col1), index_keys=)\n"
            "      DATA_PROVIDER(table=t1)\n"
            "      DATA_PROVIDER(table=t2)")));
TEST_P(SimpleCataLogTransformPassOptimizedTest, pass_optimized_test) {
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
}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
