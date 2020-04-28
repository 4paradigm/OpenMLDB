/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "vm/transform.h"
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/status.h"
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
#include "plan/planner.h"
#include "udf/udf.h"
#include "vm/test_base.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {

using fesql::sqlcase::SQLCase;
std::vector<SQLCase> InitCases(std::string yaml_path);
void InitCases(std::string yaml_path, std::vector<SQLCase>& cases);  // NOLINT

void InitCases(std::string yaml_path, std::vector<SQLCase>& cases) {  // NOLINT
    if (!SQLCase::CreateSQLCasesFromYaml(
            fesql::sqlcase::FindFesqlDirPath() + "/" + yaml_path, cases,
            std::vector<std::string>({"physical-unsupport", "plan-unsupport",
                                      "parser-unsupport"}))) {
        FAIL();
    }
}

std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}
class TransformTest : public ::testing::TestWithParam<SQLCase> {
 public:
    TransformTest() {}
    ~TransformTest() {}
};
INSTANTIATE_TEST_CASE_P(
    SqlSimpleQueryParse, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    SqlWindowQueryParse, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlWherePlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/where_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlGroupPlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/group_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlHavingPlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/having_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlOrderPlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/order_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlDistinctPlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/distinct_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryPlan, TransformTest,
    testing::ValuesIn(InitCases("cases/plan/sub_query.yaml")));

TEST_P(TransformTest, transform_physical_plan) {
    std::string sqlstr = GetParam().sql_str();
    std::cout << sqlstr << std::endl;

    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
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
        new ::fesql::storage::Table(1, 1, table_def2));
    std::shared_ptr<::fesql::storage::Table> table3(
        new ::fesql::storage::Table(1, 1, table_def3));
    std::shared_ptr<::fesql::storage::Table> table4(
        new ::fesql::storage::Table(1, 1, table_def4));
    std::shared_ptr<::fesql::storage::Table> table5(
        new ::fesql::storage::Table(1, 1, table_def5));
    std::shared_ptr<::fesql::storage::Table> table6(
        new ::fesql::storage::Table(1, 1, table_def6));

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

    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, &manager, base_status);
        std::cout << *(parser_trees[0]) << std::endl;
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::cout << *(plan_trees[0]) << std::endl;
        } else {
            std::cout << base_status.msg;
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    BatchModeTransformer transform(&manager, "db", catalog, m.get());
    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(transform.TransformPhysicalPlan(plan_trees, &physical_plan,
                                                base_status));
    physical_plan->Print(std::cout, "");
    m->print(::llvm::errs(), NULL);
}

void PhysicalPlanCheck(const std::shared_ptr<tablet::TabletCatalog>& catalog,
                       std::string sql, std::string exp) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");

    boost::to_lower(sql);
    std::cout << sql << std::endl;

    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sql, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::cout << base_status.msg;
            std::cout << *(plan_trees[0]) << std::endl;
        } else {
            std::cout << base_status.msg;
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    BatchModeTransformer transform(&manager, "db", catalog, m.get());
    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;

    bool ok = transform.TransformPhysicalPlan(plan_trees, &physical_plan,
                                              base_status);
    std::cout << base_status.msg << std::endl;
    ASSERT_TRUE(ok);

    std::ostringstream oos;
    physical_plan->Print(oos, "");
    std::cout << oos.str() << std::endl;

    std::ostringstream ss;
    PrintSchema(ss, physical_plan->output_schema_);
    std::cout << "schema:\n" << ss.str() << std::endl;

    ASSERT_EQ(oos.str(), exp);
}
TEST_F(TransformTest, pass_group_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col1;",
        "PROJECT(type=GroupAggregation, groups=(col1))\n"
        "  DATA_PROVIDER(type=IndexScan, table=t1, index=index1)"));
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
        "PROJECT(type=GroupAggregation, groups=(col1,col2))\n"
        "  DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
        "PROJECT(type=GroupAggregation, groups=(col1,col2,col3))\n"
        "  GROUP_BY(groups=(col3))\n"
        "    DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
        "PROJECT(type=GroupAggregation, groups=(col3,col2,col1))\n"
        "  GROUP_BY(groups=(col3))\n"
        "    DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    auto catalog = BuildCommonCatalog(table_def, table);

    for (auto in_out : in_outs) {
        PhysicalPlanCheck(catalog, in_out.first, in_out.second);
    }
}

TEST_F(TransformTest, pass_sort_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=WindowAggregation, groups=(col1), orders=(col5) ASC, "
        "start=-3, end=0, limit=10)\n"
        "    GROUP_AND_SORT_BY(groups=(), orders=() ASC)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index1)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2, col1 ORDER BY col5 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=WindowAggregation, groups=(col2,col1), orders=(col5) "
        "ASC, start=-3, end=0, limit=10)\n"
        "    GROUP_AND_SORT_BY(groups=(), orders=() ASC)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col3 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=WindowAggregation, groups=(col3), orders=(col5) ASC, "
        "start=-3, end=0, limit=10)\n"
        "    GROUP_AND_SORT_BY(groups=(col3), orders=(col5) ASC)\n"
        "      DATA_PROVIDER(table=t1)"));

    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    auto catalog = BuildCommonCatalog(table_def, table);

    for (auto in_out : in_outs) {
        PhysicalPlanCheck(catalog, in_out.first, in_out.second);
    }
}

TEST_F(TransformTest, pass_join_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "t2.col1, "
        "sum(t1.col2) OVER w1 as w1_col2_sum "
        "FROM t1 last join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=WindowAggregation, groups=(t1.col1), orders=(t1.col5) "
        "ASC, start=-3, end=0, limit=10)\n"
        "    JOIN(type=LastJoin, condition=, key=(t1.col1))\n"
        "      GROUP_AND_SORT_BY(groups=(), orders=() ASC)\n"
        "        DATA_PROVIDER(type=IndexScan, table=t1, index=index1)\n"
        "      GROUP_BY(groups=(t2.col1))\n"
        "        DATA_PROVIDER(table=t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "t2.col1, "
        "sum(t1.col2) OVER w1 as w1_col2_sum "
        "FROM t1 last join t2 on t1.col1 = t2.col1 AND t1.col3 > t2.col3 "
        "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=WindowAggregation, groups=(t1.col1), orders=(t1.col5) "
        "ASC, start=-3, end=0, limit=10)\n"
        "    JOIN(type=LastJoin, condition=t1.col3 > t2.col3, key=(t1.col1))\n"
        "      GROUP_AND_SORT_BY(groups=(), orders=() ASC)\n"
        "        DATA_PROVIDER(type=IndexScan, table=t1, index=index1)\n"
        "      GROUP_BY(groups=(t2.col1))\n"
        "        DATA_PROVIDER(table=t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "t2.col1, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "sum(t1.col2) OVER w1 as w1_col2_sum "
        "FROM t1 last join t2 on t1.col2 = t2.col2 "
        "WINDOW w1 AS (PARTITION BY t1.col1, t1.col2 ORDER BY t1.col5 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=WindowAggregation, groups=(t1.col1,t1.col2), "
        "orders=(t1.col5) ASC, start=-3, end=0, limit=10)\n"
        "    JOIN(type=LastJoin, condition=, key=(t1.col2))\n"
        "      GROUP_AND_SORT_BY(groups=(), orders=() ASC)\n"
        "        DATA_PROVIDER(type=IndexScan, table=t1, index=index12)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t2, index=index_col2)"));
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    auto catalog = BuildCommonCatalog(table_def, table);
    {
        fesql::type::TableDef table_def2;
        BuildTableDef(table_def2);
        table_def2.set_name("t2");
        ::fesql::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index_col2");
        index->add_first_keys("col2");
        index->set_second_key("col5");
        std::shared_ptr<::fesql::storage::Table> table2(
            new ::fesql::storage::Table(1, 1, table_def2));
        AddTable(catalog, table_def2, table2);
    }

    for (auto in_out : in_outs) {
        PhysicalPlanCheck(catalog, in_out.first, in_out.second);
    }
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
        ::fesql::node::NodeManager manager;
        node::ExprNode* condition;
        boost::to_lower(sql);
        ExtractExprFromSimpleSQL(&manager, sql, &condition);
        LOG(INFO) << "TEST condition [" << i
                  << "]: " << node::ExprString(condition);
        node::ExprListNode and_condition_list;
        FilterConditionOptimized::TransfromAndConditionList(
            condition, &and_condition_list);
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
    std::vector<std::pair<const std::string, const vm::Schema*>> name_schemas;
    type::TableDef t1;
    type::TableDef t2;
    {
        BuildTableDef(t1);
        name_schemas.push_back(std::make_pair("t1", &t1.columns()));
    }
    {
        BuildTableT2Def(t2);
        name_schemas.push_back(std::make_pair("t2", &t2.columns()));
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
        ::fesql::node::NodeManager manager;
        node::ExprNode* condition;
        boost::to_lower(sql);
        ExtractExprFromSimpleSQL(&manager, sql, &condition);
        LOG(INFO) << "TEST condition [" << i
                  << "]: " << node::ExprString(condition);
        node::ExprListNode mock_condition_list;
        mock_condition_list.AddChild(condition);

        node::ExprListNode out_condition_list;
        std::vector<vm::ExprPair> mock_expr_pairs;

        FilterConditionOptimized::TransformEqualExprPair(
            name_schemas, &mock_condition_list, &out_condition_list,
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
}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
