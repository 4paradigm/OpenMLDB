/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform_request_mode_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
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
#include "plan/planner.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"
#include "vm/test_base.h"
#include "vm/transform.h"

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
            fesql::sqlcase::FindFesqlDirPath(), yaml_path, cases,
            std::vector<std::string>({"physical-plan-unsupport",
                                      "plan-unsupport", "parser-unsupport",
                                      "request-unsupport"}))) {
        FAIL();
    }
}

std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}
class TransformRequestModeTest : public ::testing::TestWithParam<SQLCase> {
 public:
    TransformRequestModeTest() {}
    ~TransformRequestModeTest() {}
    ::fesql::node::NodeManager manager;
};

void PhysicalPlanCheck(const std::shared_ptr<tablet::TabletCatalog>& catalog,
                       std::string sql, std::string exp) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");

    boost::to_lower(sql);
    std::cout << sql << std::endl;

    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager, false);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sql, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::ostringstream oss;
            oss << *(plan_trees[0]);
            LOG(INFO) << "logical plan:\n" << oss.str();
        } else {
            std::cout << base_status.str();
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::fesql::udf::DefaultUDFLibrary::get();
    RequestModeTransformer transform(&manager, "db", catalog, m.get(), lib, {},
                                     false, false, false, false);

    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(
        transform.TransformPhysicalPlan(plan_trees, &physical_plan).isOK());
    //    m->print(::llvm::errs(), NULL);
    std::ostringstream oss;
    physical_plan->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;
    std::ostringstream ss;
    PrintSchema(ss, *physical_plan->GetOutputSchema());
    LOG(INFO) << "schema:\n" << ss.str() << std::endl;
    ASSERT_EQ(oss.str(), exp);
}
INSTANTIATE_TEST_CASE_P(
    SqlSimpleQueryParse, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    SqlWindowQueryParse, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/window_query.yaml")));

// INSTANTIATE_TEST_CASE_P(
//    SqlWherePlan, TransformRequestModeTest,
//    testing::ValuesIn(InitCases("cases/plan/where_query.yaml")));

// INSTANTIATE_TEST_CASE_P(
//   SqlGroupPlan, TransformRequestModeTest,
//    testing::ValuesIn(InitCases("cases/plan/group_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlHavingPlan, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/having_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlOrderPlan, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/order_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlDistinctPlan, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/distinct_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryPlan, TransformRequestModeTest,
    testing::ValuesIn(InitCases("cases/plan/sub_query.yaml")));

void CheckTransformPhysicalPlan(const SQLCase& sql_case,
                                bool is_cluster_optimized,
                                node::NodeManager* nm) {
    std::string sqlstr = sql_case.sql_str();
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
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(nm, false);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, nm, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::ostringstream oss;
            oss << *(plan_trees[0]) << std::endl;
            std::cout << "logical plan:\n" << oss.str() << std::endl;
        } else {
            std::cout << base_status.str();
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::fesql::udf::DefaultUDFLibrary::get();
    RequestModeTransformer transform(nm, "db", catalog, m.get(), lib, {}, false,
                                     false, false, false);
    PhysicalOpNode* physical_plan = nullptr;
    Status status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    ASSERT_TRUE(status.isOK()) << "physical plan transform fail: " << status;
    std::ostringstream oss;
    physical_plan->Print(oss, "");
    std::cout << "physical plan:\n" << sqlstr << "\n" << oss.str() << std::endl;
    std::ostringstream ss;
    PrintSchema(ss, *physical_plan->GetOutputSchema());
    std::cout << "schema:\n" << ss.str() << std::endl;
    //    m->print(::llvm::errs(), NULL);
}

TEST_P(TransformRequestModeTest, transform_physical_plan) {
    CheckTransformPhysicalPlan(GetParam(), false, &manager);
}
TEST_P(TransformRequestModeTest, cluster_transform_physical_plan) {
    CheckTransformPhysicalPlan(GetParam(), true, &manager);
}
class TransformRequestModePassOptimizedTest
    : public ::testing::TestWithParam<std::pair<std::string, std::string>> {
 public:
    TransformRequestModePassOptimizedTest() {}
    ~TransformRequestModePassOptimizedTest() {}
};

INSTANTIATE_TEST_CASE_P(
    SortOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(
        std::make_pair(
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
            "BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=() ASC, "
            "range=(col5, -3, 0), index_keys=(col1))\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair(
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2, col1 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=() ASC, "
            "range=(col5, -3, 0), index_keys=(col2,col1))\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT "
            "col1+col2 as col12, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "*, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col3 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3"
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(col3), orders=(col5) ASC, "
            "range=(col5, -3, 0), index_keys=)\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(table=t1)")));
INSTANTIATE_TEST_CASE_P(
    GroupOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1;",
            "PROJECT(type=Aggregation)\n"
            "  REQUEST_UNION(partition_keys=(), orders=, index_keys=(col1))\n"
            "    DATA_PROVIDER(request=t1)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
            "PROJECT(type=Aggregation)\n"
            "  REQUEST_UNION(partition_keys=(), orders=, "
            "index_keys=(col1,col2))\n"
            "    DATA_PROVIDER(request=t1)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
            "PROJECT(type=Aggregation)\n"
            "  REQUEST_UNION(partition_keys=(col3), orders=, "
            "index_keys=(col1,col2))\n"
            "    DATA_PROVIDER(request=t1)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
            "PROJECT(type=Aggregation)\n"
            "  REQUEST_UNION(partition_keys=(col3), orders=, "
            "index_keys=(col2,col1))\n"
            "    DATA_PROVIDER(request=t1)\n"
            "    DATA_PROVIDER(type=Partition, table=t1, index=index12)")));

INSTANTIATE_TEST_CASE_P(
    JoinFilterOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col2 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  REQUEST_JOIN(type=LastJoin, right_sort=(t2.col5) ASC, "
            "condition=t2.col5 >= t1.col5, "
            "left_keys=(t1.col1), right_keys=(t2.col2), index_keys=)\n"
            "    DATA_PROVIDER(request=t1)\n"
            "    DATA_PROVIDER(table=t2)"),
        std::make_pair(
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
            "t2 order by t2.col5 on "
            " t1.col1 = t2.col1 and t2.col5 >= t1.col5;",
            "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
            "  REQUEST_JOIN(type=LastJoin, right_sort=() ASC, "
            "condition=t2.col5 >= t1.col5, "
            "left_keys=(), right_keys=(), index_keys=(t1.col1))\n"
            "    DATA_PROVIDER(request=t1)\n"
            "    DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)"),
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
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    JOIN(type=LastJoin, right_sort=() ASC, condition=, "
            "left_keys=(), "
            "right_keys=(), index_keys=(t1.col1))\n"
            "      REQUEST_UNION(partition_keys=(), orders=() ASC, "
            "range=(t1.col5, -3, 0), index_keys=(t1.col1))\n"
            "        DATA_PROVIDER(request=t1)\n"
            "        DATA_PROVIDER(type=Partition, table=t1, index=index1)\n"
            "      DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)"),
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col2 = t2.col2 "
            "WINDOW w1 AS (PARTITION BY t1.col1, t1.col2 ORDER BY t1.col5 "
            "ROWS_RANGE "
            "BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    JOIN(type=LastJoin, right_sort=(t2.col5) ASC, condition=, "
            "left_keys=(t1.col2), "
            "right_keys=(t2.col2), index_keys=)\n"
            "      REQUEST_UNION(partition_keys=(), orders=() ASC, "
            "range=(t1.col5, -3, 0), index_keys=(t1.col1,t1.col2))\n"
            "        DATA_PROVIDER(request=t1)\n"
            "        DATA_PROVIDER(type=Partition, table=t1, index=index12)\n"
            "      DATA_PROVIDER(table=t2)"),
        std::make_pair(
            "SELECT "
            "t2.col1, "
            "sum(t1.col3) OVER w1 as w1_col3_sum, "
            "sum(t1.col2) OVER w1 as w1_col2_sum "
            "FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col1 "
            "WINDOW w1 AS (PARTITION BY t1.col1, t1.col2 ORDER BY t1.col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    JOIN(type=LastJoin, right_sort=() ASC, condition=, "
            "left_keys=(), "
            "right_keys=(), index_keys=(t1.col1))\n"
            "      REQUEST_UNION(partition_keys=(), orders=() ASC, "
            "range=(t1.col5, -3, 0), index_keys=(t1.col1,t1.col2))\n"
            "        DATA_PROVIDER(request=t1)\n"
            "        DATA_PROVIDER(type=Partition, table=t1, index=index12)\n"
            "      DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)")));

INSTANTIATE_TEST_CASE_P(
    RequestWindowUnionOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=() ASC, range=(col5, "
            "-3, 0), index_keys=(col1))\n"
            "      +-UNION(partition_keys=(col1), orders=(col5) ASC, "
            "range=(col5, -3, 0), index_keys=)\n"
            "          DATA_PROVIDER(table=t3)\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        // 1
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1,col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=() ASC, range=(col5, "
            "-3, 0), index_keys=(col1,col2))\n"
            "      +-UNION(partition_keys=(col1), orders=() ASC, range=(col5, "
            "-3, 0), index_keys=(col2))\n"
            "          DATA_PROVIDER(type=Partition, table=t3, "
            "index=index2_t3)\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=() ASC, "
            "range=(col5, -3, 0), index_keys=(col1))\n"
            "      +-UNION(partition_keys=(col1), orders=(col5) ASC, "
            "range=(col5, -3, 0), index_keys=)\n"
            "          DATA_PROVIDER(table=t3)\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, "
            "index=index1)")));
TEST_P(TransformRequestModePassOptimizedTest, pass_pass_optimized_test) {
    auto in_out = GetParam();
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
        index->set_name("index1_t2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
        std::shared_ptr<::fesql::storage::Table> table2(
            new ::fesql::storage::Table(1, 1, table_def2));
        AddTable(catalog, table_def2, table2);
    }

    {
        fesql::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_name("t3");
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index2_t3");
        index->add_first_keys("col2");
        index->set_second_key("col5");
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(3, 1, table_def));
        AddTable(catalog, table_def, table);
    }
    PhysicalPlanCheck(catalog, in_out.first, in_out.second);
}

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
