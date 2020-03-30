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
class TransformTest : public ::testing::TestWithParam<std::string> {
 public:
    TransformTest() {}
    ~TransformTest() {}
};

void BuildTableDef(::fesql::type::TableDef& table_def) {  // NOLINT
    table_def.set_name("t1");
    table_def.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col15");
    }
}

INSTANTIATE_TEST_CASE_P(
    SqlSimpleProjectPlanner, TransformTest,
    testing::Values(
        "SELECT COL1 as c1 FROM t1;", "SELECT t1.COL1 c1 FROM t1 limit 10;",
        "SELECT COL1 as c1, col2  FROM t1;",
        "SELECT COL1 + COL2 as col12 FROM t1;",
        "SELECT COL1 - COL2 as col12 FROM t1;",
        "SELECT COL1 * COL2 as col12 FROM t1;",
        "SELECT COL1 / COL2 as col12 FROM t1;",
        "SELECT COL1 % COL2 as col12 FROM t1;",
        "SELECT COL1 = COL2 as col12 FROM t1;",
        "SELECT COL1 == COL2 as col12 FROM t1;",
        "SELECT COL1 < COL2 as col12 FROM t1;",
        "SELECT COL1 > COL2 as col12 FROM t1;",
        "SELECT COL1 <= COL2 as col12 FROM t1;",
        "SELECT COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 && COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 and COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 || COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 or COL1 != COL2 as col12 FROM t1;",
        "SELECT !(COL1 >= COL2 or COL1 != COL2) as col12 FROM t1;",
        "SELECT col1-col2 as col1_2, *, col1+col2 as col12 FROM t1 limit 10;"
        "SELECT *, col1+col2 as col12 FROM t1 limit 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlWindowProjectPlanner, TransformTest,
    testing::Values(
        "SELECT COL1, COL2, `COL15`, AVG(COL3) OVER w, SUM(COL3) OVER w FROM "
        "t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `COL15` ROWS BETWEEN UNBOUNDED PRECEDING AND "
        "CURRENT ROW);",
        "SELECT COL1, SUM(col4) OVER w as w_amt_sum FROM t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `col15` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col15 ORDER BY `col15` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlWherePlan, TransformTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL1+COL2;",
        "SELECT COL1 FROM t1 where COL1;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 where COL1 > 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlLikePlan, TransformTest,
    testing::Values("SELECT COL1 FROM t1 where COL like \"%abc\";",
                    "SELECT COL1 FROM t1 where COL1 like '%123';",
                    "SELECT COL1 FROM t1 where COL not like \"%abc\";",
                    "SELECT COL1 FROM t1 where COL1 not like '%123';",
                    "SELECT COL1 FROM t1 where COL1 not like 10;",
                    "SELECT COL1 FROM t1 where COL1 like 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlInPlan, TransformTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL in (1, 2, 3, 4, 5);",
        "SELECT COL1 FROM t1 where COL1 in (\"abc\", \"xyz\", \"test\");",
        "SELECT COL1 FROM t1 where COL1 not in (1,2,3,4,5);"));

INSTANTIATE_TEST_CASE_P(
    SqlGroupPlan, TransformTest,
    testing::Values(
        "SELECT distinct sum(COL1) as col1sum FROM t1 where col2 > 10 group "
        "by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10;",
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
        "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
        "SELECT sum(COL1) FROM t1 group by COL1+COL2;",
        "SELECT sum(COL1) FROM t1 group by COL1;",
        "SELECT sum(COL1) FROM t1 group by COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT sum(COL1) FROM t1 group by COL1, COL2;",
        "SELECT sum(COL1) FROM t1 group by COL1;"));

INSTANTIATE_TEST_CASE_P(
    SqlHavingPlan, TransformTest,
    testing::Values(
        "SELECT COL1 FROM t1 having COL1+COL2;",
        "SELECT COL1 FROM t1 having COL1;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlOrderPlan, TransformTest,
    testing::Values("SELECT COL1 FROM t1 order by COL1 + COL2 - COL3;",
                    "SELECT COL1 FROM t1 order by COL1, COL2, COL3;",
                    "SELECT COL1 FROM t1 order by COL1, COL2;",
                    "SELECT COL1 FROM t1 order by COL1;"));

INSTANTIATE_TEST_CASE_P(
    SqlWhereGroupHavingOrderPlan, TransformTest,
    testing::Values(
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10;",
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1 limit 10;",
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 limit 10;",
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0;",
        "SELECT sum(COL1) as col1sum FROM t1 group by COL1, COL2 having "
        "sum(COL1) > 0;",
        "SELECT sum(COL1) as col1sum FROM t1 group by COL1, COL2 having "
        "col1sum > 0;"));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, TransformTest,
    testing::Values("SELECT * FROM t1 full join t2 on t1.col1 = t2.col2;",
                    "SELECT * FROM t1 right join t2 on t1.col1 = t2.col2;",
                    "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col2;"));

INSTANTIATE_TEST_CASE_P(
    SqlLeftJoinWindowPlan, TransformTest,
    testing::Values(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 left join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY col1 ORDER BY col15 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 left join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY col1, col2 ORDER BY col15 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlUnionPlan, TransformTest,
    testing::Values(
        "SELECT * FROM t1 UNION SELECT * FROM t2;",
        "SELECT * FROM t1 UNION DISTINCT SELECT * FROM t2;",
        "SELECT * FROM t1 UNION ALL SELECT * FROM t2;",
        "SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION SELECT * FROM t3;",
        "SELECT * FROM t1 left join t2 on t1.col1 = t2.col2 UNION ALL SELECT * "
        "FROM t3 UNION SELECT * FROM t4;",
        "SELECT sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10 UNION ALL "
        "SELECT sum(COL1) as col1sum, * FROM t1 group by COL1, COL2 having "
        "sum(COL1) > 0;",
        "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col2 UNION "
        "SELECT * FROM t3 inner join t4 on t3.col1 = t4.col2 UNION "
        "SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;"));
INSTANTIATE_TEST_CASE_P(
    SqlDistinctPlan, TransformTest,
    testing::Values(
        "SELECT distinct COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20;",
        "SELECT DISTINCT sum(COL1) as col1sum, * FROM t1 group by COL1,COL2;",
        "SELECT DISTINCT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col15 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        //        "SELECT DISTINCT COUNT(*) FROM t1;",
        "SELECT distinct COL1 FROM t1 where COL1+COL2;",
        "SELECT DISTINCT COL1 FROM t1 where COL1 > 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryPlan, TransformTest,
    testing::Values(
        "SELECT * FROM t1 WHERE COL1 > (select avg(COL1) from t1) limit 10;",
        "select * from (select * from t1 where col1>0);",
        "select * from \n"
        "    (select * from t1 where col1 = 7) s\n"
        "left join \n"
        "    (select * from t2 where col2 = 2) t\n"
        "on s.col3 = t.col3\n"
        "union\n"
        "select distinct * from \n"
        "    (select distinct * from t1 where col1 = 7) s\n"
        "right join \n"
        "    (select distinct * from t2 where col2 = 2) t\n"
        "on s.col3 = t.col3;",
        "SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;",
        "select * from \n"
        "    (select * from t1 where col1 = 7) s\n"
        "left join \n"
        "    (select * from t2 where col2 = 2) t\n"
        "on s.col3 = t.col3\n"
        "union\n"
        "select distinct * from \n"
        "    (select  * from t1 where col1 = 7) s\n"
        "right join \n"
        "    (select distinct * from t2 where col2 = 2) t\n"
        "on s.col3 = t.col3;"));

TEST_P(TransformTest, transform_physical_plan) {
    std::string sqlstr = GetParam();
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
    index->set_second_key("col15");
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
    Transform transform(&manager, "db", catalog, m.get());
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(transform.TransformPhysicalPlan(
        dynamic_cast<node::PlanNode*>(plan_trees[0]), &physical_plan,
        base_status));
    physical_plan->Print(std::cout, "");
}

void Physical_Plan_Check(const std::shared_ptr<tablet::TabletCatalog>& catalog,
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
    Transform transform(&manager, "db", catalog, m.get());
    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;

    bool ok = transform.TransformPhysicalPlan(
        dynamic_cast<node::PlanNode*>(plan_trees[0]), &physical_plan,
        base_status);
    std::cout << base_status.msg << std::endl;
    ASSERT_TRUE(ok);

    std::ostringstream oos;
    physical_plan->Print(oos, "");
    std::cout << oos.str() << std::endl;

    std::stringstream ss;
    PrintSchema(ss, physical_plan->output_schema);
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
        index->set_second_key("col15");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col15");
    }

    auto catalog = BuildCommonCatalog(table_def, table);

    for (auto in_out : in_outs) {
        Physical_Plan_Check(catalog, in_out.first, in_out.second);
    }
}

TEST_F(TransformTest, pass_sort_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col15 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10)\n"
        "  PROJECT(type=WindowAggregation, groups=(col1), orders=(col15) ASC, "
        "start=-3, end=0)\n"
        "    DATA_PROVIDER(type=IndexScan, table=t1, index=index1)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2, col1 ORDER BY col15 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10)\n"
        "  PROJECT(type=WindowAggregation, groups=(col2,col1), orders=(col15) "
        "ASC, start=-3, end=0)\n"
        "    DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    //    in_outs.push_back(std::make_pair(
    //        "SELECT "
    //        "col1, "
    //        "sum(col3) OVER w1 as w1_col3_sum, "
    //        "sum(col2) OVER w1 as w1_col2_sum "
    //        "FROM t1 WINDOW w1 AS (PARTITION BY col3 ORDER BY col15 ROWS
    //        BETWEEN 3 " "PRECEDING AND CURRENT ROW) limit 10;",
    //        "LIMIT(limit=10)\n"
    //        "  PROJECT(type=WindowAggregation, groups=(col3), orders=(col15)
    //        ASC, " "start=-3, end=0)\n" "    GROUP_AND_SORT_BY(groups=(col3),
    //        orders=(col15) ASC)\n" "      DATA_PROVIDER(table=t1)"));

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
        index->set_second_key("col15");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col15");
    }

    auto catalog = BuildCommonCatalog(table_def, table);

    for (auto in_out : in_outs) {
        Physical_Plan_Check(catalog, in_out.first, in_out.second);
    }
}

TEST_F(TransformTest, pass_join_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "col1, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 left join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY col1 ORDER BY col15 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10)\n"
        "  PROJECT(type=WindowAggregation, groups=(col1), orders=(col15) ASC, "
        "start=-3, end=0)\n"
        "    JOIN(type=LeftJoin, condition=t1.col1 = t2.col1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index1)\n"
        "      DATA_PROVIDER(table=t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 left join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY col1, col2 ORDER BY col15 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10)\n"
        "  PROJECT(type=WindowAggregation, groups=(col1,col2), orders=(col15) "
        "ASC, start=-3, end=0)\n"
        "    JOIN(type=LeftJoin, condition=t1.col1 = t2.col1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index12)\n"
        "      DATA_PROVIDER(table=t2)"));
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
        index->set_second_key("col15");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col15");
    }
    auto catalog = BuildCommonCatalog(table_def, table);
    {
        fesql::type::TableDef table_def2;
        BuildTableDef(table_def2);
        table_def2.set_name("t2");
        std::shared_ptr<::fesql::storage::Table> table2(
            new ::fesql::storage::Table(1, 1, table_def2));
        AddTable(catalog, table_def2, table2);
    }

    for (auto in_out : in_outs) {
        Physical_Plan_Check(catalog, in_out.first, in_out.second);
    }
}
}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
