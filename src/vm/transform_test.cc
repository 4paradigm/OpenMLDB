/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "vm/transform.h"
#include <stack>
#include <utility>
#include <string>
#include "base/status.h"
#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "vm/test_base.h"

namespace fesql {
namespace vm {
//class LogicalGraphTest
//    : public ::testing::TestWithParam<std::pair<std::string, int>> {
// public:
//    LogicalGraphTest() {}
//    ~LogicalGraphTest() {}
//};
//
//INSTANTIATE_TEST_CASE_P(
//    SqlSubQueryTransform, LogicalGraphTest,
//    testing::Values(
//        std::make_pair("SELECT * FROM t1 WHERE COL1 > (select avg(COL1) from "
//                       "t1) limit 10;",
//                       5),
//        std::make_pair("select * from (select * from t1 where col1>0);", 6),
//        std::make_pair(
//            "SELECT LastName,FirstName, Title, Salary FROM Employees AS T1 "
//            "WHERE Salary >=(SELECT Avg(Salary) "
//            "FROM Employees WHERE T1.Title = Employees.Title) Order by Title;",
//            6),
//        std::make_pair(
//            "select * from \n"
//            "    (select * from stu where grade = 7) s\n"
//            "left join \n"
//            "    (select * from sco where subject = \"math\") t\n"
//            "on s.id = t.stu_id\n"
//            "union\n"
//            "select distinct * from \n"
//            "    (select distinct * from stu where grade = 7) s\n"
//            "right join \n"
//            "    (select * from sco where subject = \"math\") t\n"
//            "on s.id = t.stu_id;",
//            21),
//        std::make_pair("SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;",
//                       5)));
//
//TEST_P(LogicalGraphTest, transform_logical_graph_test) {
//    auto param = GetParam();
//    const std::string sql = param.first;
//    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
//    std::cout << sql << std::endl;
//    ::fesql::node::NodeManager manager;
//    ::fesql::node::PlanNodeList plan_trees;
//    ::fesql::base::Status base_status;
//    {
//        ::fesql::plan::SimplePlanner planner(&manager);
//        ::fesql::parser::FeSQLParser parser;
//        ::fesql::node::NodePointVector parser_trees;
//        parser.parse(sql, parser_trees, &manager, base_status);
//        ASSERT_EQ(0, base_status.code);
//        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
//            0) {
//            std::cout << *(plan_trees[0]);
//        } else {
//            std::cout << base_status.msg;
//        }
//
//        ASSERT_EQ(0, base_status.code);
//        std::cout.flush();
//    }
//    LogicalGraph graph;
//    TransformLogicalTreeToLogicalGraph(
//        dynamic_cast<node::PlanNode*>(plan_trees[0]), base_status, graph);
//    graph.DfsVisit();
//    ASSERT_EQ(param.second, graph.VertexSize());
//}
//

class TransformTest
    : public ::testing::TestWithParam<std::string> {
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
    SqlExprPlanner, TransformTest,
    testing::Values(
        "SELECT COL1 FROM t1;", "SELECT COL1 as c1 FROM t1;",
        "SELECT COL1 c1 FROM t1;", "SELECT t1.COL1 FROM t1;",
        "SELECT t1.COL1 as c1 FROM t1;", "SELECT t1.COL1 c1 FROM t1;",
        "SELECT t1.COL1 c1 FROM t1 limit 10;", "SELECT * FROM t1;",
        "SELECT COUNT(*) FROM t1;", "SELECT COUNT(COL1) FROM t1;",
        "SELECT TRIM(COL1) FROM t1;", "SELECT trim(COL1) as trim_col1 FROM t1;",
        "SELECT MIN(COL1) FROM t1;", "SELECT min(COL1) FROM t1;",
        "SELECT MAX(COL1) FROM t1;", "SELECT max(COL1) as max_col1 FROM t1;",
        "SELECT SUM(COL1) FROM t1;", "SELECT sum(COL1) as sum_col1 FROM t1;",
        "SELECT COL1, COL2, `TS`, AVG(AMT) OVER w, SUM(AMT) OVER w FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN UNBOUNDED PRECEDING AND "
        "UNBOUNDED FOLLOWING);",
        "SELECT COL1, trim(COL2), `TS`, AVG(AMT) OVER w, SUM(AMT) OVER w FROM "
        "t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT COL1, SUM(AMT) OVER w as w_amt_sum FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
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

        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col15 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "SELECT COUNT(*) FROM t1;"));

TEST_P(TransformTest, transform_physical_plan) {
    std::string sqlstr = GetParam();
    std::cout << sqlstr << std::endl;

    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    std::cout << sqlstr << std::endl;

    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    auto catalog = BuildCommonCatalog(table_def, table);

    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::cout << *(plan_trees[0]);
        } else {
            std::cout << base_status.msg;
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    Transform transform("db", catalog);
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(transform.TransformPhysicalPlan(
        dynamic_cast<node::PlanNode*>(plan_trees[0]), &physical_plan, base_status));
//    ASSERT_TRUE(nullptr != physical_plan);
//    physical_plan->Print(std::cout, "");
}




}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
