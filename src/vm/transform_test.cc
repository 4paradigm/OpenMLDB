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
class TransformTest
    : public ::testing::TestWithParam<std::pair<std::string, int>> {
 public:
    TransformTest() {}
    ~TransformTest() {}
};

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryTransform, TransformTest,
    testing::Values(
        std::make_pair("SELECT * FROM t1 WHERE COL1 > (select avg(COL1) from "
                       "t1) limit 10;",
                       5),
        std::make_pair("select * from (select * from t1 where col1>0);", 6),
        std::make_pair(
            "SELECT LastName,FirstName, Title, Salary FROM Employees AS T1 "
            "WHERE Salary >=(SELECT Avg(Salary) "
            "FROM Employees WHERE T1.Title = Employees.Title) Order by Title;",
            6),
        std::make_pair(
            "select * from \n"
            "    (select * from stu where grade = 7) s\n"
            "left join \n"
            "    (select * from sco where subject = \"math\") t\n"
            "on s.id = t.stu_id\n"
            "union\n"
            "select distinct * from \n"
            "    (select distinct * from stu where grade = 7) s\n"
            "right join \n"
            "    (select * from sco where subject = \"math\") t\n"
            "on s.id = t.stu_id;",
            21),
        std::make_pair("SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;",
                       5)));
TEST_P(TransformTest, transform_logical_graph_test) {
    auto param = GetParam();
    const std::string sql = param.first;
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
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
            std::cout << *(plan_trees[0]);
        } else {
            std::cout << base_status.msg;
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }
    LogicalGraph graph;
    TransformLogicalTreeToLogicalGraph(
        dynamic_cast<node::PlanNode*>(plan_trees[0]), base_status, graph);
    graph.DfsVisit();
    ASSERT_EQ(param.second, graph.VertexSize());
}

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
