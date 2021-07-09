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

#include <stack>
#include <string>
#include <utility>
#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "vm/transform.h"

namespace hybridse {
namespace vm {
class LogicalGraphTest
    : public ::testing::TestWithParam<std::pair<std::string, int>> {
 public:
    LogicalGraphTest() {}
    ~LogicalGraphTest() {}
};

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryTransform, LogicalGraphTest,
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
        std::make_pair("select * from \n"
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

TEST_P(LogicalGraphTest, transform_logical_graph_test) {
    auto param = GetParam();
    const std::string sql = param.first;
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
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
            std::cout << *(plan_trees[0]) << std::endl;
        } else {
            std::cout << base_status.str();
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }
    LogicalGraph graph;
    TransformLogicalTreeToLogicalGraph(
        dynamic_cast<node::PlanNode*>(plan_trees[0]), &graph, base_status);
    graph.DfsVisit();
    ASSERT_EQ(param.second, graph.VertexSize());
}

}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
