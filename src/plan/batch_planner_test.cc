/*
 * batch_planner_test.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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



#include "plan/batch_planner.h"
#include <utility>
#include <vector>
#include "analyser/analyser.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
namespace fesql {
namespace plan {

using fesql::node::NodeManager;
using fesql::node::PlanNode;
using fesql::node::SQLNode;
using fesql::node::SQLNodeList;

class BatchPlannerTest : public ::testing::Test {
 public:
    BatchPlannerTest() {
        manager_ = new NodeManager();
        parser_ = new parser::FeSQLParser();
    }

    ~BatchPlannerTest() {}

 protected:
    parser::FeSQLParser *parser_;
    NodeManager *manager_;
};

TEST_F(BatchPlannerTest, SimplePlannerCreatePlanTest) {
    node::NodePointVector list;
    base::Status status;
    int ret = parser_->parse(
        "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 FROM t1 limit 10;",
        list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    node::BatchPlanTree tree;

    BatchPlanner *planner_ptr = new BatchPlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreateTree(list, &tree, status));
    ASSERT_TRUE(NULL != tree.GetRoot());
    ASSERT_EQ(node::kBatchDataset, tree.GetRoot()->GetType());
    ASSERT_EQ(1, tree.GetRoot()->GetChildren().size());
}

}  // namespace plan
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
