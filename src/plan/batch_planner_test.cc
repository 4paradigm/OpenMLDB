

#include "plan/batch_planner.h"
#include <vector>
#include <utility>
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
}

}  // namespace plan
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
