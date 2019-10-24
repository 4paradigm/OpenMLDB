
#include "plan/plan_node.h"
#include "gtest/gtest.h"

namespace fesql {
namespace plan {

// TODO: add ut: 检查SQL的语法树节点预期 2019.10.23
class PlannerTest : public ::testing::Test {

public:
    PlannerTest() {}

    ~PlannerTest() {}
};


TEST_F(PlannerTest, LeafPlanNodeTest) {

    LeafPlanNode *node_ptr = new LeafPlanNode();
    ASSERT_EQ(0, node_ptr->GetChildrenSize());
    ASSERT_EQ(false, node_ptr->AddChild(node_ptr));
    ASSERT_EQ(0, node_ptr->GetChildrenSize());
}


TEST_F(PlannerTest, UnaryPlanNodeTest) {
    LeafPlanNode *node_ptr = new LeafPlanNode();

    UnaryPlanNode *unary_node_ptr = new UnaryPlanNode();
    ASSERT_EQ(true, unary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, unary_node_ptr->GetChildrenSize());

    ASSERT_EQ(false, unary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, unary_node_ptr->GetChildrenSize());

}

TEST_F(PlannerTest, BinaryPlanNodeTest) {
    LeafPlanNode *node_ptr = new LeafPlanNode();
    ASSERT_EQ(0, node_ptr->GetChildrenSize());

    BinaryPlanNode *binary_node_ptr = new BinaryPlanNode();
    ASSERT_EQ(true, binary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, binary_node_ptr->GetChildrenSize());

    ASSERT_EQ(true, binary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(2, binary_node_ptr->GetChildrenSize());


    ASSERT_EQ(false, binary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(2, binary_node_ptr->GetChildrenSize());

}

}
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}