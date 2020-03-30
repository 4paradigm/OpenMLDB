/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * plan_node_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/30
 *--------------------------------------------------------------------------
 **/
#include "node/plan_node.h"
#include "gtest/gtest.h"
#include "node/node_manager.h"

namespace fesql {
namespace node {

class PlanNodeTest : public ::testing::Test {
 public:
    PlanNodeTest() { manager_ = new NodeManager(); }

    ~PlanNodeTest() { delete manager_; }

 protected:
    NodeManager *manager_;
};

TEST_F(PlanNodeTest, PlanNodeEqualsTest) {
    PlanNode *table1 = manager_->MakeTablePlanNode("t1");
    PlanNode *table2 = manager_->MakeTablePlanNode("t1");
    PlanNode *table3 = manager_->MakeTablePlanNode("t2");
    ASSERT_TRUE(table1->Equals(table1));
    ASSERT_TRUE(table1->Equals(table2));
    ASSERT_FALSE(table1->Equals(table3));

    PlanNode *rename1 = manager_->MakeRenamePlanNode(table1, "table1");
    PlanNode *rename2 = manager_->MakeRenamePlanNode(table2, "table1");
    PlanNode *rename3 = manager_->MakeRenamePlanNode(table2, "table2");
    ASSERT_TRUE(rename1->Equals(rename1));
    ASSERT_TRUE(rename1->Equals(rename2));
    ASSERT_FALSE(rename1->Equals(rename3));

    ExprNode *expr1 = manager_->MakeBinaryExprNode(
        manager_->MakeColumnRefNode("col1", ""),
        manager_->MakeColumnRefNode("col2", ""), node::kFnOpEq);

    ExprNode *expr2 = manager_->MakeBinaryExprNode(
        manager_->MakeColumnRefNode("col1", ""),
        manager_->MakeColumnRefNode("col2", ""), node::kFnOpEq);

    ExprNode *expr3 = manager_->MakeBinaryExprNode(
        manager_->MakeColumnRefNode("col1", ""),
        manager_->MakeColumnRefNode("col3", ""), node::kFnOpEq);

    ExprNode *expr4 = manager_->MakeBinaryExprNode(
        manager_->MakeColumnRefNode("col1", ""),
        manager_->MakeColumnRefNode("col2", ""), node::kFnOpGt);
    ASSERT_TRUE(expr1->Equals(expr1));
    ASSERT_TRUE(expr1->Equals(expr2));
    ASSERT_FALSE(expr1->Equals(expr3));
    ASSERT_FALSE(expr1->Equals(expr4));

    PlanNode *filter1 = manager_->MakeFilterPlanNode(table1, expr1);
    PlanNode *filter2 = manager_->MakeFilterPlanNode(table2, expr2);
    PlanNode *filter3 = manager_->MakeFilterPlanNode(table2, expr3);
    PlanNode *filter4 = manager_->MakeFilterPlanNode(table3, expr2);
    ASSERT_TRUE(filter1->Equals(filter1));
    ASSERT_TRUE(filter1->Equals(filter2));
    ASSERT_FALSE(filter1->Equals(filter3));
    ASSERT_FALSE(filter1->Equals(filter4));

    // expr list
    ExprListNode *expr_list1 = manager_->MakeExprList();
    expr_list1->AddChild(manager_->MakeColumnRefNode("col1", ""));
    expr_list1->AddChild(manager_->MakeColumnRefNode("col2", ""));

    ExprListNode *expr_list2 = manager_->MakeExprList();
    expr_list2->AddChild(manager_->MakeColumnRefNode("col1", ""));
    expr_list2->AddChild(manager_->MakeColumnRefNode("col2", ""));

    ExprListNode *expr_list3 = manager_->MakeExprList();
    expr_list3->AddChild(manager_->MakeColumnRefNode("c1", ""));
    expr_list3->AddChild(manager_->MakeColumnRefNode("col2", ""));
    ASSERT_TRUE(expr_list1->Equals(expr_list1));
    ASSERT_TRUE(expr_list1->Equals(expr_list2));
    ASSERT_FALSE(expr_list1->Equals(expr_list3));

    // order
    ExprNode *order1 = manager_->MakeOrderByNode(expr_list1, true);
    ExprNode *order2 = manager_->MakeOrderByNode(expr_list2, true);
    ExprNode *order3 = manager_->MakeOrderByNode(expr_list3, true);
    ExprNode *order4 = manager_->MakeOrderByNode(expr_list2, false);

    ASSERT_TRUE(order1->Equals(order1));
    ASSERT_TRUE(order1->Equals(order2));
    ASSERT_FALSE(order1->Equals(order3));
    ASSERT_FALSE(order1->Equals(order4));

    // sort
    PlanNode *sort1 = manager_->MakeSortPlanNode(
        table1, dynamic_cast<const OrderByNode *>(order1));
    PlanNode *sort2 = manager_->MakeSortPlanNode(
        table1, dynamic_cast<const OrderByNode *>(order2));
    PlanNode *sort3 = manager_->MakeSortPlanNode(
        table1, dynamic_cast<const OrderByNode *>(order3));
    PlanNode *sort4 = manager_->MakeSortPlanNode(
        table1, dynamic_cast<const OrderByNode *>(order4));
    ASSERT_TRUE(sort1->Equals(sort1));
    ASSERT_TRUE(sort1->Equals(sort2));
    ASSERT_FALSE(sort1->Equals(sort3));
    ASSERT_FALSE(sort1->Equals(sort4));

    // group
    PlanNode *group1 = manager_->MakeGroupPlanNode(filter1, expr_list1);
    PlanNode *group2 = manager_->MakeGroupPlanNode(filter2, expr_list2);
    PlanNode *group3 = manager_->MakeGroupPlanNode(filter2, expr_list3);
    PlanNode *group4 = manager_->MakeGroupPlanNode(filter3, expr_list2);

    ASSERT_TRUE(group1->Equals(group1));
    ASSERT_TRUE(group1->Equals(group2));
    ASSERT_FALSE(group1->Equals(group3));
    ASSERT_FALSE(group1->Equals(group4));

    // distinct
    PlanNode *distinct1 = manager_->MakeDistinctPlanNode(group1);
    PlanNode *distinct2 = manager_->MakeDistinctPlanNode(group2);
    PlanNode *distinct3 = manager_->MakeDistinctPlanNode(group3);
    ASSERT_TRUE(distinct1->Equals(distinct1));
    ASSERT_TRUE(distinct1->Equals(distinct2));
    ASSERT_FALSE(distinct1->Equals(distinct3));
    // join

    // union
}

TEST_F(PlanNodeTest, LeafPlanNodeTest) {
    PlanNode *node_ptr = manager_->MakeLeafPlanNode(kUnknowPlan);
    ASSERT_EQ(0, node_ptr->GetChildrenSize());
    ASSERT_EQ(false, node_ptr->AddChild(node_ptr));
    ASSERT_EQ(0, node_ptr->GetChildrenSize());
}

TEST_F(PlanNodeTest, UnaryPlanNodeTest) {
    PlanNode *node_ptr = manager_->MakeLeafPlanNode(kUnknowPlan);

    PlanNode *unary_node_ptr = manager_->MakeUnaryPlanNode(kUnknowPlan);
    ASSERT_EQ(true, unary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, unary_node_ptr->GetChildrenSize());

    ASSERT_EQ(false, unary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, unary_node_ptr->GetChildrenSize());
}

TEST_F(PlanNodeTest, BinaryPlanNodeTest) {
    PlanNode *node_ptr = manager_->MakeLeafPlanNode(kUnknowPlan);
    ASSERT_EQ(0, node_ptr->GetChildrenSize());

    PlanNode *binary_node_ptr = manager_->MakeBinaryPlanNode(kUnknowPlan);
    ASSERT_EQ(true, binary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, binary_node_ptr->GetChildrenSize());

    ASSERT_EQ(true, binary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(2, binary_node_ptr->GetChildrenSize());

    ASSERT_EQ(false, binary_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(2, binary_node_ptr->GetChildrenSize());
}

TEST_F(PlanNodeTest, MultiPlanNodeTest) {
    PlanNode *node_ptr = manager_->MakeLeafPlanNode(kUnknowPlan);
    ASSERT_EQ(0, node_ptr->GetChildrenSize());

    PlanNode *multi_node_ptr = manager_->MakeMultiPlanNode(kUnknowPlan);
    ASSERT_EQ(true, multi_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(1, multi_node_ptr->GetChildrenSize());

    ASSERT_EQ(true, multi_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(2, multi_node_ptr->GetChildrenSize());

    ASSERT_EQ(true, multi_node_ptr->AddChild(node_ptr));
    ASSERT_EQ(3, multi_node_ptr->GetChildrenSize());
}

}  // namespace node
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
