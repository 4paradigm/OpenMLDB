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

#include "node/plan_node.h"
#include "gtest/gtest.h"
#include "node/node_manager.h"

namespace hybridse {
namespace node {

class PlanNodeTest : public ::testing::Test {
 public:
    PlanNodeTest() { manager_ = new NodeManager(); }

    ~PlanNodeTest() { delete manager_; }

 protected:
    NodeManager *manager_;
};

TEST_F(PlanNodeTest, PlanNodeEqualsTest) {
    PlanNode *table1 = manager_->MakeTablePlanNode("db1", "t1");
    PlanNode *table2 = manager_->MakeTablePlanNode("db1", "t1");
    PlanNode *table3 = manager_->MakeTablePlanNode("db2", "t2");
    ASSERT_TRUE(table1->Equals(table1));
    ASSERT_TRUE(table1->Equals(table2));
    ASSERT_FALSE(table1->Equals(table3));

    PlanNode *rename1 = manager_->MakeRenamePlanNode(table1, "table1");
    PlanNode *rename2 = manager_->MakeRenamePlanNode(table2, "table1");
    PlanNode *rename3 = manager_->MakeRenamePlanNode(table2, "table2");
    ASSERT_TRUE(rename1->Equals(rename1));
    ASSERT_TRUE(rename1->Equals(rename2));
    ASSERT_FALSE(rename1->Equals(rename3));

    ExprNode *expr1 = manager_->MakeBinaryExprNode(manager_->MakeColumnRefNode("col1", ""),
                                                   manager_->MakeColumnRefNode("col2", ""), node::kFnOpEq);

    ExprNode *expr2 = manager_->MakeBinaryExprNode(manager_->MakeColumnRefNode("col1", ""),
                                                   manager_->MakeColumnRefNode("col2", ""), node::kFnOpEq);

    ExprNode *expr3 = manager_->MakeBinaryExprNode(manager_->MakeColumnRefNode("col1", ""),
                                                   manager_->MakeColumnRefNode("col3", ""), node::kFnOpEq);

    ExprNode *expr4 = manager_->MakeBinaryExprNode(manager_->MakeColumnRefNode("col1", ""),
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
    ExprListNode *order_expressions1 = manager_->MakeExprList();
    order_expressions1->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col1", ""), true));
    order_expressions1->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col2", ""), true));

    ExprListNode *order_expressions2 = manager_->MakeExprList();
    order_expressions2->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col1", ""), true));
    order_expressions2->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col2", ""), true));

    ExprListNode *order_expressions3 = manager_->MakeExprList();
    order_expressions3->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("c1", ""), true));
    order_expressions3->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col2", ""), true));

    ExprListNode *order_expressions4 = manager_->MakeExprList();
    order_expressions3->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col1", ""), true));
    order_expressions3->AddChild(manager_->MakeOrderExpression(manager_->MakeColumnRefNode("col2", ""), false));

    ASSERT_TRUE(order_expressions1->Equals(order_expressions1));
    ASSERT_TRUE(order_expressions1->Equals(order_expressions2));
    ASSERT_FALSE(order_expressions1->Equals(order_expressions3));
    ASSERT_FALSE(order_expressions1->Equals(order_expressions4));

    // order
    ExprNode *order1 = manager_->MakeOrderByNode(order_expressions1);
    ExprNode *order2 = manager_->MakeOrderByNode(order_expressions2);
    ExprNode *order3 = manager_->MakeOrderByNode(order_expressions3);
    ExprNode *order4 = manager_->MakeOrderByNode(order_expressions4);

    ASSERT_TRUE(order1->Equals(order1));
    ASSERT_TRUE(order1->Equals(order2));
    ASSERT_FALSE(order1->Equals(order3));
    ASSERT_FALSE(order1->Equals(order4));

    // sort
    PlanNode *sort1 = manager_->MakeSortPlanNode(table1, dynamic_cast<const OrderByNode *>(order1));
    PlanNode *sort2 = manager_->MakeSortPlanNode(table1, dynamic_cast<const OrderByNode *>(order2));
    PlanNode *sort3 = manager_->MakeSortPlanNode(table1, dynamic_cast<const OrderByNode *>(order3));
    PlanNode *sort4 = manager_->MakeSortPlanNode(table1, dynamic_cast<const OrderByNode *>(order4));
    ASSERT_TRUE(sort1->Equals(sort1));
    ASSERT_TRUE(sort1->Equals(sort2));
    ASSERT_FALSE(sort1->Equals(sort3));
    ASSERT_FALSE(sort1->Equals(sort4));

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
    std::ostringstream oss;
    unary_node_ptr->Print(oss, "");
    std::cout << oss.str();
    ASSERT_EQ(
        "+-[kUnknow]\n"
        "  +-[kUnknow]",
        oss.str());
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
    std::ostringstream oss;
    binary_node_ptr->Print(oss, "");
    ASSERT_EQ(
        "\n"
        "+-[kUnknow]\n"
        "  +-[kUnknow]\n"
        "  +-[kUnknow]",
        oss.str());
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
    std::ostringstream oss;
    multi_node_ptr->Print(oss, "");
    std::cout << oss.str();
    ASSERT_EQ(
        "+-[kUnknow]\n"
        "  +-children[list]:\n"
        "    +-[kUnknow]\n"
        "    +-[kUnknow]\n"
        "    +-[kUnknow]",
        oss.str());
}

TEST_F(PlanNodeTest, ExtractColumnsAndIndexsTest) {
    SqlNodeList *index_items = manager_->MakeNodeList();
    index_items->PushBack(manager_->MakeIndexKeyNode("col4"));
    index_items->PushBack(manager_->MakeIndexTsNode("col5"));
    ColumnIndexNode *index_node = dynamic_cast<ColumnIndexNode *>(manager_->MakeColumnIndexNode(index_items));
    index_node->SetName("index1");
    CreatePlanNode *node = manager_->MakeCreateTablePlanNode(
        "", "t1",
        {manager_->MakeColumnDescNode("col1", node::kInt32, true),
         manager_->MakeColumnDescNode("col2", node::kInt32, true),
         manager_->MakeColumnDescNode("col3", node::kFloat, true),
         manager_->MakeColumnDescNode("col4", node::kVarchar, true),
         manager_->MakeColumnDescNode("col5", node::kTimestamp, true), index_node},
        {manager_->MakeReplicaNumNode(3), manager_->MakePartitionNumNode(8), manager_->MakeStorageModeNode(kMemory)},
        false);
    ASSERT_TRUE(nullptr != node);
    std::vector<std::string> columns;
    std::vector<std::string> indexes;
    ASSERT_TRUE(node->ExtractColumnsAndIndexs(columns, indexes));
    ASSERT_EQ(std::vector<std::string>({"col1 int32", "col2 int32", "col3 float", "col4 string", "col5 timestamp"}),
              columns);
    ASSERT_EQ(std::vector<std::string>({"index1:col4:col5"}), indexes);
    auto table_option_list = node->GetTableOptionList();
    for (auto table_option : table_option_list) {
        if (table_option->GetType() == kReplicaNum) {
            ASSERT_EQ(3, dynamic_cast<ReplicaNumNode *>(table_option)->GetReplicaNum());
        } else if (table_option->GetType() == kPartitionNum) {
            ASSERT_EQ(8, dynamic_cast<PartitionNumNode *>(table_option)->GetPartitionNum());
        } else if (table_option->GetType() == kStorageMode) {
            ASSERT_EQ(kMemory, dynamic_cast<StorageModeNode *>(table_option)->GetStorageMode());
        }
    }
}
}  // namespace node
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
