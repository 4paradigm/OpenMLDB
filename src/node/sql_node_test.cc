/*
 * parser/sql_test.h
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
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
#include "node/sql_node.h"
#include "node_memory.h"
#include <strstream>
#include "gtest/gtest.h"

namespace fesql {
namespace node {

/**
 * TODO: add unit test for MakeXXXXXNode
 * add unit test and check attributions
 */
class SqlNodeTest : public ::testing::Test {

public:
    SqlNodeTest() {
        node_manager_ = new NodeManager();
    }

    ~SqlNodeTest() {
        delete node_manager_;
    }
protected:
    NodeManager *node_manager_;
};

//TEST_F(SqlNodeTest, MakeNode) {
//    SQLNode *node = node_manager_->MakeSQLNode(kAll);
//    std::cout << *node << std::endl;
//    ASSERT_EQ(kAll, node->GetType());
//}

TEST_F(SqlNodeTest, MakeColumnRefNodeTest) {

    SQLNode *node = node_manager_->MakeColumnRefNode("col", "t");
    ColumnRefNode *columnnode = (ColumnRefNode *) node;
    std::cout << *node << std::endl;
    ASSERT_EQ(kColumn, columnnode->GetType());
    ASSERT_EQ("t", columnnode->GetRelationName());
    ASSERT_EQ("col", columnnode->GetColumnName());

}

TEST_F(SqlNodeTest, MakeConstNodeStringTest) {

    ConstNode *node_ptr = (ConstNode *) (node_manager_->MakeConstNode("parser string test"));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kString, node_ptr->GetType());
    ASSERT_STREQ("parser string test", node_ptr->GetStr());
}

TEST_F(SqlNodeTest, MakeConstNodeIntTest) {
    ConstNode *node_ptr = (ConstNode *) (node_manager_->MakeConstNode(1));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kInt, node_ptr->GetType());
    ASSERT_EQ(1, node_ptr->GetInt());

}

TEST_F(SqlNodeTest, MakeConstNodeLongTest) {
    ConstNode *node_ptr = (ConstNode *) (node_manager_->MakeConstNode(1L));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kBigInt, node_ptr->GetType());
    ASSERT_EQ(1L, node_ptr->GetLong());

    node_ptr = (ConstNode *) (node_manager_->MakeConstNode(864000000L));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kBigInt, node_ptr->GetType());
    ASSERT_EQ(864000000LL, node_ptr->GetLong());
}

//TEST_F(SqlNodeTest, MakeConstNodeDoubleTest) {
//    ConstNode *node_ptr = (ConstNode *) (node_manager_->MakeConstNode(1.989E30));
//    std::cout << *node_ptr << std::endl;
//    ASSERT_EQ(kDouble, node_ptr->GetType());
//    ASSERT_EQ(1.989E30, node_ptr->GetDouble());
//}

TEST_F(SqlNodeTest, MakeConstNodeFloatTest) {
    ConstNode *node_ptr = (ConstNode *) (node_manager_->MakeConstNode(1.234f));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kFloat, node_ptr->GetType());
    ASSERT_EQ(1.234f, node_ptr->GetFloat());
}

TEST_F(SqlNodeTest, MakeWindowDefNodetTest) {
    SQLNodeList *partitions = node_manager_->MakeNodeList();
    SQLNode *ptr1 = node_manager_->MakeColumnRefNode("keycol", "");
    partitions->PushFront(node_manager_->MakeLinkedNode(ptr1));

    SQLNode *ptr2 = node_manager_->MakeColumnRefNode("col1", "");
    SQLNodeList *orders = node_manager_->MakeNodeList();
    orders->PushFront(node_manager_->MakeLinkedNode(ptr2));

    SQLNode
        *frame = node_manager_->MakeFrameNode(node_manager_->MakeFrameBound(kPreceding, NULL),
                                              node_manager_->MakeFrameBound(kPreceding,
                                                                            node_manager_->MakeConstNode(86400000L)));
    WindowDefNode *node_ptr = (WindowDefNode *) node_manager_->MakeWindowDefNode(partitions, orders, frame);
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
//
    NodePointVector vector1;
    vector1.push_back(ptr1);
    NodePointVector vector2;
    vector2.push_back(ptr2);
    ASSERT_EQ(vector1, node_ptr->GetPartitions());
    ASSERT_EQ(vector2, node_ptr->GetOrders());
    ASSERT_EQ(frame, node_ptr->GetFrame());
    ASSERT_EQ("", node_ptr->GetName());

}

TEST_F(SqlNodeTest, MakeWindowDefNodetWithNameTest) {
    WindowDefNode *node_ptr = (WindowDefNode *) node_manager_->MakeWindowDefNode("w1");
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
    ASSERT_EQ(NULL, node_ptr->GetFrame());
    ASSERT_EQ("w1", node_ptr->GetName());

}

TEST_F(SqlNodeTest, NewFrameNodeTest) {
    FrameNode *node_ptr = (FrameNode *) node_manager_->MakeFrameNode(node_manager_->MakeFrameBound(kPreceding, NULL),
                                                                     node_manager_->MakeFrameBound(kPreceding,
                                                                                                   node_manager_->MakeConstNode(
                                                                                                       86400000L)));
    node_manager_->MakeRangeFrameNode(node_ptr);
    std::cout << *node_ptr << std::endl;

    ASSERT_EQ(kFrames, node_ptr->GetType());
    ASSERT_EQ(kFrameRange, node_ptr->GetFrameType());

    // assert frame node start
    ASSERT_EQ(kFrameBound, node_ptr->GetStart()->GetType());
    FrameBound *start = (FrameBound *) node_ptr->GetStart();
    ASSERT_EQ(kPreceding, start->GetBoundType());
    ASSERT_EQ(NULL, start->GetOffset());

    ASSERT_EQ(kFrameBound, node_ptr->GetEnd()->GetType());
    FrameBound *end = (FrameBound *) node_ptr->GetEnd();
    ASSERT_EQ(kPreceding, end->GetBoundType());
    ASSERT_EQ(kBigInt, end->GetOffset()->GetType());
    ConstNode *const_ptr = (ConstNode *) end->GetOffset();
    ASSERT_EQ(86400000, const_ptr->GetLong());
}

} // namespace of base
} // namespace of fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



