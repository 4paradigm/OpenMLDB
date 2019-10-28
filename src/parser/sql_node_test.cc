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
#include "parser/node.h"
#include "gtest/gtest.h"
#include <strstream>

namespace fesql {
namespace parser {

/**
 * TODO: add unit test for MakeXXXXXNode
 * add unit test and check attributions
 */
class SqlNodeTest : public ::testing::Test {

public:
    SqlNodeTest() {}

    ~SqlNodeTest() {}
};

TEST_F(SqlNodeTest, MakeNode) {
    using namespace std;
    SQLNode *node = fesql::parser::MakeNode(kAll);
    cout << *node << endl;
    ASSERT_EQ(kAll, node->GetType());
}

TEST_F(SqlNodeTest, MakeColumnRefNodeTest) {

    SQLNode *node = MakeColumnRefNode("col", "t");
    ColumnRefNode *columnnode = (ColumnRefNode *) node;
    std::cout << *node << std::endl;
    ASSERT_EQ(kColumn, columnnode->GetType());
    ASSERT_EQ("t", columnnode->GetRelationName());
    ASSERT_EQ("col", columnnode->GetColumnName());

}

TEST_F(SqlNodeTest, MakeConstNodeStringTest) {

    ConstNode *node_ptr = new ConstNode("parser string test");
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kString, node_ptr->GetType());
    ASSERT_STREQ("parser string test", node_ptr->GetStr());
}

TEST_F(SqlNodeTest, MakeConstNodeIntTest) {
    ConstNode *node_ptr = new ConstNode(1);
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kInt, node_ptr->GetType());
    ASSERT_EQ(1, node_ptr->GetInt());

}

TEST_F(SqlNodeTest, MakeConstNodeLongTest) {
    ConstNode *node_ptr = new ConstNode(1L);
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kBigInt, node_ptr->GetType());
    ASSERT_EQ(1L, node_ptr->GetLong());

    delete node_ptr;

    node_ptr = new ConstNode(864000000L);
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kBigInt, node_ptr->GetType());
    ASSERT_EQ(864000000LL, node_ptr->GetLong());
}

TEST_F(SqlNodeTest, MakeConstNodeDoubleTest) {
    ConstNode *node_ptr = new ConstNode(1.989E30);
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kDouble, node_ptr->GetType());
    ASSERT_EQ(1.989E30, node_ptr->GetDouble());
}

TEST_F(SqlNodeTest, MakeConstNodeFloatTest) {
    ConstNode *node_ptr = new ConstNode(1.234f);
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kFloat, node_ptr->GetType());
    ASSERT_EQ(1.234f, node_ptr->GetFloat());
}

TEST_F(SqlNodeTest, MakeWindowDefNodetTest) {
    SQLNodeList *partitions = new SQLNodeList();
    SQLNode * ptr1 = new ColumnRefNode("keycol");
    partitions->PushFront(ptr1);

    SQLNode * ptr2 = new ColumnRefNode("col1");
    SQLNodeList *orders = new SQLNodeList();
    orders->PushFront(ptr2);

    SQLNode
        *frame = MakeFrameNode(new FrameBound(kPreceding, NULL), new FrameBound(kPreceding, new ConstNode(86400000L)));
    WindowDefNode *node_ptr = (WindowDefNode *) MakeWindowDefNode(partitions, orders, frame);
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
    WindowDefNode *node_ptr = (WindowDefNode *) MakeWindowDefNode("w1");
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
    ASSERT_EQ(NULL, node_ptr->GetFrame());
    ASSERT_EQ("w1", node_ptr->GetName());

}

TEST_F(SqlNodeTest, NewFrameNodeTest) {
    FrameNode *node_ptr = (FrameNode *) MakeFrameNode(new FrameBound(kPreceding, NULL),
                                                      new FrameBound(kPreceding, new ConstNode(86400000L)));
    MakeRangeFrameNode(node_ptr);
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



