/**
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "node/sql_node.h"
#include "gtest/gtest.h"
#include "node/node_manager.h"

namespace hybridse {
namespace node {

class SqlNodeTest : public ::testing::Test {
 public:
    SqlNodeTest() { node_manager_ = new NodeManager(); }

    ~SqlNodeTest() { delete node_manager_; }

    NodeManager *node_manager_;
};

TEST_F(SqlNodeTest, StructExprNodeTest) {
    StructExpr struct_expr("window");
    TypeNode type_int32(DataType::kInt32);
    FnParaNode *filed1 = node_manager_->MakeFnParaNode("idx", &type_int32);
    FnParaNode *filed2 = node_manager_->MakeFnParaNode("rows_ptr", &type_int32);
    FnNodeList fileds;
    fileds.AddChild(filed1);
    fileds.AddChild(filed2);
    struct_expr.SetFileds(&fileds);

    FnNodeList parameters;
    FnNodeList methods;
    TypeNode type_node(DataType::kRow);
    FnNodeFnHeander iterator("iterator", &parameters, &type_node);
    methods.AddChild(&iterator);
    struct_expr.SetMethod(&methods);

    DLOG(INFO) << struct_expr << std::endl;
    ASSERT_EQ(kExprStruct, struct_expr.GetExprType());
    ASSERT_EQ(&methods, struct_expr.GetMethods());
    ASSERT_EQ(&fileds, struct_expr.GetFileds());
}

TEST_F(SqlNodeTest, MakeColumnRefNodeTest) {
    SqlNode *node = node_manager_->MakeColumnRefNode("col", "t");
    ColumnRefNode *columnnode = dynamic_cast<ColumnRefNode *>(node);
    DLOG(INFO) << *node << std::endl;
    ASSERT_EQ(kExprColumnRef, columnnode->GetExprType());
    ASSERT_EQ("t", columnnode->GetRelationName());
    ASSERT_EQ("col", columnnode->GetColumnName());
}

TEST_F(SqlNodeTest, MakeGetFieldExprTest) {
    auto row = node_manager_->MakeExprIdNode("row");
    auto node = node_manager_->MakeGetFieldExpr(row, 0);
    DLOG(INFO) << *node << std::endl;
    ASSERT_EQ(kExprGetField, node->GetExprType());
    ASSERT_EQ("0", node->GetColumnName());
    ASSERT_EQ(kExprId, node->GetRow()->GetExprType());
}

TEST_F(SqlNodeTest, MakeConstNodeStringTest) {
    ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode("parser string test"));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kVarchar, node_ptr->GetDataType());
    ASSERT_STREQ("parser string test", node_ptr->GetStr());
}

TEST_F(SqlNodeTest, MakeConstNodeIntTest) {
    ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kInt32, node_ptr->GetDataType());
    ASSERT_EQ(1, node_ptr->GetInt());

    ASSERT_TRUE(node_ptr->ConvertNegative());
    ASSERT_EQ(-1, node_ptr->GetInt());
}

TEST_F(SqlNodeTest, MakeConstNodeLongTest) {
    int64_t val1 = 1;
    int64_t val2 = 864000000L;
    ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(val1));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kInt64, node_ptr->GetDataType());
    ASSERT_EQ(val1, node_ptr->GetLong());

    node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(val2));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kInt64, node_ptr->GetDataType());
    ASSERT_EQ(val2, node_ptr->GetLong());

    ASSERT_TRUE(node_ptr->ConvertNegative());
    ASSERT_EQ(-val2, node_ptr->GetLong());
}

TEST_F(SqlNodeTest, MakeConstNodeDoubleTest) {
    ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1.989E30));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kDouble, node_ptr->GetDataType());
    ASSERT_EQ(1.989E30, node_ptr->GetDouble());

    ASSERT_TRUE(node_ptr->ConvertNegative());
    ASSERT_EQ(-1.989E30, node_ptr->GetDouble());
}

TEST_F(SqlNodeTest, MakeConstNodeFloatTest) {
    ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1.234f));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kFloat, node_ptr->GetDataType());
    ASSERT_EQ(1.234f, node_ptr->GetFloat());

    ASSERT_TRUE(node_ptr->ConvertNegative());
    ASSERT_EQ(-1.234f, node_ptr->GetFloat());
}
TEST_F(SqlNodeTest, MakeConstTimeTest) {
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(10, node::kSecond));
        DLOG(INFO) << *node_ptr << std::endl;
        ASSERT_EQ(hybridse::node::kSecond, node_ptr->GetDataType());
        ASSERT_EQ(10000, node_ptr->GetMillis());

        ASSERT_TRUE(node_ptr->ConvertNegative());
        ASSERT_EQ(-10000, node_ptr->GetMillis());
    }
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(10, node::kMinute));
        DLOG(INFO) << *node_ptr << std::endl;
        ASSERT_EQ(hybridse::node::kMinute, node_ptr->GetDataType());
        ASSERT_EQ(10000 * 60, node_ptr->GetMillis());

        ASSERT_TRUE(node_ptr->ConvertNegative());
        ASSERT_EQ(-10000 * 60, node_ptr->GetMillis());
    }
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(10, node::kHour));
        DLOG(INFO) << *node_ptr << std::endl;
        ASSERT_EQ(hybridse::node::kHour, node_ptr->GetDataType());
        ASSERT_EQ(10000 * 3600, node_ptr->GetMillis());

        ASSERT_TRUE(node_ptr->ConvertNegative());
        ASSERT_EQ(-10000 * 3600, node_ptr->GetMillis());
    }
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(10, node::kDay));
        DLOG(INFO) << *node_ptr << std::endl;
        ASSERT_EQ(hybridse::node::kDay, node_ptr->GetDataType());
        ASSERT_EQ(10000 * 3600 * 24, node_ptr->GetMillis());

        ASSERT_TRUE(node_ptr->ConvertNegative());
        ASSERT_EQ(-10000 * 3600 * 24, node_ptr->GetMillis());
    }
}

TEST_F(SqlNodeTest, MakeConstNodeTTLTypeTest) {
    // TODO(chenjing): assign DataType::kTTL to TTLType
    auto node = node_manager_->MakeConstNode(static_cast<int64_t>(10), node::TTLType::kLatest);
    ASSERT_EQ(node::DataType::kInt64, node->GetDataType());
    ASSERT_EQ(10L, node->GetAsInt64());
    ASSERT_EQ(node::TTLType::kLatest, node->GetTTLType());
}
TEST_F(SqlNodeTest, TimeIntervalConstGetMillisTest) {
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1, node::DataType::kSecond));
        ASSERT_EQ(hybridse::node::kSecond, node_ptr->GetDataType());
        ASSERT_EQ(1000L, node_ptr->GetMillis());
    }
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1.234f));
        DLOG(INFO) << *node_ptr << std::endl;
        ASSERT_EQ(hybridse::node::kFloat, node_ptr->GetDataType());
        ASSERT_EQ(-1, node_ptr->GetMillis());
    }
}
TEST_F(SqlNodeTest, StringConstGetAsDateTest) {
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode("2021-05-01"));
        ASSERT_EQ(hybridse::node::kVarchar, node_ptr->GetDataType());
        int year = 0;
        int month = 0;
        int day = 0;
        ASSERT_TRUE(node_ptr->GetAsDate(&year, &month, &day));
        ASSERT_EQ(2021, year);
        ASSERT_EQ(5, month);
        ASSERT_EQ(1, day);
    }
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(""));
        ASSERT_EQ(hybridse::node::kVarchar, node_ptr->GetDataType());
        int year = 0;
        int month = 0;
        int day = 0;
        ASSERT_FALSE(node_ptr->GetAsDate(&year, &month, &day));
    }
}
TEST_F(SqlNodeTest, MakeWindowDefNodetTest) {
    int64_t val = 86400000L;

    ExprListNode *partitions = node_manager_->MakeExprList();
    ExprNode *ptr1 = node_manager_->MakeColumnRefNode("keycol", "");
    partitions->PushBack(ptr1);

    ExprNode *ptr2 = node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col1", ""), true);
    ExprListNode *orders = node_manager_->MakeExprList();
    orders->PushBack(ptr2);

    int64_t maxsize = 0;
    SqlNode *frame =
        node_manager_->MakeFrameNode(kFrameRange,
                                     node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding),
                                                                    node_manager_->MakeFrameBound(kPreceding, val)),
                                     maxsize);
    WindowDefNode *node_ptr = dynamic_cast<WindowDefNode *>(
        node_manager_->MakeWindowDefNode(partitions, node_manager_->MakeOrderByNode(orders), frame));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
    ASSERT_EQ("(keycol)", ExprString(node_ptr->GetPartitions()));
    ASSERT_EQ("(col1 ASC)", ExprString(node_ptr->GetOrders()));
    ASSERT_EQ(frame, node_ptr->GetFrame());
    ASSERT_EQ("", node_ptr->GetName());
}

TEST_F(SqlNodeTest, MakeWindowDefNodetWithNameTest) {
    WindowDefNode *node_ptr = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode("w1"));
    DLOG(INFO) << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
    ASSERT_EQ(NULL, node_ptr->GetFrame());
    ASSERT_EQ("w1", node_ptr->GetName());
}

TEST_F(SqlNodeTest, MakeExternalFnDefNodeTest) {
    auto *node_ptr = dynamic_cast<ExternalFnDefNode *>(node_manager_->MakeUnresolvedFnDefNode("extern_f"));
    ASSERT_EQ(kExternalFnDef, node_ptr->GetType());
    ASSERT_EQ("extern_f", node_ptr->function_name());
}

TEST_F(SqlNodeTest, MakeUdafDefNodeTest) {
    auto zero = node_manager_->MakeConstNode(1);
    auto f1 = dynamic_cast<ExternalFnDefNode *>(node_manager_->MakeUnresolvedFnDefNode("f1"));
    auto f2 = dynamic_cast<ExternalFnDefNode *>(node_manager_->MakeUnresolvedFnDefNode("f2"));
    auto f3 = dynamic_cast<ExternalFnDefNode *>(node_manager_->MakeUnresolvedFnDefNode("f3"));
    auto *udaf = dynamic_cast<UdafDefNode *>(node_manager_->MakeUdafDefNode("udaf", {}, zero, f1, f2, f3));
    ASSERT_EQ(kUdafDef, udaf->GetType());
    ASSERT_EQ(true, udaf->init_expr()->Equals(zero));
    ASSERT_EQ(true, udaf->update_func()->Equals(f1));
    ASSERT_EQ(true, udaf->merge_func()->Equals(f2));
    ASSERT_EQ(true, udaf->output_func()->Equals(f3));
}

TEST_F(SqlNodeTest, NewFrameNodeTest) {
    FrameNode *node_ptr = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        node::kFrameRange,
        node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding),
                                       node_manager_->MakeFrameBound(kPreceding, 86400000)),
        node_manager_->MakeConstNode(100)));
    DLOG(INFO) << *node_ptr << std::endl;

    ASSERT_EQ(kFrames, node_ptr->GetType());
    ASSERT_EQ(kFrameRange, node_ptr->frame_type());

    // assert frame node start
    ASSERT_EQ(kFrameBound, node_ptr->frame_range()->start()->GetType());
    FrameBound *start = dynamic_cast<FrameBound *>(node_ptr->frame_range()->start());
    ASSERT_EQ(kPreceding, start->bound_type());
    ASSERT_EQ(0L, start->GetOffset());

    ASSERT_EQ(kFrameBound, node_ptr->frame_range()->end()->GetType());
    FrameBound *end = dynamic_cast<FrameBound *>(node_ptr->frame_range()->end());
    ASSERT_EQ(kPreceding, end->bound_type());

    ASSERT_EQ(86400000, end->GetOffset());
    ASSERT_EQ(100L, node_ptr->frame_maxsize());
}

TEST_F(SqlNodeTest, MakeInsertNodeTest) {
    ExprListNode *column_expr_list = node_manager_->MakeExprList();
    ExprNode *ptr1 = node_manager_->MakeColumnRefNode("col1", "");
    column_expr_list->PushBack(ptr1);

    ExprNode *ptr2 = node_manager_->MakeColumnRefNode("col2", "");
    column_expr_list->PushBack(ptr2);

    ExprNode *ptr3 = node_manager_->MakeColumnRefNode("col3", "");
    column_expr_list->PushBack(ptr3);

    ExprNode *ptr4 = node_manager_->MakeColumnRefNode("col4", "");
    column_expr_list->PushBack(ptr4);

    ExprListNode *value_expr_list = node_manager_->MakeExprList();
    ExprNode *value1 = node_manager_->MakeConstNode(1);
    ExprNode *value2 = node_manager_->MakeConstNode(2.3f);
    ExprNode *value3 = node_manager_->MakeConstNode(2.3);
    ExprNode *value4 = node_manager_->MakeParameterExpr(1);
    value_expr_list->PushBack(value1);
    value_expr_list->PushBack(value2);
    value_expr_list->PushBack(value3);
    value_expr_list->PushBack(value4);
    ExprListNode *insert_values = node_manager_->MakeExprList();
    insert_values->PushBack(value_expr_list);
    SqlNode *node_ptr = node_manager_->MakeInsertTableNode("", "t1", column_expr_list, insert_values, InsertStmt::InsertMode::DEFAULT_MODE);

    ASSERT_EQ(kInsertStmt, node_ptr->GetType());
    InsertStmt *insert_stmt = dynamic_cast<InsertStmt *>(node_ptr);
    ASSERT_EQ(false, insert_stmt->is_all_);
    ASSERT_EQ(std::vector<std::string>({"col1", "col2", "col3", "col4"}), insert_stmt->columns_);

    auto value = dynamic_cast<ExprListNode *>(insert_stmt->values_[0])->children_;
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[0])->GetInt(), 1);
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[1])->GetFloat(), 2.3f);
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[2])->GetDouble(), 2.3);
    ASSERT_EQ(dynamic_cast<ParameterExpr *>(value[3])->position(), 1);
}
TEST_F(SqlNodeTest, AllNodeTest) {
    {
        auto all_node = node_manager_->MakeAllNode("t1", "db");
        ASSERT_EQ("db", all_node->GetDBName());
        ASSERT_EQ("t1", all_node->GetRelationName());
    }
}
TEST_F(SqlNodeTest, NullFrameHistoryStartEndTest) {
    {
        FrameNode *frame_null = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(kFrameRowsRange, nullptr));
        ASSERT_EQ(INT64_MIN, frame_null->GetHistoryRangeStart());
        ASSERT_EQ(0, frame_null->GetHistoryRangeEnd());
    }
    {
        FrameNode *frame_null = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(kFrameRows, nullptr));
        ASSERT_EQ(INT64_MIN, frame_null->GetHistoryRowsStart());
        ASSERT_EQ(INT64_MIN, frame_null->GetHistoryRowsEnd());
    }
}
TEST_F(SqlNodeTest, FrameHistoryStartEndTest) {
    // RowsRange between preceding 1d and current
    {
        FrameNode *frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRowsRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        ASSERT_EQ(-86400000, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(0, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Range between preceding 1d and current
    {
        FrameNode *frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        ASSERT_EQ(-86400000, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(0, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Range between open preceding 1d and current
    {
        FrameNode *frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kOpenPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        ASSERT_EQ(-86400000 + 1, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(0, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Rows between preceding 100 and current
    {
        FrameNode *frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRows,
            node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(100)),
                                           node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        ASSERT_EQ(0, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(-100, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Rows between open preceding 100 and current
    {
        FrameNode *frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRows,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kOpenPreceding, node_manager_->MakeConstNode(100)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        ASSERT_EQ(0, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(-99, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Merge [-1d, 0] U [100, 0]
    {
        // RowsRange between preceding 1d and current
        FrameNode *range_frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRowsRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        // Rows between preceding 1d and current
        FrameNode *range_frame2 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRows,
            node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(100)),
                                           node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        auto frame3 = node_manager_->MergeFrameNode(range_frame1, range_frame2);
        ASSERT_EQ(kFrameRowsMergeRowsRange, frame3->frame_type());
        ASSERT_EQ(-86400000, frame3->GetHistoryRangeStart());
        ASSERT_EQ(0, frame3->GetHistoryRangeEnd());
        ASSERT_EQ(-100, frame3->GetHistoryRowsStart());
        ASSERT_EQ(0, frame3->GetHistoryRowsEnd());
    }

    // Merge (-1d, 0] U (100, 0]
    {
        // RowsRange between preceding 1d and current
        FrameNode *range_frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRowsRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kOpenPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        // Rows between preceding 1d and current
        FrameNode *range_frame2 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRows,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kOpenPreceding, node_manager_->MakeConstNode(100)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));
        auto frame3 = node_manager_->MergeFrameNode(range_frame1, range_frame2);
        ASSERT_EQ(-86400000 + 1, frame3->GetHistoryRangeStart());
        ASSERT_EQ(0, frame3->GetHistoryRangeEnd());
        ASSERT_EQ(-100 + 1, frame3->GetHistoryRowsStart());
        ASSERT_EQ(0, frame3->GetHistoryRowsEnd());
    }
}
TEST_F(SqlNodeTest, WindowAndFrameNodeMergeTest) {
    // Range between preceding 1d and current
    FrameNode *range_frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        kFrameRange,
        node_manager_->MakeFrameExtent(
            node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
            node_manager_->MakeFrameBound(kCurrent)),
        nullptr));

    // Range between preceding 2d and current
    FrameNode *range_frame2 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        kFrameRange,
        node_manager_->MakeFrameExtent(
            node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(2, node::kDay)),
            node_manager_->MakeFrameBound(kCurrent)),
        nullptr));

    // Range and Range can't be merge
    ASSERT_FALSE(range_frame1->CanMergeWith(range_frame2));

    // RowsRange between preceding 1d and current
    FrameNode *rowsrange_frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        kFrameRowsRange,
        node_manager_->MakeFrameExtent(
            node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
            node_manager_->MakeFrameBound(kCurrent)),
        nullptr));

    // RowsRange between preceding 2d and current
    FrameNode *rowsrange_frame2 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        kFrameRowsRange,
        node_manager_->MakeFrameExtent(
            node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(2, node::kDay)),
            node_manager_->MakeFrameBound(kCurrent)),
        nullptr));

    // Range and Range can be merge
    ASSERT_TRUE(rowsrange_frame1->CanMergeWith(rowsrange_frame2));

    // Rows between preceding 200 and current
    FrameNode *rows_frame1 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        kFrameRows,
        node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(200)),
                                       node_manager_->MakeFrameBound(kCurrent)),
        nullptr));

    // Rows between preceding 100 and current
    FrameNode *rows_frame2 = dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
        kFrameRows,
        node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding, node_manager_->MakeConstNode(100)),
                                       node_manager_->MakeFrameBound(kCurrent)),
        nullptr));

    // Rows and Rows can be merge
    ASSERT_TRUE(rows_frame1->CanMergeWith(rows_frame2));

    // Range and Rows can't be merged
    ASSERT_FALSE(range_frame1->CanMergeWith(rows_frame1));
    ASSERT_FALSE(rows_frame1->CanMergeWith(range_frame1));

    // RowsRange and Rows can be merged
    ASSERT_TRUE(rows_frame1->CanMergeWith(rowsrange_frame1));
    ASSERT_TRUE(rows_frame1->CanMergeWith(rowsrange_frame2));
    ASSERT_TRUE(rowsrange_frame1->CanMergeWith(rows_frame1));
    ASSERT_TRUE(rowsrange_frame1->CanMergeWith(rows_frame2));

    ExprListNode *pk1 = node_manager_->MakeExprList();
    pk1->AddChild(node_manager_->MakeColumnRefNode("col1", "t1"));

    ExprListNode *pk2 = node_manager_->MakeExprList();
    pk2->AddChild(node_manager_->MakeColumnRefNode("col1", "t1"));
    pk2->AddChild(node_manager_->MakeColumnRefNode("col2", "t1"));

    ExprListNode *pk3 = node_manager_->MakeExprList();
    pk1->AddChild(node_manager_->MakeColumnRefNode("col1", "t2"));

    ExprListNode *orders1_exprs = node_manager_->MakeExprList();
    orders1_exprs->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("ts1", "t1"), false));
    ExprNode *orders1 = node_manager_->MakeOrderByNode(orders1_exprs);

    ExprListNode *orders2_exprs = node_manager_->MakeExprList();
    orders2_exprs->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("ts2", "t1"), false));
    ExprNode *orders2 = node_manager_->MakeOrderByNode(orders2_exprs);

    node::SqlNodeList unions1;
    unions1.PushBack(node_manager_->MakeTableNode("ta", ""));
    unions1.PushBack(node_manager_->MakeTableNode("tb", ""));

    node::SqlNodeList unions2;
    unions2.PushBack(node_manager_->MakeTableNode("ta", ""));
    unions2.PushBack(node_manager_->MakeTableNode("tb", ""));

    node::SqlNodeList unions3;
    unions3.PushBack(node_manager_->MakeTableNode("ta", ""));

    WindowDefNode *w1 = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk1, orders1, rows_frame1));
    WindowDefNode *w2 = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk1, orders1, rows_frame2));
    ASSERT_TRUE(w1->CanMergeWith(w2));
    ASSERT_TRUE(w2->CanMergeWith(w1));

    // Window with same union table and pk and orders can be merged
    {
        ASSERT_TRUE(dynamic_cast<WindowDefNode *>(
                        node_manager_->MakeWindowDefNode(&unions1, pk1, orders1, rows_frame1, false, true))
                        ->CanMergeWith(dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(
                            &unions2, pk1, orders1, rows_frame1, false, true))));
    }

    // Window with different pks, can't be merged
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk2, orders1, rows_frame2));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk3, orders1, rows_frame2));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    // Window with different orders, can't be merged
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk1, orders2, rows_frame2));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    // Window with different unions, can't be merged
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(
            node_manager_->MakeWindowDefNode(&unions1, pk1, orders1, rows_frame1, false, false));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    {
        ASSERT_FALSE(dynamic_cast<WindowDefNode *>(
                         node_manager_->MakeWindowDefNode(&unions1, pk1, orders1, rows_frame1, false, true))
                         ->CanMergeWith(dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(
                             &unions3, pk1, orders1, rows_frame1, false, true))));
    }

    {
        ASSERT_FALSE(dynamic_cast<WindowDefNode *>(
                         node_manager_->MakeWindowDefNode(&unions1, pk1, orders1, rows_frame1, false, true))
                         ->CanMergeWith(dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(
                             &unions1, pk1, orders1, rows_frame1, false, false))));
    }

    // Window can't be merged when their frame can't be merge
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk1, orders1, range_frame1));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    // Window can't be merged when open_interval_window are different
    {
        WindowDefNode *w =
            dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(pk1, orders1, range_frame1, true));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }
}

TEST_F(SqlNodeTest, ColumnOfExpressionTest) {
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(dynamic_cast<ExprNode *>(node_manager_->MakeColumnRefNode("c1", "t1")), &columns);
        ASSERT_EQ(1, columns.size());
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c1", "t1"), columns[0]));
    }
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(dynamic_cast<ExprNode *>(node_manager_->MakeCastNode(
                                     kDouble, node_manager_->MakeColumnRefNode("c2", "t1"))),
                                 &columns);
        ASSERT_EQ(1, columns.size());
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c2", "t1"), columns[0]));
    }
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(dynamic_cast<ExprNode *>(node_manager_->MakeBinaryExprNode(
                                     node_manager_->MakeColumnRefNode("c1", "t1"),
                                     node_manager_->MakeColumnRefNode("c2", "t1"), node::kFnOpEq)),
                                 &columns);
        ASSERT_EQ(2, columns.size());
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c1", "t1"), columns[0]));
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c2", "t1"), columns[1]));
    }
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(dynamic_cast<ExprNode *>(node_manager_->MakeBinaryExprNode(
                                     node_manager_->MakeConstNode(1), node_manager_->MakeConstNode(2), node::kFnOpAdd)),
                                 &columns);
        ASSERT_EQ(0, columns.size());
    }
}

TEST_F(SqlNodeTest, IndexVersionNodeTest) {
    {
        auto node = dynamic_cast<IndexVersionNode *>(node_manager_->MakeIndexVersionNode("col1"));
        ASSERT_EQ("col1", node->GetColumnName());
        ASSERT_EQ(1, node->GetCount());
    }
    {
        auto node = dynamic_cast<IndexVersionNode *>(node_manager_->MakeIndexVersionNode("col1", 3));
        ASSERT_EQ("col1", node->GetColumnName());
        ASSERT_EQ(3, node->GetCount());
    }
}

TEST_F(SqlNodeTest, CreateIndexNodeTest) {
    SqlNodeList *index_items = node_manager_->MakeNodeList();
    index_items->PushBack(node_manager_->MakeIndexKeyNode("col4"));
    index_items->PushBack(node_manager_->MakeIndexTsNode("col5"));
    ColumnIndexNode *index_node = dynamic_cast<ColumnIndexNode *>(node_manager_->MakeColumnIndexNode(index_items));
    CreatePlanNode *node = node_manager_->MakeCreateTablePlanNode(
        "", "t1",
        {node_manager_->MakeColumnDescNode("col1", node::kInt32, true),
         node_manager_->MakeColumnDescNode("col2", node::kInt32, true),
         node_manager_->MakeColumnDescNode("col3", node::kFloat, true),
         node_manager_->MakeColumnDescNode("col4", node::kVarchar, true),
         node_manager_->MakeColumnDescNode("col5", node::kTimestamp, true), index_node},
        {node_manager_->MakeReplicaNumNode(3), node_manager_->MakePartitionNumNode(8),
         node_manager_->MakeNode<StorageModeNode>(kMemory)},
        false);
    ASSERT_TRUE(nullptr != node);
    std::vector<std::string> columns;
    std::vector<std::string> indexes;
    ASSERT_TRUE(node->ExtractColumnsAndIndexs(columns, indexes));
    ASSERT_EQ(std::vector<std::string>({"col1 int32", "col2 int32", "col3 float", "col4 string", "col5 timestamp"}),
              columns);
    ASSERT_EQ(std::vector<std::string>({"index1:col4:col5"}), indexes);

    CreateIndexNode *create_index_node =
        dynamic_cast<node::CreateIndexNode *>(node_manager_->MakeCreateIndexNode("index1", "", "t1", index_node));

    ASSERT_TRUE(nullptr != create_index_node);
    std::ostringstream oss;
    create_index_node->Print(oss, "");
    ASSERT_EQ(
        "+-node[kCreateIndexStmt]\n"
        "  +-index_name: index1\n"
        "  +-table_name: t1\n"
        "  +-index:\n"
        "    +-node[kColumnIndex]\n"
        "      +-keys: [col4]\n"
        "      +-ts_col: col5\n"
        "      +-abs_ttl: -2\n"
        "      +-lat_ttl: -2\n"
        "      +-ttl_type: <nil>\n"
        "      +-version_column: <nil>\n"
        "      +-version_count: 0",
        oss.str());
}
TEST_F(SqlNodeTest, FnNodeTest) {
    node::FnNodeList *params = node_manager_->MakeFnListNode();
    params->AddChild(node_manager_->MakeFnParaNode("x", node_manager_->MakeTypeNode(node::kInt32)));
    params->AddChild(node_manager_->MakeFnParaNode("y", node_manager_->MakeTypeNode(node::kInt32)));

    node::FnIfNode *if_node =
        dynamic_cast<node::FnIfNode *>(node_manager_->MakeIfStmtNode(node_manager_->MakeBinaryExprNode(
            node_manager_->MakeUnresolvedExprId("x"), node_manager_->MakeConstNode(1), node::kFnOpGt)));
    node::FnIfBlock *if_block = node_manager_->MakeFnIfBlock(
        if_node,
        node_manager_->MakeFnListNode(node_manager_->MakeReturnStmtNode(node_manager_->MakeBinaryExprNode(
            node_manager_->MakeUnresolvedExprId("x"), node_manager_->MakeUnresolvedExprId("y"), node::kFnOpAdd))));
    std::vector<node::FnNode *> elif_blocks;
    node::FnElifBlock *elif_block = node_manager_->MakeFnElifBlock(
        dynamic_cast<node::FnElifNode *>(node_manager_->MakeElifStmtNode(node_manager_->MakeBinaryExprNode(
            node_manager_->MakeUnresolvedExprId("y"), node_manager_->MakeConstNode(2), node::kFnOpGt))),
        node_manager_->MakeFnListNode(node_manager_->MakeReturnStmtNode(node_manager_->MakeBinaryExprNode(
            node_manager_->MakeUnresolvedExprId("x"), node_manager_->MakeUnresolvedExprId("y"), node::kFnOpMinus))));
    elif_blocks.push_back(elif_block);
    node::FnElseBlock *else_block = node_manager_->MakeFnElseBlock(
        node_manager_->MakeFnListNode(node_manager_->MakeReturnStmtNode(node_manager_->MakeBinaryExprNode(
            node_manager_->MakeUnresolvedExprId("x"), node_manager_->MakeUnresolvedExprId("y"), node::kFnOpMulti))));

    node::FnNodeFnDef *fn_def = dynamic_cast<node::FnNodeFnDef *>(node_manager_->MakeFnDefNode(
        node_manager_->MakeFnHeaderNode("test", params, node_manager_->MakeTypeNode(node::kInt32)),
        node_manager_->MakeFnListNode(node_manager_->MakeFnIfElseBlock(if_block, elif_blocks, else_block))));

    std::ostringstream oss;
    fn_def->Print(oss, "");
    ASSERT_EQ(
        "+-node[kFnDef]\n"
        "  +-header:\n"
        "  |  +-node[kFnHeader]\n"
        "  |    +-func_name: test\n"
        "  |    +-return_type:\n"
        "  |      +-node[kType]\n"
        "  |        +-type: int32\n"
        "  |    +-parameters:\n"
        "  |      +-node[kFnList]\n"
        "  |        +-list[list]:\n"
        "  |          +-0:\n"
        "  |          |  +-node[kFnPara]\n"
        "  |          |    +-x:\n"
        "  |          |      +-node[kType]\n"
        "  |          |        +-type: int32\n"
        "  |          +-1:\n"
        "  |            +-node[kFnPara]\n"
        "  |              +-y:\n"
        "  |                +-node[kType]\n"
        "  |                  +-type: int32\n"
        "  +-block:\n"
        "    +-node[kFnList]\n"
        "      +-list[list]:\n"
        "        +-0:\n"
        "          +-node[kFnIfElseBlock]\n"
        "            +-if:\n"
        "            |  +-node[kFnIfBlock]\n"
        "            |    +-if:\n"
        "            |    |  +-node[kFnIfStmt]\n"
        "            |    |    +-if:\n"
        "            |    |      +-expr[binary]\n"
        "            |    |        +->[list]:\n"
        "            |    |          +-0:\n"
        "            |    |          |  +-expr[id]\n"
        "            |    |          |    +-var: %-1(x)\n"
        "            |    |          +-1:\n"
        "            |    |            +-expr[primary]\n"
        "            |    |              +-value: 1\n"
        "            |    |              +-type: int32\n"
        "            |    +-block:\n"
        "            |      +-node[kFnList]\n"
        "            |        +-list[list]:\n"
        "            |          +-0:\n"
        "            |            +-node[kFnReturnStmt]\n"
        "            |              +-return:\n"
        "            |                +-expr[binary]\n"
        "            |                  +-+[list]:\n"
        "            |                    +-0:\n"
        "            |                    |  +-expr[id]\n"
        "            |                    |    +-var: %-1(x)\n"
        "            |                    +-1:\n"
        "            |                      +-expr[id]\n"
        "            |                        +-var: %-1(y)\n"
        "            +-elif_list[list]:\n"
        "            |  +-0:\n"
        "            |    +-node[kFnElIfBlock]\n"
        "            |      +-elif:\n"
        "            |      |  +-node[kFnElseifStmt]\n"
        "            |      |    +-elif:\n"
        "            |      |      +-expr[binary]\n"
        "            |      |        +->[list]:\n"
        "            |      |          +-0:\n"
        "            |      |          |  +-expr[id]\n"
        "            |      |          |    +-var: %-1(y)\n"
        "            |      |          +-1:\n"
        "            |      |            +-expr[primary]\n"
        "            |      |              +-value: 2\n"
        "            |      |              +-type: int32\n"
        "            |      +-block:\n"
        "            |        +-node[kFnList]\n"
        "            |          +-list[list]:\n"
        "            |            +-0:\n"
        "            |              +-node[kFnReturnStmt]\n"
        "            |                +-return:\n"
        "            |                  +-expr[binary]\n"
        "            |                    +--[list]:\n"
        "            |                      +-0:\n"
        "            |                      |  +-expr[id]\n"
        "            |                      |    +-var: %-1(x)\n"
        "            |                      +-1:\n"
        "            |                        +-expr[id]\n"
        "            |                          +-var: %-1(y)\n"
        "            +-else:\n"
        "              +-node[kFnElseBlock]\n"
        "                +-block:\n"
        "                  +-node[kFnList]\n"
        "                    +-list[list]:\n"
        "                      +-0:\n"
        "                        +-node[kFnReturnStmt]\n"
        "                          +-return:\n"
        "                            +-expr[binary]\n"
        "                              +-*[list]:\n"
        "                                +-0:\n"
        "                                |  +-expr[id]\n"
        "                                |    +-var: %-1(x)\n"
        "                                +-1:\n"
        "                                  +-expr[id]\n"
        "                                    +-var: %-1(y)",
        oss.str());
}
TEST_F(SqlNodeTest, ExprIsConstTest) {
    ASSERT_TRUE(node::ExprIsConst(node_manager_->MakeConstNode(1)));

    ASSERT_TRUE(node::ExprIsConst(node_manager_->MakeBetweenExpr(
        node_manager_->MakeConstNode(10), node_manager_->MakeConstNode(0), node_manager_->MakeConstNode(100), true)));
    ASSERT_FALSE(node::ExprIsConst(node_manager_->MakeBetweenExpr(node_manager_->MakeColumnRefNode("col1", ""),
                                                                  node_manager_->MakeConstNode(0),
                                                                  node_manager_->MakeConstNode(100), true)));

    {
        node::ExprListNode *args = node_manager_->MakeExprList();
        args->AddChild(node_manager_->MakeConstNode("s1"));
        args->AddChild(node_manager_->MakeConstNode("s2"));
        ASSERT_TRUE(node::ExprIsConst(node_manager_->MakeFuncNode("concat", args, nullptr)));
    }
    {
        node::ExprListNode *args = node_manager_->MakeExprList();
        args->AddChild(node_manager_->MakeColumnRefNode("col1", ""));
        node::SqlNode *over = node_manager_->MakeWindowDefNode(
            node_manager_->MakeExprList(node_manager_->MakeColumnRefNode("key1", "")),
            node_manager_->MakeOrderByNode(node_manager_->MakeExprList(
                node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("std_ts", ""), true))),
            node_manager_->MakeFrameNode(node::FrameType::kFrameRows,
                                         node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(kPreceding, 100),
                                                                        node_manager_->MakeFrameBound(kCurrent))));
        ASSERT_FALSE(node::ExprIsConst(node_manager_->MakeFuncNode("sum", args, over)));
    }
    {
        node::ExprListNode *args = node_manager_->MakeExprList();
        args->AddChild(node_manager_->MakeConstNode("s1"));
        args->AddChild(node_manager_->MakeColumnRefNode("col1", ""));
        ASSERT_FALSE(node::ExprIsConst(node_manager_->MakeFuncNode("concat", args, nullptr)));
    }
}
TEST_F(SqlNodeTest, CallExprTest) {
    node::ExprListNode *args = node_manager_->MakeExprList();
    args->AddChild(node_manager_->MakeConstNode("s1"));
    args->AddChild(node_manager_->MakeColumnRefNode("col1", ""));
    node::CallExprNode *node = node_manager_->MakeFuncNode("concat", args, nullptr);
    ASSERT_EQ("concat", node->GetFnDef()->GetName());
    ASSERT_EQ(2, node->GetChildNum());
    std::ostringstream oss;
    node->Print(oss, "");
    DLOG(INFO) << oss.str();
    ASSERT_EQ(
        "+-expr[function]\n"
        "  +-function:\n"
        "  |  [Unresolved](concat)\n"
        "  +-arg[0]:\n"
        "  |  +-expr[primary]\n"
        "  |    +-value: s1\n"
        "  |    +-type: string\n"
        "  +-arg[1]:\n"
        "    +-expr[column ref]\n"
        "      +-relation_name: <nil>\n"
        "      +-column_name: col1",
        oss.str());
}
TEST_F(SqlNodeTest, ColumnIdTest) {
    node::ColumnIdNode *node = node_manager_->MakeColumnIdNode(1);
    ASSERT_EQ(1, node->GetColumnID());
    ASSERT_EQ("#1", node->GenerateExpressionName());
    ASSERT_EQ("#1", node->GetExprString());

    ASSERT_TRUE(node::ExprEquals(node, node));
    ASSERT_TRUE(node::ExprEquals(node, node_manager_->MakeColumnIdNode(1)));

    ASSERT_FALSE(node::ExprEquals(node, node_manager_->MakeColumnIdNode(2)));
    ASSERT_FALSE(node::ExprEquals(node, nullptr));
    ASSERT_FALSE(node::ExprEquals(node, node_manager_->MakeColumnRefNode("col1", "")));
    std::ostringstream oss;
    node->Print(oss, "");
    DLOG(INFO) << oss.str();
    ASSERT_EQ(
        "+-expr[column id]\n"
        "  +-column_id: 1",
        oss.str());
}

TEST_F(SqlNodeTest, QueryTypeNameTest) {
    ASSERT_EQ("kQuerySelect", node::QueryTypeName(node::kQuerySelect));
    ASSERT_EQ("kQuerySetOperation", node::QueryTypeName(node::kQuerySetOperation));
    ASSERT_EQ("kQuerySub", node::QueryTypeName(node::kQuerySub));
}

TEST_F(SqlNodeTest, OrderByNodeTest) {
    // expr list
    ExprListNode *expr_list1 = node_manager_->MakeExprList();
    OrderExpression* order_expression = node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col1", ""),
                                                                           true);
    expr_list1->AddChild(order_expression);
    expr_list1->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col2", ""), true));

    ExprListNode *expr_list2 = node_manager_->MakeExprList();
    expr_list2->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col1", ""), true));
    expr_list2->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col2", ""), true));

    ExprListNode *expr_list3 = node_manager_->MakeExprList();
    expr_list3->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("c1", ""), true));
    expr_list3->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col2", ""), true));

    ExprListNode *expr_list4 = node_manager_->MakeExprList();
    expr_list4->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col1", ""), true));
    expr_list4->AddChild(node_manager_->MakeOrderExpression(node_manager_->MakeColumnRefNode("col2", ""), false));
    ASSERT_TRUE(expr_list1->Equals(expr_list1));
    ASSERT_TRUE(expr_list1->Equals(expr_list2));
    ASSERT_FALSE(expr_list1->Equals(expr_list3));
    ASSERT_FALSE(expr_list1->Equals(expr_list4));

    // order
    ExprNode *order1 = node_manager_->MakeOrderByNode(expr_list1);
    ExprNode *order2 = node_manager_->MakeOrderByNode(expr_list1);
    ExprNode *order3 = node_manager_->MakeOrderByNode(expr_list3);
    ExprNode *order4 = node_manager_->MakeOrderByNode(expr_list4);

    ASSERT_TRUE(order1->Equals(order1));
    ASSERT_TRUE(order1->Equals(order2));
    ASSERT_FALSE(order1->Equals(order3));
    ASSERT_FALSE(order1->Equals(order4));
    ASSERT_EQ("(col1 ASC, col2 ASC)", order1->GetExprString());
    ASSERT_EQ("(col1 ASC, col2 ASC)", order2->GetExprString());
    ASSERT_EQ("(c1 ASC, col2 ASC)", order3->GetExprString());
    ASSERT_EQ("(col1 ASC, col2 DESC)", order4->GetExprString());

    {
        std::ostringstream oss;
        order1->Print(oss, "");
        ASSERT_EQ(
            "+-node[kExpr]\n"
            "  +-order_expressions: (col1 ASC, col2 ASC)",
            oss.str());
    }
    {
        std::ostringstream oss;
        order_expression->Print(oss, "");
        ASSERT_EQ("col1 ASC", order_expression->GetExprString());
        ASSERT_EQ("+-node[kExpr]\n"
            "  +-order_expression: col1 ASC", oss.str());
    }
}
TEST_F(SqlNodeTest, ParameterExprTest) {
    ParameterExpr* parameter_expr1 = node_manager_->MakeParameterExpr(1);
    ParameterExpr* parameter_expr2 = node_manager_->MakeParameterExpr(1);
    ParameterExpr* parameter_expr3 = node_manager_->MakeParameterExpr(2);
    ASSERT_TRUE(node::ExprEquals(parameter_expr1, parameter_expr1));
    ASSERT_TRUE(node::ExprEquals(parameter_expr1, parameter_expr2));
    ASSERT_FALSE(node::ExprEquals(parameter_expr1, parameter_expr3));

    ASSERT_EQ("?1", parameter_expr1->GetExprString());
}
TEST_F(SqlNodeTest, LimitNodeTest) {
    SqlNode *node1 = node_manager_->MakeLimitNode(100);
    SqlNode *node2 = node_manager_->MakeLimitNode(100);
    SqlNode *node3 = node_manager_->MakeLimitNode(200);
    ASSERT_TRUE(node::SqlEquals(node1, node1));
    ASSERT_TRUE(node::SqlEquals(node1, node2));
    ASSERT_FALSE(node::SqlEquals(node1, node3));
    std::ostringstream oss;
    node1->Print(oss, "");
    ASSERT_EQ(
        "+-node[kLimit]\n"
        "  +-limit_cnt: 100",
        oss.str());
}

TEST_F(SqlNodeTest, FrameExtent) {
    // [ ( start_frame_type, star_frame_offset, end_frame_type, end_frame_offset,
    //     start_frame_expect, end_frame_expect ) ]
    std::initializer_list<std::tuple<BoundType, int64_t, BoundType, int64_t, int64_t, int64_t>> cases = {
        { BoundType::kOpenFollowing, 12, BoundType::kOpenPreceding, 12, 13, -13 },
        { BoundType::kOpenPreceding, 12, BoundType::kOpenFollowing, 12, -11, 11 },
        { BoundType::kPreceding, 12, BoundType::kPreceding, 4, -12, -4 },
        { BoundType::kFollowing, 4, BoundType::kFollowing, 12, 4, 12 },
    };

    auto test = [this](BoundType start_frame_type, int64_t off1, BoundType end_frame_type, int64_t off2, int64_t exp1,
                       int64_t exp2) {
        SqlNode* start = node_manager_->MakeFrameBound(start_frame_type, off1);
        SqlNode* end = node_manager_->MakeFrameBound(end_frame_type, off2);
        FrameExtent* ext = node_manager_->MakeFrameExtent(start, end);
        EXPECT_EQ(exp1, ext->GetStartOffset());
        EXPECT_EQ(exp2, ext->GetEndOffset());
    };

    for (auto& val : cases) {
        test(std::get<0>(val), std::get<1>(val), std::get<2>(val), std::get<3>(val), std::get<4>(val),
             std::get<5>(val));
    }
}

}  // namespace node
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
