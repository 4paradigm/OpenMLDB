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

    std::cout << struct_expr << std::endl;
    ASSERT_EQ(kExprStruct, struct_expr.GetExprType());
    ASSERT_EQ(&methods, struct_expr.GetMethods());
    ASSERT_EQ(&fileds, struct_expr.GetFileds());
}

TEST_F(SqlNodeTest, MakeColumnRefNodeTest) {
    SqlNode *node = node_manager_->MakeColumnRefNode("col", "t");
    ColumnRefNode *columnnode = dynamic_cast<ColumnRefNode *>(node);
    std::cout << *node << std::endl;
    ASSERT_EQ(kExprColumnRef, columnnode->GetExprType());
    ASSERT_EQ("t", columnnode->GetRelationName());
    ASSERT_EQ("col", columnnode->GetColumnName());
}

TEST_F(SqlNodeTest, MakeGetFieldExprTest) {
    auto row = node_manager_->MakeExprIdNode("row");
    auto node = node_manager_->MakeGetFieldExpr(row, 0);
    std::cout << *node << std::endl;
    ASSERT_EQ(kExprGetField, node->GetExprType());
    ASSERT_EQ("0", node->GetColumnName());
    ASSERT_EQ(kExprId, node->GetRow()->GetExprType());
}

TEST_F(SqlNodeTest, MakeConstNodeStringTest) {
    ConstNode *node_ptr = dynamic_cast<ConstNode *>(
        node_manager_->MakeConstNode("parser string test"));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kVarchar, node_ptr->GetDataType());
    ASSERT_STREQ("parser string test", node_ptr->GetStr());
}

TEST_F(SqlNodeTest, MakeConstNodeIntTest) {
    ConstNode *node_ptr =
        dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kInt32, node_ptr->GetDataType());
    ASSERT_EQ(1, node_ptr->GetInt());
}

TEST_F(SqlNodeTest, MakeConstNodeLongTest) {
    int64_t val1 = 1;
    int64_t val2 = 864000000L;
    ConstNode *node_ptr =
        dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(val1));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kInt64, node_ptr->GetDataType());
    ASSERT_EQ(val1, node_ptr->GetLong());

    node_ptr = dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(val2));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kInt64, node_ptr->GetDataType());
    ASSERT_EQ(val2, node_ptr->GetLong());
}

TEST_F(SqlNodeTest, MakeConstNodeDoubleTest) {
    ConstNode *node_ptr =
        dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1.989E30));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kDouble, node_ptr->GetDataType());
    ASSERT_EQ(1.989E30, node_ptr->GetDouble());
}

TEST_F(SqlNodeTest, MakeConstNodeFloatTest) {
    ConstNode *node_ptr =
        dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1.234f));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(hybridse::node::kFloat, node_ptr->GetDataType());
    ASSERT_EQ(1.234f, node_ptr->GetFloat());
}

TEST_F(SqlNodeTest, MakeConstNodeTTLTypeTest) {
    // TODO(chenjing): assign DataType::kTTL to TTLType
    auto node = node_manager_->MakeConstNode(static_cast<int64_t>(10),
                                             node::TTLType::kLatest);
    ASSERT_EQ(node::DataType::kInt64, node->GetDataType());
    ASSERT_EQ(10L, node->GetAsInt64());
    ASSERT_EQ(node::TTLType::kLatest, node->GetTTLType());
}
TEST_F(SqlNodeTest, TimeIntervalConstGetMillisTest) {
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(
            node_manager_->MakeConstNode(1, node::DataType::kSecond));
        ASSERT_EQ(hybridse::node::kSecond, node_ptr->GetDataType());
        ASSERT_EQ(1000L, node_ptr->GetMillis());
    }
    {
        ConstNode *node_ptr =
            dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(1.234f));
        std::cout << *node_ptr << std::endl;
        ASSERT_EQ(hybridse::node::kFloat, node_ptr->GetDataType());
        ASSERT_EQ(-1, node_ptr->GetMillis());
    }
}
TEST_F(SqlNodeTest, StringConstGetAsDateTest) {
    {
        ConstNode *node_ptr = dynamic_cast<ConstNode *>(
            node_manager_->MakeConstNode("2021-05-01"));
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
        ConstNode *node_ptr =
            dynamic_cast<ConstNode *>(node_manager_->MakeConstNode(""));
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

    ExprNode *ptr2 = node_manager_->MakeColumnRefNode("col1", "");
    ExprListNode *orders = node_manager_->MakeExprList();
    orders->PushBack(ptr2);

    int64_t maxsize = 0;
    SqlNode *frame = node_manager_->MakeFrameNode(
        kFrameRange,
        node_manager_->MakeFrameExtent(
            node_manager_->MakeFrameBound(kPreceding),
            node_manager_->MakeFrameBound(kPreceding, val)),
        maxsize);
    WindowDefNode *node_ptr =
        dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(
            partitions, node_manager_->MakeOrderByNode(orders, true), frame));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
    //

    ASSERT_EQ("(keycol)", ExprString(node_ptr->GetPartitions()));
    ASSERT_EQ(("(col1) ASC"), ExprString(node_ptr->GetOrders()));
    ASSERT_EQ(frame, node_ptr->GetFrame());
    ASSERT_EQ("", node_ptr->GetName());
}

TEST_F(SqlNodeTest, MakeWindowDefNodetWithNameTest) {
    WindowDefNode *node_ptr =
        dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode("w1"));
    std::cout << *node_ptr << std::endl;
    ASSERT_EQ(kWindowDef, node_ptr->GetType());
    ASSERT_EQ(NULL, node_ptr->GetFrame());
    ASSERT_EQ("w1", node_ptr->GetName());
}

TEST_F(SqlNodeTest, MakeExternalFnDefNodeTest) {
    auto *node_ptr = dynamic_cast<ExternalFnDefNode *>(
        node_manager_->MakeUnresolvedFnDefNode("extern_f"));
    ASSERT_EQ(kExternalFnDef, node_ptr->GetType());
    ASSERT_EQ("extern_f", node_ptr->function_name());
}

TEST_F(SqlNodeTest, MakeUdafDefNodeTest) {
    auto zero = node_manager_->MakeConstNode(1);
    auto f1 = dynamic_cast<ExternalFnDefNode *>(
        node_manager_->MakeUnresolvedFnDefNode("f1"));
    auto f2 = dynamic_cast<ExternalFnDefNode *>(
        node_manager_->MakeUnresolvedFnDefNode("f2"));
    auto f3 = dynamic_cast<ExternalFnDefNode *>(
        node_manager_->MakeUnresolvedFnDefNode("f3"));
    auto *udaf = dynamic_cast<UdafDefNode *>(
        node_manager_->MakeUdafDefNode("udaf", {}, zero, f1, f2, f3));
    ASSERT_EQ(kUdafDef, udaf->GetType());
    ASSERT_EQ(true, udaf->init_expr()->Equals(zero));
    ASSERT_EQ(true, udaf->update_func()->Equals(f1));
    ASSERT_EQ(true, udaf->merge_func()->Equals(f2));
    ASSERT_EQ(true, udaf->output_func()->Equals(f3));
}

TEST_F(SqlNodeTest, NewFrameNodeTest) {
    FrameNode *node_ptr =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            node::kFrameRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(kPreceding),
                node_manager_->MakeFrameBound(kPreceding, 86400000)),
            node_manager_->MakeConstNode(100)));
    std::cout << *node_ptr << std::endl;

    ASSERT_EQ(kFrames, node_ptr->GetType());
    ASSERT_EQ(kFrameRange, node_ptr->frame_type());

    // assert frame node start
    ASSERT_EQ(kFrameBound, node_ptr->frame_range()->start()->GetType());
    FrameBound *start =
        dynamic_cast<FrameBound *>(node_ptr->frame_range()->start());
    ASSERT_EQ(kPreceding, start->bound_type());
    ASSERT_EQ(0L, start->GetOffset());

    ASSERT_EQ(kFrameBound, node_ptr->frame_range()->end()->GetType());
    FrameBound *end =
        dynamic_cast<FrameBound *>(node_ptr->frame_range()->end());
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
    ExprNode *value4 = node_manager_->MakeConstNodePlaceHolder();
    value_expr_list->PushBack(value1);
    value_expr_list->PushBack(value2);
    value_expr_list->PushBack(value3);
    value_expr_list->PushBack(value4);
    ExprListNode *insert_values = node_manager_->MakeExprList();
    insert_values->PushBack(value_expr_list);
    SqlNode *node_ptr = node_manager_->MakeInsertTableNode(
        "t1", column_expr_list, insert_values);

    ASSERT_EQ(kInsertStmt, node_ptr->GetType());
    InsertStmt *insert_stmt = dynamic_cast<InsertStmt *>(node_ptr);
    ASSERT_EQ(false, insert_stmt->is_all_);
    ASSERT_EQ(std::vector<std::string>({"col1", "col2", "col3", "col4"}),
              insert_stmt->columns_);

    auto value =
        dynamic_cast<ExprListNode *>(insert_stmt->values_[0])->children_;
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[0])->GetInt(), 1);
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[1])->GetFloat(), 2.3f);
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[2])->GetDouble(), 2.3);
    ASSERT_EQ(dynamic_cast<ConstNode *>(value[3])->GetDataType(),
              hybridse::node::kPlaceholder);
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
        FrameNode *frame_null = dynamic_cast<FrameNode *>(
            node_manager_->MakeFrameNode(kFrameRowsRange, nullptr));
        ASSERT_EQ(INT64_MIN, frame_null->GetHistoryRangeStart());
        ASSERT_EQ(0, frame_null->GetHistoryRangeEnd());
    }
    {
        FrameNode *frame_null = dynamic_cast<FrameNode *>(
            node_manager_->MakeFrameNode(kFrameRows, nullptr));
        ASSERT_EQ(INT64_MIN, frame_null->GetHistoryRowsStart());
        ASSERT_EQ(INT64_MIN, frame_null->GetHistoryRowsEnd());
    }
}
TEST_F(SqlNodeTest, FrameHistoryStartEndTest) {
    // RowsRange between preceding 1d and current
    {
        FrameNode *frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRowsRange,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kPreceding,
                        node_manager_->MakeConstNode(1, node::kDay)),
                    node_manager_->MakeFrameBound(kCurrent)),
                nullptr));
        ASSERT_EQ(-86400000, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(0, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Range between preceding 1d and current
    {
        FrameNode *frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRange,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kPreceding,
                        node_manager_->MakeConstNode(1, node::kDay)),
                    node_manager_->MakeFrameBound(kCurrent)),
                nullptr));
        ASSERT_EQ(-86400000, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(0, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Range between open preceding 1d and current
    {
        FrameNode *frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRange,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kOpenPreceding,
                        node_manager_->MakeConstNode(1, node::kDay)),
                    node_manager_->MakeFrameBound(kCurrent)),
                nullptr));
        ASSERT_EQ(-86400000 + 1, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(0, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Rows between preceding 100 and current
    {
        FrameNode *frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRows,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kPreceding, node_manager_->MakeConstNode(100)),
                    node_manager_->MakeFrameBound(kCurrent)),
                nullptr));
        ASSERT_EQ(0, frame1->GetHistoryRangeStart());
        ASSERT_EQ(0, frame1->GetHistoryRangeEnd());
        ASSERT_EQ(-100, frame1->GetHistoryRowsStart());
        ASSERT_EQ(0, frame1->GetHistoryRowsEnd());
    }

    // Rows between open preceding 100 and current
    {
        FrameNode *frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRows,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kOpenPreceding, node_manager_->MakeConstNode(100)),
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
        FrameNode *range_frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRowsRange,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kPreceding,
                        node_manager_->MakeConstNode(1, node::kDay)),
                    node_manager_->MakeFrameBound(kCurrent)),
                nullptr));
        // Rows between preceding 1d and current
        FrameNode *range_frame2 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRows,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kPreceding, node_manager_->MakeConstNode(100)),
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
        FrameNode *range_frame1 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRowsRange,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kOpenPreceding,
                        node_manager_->MakeConstNode(1, node::kDay)),
                    node_manager_->MakeFrameBound(kCurrent)),
                nullptr));
        // Rows between preceding 1d and current
        FrameNode *range_frame2 =
            dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
                kFrameRows,
                node_manager_->MakeFrameExtent(
                    node_manager_->MakeFrameBound(
                        kOpenPreceding, node_manager_->MakeConstNode(100)),
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
    FrameNode *range_frame1 =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(
                    kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));

    // Range between preceding 2d and current
    FrameNode *range_frame2 =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(
                    kPreceding, node_manager_->MakeConstNode(2, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));

    // Range and Range can't be merge
    ASSERT_FALSE(range_frame1->CanMergeWith(range_frame2));

    // RowsRange between preceding 1d and current
    FrameNode *rowsrange_frame1 =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRowsRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(
                    kPreceding, node_manager_->MakeConstNode(1, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));

    // RowsRange between preceding 2d and current
    FrameNode *rowsrange_frame2 =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRowsRange,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(
                    kPreceding, node_manager_->MakeConstNode(2, node::kDay)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));

    // Range and Range can be merge
    ASSERT_TRUE(rowsrange_frame1->CanMergeWith(rowsrange_frame2));

    // Rows between preceding 200 and current
    FrameNode *rows_frame1 =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRows,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(
                    kPreceding, node_manager_->MakeConstNode(200)),
                node_manager_->MakeFrameBound(kCurrent)),
            nullptr));

    // Rows between preceding 100 and current
    FrameNode *rows_frame2 =
        dynamic_cast<FrameNode *>(node_manager_->MakeFrameNode(
            kFrameRows,
            node_manager_->MakeFrameExtent(
                node_manager_->MakeFrameBound(
                    kPreceding, node_manager_->MakeConstNode(100)),
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
    orders1_exprs->AddChild(node_manager_->MakeColumnRefNode("ts1", "t1"));
    ExprNode *orders1 = node_manager_->MakeOrderByNode(orders1_exprs, false);

    ExprListNode *orders2_exprs = node_manager_->MakeExprList();
    orders2_exprs->AddChild(node_manager_->MakeColumnRefNode("ts2", "t1"));
    ExprNode *orders2 = node_manager_->MakeOrderByNode(orders2_exprs, false);

    node::SqlNodeList unions1;
    unions1.PushBack(node_manager_->MakeTableNode("ta", ""));
    unions1.PushBack(node_manager_->MakeTableNode("tb", ""));

    node::SqlNodeList unions2;
    unions2.PushBack(node_manager_->MakeTableNode("ta", ""));
    unions2.PushBack(node_manager_->MakeTableNode("tb", ""));

    node::SqlNodeList unions3;
    unions3.PushBack(node_manager_->MakeTableNode("ta", ""));

    WindowDefNode *w1 = dynamic_cast<WindowDefNode *>(
        node_manager_->MakeWindowDefNode(pk1, orders1, rows_frame1));
    WindowDefNode *w2 = dynamic_cast<WindowDefNode *>(
        node_manager_->MakeWindowDefNode(pk1, orders1, rows_frame2));
    ASSERT_TRUE(w1->CanMergeWith(w2));
    ASSERT_TRUE(w2->CanMergeWith(w1));

    // Window with same union table and pk and orders can be merged
    {
        ASSERT_TRUE(
            dynamic_cast<WindowDefNode *>(
                node_manager_->MakeWindowDefNode(&unions1, pk1, orders1,
                                                 rows_frame1, false, true))
                ->CanMergeWith(dynamic_cast<WindowDefNode *>(
                    node_manager_->MakeWindowDefNode(
                        &unions2, pk1, orders1, rows_frame1, false, true))));
    }

    // Window with different pks, can't be merged
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(
            node_manager_->MakeWindowDefNode(pk2, orders1, rows_frame2));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(
            node_manager_->MakeWindowDefNode(pk3, orders1, rows_frame2));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    // Window with different orders, can't be merged
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(
            node_manager_->MakeWindowDefNode(pk1, orders2, rows_frame2));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    // Window with different unions, can't be merged
    {
        WindowDefNode *w =
            dynamic_cast<WindowDefNode *>(node_manager_->MakeWindowDefNode(
                &unions1, pk1, orders1, rows_frame1, false, false));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    {
        ASSERT_FALSE(
            dynamic_cast<WindowDefNode *>(
                node_manager_->MakeWindowDefNode(&unions1, pk1, orders1,
                                                 rows_frame1, false, true))
                ->CanMergeWith(dynamic_cast<WindowDefNode *>(
                    node_manager_->MakeWindowDefNode(
                        &unions3, pk1, orders1, rows_frame1, false, true))));
    }

    {
        ASSERT_FALSE(
            dynamic_cast<WindowDefNode *>(
                node_manager_->MakeWindowDefNode(&unions1, pk1, orders1,
                                                 rows_frame1, false, true))
                ->CanMergeWith(dynamic_cast<WindowDefNode *>(
                    node_manager_->MakeWindowDefNode(
                        &unions1, pk1, orders1, rows_frame1, false, false))));
    }

    // Window can't be merged when their frame can't be merge
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(
            node_manager_->MakeWindowDefNode(pk1, orders1, range_frame1));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }

    // Window can't be merged when open_interval_window are different
    {
        WindowDefNode *w = dynamic_cast<WindowDefNode *>(
            node_manager_->MakeWindowDefNode(pk1, orders1, range_frame1, true));
        ASSERT_FALSE(w1->CanMergeWith(w));
    }
}

TEST_F(SqlNodeTest, ColumnOfExpressionTest) {
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(
            dynamic_cast<ExprNode *>(
                node_manager_->MakeColumnRefNode("c1", "t1")),
            &columns);
        ASSERT_EQ(1, columns.size());
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c1", "t1"),
                               columns[0]));
    }
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(
            dynamic_cast<ExprNode *>(node_manager_->MakeCastNode(
                kDouble, node_manager_->MakeColumnRefNode("c2", "t1"))),
            &columns);
        ASSERT_EQ(1, columns.size());
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c2", "t1"),
                               columns[0]));
    }
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(
            dynamic_cast<ExprNode *>(node_manager_->MakeBinaryExprNode(
                node_manager_->MakeColumnRefNode("c1", "t1"),
                node_manager_->MakeColumnRefNode("c2", "t1"), node::kFnOpEq)),
            &columns);
        ASSERT_EQ(2, columns.size());
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c1", "t1"),
                               columns[0]));
        ASSERT_TRUE(ExprEquals(node_manager_->MakeColumnRefNode("c2", "t1"),
                               columns[1]));
    }
    {
        std::vector<const node::ExprNode *> columns;
        node::ColumnOfExpression(
            dynamic_cast<ExprNode *>(node_manager_->MakeBinaryExprNode(
                node_manager_->MakeConstNode(1),
                node_manager_->MakeConstNode(2), node::kFnOpAdd)),
            &columns);
        ASSERT_EQ(0, columns.size());
    }
}

TEST_F(SqlNodeTest, IndexVersionNodeTest) {
    {
        auto node = dynamic_cast<IndexVersionNode *>(
            node_manager_->MakeIndexVersionNode("col1"));
        ASSERT_EQ("col1", node->GetColumnName());
        ASSERT_EQ(1, node->GetCount());
    }
    {
        auto node = dynamic_cast<IndexVersionNode *>(
            node_manager_->MakeIndexVersionNode("col1", 3));
        ASSERT_EQ("col1", node->GetColumnName());
        ASSERT_EQ(3, node->GetCount());
    }
}

}  // namespace node
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
