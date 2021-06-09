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

#include "planv2/ast_node_converter.h"
#include "gtest/gtest.h"

namespace hybridse {
namespace plan {

class ASTNodeConverterTest : public ::testing::Test {
 public:
    ASTNodeConverterTest() {}

    ~ASTNodeConverterTest() {}
};

TEST_F(ASTNodeConverterTest, UnSupportBinaryOp) {
    zetasql::ASTNullLiteral null1;
    zetasql::ASTNullLiteral null2;
    zetasql::ASTBinaryExpression binary_expression;
    binary_expression.AddChildren({&null1, &null2});
    binary_expression.set_op(zetasql::ASTBinaryExpression::Op::NOT_SET);
    node::NodeManager node_manager;
    node::ExprNode* output;
    base::Status status = ConvertExprNode(&binary_expression, &node_manager, &output);
    ASSERT_FALSE(status.isOK());
    ASSERT_EQ("Unsupport binary operator: <UNKNOWN OPERATOR>", status.msg);
}
TEST_F(ASTNodeConverterTest, InvalidASTIntLiteralTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTIntLiteral expression;
        expression.set_image("0XFFFF");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Un-support hex integer literal: 0XFFFF", status.msg);
    }
    {
        zetasql::ASTIntLiteral expression;
        expression.set_image("abc123");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid integer literal: abc123", status.msg);
    }
    {
        zetasql::ASTIntLiteral expression;
        expression.set_image("abc123L");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid long integer literal: abc123L", status.msg);
    }
}
TEST_F(ASTNodeConverterTest, InvalidASTFloatLiteralTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTFloatLiteral expression;
        expression.set_image("abc123.456F");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid float literal: abc123.456F", status.msg);
    }
    {
        zetasql::ASTFloatLiteral expression;
        expression.set_image("abc123.456");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid double literal: abc123.456", status.msg);
    }
}
TEST_F(ASTNodeConverterTest, InvalidASTIntervalLiteralTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1s");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kSecond, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1m");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kMinute, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1h");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kHour, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1D");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kDay, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1S");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kSecond, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1M");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kMinute, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1H");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kHour, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1D");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kDay, dynamic_cast<node::ConstNode*>(output)->GetDataType());
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1X");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid interval literal: 1X", status.msg);
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1abds");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid interval literal: 1abds", status.msg);
    }
}
TEST_F(ASTNodeConverterTest, InvalidASTBollLiteralTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTBooleanLiteral expression;
        expression.set_image("InvalidBool");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid bool literal: InvalidBool", status.msg);
    }
}

TEST_F(ASTNodeConverterTest, ConvertQueryNodeNullTest) {
    node::NodeManager node_manager;
    {
        node::QueryNode* output = nullptr;
        base::Status status = ConvertQueryNode(nullptr, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_TRUE(nullptr == output);
    }
}

TEST_F(ASTNodeConverterTest, ConvertFrameBoundTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTWindowFrameExpr expression;
        expression.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        node::FrameBound* output = nullptr;
        base::Status status = ConvertFrameBound(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrameExpr expression;
        zetasql::ASTIntLiteral offset;
        offset.set_image("1000");
        expression.AddChildren({&offset});
        expression.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_PRECEDING);
        node::FrameBound* output = nullptr;
        base::Status status = ConvertFrameBound(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrameExpr expression;
        zetasql::ASTIntLiteral offset;
        offset.set_image("1000");
        expression.AddChildren({&offset});
        expression.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_PRECEDING);
        expression.set_is_open_boundary(true);
        node::FrameBound* output = nullptr;
        base::Status status = ConvertFrameBound(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrameExpr expression;
        zetasql::ASTIntLiteral offset;
        offset.set_image("1000");
        expression.AddChildren({&offset});
        expression.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_FOLLOWING);
        expression.set_is_open_boundary(true);
        node::FrameBound* output = nullptr;
        base::Status status = ConvertFrameBound(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrameExpr expression;
        expression.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        node::FrameBound* output = nullptr;
        base::Status status = ConvertFrameBound(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrameExpr expression;
        expression.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        node::FrameBound* output = nullptr;
        base::Status status = ConvertFrameBound(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
}

TEST_F(ASTNodeConverterTest, ConvertFrameNodeTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTWindowFrame expression;
        zetasql::ASTWindowFrameExpr start;
        zetasql::ASTWindowFrameExpr end;
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING);
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        expression.AddChildren({&start, &end});
        expression.set_unit(zetasql::ASTWindowFrame::ROWS_RANGE);
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrame expression;
        zetasql::ASTWindowFrameExpr start;
        zetasql::ASTWindowFrameExpr end;
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING);
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        expression.AddChildren({&start, &end});
        expression.set_unit(zetasql::ASTWindowFrame::RANGE);
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
    {
        zetasql::ASTWindowFrame expression;
        zetasql::ASTWindowFrameExpr start;
        zetasql::ASTWindowFrameExpr end;
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING);
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        expression.AddChildren({&start, &end});
        expression.set_unit(zetasql::ASTWindowFrame::ROWS);
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
}
}  // namespace plan
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
