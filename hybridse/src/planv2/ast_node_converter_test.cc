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

#include <ctime>
#include <memory>
#include <random>
#include <vector>

#include "absl/strings/match.h"
#include "case/sql_case.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing//status_matchers.h"
#include "zetasql/parser/ast_node.h"

namespace hybridse {
namespace plan {
class ASTNodeConverterTest : public ::testing::TestWithParam<sqlcase::SqlCase> {
 public:
    ASTNodeConverterTest() { manager_ = new node::NodeManager(); }
    ~ASTNodeConverterTest() { delete manager_; }

 protected:
    node::NodeManager* manager_;
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(ASTNodeConverterTest);
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

using LiteralCase = std::pair<absl::string_view, std::variant<absl::string_view, int32_t, int64_t>>;
static const std::vector<LiteralCase>& GetCastCases() {
    using T = LiteralCase::second_type;
    static const auto& ret = *new auto(std::vector<LiteralCase>{
        // whitespace, signs never appear in IntLiteral, hence won't tested
        {"0XFFFF", "Un-support hex integer literal: 0XFFFF"},
        {"abc123", "Invalid integer literal<INVALID_ARGUMENT: abc123 (no digitals found)>"},
        {"abc123L", "Invalid integer literal<INVALID_ARGUMENT: abc123 (no digitals found)>"},
        {"12", T{std::in_place_type<int32_t>, 12}},
        {"012", T{std::in_place_type<int32_t>, 12}},
        {"0", T{std::in_place_type<int32_t>, 0}},
        {"9223372036854775807", T{std::in_place_type<int64_t>, INT64_MAX}},
        {"89223372036854775807", "Invalid integer literal<OUT_OF_RANGE: 89223372036854775807 (overflow)>"},
    });
    return ret;
}

class AstLiteralTest: public ::testing::TestWithParam<LiteralCase> {
 protected:
    node::NodeManager nm;
};

INSTANTIATE_TEST_SUITE_P(NumericLiteral, AstLiteralTest, ::testing::ValuesIn(GetCastCases()));

TEST_P(AstLiteralTest, IntegerLiteral) {
    auto [input, result] = GetParam();
    zetasql::ASTIntLiteral expression;
    expression.set_image(std::string(input));
    node::ExprNode* output = nullptr;
    base::Status status = ConvertExprNode(&expression, &nm, &output);
    if (std::holds_alternative<absl::string_view>(result)) {
        EXPECT_FALSE(status.isOK());
        EXPECT_EQ(std::get<absl::string_view>(result), status.msg);
    } else if (std::holds_alternative<int64_t>(result)) {
        EXPECT_TRUE(status.isOK()) << status;
        ASSERT_TRUE(output->GetExprType() == node::kExprPrimary);
        ASSERT_TRUE(dynamic_cast<node::ConstNode*>(output)->GetDataType() == node::DataType::kInt64);
        EXPECT_EQ(std::get<int64_t>(result), dynamic_cast<node::ConstNode*>(output)->GetAsInt64());
    } else if (std::holds_alternative<int32_t>(result)) {
        EXPECT_TRUE(status.isOK()) << status;
        ASSERT_TRUE(output->GetExprType() == node::kExprPrimary);
        ASSERT_TRUE(dynamic_cast<node::ConstNode*>(output)->GetDataType() == node::DataType::kInt32);
        EXPECT_EQ(std::get<int32_t>(result), dynamic_cast<node::ConstNode*>(output)->GetAsInt32());
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
        ASSERT_EQ("Invalid interval literal 1X: invalid interval unit", status.msg);
    }
    {
        zetasql::ASTIntervalLiteral expression;
        expression.set_image("1abds");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid interval literal<INVALID_ARGUMENT: 1abd (digitals with extra non-space chars following)>", status.msg);
    }
}
TEST_F(ASTNodeConverterTest, InvalidASTBoolLiteralTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTBooleanLiteral expression;
        expression.set_image("InvalidBool");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid bool literal: InvalidBool", status.msg);
    }
    {
        zetasql::ASTBooleanLiteral expression;
        expression.set_image("");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQ("Invalid bool literal: ", status.msg);
    }
}

TEST_F(ASTNodeConverterTest, ASTBoolLiteralTest) {
    node::NodeManager node_manager;
    {
        zetasql::ASTBooleanLiteral expression;
        expression.set_image("False");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kBool, dynamic_cast<node::ConstNode*>(output)->GetDataType());
        ASSERT_EQ(false, dynamic_cast<node::ConstNode*>(output)->GetBool());
    }
    {
        zetasql::ASTBooleanLiteral expression;
        expression.set_image("false");
        node::ExprNode* output = nullptr;
        base::Status status = ConvertExprNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
        ASSERT_EQ(node::kExprPrimary, output->GetExprType());
        ASSERT_EQ(node::kBool, dynamic_cast<node::ConstNode*>(output)->GetDataType());
        ASSERT_EQ(false, dynamic_cast<node::ConstNode*>(output)->GetBool());
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
        zetasql::ASTWindowFrameExpr start;
        zetasql::ASTWindowFrameExpr end;
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        zetasql::ASTWindowFrame expression;
        expression.set_unit(zetasql::ASTWindowFrame::ROWS_RANGE);
        expression.AddChildren({&start, &end});
        dynamic_cast<zetasql::ASTNode*>(&expression)->InitFields();
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK()) << status;
    }
    {
        zetasql::ASTWindowFrame expression;
        zetasql::ASTWindowFrameExpr start;
        zetasql::ASTWindowFrameExpr end;
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        expression.AddChildren({&start, &end});
        expression.set_unit(zetasql::ASTWindowFrame::RANGE);
        dynamic_cast<zetasql::ASTNode*>(&expression)->InitFields();
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK()) << status;
    }
    {
        zetasql::ASTWindowFrame expression;
        zetasql::ASTWindowFrameExpr start;
        zetasql::ASTWindowFrameExpr end;
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        expression.AddChildren({&start, &end});
        expression.set_unit(zetasql::ASTWindowFrame::ROWS);
        dynamic_cast<zetasql::ASTNode*>(&expression)->InitFields();
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK()) << status;
    }
}

TEST_F(ASTNodeConverterTest, ConvertCreateTableNodeOkTest) {
    node::NodeManager node_manager;
    {
        const std::string sql =
            "create table t1 (a int, b string, index(key=(a, b), dump='12', ts=column2, ttl=1d, ttl_type=absolute, "
            "version=(column5, 3) ) ) options (replicanum = 3, partitionnum = 3, invalid_option = 'abc', distribution "
            "= [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlAstError, status.code) << status;
    }
    {
        const std::string sql =
            "create table t1 (a int, b string, index(key=(a, b), dump='12', ts=column2, ttl=1d, ttl_type=absolute, "
            "version=(column5, 3) ) ) options (replicanum = 3, partitionnum = 3, distribution "
            "= [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status;
        EXPECT_STREQ("t1", output->GetTableName().c_str());
        EXPECT_EQ(false, output->GetOpIfNotExist());
        auto table_option_list = output->GetTableOptionList();
        node::NodePointVector distribution_list;
        for (auto table_option : table_option_list) {
            switch (table_option->GetType()) {
                case node::kReplicaNum: {
                    ASSERT_EQ(3, dynamic_cast<node::ReplicaNumNode *>(table_option)->GetReplicaNum());
                    break;
                }
                case node::kPartitionNum: {
                    ASSERT_EQ(3, dynamic_cast<node::PartitionNumNode *>(table_option)->GetPartitionNum());
                    break;
                }
                case node::kDistributions: {
                    distribution_list  = dynamic_cast<node::DistributionsNode *>(table_option)->GetDistributionList();
                    break;
                }
                default: {
                    LOG(WARNING) << "can not handle type " << NameOfSqlNodeType(table_option->GetType())
                                 << " for table node";
                }
            }
        }
        ASSERT_EQ(1, distribution_list.size());
        auto partition_mata_nodes = dynamic_cast<hybridse::node::SqlNodeList*>(distribution_list.front());
        const auto& partition_meta_list = partition_mata_nodes->GetList();
        ASSERT_EQ(3, partition_meta_list.size());
        {
            ASSERT_EQ(node::kPartitionMeta, partition_meta_list[0]->GetType());
            node::PartitionMetaNode *partition = dynamic_cast<node::PartitionMetaNode *>(partition_meta_list[0]);
            ASSERT_EQ(node::RoleType::kLeader, partition->GetRoleType());
            ASSERT_EQ("leader1", partition->GetEndpoint());
        }
        {
            ASSERT_EQ(node::kPartitionMeta, partition_meta_list[1]->GetType());
            node::PartitionMetaNode *partition = dynamic_cast<node::PartitionMetaNode *>(partition_meta_list[1]);
            ASSERT_EQ(node::RoleType::kFollower, partition->GetRoleType());
            ASSERT_EQ("fo1", partition->GetEndpoint());
        }
        {
            ASSERT_EQ(node::kPartitionMeta, partition_meta_list[2]->GetType());
            node::PartitionMetaNode *partition = dynamic_cast<node::PartitionMetaNode *>(partition_meta_list[2]);
            ASSERT_EQ(node::RoleType::kFollower, partition->GetRoleType());
            ASSERT_EQ("fo2", partition->GetEndpoint());
        }
    }
    {
        const std::string sql =
            "create table if not exists t1 (a i16, b float32, index(key=a, ignored_key='seb', ts=b, ttl=(1h, 1800), "
            "ttl_type=latest, version=a ) ) options (replicanum = 2, partitionnum = 5, "
            "distribution = [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status;
        EXPECT_STREQ("t1", output->GetTableName().c_str());
        EXPECT_EQ(true, output->GetOpIfNotExist());
        auto table_option_list = output->GetTableOptionList();
        for (auto table_option : table_option_list) {
            if (table_option->GetType() == node::kReplicaNum) {
                ASSERT_EQ(2, dynamic_cast<node::ReplicaNumNode *>(table_option)->GetReplicaNum());
            } else if (table_option->GetType() == node::kPartitionNum) {
                ASSERT_EQ(5, dynamic_cast<node::PartitionNumNode *>(table_option)->GetPartitionNum());
            }
        }
    }
    {
        const std::string sql =
            "create table if not exists t3 (a int32, b timestamp, index(key=a, ignored_key='seb', ts=b, ttl=1800, "
            "ttl_type=absorlat, version=a ) ) options (replicanum = 4, partitionnum = 5, "
            "distribution = [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status;
        EXPECT_STREQ("t3", output->GetTableName().c_str());
        EXPECT_EQ(true, output->GetOpIfNotExist());
        auto table_option_list = output->GetTableOptionList();
        for (auto table_option : table_option_list) {
            if (table_option->GetType() == node::kReplicaNum) {
                ASSERT_EQ(4, dynamic_cast<node::ReplicaNumNode *>(table_option)->GetReplicaNum());
            } else if (table_option->GetType() == node::kPartitionNum) {
                ASSERT_EQ(5, dynamic_cast<node::PartitionNumNode *>(table_option)->GetPartitionNum());
            }
        }
    }
    {
        // empty table element and option list
        const std::string sql = "create table t4;";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status;
        EXPECT_STREQ("t4", output->GetTableName().c_str());
    }

    {
        const std::string sql =
            "create table if not exists t3 (a int32, b timestamp, index(key=a, ignored_key='seb', ts=b, ttl=1800, "
            "ttl_type=absorlat, version=a ) ) options (replicanum = 4, partitionnum = 5, "
            "distribution = [ ('leader1', ['fo1', 'fo2']), ('leader2', ['fo3', 'fo4']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code);
    }
}

TEST_F(ASTNodeConverterTest, ConvertDeleteNodeTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql, bool expect) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTDeleteStatement>());

        const auto delete_stmt = statement->GetAsOrDie<zetasql::ASTDeleteStatement>();
        node::SqlNode* delete_node = nullptr;
        auto s = ConvertStatement(delete_stmt, &node_manager, &delete_node);
        EXPECT_EQ(expect, s.isOK());
    };
    expect_converted("delete from t1", false);
    expect_converted("delete from job", false);
    expect_converted("delete from job 111", true);
    expect_converted("delete job 222", true);
    expect_converted("delete from t1 where c1 = 'aa'", true);
    expect_converted("delete from job where c1 = 'aa'", true);
    expect_converted("delete from db1.t1 where c1 = 'aa'", true);
    expect_converted("delete from t2 where c1 > 'aa' and c2 = 123", true);
    expect_converted("delete from t1 where c1 = 'aa' and c2 = ?", true);
    expect_converted("delete from t1 where c1 = ?", true);
    expect_converted("delete from t1 where c1 = ? or c2 = ?", true);
}

TEST_F(ASTNodeConverterTest, ConvertCreateProcedureOKTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateProcedureStatement>());

        const auto create_sp = statement->GetAsOrDie<zetasql::ASTCreateProcedureStatement>();
        node::CreateSpStmt* stmt;
        auto s = ConvertCreateProcedureNode(create_sp, &node_manager, &stmt);
        EXPECT_EQ(common::kOk, s.code);
    };

    const std::string sql1 = R"sql(
        CREATE OR REPLACE TEMP PROCEDURE IF NOT EXISTS procedure_name()
        OPTIONS()
        BEGIN
        END;
        )sql";

    // valid statement with multiple arguments
    const std::string sql2 = R"sql(
    CREATE PROCEDURE procedure_name(
      param_a string,
      param_b int32
      )
    BEGIN
    END;
    )sql";

    // with select query
    const std::string sql3 = R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
          SELECT 1;
        END;
    )sql";

    // with select union query
    const std::string sql4 = R"sql(
        CREATE PROCEDURE procedure_name(
            param_a i16,
            param_b timestamp,
            param_c date,
            param_d double
        )
        BEGIN
          SELECT 1 UNION ALL SELECT 2;
        END;
    )sql";
    const std::string sql5 = R"sql(
        CREATE PROCEDURE procedure_name(
            param_a i64,
            param_b timestamp,
            param_c smallint,
            param_d double
        )
        BEGIN
          SELECT 1 UNION DISTINCT SELECT 2 UNION DISTINCT SELECT 3;
        END;
    )sql";

    expect_converted(sql1);
    expect_converted(sql2);
    expect_converted(sql3);
    expect_converted(sql4);
    expect_converted(sql5);
}

TEST_F(ASTNodeConverterTest, ConvertCreateProcedureFailTest) {
    node::NodeManager node_manager;

    auto expect_converted = [&](absl::string_view sql, int code, absl::string_view msg) {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateProcedureStatement>());

        const auto create_sp = statement->GetAsOrDie<zetasql::ASTCreateProcedureStatement>();
        node::CreateSpStmt* stmt;
        auto s = ConvertCreateProcedureNode(create_sp, &node_manager, &stmt);
        EXPECT_EQ(code, s.code);
        EXPECT_TRUE(absl::StrContains(s.msg, msg)) << s << "\nexpect msg: " << msg;
    };

    // unsupported param type
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name(
            param_a i64,
            param_b timestamp,
            param_c smallint,
            param_d ANY TYPE
        )
        BEGIN
          SELECT 1 UNION DISTINCT SELECT 2 UNION DISTINCT SELECT 3;
        END;
        )sql",
                     common::kSqlAstError, "Un-support templated_parameter or tvf_schema type");

    // unknown param type
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name(
            param_a i64,
            param_b timestamp,
            param_c smallint,
            param_d unknown_type
        )
        BEGIN
          SELECT 1 UNION DISTINCT SELECT 2 UNION DISTINCT SELECT 3;
        END;
        )sql",
                     common::kTypeError, "Unknow DataType identifier: unknown_type");

    // unsupport param type
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name(
            param_c bigint,
            param_d ARRAY <int>
        )
        BEGIN
          SELECT 1 UNION DISTINCT SELECT 2;
        END;
        )sql",
                     common::kSqlAstError, "Un-support: func parameter accept only basic type, but get ARRAY<INT32>");

    // unsupport set operation
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name(
            param_c bigint,
            param_d string
        )
        BEGIN
          SELECT 1 EXCEPT DISTINCT SELECT 2;
        END;
        )sql",
                     common::kSqlAstError, "Un-support set operation: EXCEPT DISTINCT");

    // unsupport statement type
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
          DECLARE ABC INTERVAL;
        END;
    )sql",
                     common::kSqlAstError, "Un-support statement type inside ASTBeginEndBlock: VariableDeclaration");
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
            BEGIN select 1; END;
        END;
    )sql",
                     common::kSqlAstError, "Un-support statement type inside ASTBeginEndBlock: BeginEndBlock");
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
            select 1;
            select 2;
        END;
    )sql",
                     common::kSqlAstError, "Un-support multiple statements inside ASTBeginEndBlock");
}

TEST_F(ASTNodeConverterTest, ConvertCreateFunctionOKTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql, node::CreateFunctionNode** output) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateFunctionStatement>());

        const auto create_function = statement->GetAsOrDie<zetasql::ASTCreateFunctionStatement>();
        auto s = ConvertCreateFunctionNode(create_function, &node_manager, output);
        EXPECT_EQ(common::kOk, s.code);
    };

    std::string sql1 = "CREATE FUNCTION fun (x INT) RETURNS INT OPTIONS (PATH='/tmp/libmyfun.so');";
    node::CreateFunctionNode* create_fun_stmt = nullptr;
    expect_converted(sql1, &create_fun_stmt);
    ASSERT_EQ(create_fun_stmt->Name(), "fun");
    ASSERT_FALSE(create_fun_stmt->IsAggregate());
    ASSERT_EQ(create_fun_stmt->GetReturnType()->base(), node::DataType::kInt32);
    const node::NodePointVector& args = create_fun_stmt->GetArgsType();
    ASSERT_EQ(args.size(), 1);
    ASSERT_EQ((dynamic_cast<node::TypeNode*>(args.front()))->base(), node::DataType::kInt32);
    auto option = create_fun_stmt->Options();
    ASSERT_EQ(option->size(), 1);
    ASSERT_EQ(option->begin()->first, "PATH");
    ASSERT_EQ(option->begin()->second->GetAsString(), "/tmp/libmyfun.so");

    std::string sql2 = "CREATE AGGREGATE FUNCTION fun1 (x BIGINT) RETURNS STRING OPTIONS (PATH='/tmp/libmyfun.so');";
    create_fun_stmt = nullptr;
    expect_converted(sql2, &create_fun_stmt);
    ASSERT_EQ(create_fun_stmt->GetArgsType().size(), 1);
    ASSERT_EQ((dynamic_cast<node::TypeNode*>(create_fun_stmt->GetArgsType().front()))->base(), node::DataType::kInt64);
    ASSERT_EQ(create_fun_stmt->Name(), "fun1");
    ASSERT_TRUE(create_fun_stmt->IsAggregate());
    ASSERT_EQ(create_fun_stmt->GetReturnType()->base(), node::DataType::kVarchar);
}

TEST_F(ASTNodeConverterTest, ConvertCreateIndexOKTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateIndexStatement>());

        const auto create_index = statement->GetAsOrDie<zetasql::ASTCreateIndexStatement>();
        node::CreateIndexNode* stmt;
        auto s = ConvertCreateIndexStatement(create_index, &node_manager, &stmt);
        EXPECT_EQ(common::kOk, s.code);
    };

    const std::string sql1 = R"sql(
        CREATE INDEX index1 ON t1 (col1, col2)
        OPTIONS(ts=std_ts, ttl_type=absolute, ttl=30d);
        )sql";
    expect_converted(sql1);

    const std::string sql2 = R"sql(
        CREATE INDEX index1 ON db1.t1 (col1, col2)
        OPTIONS(ts=std_ts, ttl_type=absolute, ttl=30d);
        )sql";
    expect_converted(sql2);
    expect_converted("CREATE INDEX index1 ON db1.t1 (col1);");
}

TEST_F(ASTNodeConverterTest, ConvertCreateIndexFailTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql, const int code, std::string msg) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        node::SqlNode* stmt;
        auto s = ConvertStatement(statement, &node_manager, &stmt);
        EXPECT_EQ(code, s.code);
        EXPECT_EQ(msg, s.msg) << s << "\nexpect msg: " << msg;
    };
    {
        const std::string sql = R"sql(
        CREATE INDEX index1 ON t1 (col1 ASC, col2 DESC)
        OPTIONS(ts=std_ts, ttl_type=absolute, ttl=30d);
        )sql";
        expect_converted(sql, common::kSqlAstError, "Un-support descending index key");
    }
    {
        const std::string sql = R"sql(
        CREATE INDEX index1 ON t1 (col1 DESC, col2 DESC)
        OPTIONS(ts=std_ts, ttl_type=absolute, ttl=30d);
        )sql";
        expect_converted(sql, common::kSqlAstError, "Un-support descending index key");
    }
}

TEST_F(ASTNodeConverterTest, ConvertInsertStmtOKTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTInsertStatement>());

        const auto index_stmt = statement->GetAsOrDie<zetasql::ASTInsertStatement>();
        node::InsertStmt* stmt;
        auto s = ConvertInsertStatement(index_stmt, &node_manager, &stmt);
        EXPECT_EQ(common::kOk, s.code);
    };
    {
        const std::string sql = R"sql(
        INSERT into t1 values (1, 2L, 3.0f, 4.0, "hello", "world", "2021-05-23")
        )sql";
        expect_converted(sql);
    }
    {
        const std::string sql = R"sql(
        INSERT into t1 (col1, col2, col3, col4, col5, col6)values (1, 2L, 3.0f, 4.0, "hello", "world", "2021-05-23")
        )sql";
        expect_converted(sql);
    }
    {
        const std::string sql = R"sql(
        INSERT into t1 values (1, 2L, ?, ?, "hello", ?, ?)
        )sql";
        expect_converted(sql);
    }
    {
        const std::string sql = R"sql(
        INSERT into db1.t1 values (1, 2L, 3.0f, 4.0, "hello", "world", "2021-05-23")
        )sql";
        expect_converted(sql);
    }
}

TEST_F(ASTNodeConverterTest, ConvertInsertStmtFailTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql, const int code, std::string msg) -> void {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTInsertStatement>());

        const auto index_stmt = statement->GetAsOrDie<zetasql::ASTInsertStatement>();
        node::InsertStmt* stmt;
        auto s = ConvertInsertStatement(index_stmt, &node_manager, &stmt);
        EXPECT_EQ(code, s.code);
        EXPECT_EQ(msg, s.msg) << s << "\nexpect msg: " << msg;
    };
    {
        const std::string sql = R"sql(
        INSERT into t1 values (1, @ a, @ b)
        )sql";
        expect_converted(sql, common::kSqlAstError, "Un-support Named Parameter Expression a");
    }
    {
        const std::string sql = R"sql(
        INSERT into t1 values (1, 2L, aaa)
        )sql";
        expect_converted(sql, common::kSqlAstError, "Un-support insert statement with un-const value");
    }
}
TEST_F(ASTNodeConverterTest, ConvertStmtFailTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql, const int code, const std::string& msg) {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();

        node::SqlNode* stmt;
        auto s = ConvertStatement(statement, &node_manager, &stmt);
        EXPECT_EQ(code, s.code);
        EXPECT_STREQ(msg.c_str(), s.msg.c_str()) << s;
    };

    expect_converted(R"sql(
        SHOW procedurxs;
    )sql",
                     common::kSqlAstError, "Un-support SHOW: procedurxs");

    expect_converted(R"sql(
        SHOW create procedure ab.cd.name;
    )sql",
                     common::kSqlAstError, "Invalid target name: ab.cd.name");

    // Non-support local variable
    expect_converted(R"sql(
        SET local_var = 'xxxx'
    )sql",
                     common::kSqlAstError,
                     "Un-support statement type: SingleAssignment");

    // Non-support parameter variable currently
    expect_converted(R"sql(
        SET @user_var = 'xxxx'
    )sql",
                     common::kSqlAstError,
                     "Un-support statement type: ParameterAssignment");

    expect_converted(R"sql(
        SET @@level1.level2.var = 'xxxx'
    )sql",
                     common::kSqlAstError,
                     "Non-support system variable with more than 2 path levels. Try @@global.var_name or "
                     "@@session.var_name or @@var_name");
    expect_converted(R"sql(
        SET @@unknow.var = 'xxxx'
    )sql",
                     common::kSqlAstError,
                     "Non-support system variable under unknow scope, try @@global or @@session scope");
    expect_converted(R"sql(
        SHOW GLOBAL VARIABLES LIKE 'execute%'
    )sql",
                     common::kSqlAstError,
                     "Non-support LIKE in SHOW GLOBAL VARIABLES statement");
}

TEST_F(ASTNodeConverterTest, ConvertCreateTableNodeErrorTest) {
    node::NodeManager node_manager;
    {
        // invalid datatype
        const std::string sql = "create table if not exists t (a invalid_type) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kTypeError, status.code);
    }
    {
        // not supported schema
        const std::string sql = "create table t (a Array<int64>) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlAstError, status.code);
    }
    {
        // not supported table element
        const std::string sql = "create table t (a int64, primary key (a)) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlAstError, status.code);
    }
    {
        // not supported index key option value type
        const std::string sql = "create table t (a int64, index(key=['a'])) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlAstError, status.code);
    }
    {
        // not supported index ttl option value type
        const std::string sql = "create table t (a int64, index(ttl=['12'])) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlAstError, status.code);
    }
    {
        // not supported index version option value type
        const std::string sql = "create table t (a int64, index(version=['nonon'])) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlAstError, status.code);
    }
    {
        // not supported table option value type
        const std::string sql = "create table t (a int64) options (distribution = ['a', 'b']) ";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code);
    }
}

TEST_F(ASTNodeConverterTest, AstStringLiteralToStringTest) {
    zetasql::ASTStringLiteral literal;
    literal.set_string_value("random");
    std::string output;
    auto status = AstStringLiteralToString(&literal, &output);
    ASSERT_EQ(status.code, common::kOk);
    ASSERT_STREQ("random", output.c_str());
}

TEST_F(ASTNodeConverterTest, ASTIntLiteralToNumberTest) {
    zetasql::ASTIntLiteral literal;
    int64_t output;

    {
        std::mt19937 rng(std::time(0));
        int32_t v1 = rng();
        literal.set_image(std::to_string(v1));
        auto status = ASTIntLiteralToNum(&literal, &output);
        EXPECT_EQ(status.code, common::kOk) << status.str();
        EXPECT_EQ(v1, output);
    }

    std::mt19937_64 rng_64(std::time(0));

    {
        int64_t v2 = rng_64();
        literal.set_image(std::to_string(v2) + 'l');
        output = 0;
        auto s2 = ASTIntLiteralToNum(&literal, &output);
        EXPECT_EQ(s2.code, common::kOk) << s2.str();
        EXPECT_EQ(v2, output);
    }

    {
        int64_t v3 = 18;
        std::stringstream ss;
        ss << "0x" << std::hex << v3;
        literal.set_image(ss.str());
        output = 0;
        auto s3 = ASTIntLiteralToNum(&literal, &output);
        EXPECT_EQ(s3.code, common::kOk) << s3.str();
        EXPECT_EQ(v3, output);
    }
}

TEST_F(ASTNodeConverterTest, ASTIntervalLiteralToNumberTest) {
    {
        std::string expr = "18m";
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseExpression(expr, zetasql::ParserOptions(), &parser_output));
        ASSERT_TRUE(parser_output->expression()->Is<zetasql::ASTIntervalLiteral>());

        int64_t val = 0;
        node::DataType unit;
        auto status = ASTIntervalLIteralToNum(parser_output->expression(), &val, &unit);
        ASSERT_EQ(common::kOk, status.code);
        ASSERT_EQ(18, val);
        ASSERT_EQ(node::DataType::kMinute, unit);
    }
    {
        std::string expr = "30s";
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseExpression(expr, zetasql::ParserOptions(), &parser_output));
        ASSERT_TRUE(parser_output->expression()->Is<zetasql::ASTIntervalLiteral>());

        int64_t val = 0;
        node::DataType unit;
        auto status = ASTIntervalLIteralToNum(parser_output->expression(), &val, &unit);
        ASSERT_EQ(common::kOk, status.code);
        ASSERT_EQ(30, val);
        ASSERT_EQ(node::DataType::kSecond, unit);
    }
    {
        std::string expr = "45h";
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseExpression(expr, zetasql::ParserOptions(), &parser_output));
        ASSERT_TRUE(parser_output->expression()->Is<zetasql::ASTIntervalLiteral>());

        int64_t val = 0;
        node::DataType unit;
        auto status = ASTIntervalLIteralToNum(parser_output->expression(), &val, &unit);
        ASSERT_EQ(common::kOk, status.code);
        ASSERT_EQ(45, val);
        ASSERT_EQ(node::DataType::kHour, unit);
    }
    {
        std::string expr = "1d";
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseExpression(expr, zetasql::ParserOptions(), &parser_output));
        ASSERT_TRUE(parser_output->expression()->Is<zetasql::ASTIntervalLiteral>());

        int64_t val = 0;
        node::DataType unit;
        auto status = ASTIntervalLIteralToNum(parser_output->expression(), &val, &unit);
        ASSERT_EQ(common::kOk, status.code);
        ASSERT_EQ(1, val);
        ASSERT_EQ(node::DataType::kDay, unit);
    }
    {
        zetasql::ASTIntervalLiteral literal;
        literal.set_image("12x");
        int64_t val = 0;
        node::DataType unit;
        auto status = ASTIntervalLIteralToNum(&literal, &val, &unit);
        ASSERT_EQ(common::kTypeError, status.code);
    }
}
TEST_F(ASTNodeConverterTest, ConvertTypeFailTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string& sql, const int code, const std::string& msg) {
      std::unique_ptr<zetasql::ParserOutput> parser_output;
      ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
      const auto* statement = parser_output->statement();

      node::SqlNode* stmt;
      auto s = ConvertStatement(statement, &node_manager, &stmt);
      EXPECT_EQ(code, s.code);
      EXPECT_STREQ(msg.c_str(), s.msg.c_str()) << s;
    };

    expect_converted(R"sql(
        SELECT cast(col1 as TYPE_UNKNOW);
    )sql",
                     common::kTypeError, "Unknow DataType identifier: TYPE_UNKNOW");
}
// expect tree string equal for converted CreateStmt
TEST_P(ASTNodeConverterTest, SqlNodeTreeEqual) {
    const auto& sql_case = GetParam();
    auto& sql = sql_case.sql_str();

    if (!sql_case.expect().success_) {
        // skip case that expect failure, ASTNodeConverterTest can't ensure the convert
        // will fail in `ParseStatement` or `ConvertStatement`, those failure case will
        // check in planner_v2_test
        return;
    }

    std::unique_ptr<zetasql::ParserOutput> parser_output;
    zetasql::ParserOptions parser_opts;
    zetasql::LanguageOptions language_opts;
    language_opts.EnableLanguageFeature(zetasql::FEATURE_V_1_3_COLUMN_DEFAULT_VALUE);
    parser_opts.set_language_options(&language_opts);
    ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, parser_opts, &parser_output));
    const auto* statement = parser_output->statement();
    DLOG(INFO) << "\n" << statement->DebugString();
    node::SqlNode* output;
    base::Status status;
    status = ConvertStatement(statement, manager_, &output);
    EXPECT_EQ(common::kOk, status.code) << status;
    if (status.isOK() && !sql_case.expect().node_tree_str_.empty()) {
        EXPECT_EQ(sql_case.expect().node_tree_str_, output->GetTreeString()) << output->GetTreeString();
    }
}
const std::vector<std::string> FILTERS({"logical-plan-unsupport", "parser-unsupport", "zetasql-unsupport"});

INSTANTIATE_TEST_SUITE_P(ASTCreateStatementTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/create.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTInsertStatementTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/insert.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTCmdStatementTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/cmd.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTBackQuoteIDTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/back_quote_identifier.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTFilterStatementTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTSimpleSelectTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTJoinTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTHavingStatementTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/having_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTHWindowQueryTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(ASTUnionQueryTest, ASTNodeConverterTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/union_query.yaml", FILTERS)));
}  // namespace plan
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
