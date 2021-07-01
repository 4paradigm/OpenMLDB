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
        ASSERT_EQ("Invalid integer literal: abc123L", status.msg);
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
        start.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING);
        end.set_boundary_type(zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW);
        expression.AddChildren({&start, &end});
        expression.set_unit(zetasql::ASTWindowFrame::RANGE);
        dynamic_cast<zetasql::ASTNode*>(&expression)->InitFields();
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
        dynamic_cast<zetasql::ASTNode*>(&expression)->InitFields();
        node::FrameNode* output = nullptr;
        base::Status status = ConvertFrameNode(&expression, &node_manager, &output);
        ASSERT_TRUE(status.isOK());
    }
}

TEST_F(ASTNodeConverterTest, ConvertCreateTableNodeOkTest) {
    node::NodeManager node_manager;
    {
        const std::string sql =
            "create table t1 (a int, b string, index(key=(a, b), dump='12', ts=column2, ttl=1d, ttl_type=absolute, "
            "version=(column5, 3) ) ) options (replicanum = 3, partitionnum = 3, ignored_option = 'abc', distribution "
            "= [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status.msg << status.trace;
        EXPECT_STREQ("t1", output->GetTableName().c_str());
        EXPECT_EQ(false, output->GetOpIfNotExist());
        EXPECT_EQ(3, output->GetPartitionNum());
        EXPECT_EQ(3, output->GetReplicaNum());
        EXPECT_EQ(3, output->GetDistributionList().size());
        {
            EXPECT_EQ(node::kPartitionMeta, output->GetDistributionList()[0]->GetType());
            node::PartitionMetaNode* partition_mata =
                dynamic_cast<node::PartitionMetaNode*>(output->GetDistributionList()[0]);
            ASSERT_EQ(node::RoleType::kLeader, partition_mata->GetRoleType());
            ASSERT_EQ("leader1", partition_mata->GetEndpoint());
        }
        {
            EXPECT_EQ(node::kPartitionMeta, output->GetDistributionList()[0]->GetType());
            node::PartitionMetaNode* partition_mata =
                dynamic_cast<node::PartitionMetaNode*>(output->GetDistributionList()[1]);
            ASSERT_EQ(node::RoleType::kFollower, partition_mata->GetRoleType());
            ASSERT_EQ("fo1", partition_mata->GetEndpoint());
        }
        {
            EXPECT_EQ(node::kPartitionMeta, output->GetDistributionList()[0]->GetType());
            node::PartitionMetaNode* partition_mata =
                dynamic_cast<node::PartitionMetaNode*>(output->GetDistributionList()[2]);
            ASSERT_EQ(node::RoleType::kFollower, partition_mata->GetRoleType());
            ASSERT_EQ("fo2", partition_mata->GetEndpoint());
        }
    }
    {
        const std::string sql =
            "create table if not exists t1 (a i16, b float32, index(key=a, ignored_key='seb', ts=b, ttl=(1h, 1800), "
            "ttl_type=latest, version=a ) ) options (replicanum = 2, partitionnum = 5, ignored_option = 'abc', "
            "distribution = [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status.msg << status.trace;
        EXPECT_STREQ("t1", output->GetTableName().c_str());
        EXPECT_EQ(true, output->GetOpIfNotExist());
        EXPECT_EQ(5, output->GetPartitionNum());
        EXPECT_EQ(2, output->GetReplicaNum());
    }
    {
        const std::string sql =
            "create table if not exists t3 (a int32, b timestamp, index(key=a, ignored_key='seb', ts=b, ttl=1800, "
            "ttl_type=absorlat, version=a ) ) options (replicanum = 4, partitionnum = 5, ignored_option = 'abc', "
            "distribution = [ ('leader1', ['fo1', 'fo2']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kOk, status.code) << status.msg << status.trace;
        EXPECT_STREQ("t3", output->GetTableName().c_str());
        EXPECT_EQ(true, output->GetOpIfNotExist());
        EXPECT_EQ(5, output->GetPartitionNum());
        EXPECT_EQ(4, output->GetReplicaNum());
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
        EXPECT_EQ(common::kOk, status.code) << status.msg << status.trace;
        EXPECT_STREQ("t4", output->GetTableName().c_str());
    }

    {
        const std::string sql =
            "create table if not exists t3 (a int32, b timestamp, index(key=a, ignored_key='seb', ts=b, ttl=1800, "
            "ttl_type=absorlat, version=a ) ) options (replicanum = 4, partitionnum = 5, ignored_option = 'abc', "
            "distribution = [ ('leader1', ['fo1', 'fo2']), ('leader2', ['fo3', 'fo4']) ]);";

        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateTableStatement>());

        const auto create_stmt = statement->GetAsOrDie<zetasql::ASTCreateTableStatement>();
        node::CreateStmt* output = nullptr;
        auto status = ConvertCreateTableNode(create_stmt, &node_manager, &output);
        EXPECT_EQ(common::kSqlError, status.code);
    }
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

    auto expect_converted = [&](const std::string& sql, const int code, const std::string& msg) {
        std::unique_ptr<zetasql::ParserOutput> parser_output;
        ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
        const auto* statement = parser_output->statement();
        ASSERT_TRUE(statement->Is<zetasql::ASTCreateProcedureStatement>());

        const auto create_sp = statement->GetAsOrDie<zetasql::ASTCreateProcedureStatement>();
        node::CreateSpStmt* stmt;
        auto s = ConvertCreateProcedureNode(create_sp, &node_manager, &stmt);
        EXPECT_EQ(code, s.code);
        EXPECT_TRUE(boost::contains(s.msg, msg)) << s << "\nexpect msg: " << msg;
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
                     common::kSqlError, "Un-support templated_parameter or tvf_schema type");

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
                     common::kSqlError, "Un-support type: ArrayType");

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
                     common::kSqlError, "Un-support set operation: EXCEPT DISTINCT");

    // unsupport statement type
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
          DECLARE ABC INTERVAL;
        END;
    )sql",
                     common::kSqlError, "Un-support statement type inside ASTBeginEndBlock: VariableDeclaration");
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
            BEGIN select 1; END;
        END;
    )sql",
                     common::kSqlError, "Un-support statement type inside ASTBeginEndBlock: BeginEndBlock");
    expect_converted(R"sql(
        CREATE PROCEDURE procedure_name()
        BEGIN
            select 1;
            select 2;
        END;
    )sql",
                     common::kSqlError, "Un-support multiple statements inside ASTBeginEndBlock");
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
        expect_converted(sql, common::kSqlError, "Un-support descending index key");
    }
    {
        const std::string sql = R"sql(
        CREATE INDEX index1 ON t1 (col1 DESC, col2 DESC)
        OPTIONS(ts=std_ts, ttl_type=absolute, ttl=30d);
        )sql";
        expect_converted(sql, common::kSqlError, "Un-support descending index key");
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
        stmt->Print(std::cout, "");
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
        expect_converted(sql, common::kSqlError, "Un-support Named Parameter Expression a");
    }
    {
        const std::string sql = R"sql(
        INSERT into t1 values (1, 2L, aaa)
        )sql";
        expect_converted(sql, common::kSqlError, "Un-support insert statement with un-const value");
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
        EXPECT_STREQ(msg.c_str(), s.msg.c_str()) << s.msg << s.trace;
    };

    expect_converted(R"sql(
        ALTER TABLE foo ALTER COLUMN bar SET DATA TYPE STRING;
    )sql",
                     common::kSqlError, "Un-support statement type: AlterTableStatement");
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
        EXPECT_EQ(common::kSqlError, status.code);
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
        EXPECT_EQ(common::kSqlError, status.code);
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
        EXPECT_EQ(common::kSqlError, status.code);
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
        EXPECT_EQ(common::kSqlError, status.code);
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
        EXPECT_EQ(common::kSqlError, status.code);
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
        EXPECT_EQ(common::kSqlError, status.code);
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
        ASSERT_EQ(status.code, common::kOk);
        ASSERT_EQ(v1, output);
    }

    std::mt19937_64 rng_64(std::time(0));

    {
        int64_t v2 = rng_64();
        literal.set_image(std::to_string(v2) + 'l');
        output = 0;
        auto s2 = ASTIntLiteralToNum(&literal, &output);
        ASSERT_EQ(s2.code, common::kOk);
        ASSERT_EQ(v2, output);
    }

    {
        int64_t v3 = 18;
        std::stringstream ss;
        ss << "0x" << std::hex << v3;
        literal.set_image(ss.str());
        output = 0;
        auto s3 = ASTIntLiteralToNum(&literal, &output);
        ASSERT_EQ(s3.code, common::kOk);
        ASSERT_EQ(v3, output);
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

// expect tree string equal for converted CreateStmt
TEST_P(ASTNodeConverterTest, SqlNodeTreeEqual) {
    auto& sql = GetParam().sql_str();

    std::unique_ptr<zetasql::ParserOutput> parser_output;
    ZETASQL_ASSERT_OK(zetasql::ParseStatement(sql, zetasql::ParserOptions(), &parser_output));
    const auto* statement = parser_output->statement();
    DLOG(INFO) << "\n" << statement->DebugString();
    node::SqlNode* output;
    base::Status status;
    status = ConvertStatement(statement, manager_, &output);
    EXPECT_EQ(common::kOk, status.code) << status.msg << status.trace;
    if (status.isOK() && !GetParam().expect().node_tree_str_.empty()) {
        LOG(INFO) << "\n" << output->GetTreeString();
        EXPECT_EQ(GetParam().expect().node_tree_str_, output->GetTreeString());
    }
}
const std::vector<std::string> FILTERS({"logical-plan-unsupport", "parser-unsupport", "zetasql-unsupport"});
INSTANTIATE_TEST_CASE_P(ASTCreateStatementTest, ASTNodeConverterTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/create.yaml", FILTERS)));
INSTANTIATE_TEST_CASE_P(ASTInsertStatementTest, ASTNodeConverterTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/insert.yaml", FILTERS)));
INSTANTIATE_TEST_CASE_P(ASTCmdStatementTest, ASTNodeConverterTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/cmd.yaml", FILTERS)));

}  // namespace plan
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
