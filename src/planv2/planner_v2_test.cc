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

#include "planv2/planner_v2.h"
#include <memory>
#include <utility>
#include <vector>
#include "case/sql_case.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
namespace hybridse {
namespace plan {

using hybridse::node::NodeManager;
using hybridse::node::PlanNode;
using hybridse::node::SqlNode;
using hybridse::node::SqlNodeList;
using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"logical-plan-unsupport", "parser-unsupport", "zetasql-unsupport"});
class PlannerV2Test : public ::testing::TestWithParam<SqlCase> {
 public:
    PlannerV2Test() { manager_ = new NodeManager(); }

    ~PlannerV2Test() { delete manager_; }

 protected:
    NodeManager *manager_;
};

INSTANTIATE_TEST_CASE_P(SqlSimpleQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlRenameQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/rename_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlWindowQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlDistinctParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/distinct_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlWhereParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlGroupParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/group_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlHavingParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/having_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlOrderParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/order_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlJoinParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

// INSTANTIATE_TEST_CASE_P(SqlUnionParse, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/union_query.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlSubQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/sub_query.yaml", FILTERS)));

// INSTANTIATE_TEST_CASE_P(UdfParse, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/udf.yaml", FILTERS)));
//
// INSTANTIATE_TEST_CASE_P(SQLCreate, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/create.yaml", FILTERS)));
//
// INSTANTIATE_TEST_CASE_P(SQLInsert, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/insert.yaml", FILTERS)));
//
// INSTANTIATE_TEST_CASE_P(SQLCmdParserTest, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/cmd.yaml", FILTERS)));
TEST_P(PlannerV2Test, PlannerSucessTest) {
    std::string sqlstr = GetParam().sql_str();
    std::cout << sqlstr << std::endl;

    std::unique_ptr<zetasql::ParserOutput> parser_output;
    base::Status status;
    auto zetasql_status = zetasql::ParseScript(sqlstr, zetasql::ParserOptions(),
                                               zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output);
    zetasql::ErrorLocation location;
    GetErrorLocation(zetasql_status, &location);
    ZETASQL_ASSERT_OK(zetasql_status) << "ERROR:" << zetasql::FormatError(zetasql_status) << "\n"
                                      << GetErrorStringWithCaret(sqlstr, location);
    const zetasql::ASTScript *script = parser_output->script();
    std::cout << "script node: \n" << script->DebugString();

    SimplePlannerV2 *planner_ptr = new SimplePlannerV2(manager_);
    node::PlanNodeList plan_trees;
    ASSERT_EQ(0, planner_ptr->CreateASTScriptPlan(script, plan_trees, status)) << status;
    LOG(INFO) << "logical plan:\n";
    for (auto tree : plan_trees) {
        LOG(INFO) << "statement : " << *tree << std::endl;
    }
}
TEST_P(PlannerV2Test, PlannerClusterOptTest) {
    auto sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    std::cout << sqlstr << std::endl;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode();
        return;
    }
    std::unique_ptr<zetasql::ParserOutput> parser_output;
    base::Status status;
    auto zetasql_status = zetasql::ParseScript(sqlstr, zetasql::ParserOptions(),
                                               zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output);
    zetasql::ErrorLocation location;
    GetErrorLocation(zetasql_status, &location);
    ZETASQL_ASSERT_OK(zetasql_status) << "ERROR:\n" << GetErrorStringWithCaret(sqlstr, location);
    const zetasql::ASTScript *script = parser_output->script();
    std::cout << "script node: \n" << script->DebugString();

    SimplePlannerV2 *planner_ptr = new SimplePlannerV2(manager_, false, true);
    node::PlanNodeList plan_trees;
    ASSERT_EQ(0, planner_ptr->CreateASTScriptPlan(script, plan_trees, status)) << status;
    LOG(INFO) << "logical plan:\n";
    for (auto tree : plan_trees) {
        LOG(INFO) << "statement : " << *tree << std::endl;
    }
}

class PlannerV2ErrorTest : public ::testing::TestWithParam<SqlCase> {
 public:
    PlannerV2ErrorTest() { manager_ = new NodeManager(); }

    ~PlannerV2ErrorTest() { delete manager_; }

 protected:
    NodeManager *manager_;
};
INSTANTIATE_TEST_CASE_P(SqlErrorQuery, PlannerV2ErrorTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/error_query.yaml", FILTERS)));
INSTANTIATE_TEST_CASE_P(SqlUnsupporQuery, PlannerV2ErrorTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/error_unsupport_sql.yaml", FILTERS)));

INSTANTIATE_TEST_CASE_P(SqlErrorRequestQuery, PlannerV2ErrorTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/error_request_query.yaml", FILTERS)));

TEST_P(PlannerV2ErrorTest, RequestModePlanErrorTest) {
    auto sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    std::cout << sqlstr << std::endl;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "request-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode();
        return;
    }
    std::unique_ptr<zetasql::ParserOutput> parser_output;
    base::Status status;
    auto zetasql_status = zetasql::ParseScript(sqlstr, zetasql::ParserOptions(),
                                               zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output);
    zetasql::ErrorLocation location;
    GetErrorLocation(zetasql_status, &location);
    ZETASQL_ASSERT_OK(zetasql_status) << "ERROR:" << zetasql::FormatError(zetasql_status) << "\n"
                                      << GetErrorStringWithCaret(sqlstr, location);
    const zetasql::ASTScript *script = parser_output->script();
    std::cout << "script node: \n" << script->DebugString();

    SimplePlannerV2 *planner_ptr = new SimplePlannerV2(manager_, false);
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(0 != planner_ptr->CreateASTScriptPlan(script, plan_trees, status)) << status;
}
}  // namespace plan
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
