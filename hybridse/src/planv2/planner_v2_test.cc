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
#include "plan/plan_api.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
namespace hybridse {
namespace plan {

using hybridse::node::NodeManager;
using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"logical-plan-unsupport", "parser-unsupport", "zetasql-unsupport"});
class PlannerV2Test : public ::testing::TestWithParam<SqlCase> {
 public:
    PlannerV2Test() { manager_ = new NodeManager(); }

    ~PlannerV2Test() { delete manager_; }

 protected:
    NodeManager *manager_;
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(PlannerV2Test);
INSTANTIATE_TEST_SUITE_P(SqlConstQueryParse, PlannerV2Test,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/const_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlSimpleQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlRenameQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/rename_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlWindowQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlDistinctParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/distinct_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlWhereParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/where_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlGroupParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/group_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlHavingParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/having_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlOrderParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/order_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlJoinParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

// INSTANTIATE_TEST_SUITE_P(SqlUnionParse, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/union_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SqlSubQueryParse, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/sub_query.yaml", FILTERS)));

// INSTANTIATE_TEST_SUITE_P(UdfParse, PlannerV2Test,
//                        testing::ValuesIn(sqlcase::InitCases("cases/plan/udf.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(NativeUdafFunction, PlannerV2Test,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/table_aggregation_query.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SQLCreate, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/create.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SQLInsert, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/insert.yaml", FILTERS)));

INSTANTIATE_TEST_SUITE_P(SQLCmdParserTest, PlannerV2Test,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/cmd.yaml", FILTERS)));
TEST_P(PlannerV2Test, PlannerSucessTest) {
    const auto& param = GetParam();
    std::string sqlstr = param.sql_str();
    std::cout << sqlstr << std::endl;
    base::Status status;
    node::PlanNodeList plan_trees;
    EXPECT_EQ(param.expect().success_, PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, manager_, status))
        << status;
    if (!param.expect().plan_tree_str_.empty()) {
        // HACK: weak implementation, but usually it works
        EXPECT_EQ(param.expect().plan_tree_str_, plan_trees.at(0)->GetTreeString());
    }
}
TEST_P(PlannerV2Test, PlannerClusterOnlineServingOptTest) {
    auto sql_case = GetParam();
    std::string sqlstr = sql_case.sql_str();
    std::cout << sqlstr << std::endl;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode();
        return;
    }
    base::Status status;
    node::PlanNodeList plan_trees;
    // TODO(ace): many tests defined in 'cases/plan/' do not pass, should annotated in yaml file
    plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, manager_, status, false, true);
}

TEST_F(PlannerV2Test, SimplePlannerCreatePlanTest) {
    node::NodePointVector list;
    base::Status status;
    std::string sql = "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 FROM t1 limit 10;";
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees.front();
    std::cout << *(plan_ptr) << std::endl;
    //     validate select plan
    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
    plan_ptr = plan_ptr->GetChildren()[0];

    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
    node::LimitPlanNode *limit_ptr = (node::LimitPlanNode *)plan_ptr;
    // validate project list based on current row
    ASSERT_EQ(10, limit_ptr->GetLimitCnt());
    ASSERT_EQ(node::kPlanTypeProject, limit_ptr->GetChildren().at(0)->GetType());

    node::ProjectPlanNode *project_plan_node = (node::ProjectPlanNode *)limit_ptr->GetChildren().at(0);
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    node::ProjectListNode *project_list =
        dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[0]);

    ASSERT_EQ(0u, dynamic_cast<node::ProjectNode *>(project_list->GetProjects()[0])->GetPos());
    ASSERT_EQ(1u, dynamic_cast<node::ProjectNode *>(project_list->GetProjects()[1])->GetPos());
    ASSERT_EQ(2u, dynamic_cast<node::ProjectNode *>(project_list->GetProjects()[2])->GetPos());

    plan_ptr = project_plan_node->GetChildren()[0];
    // validate limit 10
    ASSERT_EQ(node::kPlanTypeTable, plan_ptr->GetType());
    node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(plan_ptr);
    ASSERT_EQ("t1", relation_node->table_);
}

TEST_F(PlannerV2Test, SelectPlanWithWindowProjectTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    std::string sql =
        "SELECT COL1, SUM(AMT) OVER w1 as w_amt_sum FROM t \n"
        "WINDOW w1 AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS_RANGE BETWEEN 3 PRECEDING AND "
        "CURRENT ROW"
        ") limit 10;";
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
    plan_ptr = plan_ptr->GetChildren()[0];
    // validate limit node
    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
    node::LimitPlanNode *limit_ptr = (node::LimitPlanNode *)plan_ptr;
    // validate project list based on current row
    ASSERT_EQ(10, limit_ptr->GetLimitCnt());
    ASSERT_EQ(node::kPlanTypeProject, limit_ptr->GetChildren().at(0)->GetType());

    node::ProjectPlanNode *project_plan_node = (node::ProjectPlanNode *)limit_ptr->GetChildren().at(0);
    plan_ptr = project_plan_node;
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    // validate projection 0
    node::ProjectListNode *project_list =
        dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[0]);

    ASSERT_EQ(2u, project_list->GetProjects().size());

    ASSERT_TRUE(nullptr != project_list->GetW());
    ASSERT_EQ(-3, project_list->GetW()->GetStartOffset());
    ASSERT_EQ(0, project_list->GetW()->GetEndOffset());

    ASSERT_EQ("(COL2)", node::ExprString(project_list->GetW()->GetKeys()));
    ASSERT_TRUE(project_list->HasAggProject());

    plan_ptr = plan_ptr->GetChildren()[0];
    ASSERT_EQ(node::kPlanTypeTable, plan_ptr->GetType());
    node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(plan_ptr);
    ASSERT_EQ("t", relation_node->table_);
}

TEST_F(PlannerV2Test, SelectPlanWithMultiWindowProjectTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    const std::string sql =
        "SELECT sum(col1) OVER w1 as w1_col1_sum, sum(col1) OVER w2 as "
        "w2_col1_sum FROM t1 "
        "WINDOW "
        "w1 AS (PARTITION BY col2 ORDER BY `TS` ROWS_RANGE BETWEEN 1d "
        "PRECEDING AND "
        "1s PRECEDING), "
        "w2 AS (PARTITION BY col3 ORDER BY `TS` ROWS_RANGE BETWEEN 2d "
        "PRECEDING AND "
        "1s PRECEDING) "
        "limit 10;";
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
    plan_ptr = plan_ptr->GetChildren()[0];
    // validate limit node
    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
    node::LimitPlanNode *limit_ptr = (node::LimitPlanNode *)plan_ptr;
    // validate project list based on current row
    ASSERT_EQ(10, limit_ptr->GetLimitCnt());
    ASSERT_EQ(node::kPlanTypeProject, limit_ptr->GetChildren().at(0)->GetType());

    node::ProjectPlanNode *project_plan_node = (node::ProjectPlanNode *)limit_ptr->GetChildren().at(0);
    plan_ptr = project_plan_node;
    ASSERT_EQ(2u, project_plan_node->project_list_vec_.size());

    // validate projection 1: window agg over w1 [-1d, 1s]
    node::ProjectListNode *project_list =
        dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[0]);

    ASSERT_EQ(1u, project_list->GetProjects().size());
    ASSERT_TRUE(nullptr != project_list->GetW());

    ASSERT_TRUE(project_list->HasAggProject());

    ASSERT_EQ(-1 * 86400000, project_list->GetW()->GetStartOffset());
    ASSERT_EQ(-1000, project_list->GetW()->GetEndOffset());
    ASSERT_EQ("(col2)", node::ExprString(project_list->GetW()->GetKeys()));
    ASSERT_FALSE(project_list->GetW()->instance_not_in_window());

    // validate projection 1: window agg over w2 [-2d, 1s]
    project_list = dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[1]);
    ASSERT_EQ(1u, project_list->GetProjects().size());
    ASSERT_TRUE(nullptr != project_list->GetW());
    ASSERT_EQ(-2 * 86400000, project_list->GetW()->GetStartOffset());
    ASSERT_EQ(-1000, project_list->GetW()->GetEndOffset());

    ASSERT_EQ("(col3)", node::ExprString(project_list->GetW()->GetKeys()));
    ASSERT_TRUE(project_list->HasAggProject());
    ASSERT_FALSE(project_list->GetW()->instance_not_in_window());

    plan_ptr = plan_ptr->GetChildren()[0];
    ASSERT_EQ(node::kPlanTypeTable, plan_ptr->GetType());
    node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(plan_ptr);
    ASSERT_EQ("t1", relation_node->table_);
}

TEST_F(PlannerV2Test, WindowWithUnionTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    const std::string sql =
        "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
        "      WINDOW w1 AS (UNION t2,t3 PARTITION BY col1 ORDER BY col5 RANGE "
        "BETWEEN 3 PRECEDING AND CURRENT ROW INSTANCE_NOT_IN_WINDOW) limit 10;";
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
    plan_ptr = plan_ptr->GetChildren()[0];
    // validate limit node
    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
    node::LimitPlanNode *limit_ptr = (node::LimitPlanNode *)plan_ptr;
    // validate project list based on current row
    ASSERT_EQ(10, limit_ptr->GetLimitCnt());
    ASSERT_EQ(node::kPlanTypeProject, limit_ptr->GetChildren().at(0)->GetType());

    node::ProjectPlanNode *project_plan_node = (node::ProjectPlanNode *)limit_ptr->GetChildren().at(0);
    plan_ptr = project_plan_node;
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    // validate projection 1: window agg over w1
    node::ProjectListNode *project_list =
        dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[0]);

    ASSERT_EQ(3u, project_list->GetProjects().size());
    ASSERT_TRUE(nullptr != project_list->GetW());

    ASSERT_TRUE(project_list->HasAggProject());

    ASSERT_EQ(-3, project_list->GetW()->GetStartOffset());
    ASSERT_EQ(0, project_list->GetW()->GetEndOffset());
    ASSERT_EQ("(col1)", node::ExprString(project_list->GetW()->GetKeys()));
    ASSERT_TRUE(project_list->GetW()->instance_not_in_window());
    ASSERT_EQ(2u, project_list->GetW()->union_tables().size());

    plan_ptr = plan_ptr->GetChildren()[0];
    ASSERT_EQ(node::kPlanTypeTable, plan_ptr->GetType());
    node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(plan_ptr);
    ASSERT_EQ("t1", relation_node->table_);
}

inline void AssertPos(const node::ProjectListNode* list, uint32_t idx, uint32_t expect_pos) {
    node::ProjectNode *project = dynamic_cast<node::ProjectNode *>(list->GetProjects()[idx]);
    ASSERT_EQ(expect_pos, project->GetPos());
}

TEST_F(PlannerV2Test, MultiProjectListPlanPostTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    // rows window merged first, then rows range window
    const std::string sql =
        R"sql(SELECT
        sum(col1) OVER w1 as w1_col1_sum,
        sum(col3) OVER w2 as w2_col3_sum,
        sum(col4) OVER w2 as w2_col4_sum,
        lag(col1, 0) OVER w2 as w2_col1_at_0,
        col1,
        sum(col3) OVER w1 as w1_col3_sum,
        col2,
        sum(col1) OVER w2 as w2_col1_sum,
        lag(col1, 1) OVER w2 as w2_col1_at_1
        FROM t1 WINDOW
        w1 AS (PARTITION BY col2 ORDER BY `TS` ROWS_RANGE BETWEEN 1d PRECEDING AND 1s PRECEDING),
        w2 AS (PARTITION BY col3 ORDER BY `TS` ROWS_RANGE BETWEEN 2d PRECEDING AND 1s PRECEDING) limit 10;)sql";

    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());

    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    // validate select plan
    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
    plan_ptr = plan_ptr->GetChildren()[0];

    // validate limit node
    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
    node::LimitPlanNode *limit_ptr = dynamic_cast<node::LimitPlanNode *>(plan_ptr);

    // validate project list based on current row
    ASSERT_EQ(10, limit_ptr->GetLimitCnt());
    ASSERT_EQ(node::kPlanTypeProject, limit_ptr->GetChildren().at(0)->GetType());

    node::ProjectPlanNode *project_plan_node = dynamic_cast<node::ProjectPlanNode *>(limit_ptr->GetChildren().at(0));
    plan_ptr = project_plan_node;

    ASSERT_EQ(3u, project_plan_node->project_list_vec_.size());

    const std::vector<std::pair<uint32_t, uint32_t>>& pos_mapping = project_plan_node->pos_mapping_;
    ASSERT_EQ(9u, pos_mapping.size());
    ASSERT_EQ(std::make_pair(1u, 0u), pos_mapping[0]);
    ASSERT_EQ(std::make_pair(2u, 0u), pos_mapping[1]);
    ASSERT_EQ(std::make_pair(2u, 1u), pos_mapping[2]);
    ASSERT_EQ(std::make_pair(0u, 0u), pos_mapping[3]);
    ASSERT_EQ(std::make_pair(0u, 1u), pos_mapping[4]);
    ASSERT_EQ(std::make_pair(1u, 1u), pos_mapping[5]);
    ASSERT_EQ(std::make_pair(0u, 2u), pos_mapping[6]);
    ASSERT_EQ(std::make_pair(2u, 2u), pos_mapping[7]);
    ASSERT_EQ(std::make_pair(0u, 3u), pos_mapping[8]);

    {
        // validate projection 0: lag agg over w2
        node::ProjectListNode *project_list =
            dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_.at(0));
        ASSERT_EQ(4u, project_list->GetProjects().size());
        ASSERT_TRUE(nullptr != project_list->GetW());
        ASSERT_EQ(1, project_list->GetW()->frame_node()->GetHistoryRowsStartPreceding());
        ASSERT_EQ(0, project_list->GetW()->frame_node()->GetHistoryRowsEnd());
        ASSERT_EQ("(col3)", node::ExprString(project_list->GetW()->GetKeys()));
        ASSERT_TRUE(project_list->HasAggProject());

        // validate w2_col1_at_0 pos 7
        AssertPos(project_list, 0, 3);
        AssertPos(project_list, 1, 4);
        AssertPos(project_list, 2, 6);
        AssertPos(project_list, 3, 8);
    }
    {
        // validate projection 1: window agg over w1 [-1d, -1s]
        node::ProjectListNode *project_list =
            dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_.at(1));

        ASSERT_EQ(2u, project_list->GetProjects().size());
        ASSERT_FALSE(nullptr == project_list->GetW());
        ASSERT_EQ(-1 * 86400000, project_list->GetW()->GetStartOffset());
        ASSERT_EQ(-1000, project_list->GetW()->GetEndOffset());

        AssertPos(project_list, 0, 0);
        AssertPos(project_list, 1, 5);
    }
    {
        // validate projection 2: window agg over w2
        node::ProjectListNode *project_list =
            dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_.at(2));

        ASSERT_EQ(3u, project_list->GetProjects().size());
        ASSERT_TRUE(nullptr != project_list->GetW());
        ASSERT_EQ(-2 * 86400000, project_list->GetW()->GetStartOffset());
        ASSERT_EQ(-1000, project_list->GetW()->GetEndOffset());
        ASSERT_EQ("(col3)", node::ExprString(project_list->GetW()->GetKeys()));
        ASSERT_TRUE(project_list->HasAggProject());

        AssertPos(project_list, 0, 1);
        AssertPos(project_list, 1, 2);
        AssertPos(project_list, 2, 7);
    }

    plan_ptr = plan_ptr->GetChildren()[0];
    ASSERT_EQ(node::kPlanTypeTable, plan_ptr->GetType());
    node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(plan_ptr);
    ASSERT_EQ("t1", relation_node->table_);
}

TEST_F(PlannerV2Test, LastJoinPlanTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    const std::string sql =
        "SELECT t1.col1 as t1_col1, t2.col1 as t2_col2 from t1 LAST JOIN t2 "
        "order by t2.col5 on "
        "t1.col1 = t2.col1 and t2.col5 between t1.col5 - 30d and t1.col5 "
        "- 1d limit 10;";
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());

    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
    plan_ptr = plan_ptr->GetChildren()[0];

    // validate limit node
    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
    node::LimitPlanNode *limit_ptr = (node::LimitPlanNode *)plan_ptr;
    ASSERT_EQ(10, limit_ptr->limit_cnt_);

    // validate project list based on current row
    ASSERT_EQ(node::kPlanTypeProject, limit_ptr->GetChildren().at(0)->GetType());

    node::ProjectPlanNode *project_plan_node = (node::ProjectPlanNode *)limit_ptr->GetChildren().at(0);
    plan_ptr = project_plan_node;
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    const std::vector<std::pair<uint32_t, uint32_t>> pos_mapping = project_plan_node->pos_mapping_;
    ASSERT_EQ(2u, pos_mapping.size());
    ASSERT_EQ(std::make_pair(0u, 0u), pos_mapping[0]);
    ASSERT_EQ(std::make_pair(0u, 1u), pos_mapping[1]);

    // validate projection 0: window agg over w1
    {
        node::ProjectListNode *project_list =
            dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_.at(0));

        ASSERT_EQ(2u, project_list->GetProjects().size());
        ASSERT_TRUE(nullptr == project_list->GetW());
        // validate t1_col1 pos 0
        {
            node::ProjectNode *project = dynamic_cast<node::ProjectNode *>(project_list->GetProjects()[0]);
            ASSERT_EQ(0u, project->GetPos());
        }
        // validate t2_col1 pos 1
        {
            node::ProjectNode *project = dynamic_cast<node::ProjectNode *>(project_list->GetProjects()[1]);
            ASSERT_EQ(1u, project->GetPos());
        }
    }

    plan_ptr = plan_ptr->GetChildren()[0];
    ASSERT_EQ(node::kPlanTypeJoin, plan_ptr->GetType());
    auto join = dynamic_cast<node::JoinPlanNode *>(plan_ptr);
    ASSERT_EQ(node::kJoinTypeLast, join->join_type_);
    ASSERT_EQ(
        "t1.col1 = t2.col1 AND t2.col5 between t1.col5 - 30d and t1.col5 - "
        "1d",
        join->condition_->GetExprString());

    ASSERT_EQ("(t2.col5 ASC)", join->orders_->GetExprString());
    auto left = plan_ptr->GetChildren()[0];
    ASSERT_EQ(node::kPlanTypeTable, left->GetType());
    {
        node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(left);
        ASSERT_EQ("t1", relation_node->table_);
    }

    auto right = plan_ptr->GetChildren()[1];
    ASSERT_EQ(node::kPlanTypeTable, right->GetType());
    {
        node::TablePlanNode *relation_node = reinterpret_cast<node::TablePlanNode *>(right);
        ASSERT_EQ("t2", relation_node->table_);
    }
}

TEST_F(PlannerV2Test, CreateFunctionPlanTest) {
    std::string sql_str = "CREATE FUNCTION fun(x INT) RETURNS STRING OPTIONS (PATH='/tmp/libmyfun.so');";
    node::PlanNodeList trees;
    base::Status status;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;

    // validate create plan
    ASSERT_EQ(node::kPlanTypeCreateFunction, plan_ptr->GetType());
    auto create_function_plan = dynamic_cast<node::CreateFunctionPlanNode *>(plan_ptr);
    ASSERT_EQ("fun", create_function_plan->Name());
    ASSERT_FALSE(create_function_plan->IsAggregate());
    ASSERT_EQ("string", (dynamic_cast<const node::TypeNode*>(create_function_plan->GetReturnType()))->GetName());
    ASSERT_EQ(1, create_function_plan->GetArgsType().size());
    ASSERT_EQ("int32", (dynamic_cast<node::TypeNode*>(create_function_plan->GetArgsType().front()))->GetName());
    ASSERT_EQ(1, create_function_plan->Options()->size());
    ASSERT_EQ("PATH", create_function_plan->Options()->begin()->first);
    ASSERT_EQ("/tmp/libmyfun.so", create_function_plan->Options()->begin()->second->GetExprString());
}

TEST_F(PlannerV2Test, CreateAggregateFunctionPlanTest) {
    std::string sql_str = "CREATE AGGREGATE FUNCTION fun(x INT) RETURNS STRING OPTIONS (PATH='/tmp/libmyfun.so');";
    node::PlanNodeList trees;
    base::Status status;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;

    // validate create plan
    ASSERT_EQ(node::kPlanTypeCreateFunction, plan_ptr->GetType());
    auto create_function_plan = dynamic_cast<node::CreateFunctionPlanNode *>(plan_ptr);
    ASSERT_EQ("fun", create_function_plan->Name());
    ASSERT_TRUE(create_function_plan->IsAggregate());
    ASSERT_EQ("string", (dynamic_cast<const node::TypeNode*>(create_function_plan->GetReturnType()))->GetName());
    ASSERT_EQ(1, create_function_plan->GetArgsType().size());
    ASSERT_EQ("int32", (dynamic_cast<node::TypeNode*>(create_function_plan->GetArgsType().front()))->GetName());
    ASSERT_EQ(1, create_function_plan->Options()->size());
    ASSERT_EQ("PATH", create_function_plan->Options()->begin()->first);
    ASSERT_EQ("/tmp/libmyfun.so", create_function_plan->Options()->begin()->second->GetExprString());
}

TEST_F(PlannerV2Test, CreateTableStmtPlanTest) {
    const std::string sql_str =
        "create table IF NOT EXISTS db1.test(\n"
        "    column1 int NOT NULL,\n"
        "    column2 timestamp NOT NULL,\n"
        "    column3 int NOT NULL,\n"
        "    column4 string NOT NULL,\n"
        "    column5 int NOT NULL,\n"
        "    index(key=(column4, column3), ts=column2, ttl=60d, version = (column4, 10))\n"
        ") OPTIONS (\n"
        "            partitionnum=8, replicanum=3,\n"
        "            distribution = [\n"
        "              (\"127.0.0.1:9927\", [\"127.0.0.1:9926\", \"127.0.0.1:9928\"])\n"
        "              ]\n"
        "          );";

    node::PlanNodeList trees;
    base::Status status;

    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;

    // validate create plan
    ASSERT_EQ(node::kPlanTypeCreate, plan_ptr->GetType());
    node::CreatePlanNode *createStmt = (node::CreatePlanNode *)plan_ptr;
    ASSERT_EQ("db1", createStmt->GetDatabase());
    auto table_option_list = createStmt->GetTableOptionList();
    node::NodePointVector distribution_list;
    for (auto table_option : table_option_list) {
        switch (table_option->GetType()) {
            case node::kReplicaNum: {
                ASSERT_EQ(3, dynamic_cast<node::ReplicaNumNode *>(table_option)->GetReplicaNum());
                break;
            }
            case node::kPartitionNum: {
                ASSERT_EQ(8, dynamic_cast<node::PartitionNumNode *>(table_option)->GetPartitionNum());
                break;
            }
            case node::kDistributions: {
                distribution_list = dynamic_cast<node::DistributionsNode *>(table_option)->GetDistributionList();
                break;
            }
            case hybridse::node::kStorageMode: {
                ASSERT_EQ(node::kMemory,
                          dynamic_cast<hybridse::node::StorageModeNode *>(table_option)->GetStorageMode());
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
        ASSERT_EQ("127.0.0.1:9927", partition->GetEndpoint());
    }
    {
        ASSERT_EQ(node::kPartitionMeta, partition_meta_list[1]->GetType());
        node::PartitionMetaNode *partition = dynamic_cast<node::PartitionMetaNode *>(partition_meta_list[1]);
        ASSERT_EQ(node::RoleType::kFollower, partition->GetRoleType());
        ASSERT_EQ("127.0.0.1:9926", partition->GetEndpoint());
    }
    {
        ASSERT_EQ(node::kPartitionMeta, partition_meta_list[2]->GetType());
        node::PartitionMetaNode *partition = dynamic_cast<node::PartitionMetaNode *>(partition_meta_list[2]);
        ASSERT_EQ(node::RoleType::kFollower, partition->GetRoleType());
        ASSERT_EQ("127.0.0.1:9928", partition->GetEndpoint());
    }

    type::TableDef table_def;
    ASSERT_TRUE(
        Planner::TransformTableDef(createStmt->GetTableName(), createStmt->GetColumnDescList(), &table_def).isOK());

    type::TableDef *table = &table_def;
    ASSERT_EQ("test", table->name());
    ASSERT_EQ(5, table->columns_size());
    ASSERT_EQ("column1", table->columns(0).name());
    ASSERT_EQ("column2", table->columns(1).name());
    ASSERT_EQ("column3", table->columns(2).name());
    ASSERT_EQ("column4", table->columns(3).name());
    ASSERT_EQ("column5", table->columns(4).name());
    ASSERT_EQ(type::Type::kInt32, table->columns(0).type());
    ASSERT_EQ(type::Type::kTimestamp, table->columns(1).type());
    ASSERT_EQ(type::Type::kInt32, table->columns(2).type());
    ASSERT_EQ(type::Type::kVarchar, table->columns(3).type());
    ASSERT_EQ(type::Type::kInt32, table->columns(4).type());
    ASSERT_EQ(1, table->indexes_size());
    ASSERT_EQ(60 * 86400000UL, table->indexes(0).ttl(0));
    ASSERT_EQ(2, table->indexes(0).first_keys_size());
    ASSERT_EQ("column4", table->indexes(0).first_keys(0));
    ASSERT_EQ("column3", table->indexes(0).first_keys(1));
    ASSERT_EQ("column2", table->indexes(0).second_key());
}

TEST_F(PlannerV2Test, CmdStmtPlanTest) {
    {
        const std::string sql_str = "show databases;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdShowDatabases, cmd_plan->GetCmdType());
    }
    {
        const std::string sql_str = "show tables;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdShowTables, cmd_plan->GetCmdType());
    }
    {
        const std::string sql_str = "show table status;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdShowTableStatus, cmd_plan->GetCmdType());
        ASSERT_EQ(0, cmd_plan->GetArgs().size());
    }
    {
        const std::string sql_str = "show table status like '*';";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdShowTableStatus, cmd_plan->GetCmdType());
        ASSERT_EQ(1, cmd_plan->GetArgs().size());
        ASSERT_EQ("*", cmd_plan->GetArgs()[0]);
    }
    {
        const std::string sql_str = "show procedures;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdShowProcedures, cmd_plan->GetCmdType());
    }
    {
        const std::string sql_str = "drop procedure sp1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropSp, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"sp1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop procedure db1.sp1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropSp, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"sp1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop table db1.t1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropTable, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"db1", "t1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop table t1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropTable, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"t1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop database db1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropDatabase, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"db1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop index db1.t1.index1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropIndex, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"db1", "t1", "index1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop index t1.index1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
        ASSERT_EQ(1u, trees.size());
        PlanNode *plan_ptr = trees[0];
        // validate create plan
        ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
        node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
        ASSERT_EQ(node::kCmdDropIndex, cmd_plan->GetCmdType());
        ASSERT_EQ(std::vector<std::string>({"t1", "index1"}), cmd_plan->GetArgs());
    }
    {
        const std::string sql_str = "drop index index1;";
        node::PlanNodeList trees;
        base::Status status;
        ASSERT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sql_str, trees, manager_, status)) << status;
    }
}
//
// TEST_F(PlannerTest, FunDefPlanTest) {
//    const std::string sql_str =
//        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
//        "d\nend";
//
//    node::NodePointVector list;
//    node::PlanNodeList trees;
//    base::Status status;
//    int ret = parser_->parse(sql_str, list, manager_, status);
//    ASSERT_EQ(0, ret);
//    ASSERT_EQ(1u, list.size());
//    std::cout << *(list[0]) << std::endl;
//
//    Planner *planner_ptr = new SimplePlanner(manager_);
//    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
//    std::cout << status << std::endl;
//    ASSERT_EQ(1u, trees.size());
//    PlanNode *plan_ptr = trees[0];
//    ASSERT_TRUE(NULL != plan_ptr);
//
//    std::cout << *plan_ptr << std::endl;
//
//    // validate create plan
//    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
//    node::FuncDefPlanNode *plan =
//        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
//    ASSERT_TRUE(nullptr != plan->fn_def_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->header_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->block_);
//}
//
// TEST_F(PlannerTest, FunDefAndSelectPlanTest) {
//    const std::string sql_str =
//        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
//        "d\nend\n%%sql\nselect col1, test(col1, col2) from t1 limit 1;";
//
//    node::NodePointVector list;
//    node::PlanNodeList trees;
//    base::Status status;
//    int ret = parser_->parse(sql_str, list, manager_, status);
//    ASSERT_EQ(0, ret);
//    ASSERT_EQ(2u, list.size());
//    std::cout << *(list[0]) << std::endl;
//    std::cout << *(list[1]) << std::endl;
//
//    Planner *planner_ptr = new SimplePlanner(manager_);
//    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
//    std::cout << status << std::endl;
//    ASSERT_EQ(2u, trees.size());
//    PlanNode *plan_ptr = trees[0];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//
//    // validate fundef plan
//    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
//    node::FuncDefPlanNode *plan =
//        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
//
//    ASSERT_TRUE(nullptr != plan->fn_def_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->header_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->block_);
//
//    // validate select plan
//    plan_ptr = trees[1];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//    // validate select plan
//
//    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
//    plan_ptr = plan_ptr->GetChildren()[0];
//    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
//    node::LimitPlanNode *limit_plan =
//        dynamic_cast<node::LimitPlanNode *>(plan_ptr);
//    ASSERT_EQ(1, limit_plan->GetLimitCnt());
//}
//
// TEST_F(PlannerTest, FunDefIfElsePlanTest) {
//    const std::string sql_str =
//        "%%fun\n"
//        "def test(a:i32,b:i32):i32\n"
//        "    c=a+b\n"
//        "\td=c+1\n"
//        "\tif a<b\n"
//        "\t\treturn c\n"
//        "\telif c > d\n"
//        "\t\treturn d\n"
//        "\telif d > 1\n"
//        "\t\treturn c+d\n"
//        "\telse \n"
//        "\t\treturn d\n"
//        "end\n"
//        "%%sql\n"
//        "select col1, test(col1, col2) from t1 limit 1;";
//    std::cout << sql_str;
//
//    node::NodePointVector list;
//    node::PlanNodeList trees;
//    base::Status status;
//    int ret = parser_->parse(sql_str, list, manager_, status);
//    ASSERT_EQ(0, ret);
//    ASSERT_EQ(2u, list.size());
//    std::cout << *(list[0]) << std::endl;
//    std::cout << *(list[1]) << std::endl;
//
//    Planner *planner_ptr = new SimplePlanner(manager_);
//    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
//    std::cout << status << std::endl;
//    ASSERT_EQ(2u, trees.size());
//    PlanNode *plan_ptr = trees[0];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//
//    // validate fundef plan
//    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
//    node::FuncDefPlanNode *plan =
//        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
//
//    ASSERT_TRUE(nullptr != plan->fn_def_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->header_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->block_);
//    ASSERT_EQ(3u, plan->fn_def_->block_->children.size());
//    ASSERT_EQ(node::kFnAssignStmt,
//              plan->fn_def_->block_->children[0]->GetType());
//    ASSERT_EQ(node::kFnAssignStmt,
//              plan->fn_def_->block_->children[1]->GetType());
//    ASSERT_EQ(node::kFnIfElseBlock,
//              plan->fn_def_->block_->children[2]->GetType());
//
//    // validate select plan
//    plan_ptr = trees[1];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
//    plan_ptr = plan_ptr->GetChildren()[0];
//    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
//    node::LimitPlanNode *limit_plan =
//        dynamic_cast<node::LimitPlanNode *>(plan_ptr);
//    ASSERT_EQ(1, limit_plan->GetLimitCnt());
//}
//
// TEST_F(PlannerTest, FunDefIfElseComplexPlanTest) {
//    const std::string sql_str =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    if x > 1\n"
//        "    \tc=x+y\n"
//        "    elif y >1\n"
//        "    \tif x-y >0\n"
//        "    \t\td=x-y\n"
//        "    \t\tc=d+1\n"
//        "    \telif x-y <0\n"
//        "    \t\tc = y-x\n"
//        "    \telse\n"
//        "    \t\tc = 9999\n"
//        "    else\n"
//        "    \tif x < -100\n"
//        "    \t\tc = x+100\n"
//        "    \telif y < -100\n"
//        "    \t\tc = y+100\n"
//        "    \telse\n"
//        "    \t\tc=x*y\n"
//        "    return c\n"
//        "end\n"
//        "%%sql\n"
//        "select col1, test(col1, col2) from t1 limit 1;";
//    std::cout << sql_str;
//
//    node::NodePointVector list;
//    node::PlanNodeList trees;
//    base::Status status;
//    int ret = parser_->parse(sql_str, list, manager_, status);
//    ASSERT_EQ(0, ret);
//    ASSERT_EQ(2u, list.size());
//    std::cout << *(list[0]) << std::endl;
//    std::cout << *(list[1]) << std::endl;
//
//    Planner *planner_ptr = new SimplePlanner(manager_);
//    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
//    std::cout << status << std::endl;
//    ASSERT_EQ(2u, trees.size());
//    PlanNode *plan_ptr = trees[0];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//
//    // validate fundef plan
//    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
//    node::FuncDefPlanNode *plan =
//        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
//
//    ASSERT_TRUE(nullptr != plan->fn_def_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->header_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->block_);
//    ASSERT_EQ(2u, plan->fn_def_->block_->children.size());
//    ASSERT_EQ(node::kFnIfElseBlock,
//              plan->fn_def_->block_->children[0]->GetType());
//    ASSERT_EQ(node::kFnReturnStmt,
//              plan->fn_def_->block_->children[1]->GetType());
//
//    {
//        node::FnIfElseBlock *block = dynamic_cast<node::FnIfElseBlock *>(
//            plan->fn_def_->block_->children[0]);
//        // if block check: if x>1
//        {
//            ASSERT_EQ(node::kExprBinary,
//                      block->if_block_->if_node->expression_->GetExprType());
//            // c = x+y
//            ASSERT_EQ(1u, block->if_block_->block_->children.size());
//        }
//        ASSERT_EQ(1u, block->elif_blocks_.size());
//
//        {
//            // elif block check: elif y>1
//            ASSERT_EQ(node::kFnElifBlock, block->elif_blocks_[0]->GetType());
//            node::FnElifBlock *elif_block =
//                dynamic_cast<node::FnElifBlock *>(block->elif_blocks_[0]);
//            ASSERT_EQ(node::kExprBinary,
//                      elif_block->elif_node_->expression_->GetExprType());
//            ASSERT_EQ(1u, elif_block->block_->children.size());
//            ASSERT_EQ(node::kFnIfElseBlock,
//                      elif_block->block_->children[0]->GetType());
//            // check if elif else block
//            {
//                node::FnIfElseBlock *block =
//                    dynamic_cast<node::FnIfElseBlock *>(
//                        elif_block->block_->children[0]);
//                // check if x-y>0
//                //          c = x-y
//                {
//                    ASSERT_EQ(
//                        node::kExprBinary,
//                        block->if_block_->if_node->expression_->GetExprType());
//                    // c = x-y
//                    ASSERT_EQ(2u, block->if_block_->block_->children.size());
//                    ASSERT_EQ(node::kFnAssignStmt,
//                              block->if_block_->block_->children[0]->GetType());
//                    ASSERT_TRUE(dynamic_cast<node::FnAssignNode *>(
//                                    block->if_block_->block_->children[0])
//                                    ->IsSSA());
//                    ASSERT_EQ(node::kFnAssignStmt,
//                              block->if_block_->block_->children[1]->GetType());
//                    ASSERT_FALSE(dynamic_cast<node::FnAssignNode *>(
//                                     block->if_block_->block_->children[1])
//                                     ->IsSSA());
//                }
//                ASSERT_EQ(1u, block->elif_blocks_.size());
//                // check elif x-y<0
//                //          c = y-x
//                {
//                    ASSERT_EQ(node::kFnElifBlock,
//                              block->elif_blocks_[0]->GetType());
//                    node::FnElifBlock *elif_block =
//                        dynamic_cast<node::FnElifBlock *>(
//                            block->elif_blocks_[0]);
//                    ASSERT_EQ(
//                        node::kExprBinary,
//                        elif_block->elif_node_->expression_->GetExprType());
//                    ASSERT_EQ(1u, elif_block->block_->children.size());
//                    ASSERT_EQ(node::kFnAssignStmt,
//                              elif_block->block_->children[0]->GetType());
//                }
//                // check c = 9999
//                ASSERT_EQ(1u, block->else_block_->block_->children.size());
//                ASSERT_EQ(node::kFnAssignStmt,
//                          block->else_block_->block_->children[0]->GetType());
//            }
//        }
//        // else block check
//        {
//            ASSERT_EQ(1u, block->else_block_->block_->children.size());
//            ASSERT_EQ(node::kFnIfElseBlock,
//                      block->else_block_->block_->children[0]->GetType());
//        }
//    }
//    // validate select plan
//    plan_ptr = trees[1];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
//    plan_ptr = plan_ptr->GetChildren()[0];
//    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
//    node::LimitPlanNode *limit_plan =
//        dynamic_cast<node::LimitPlanNode *>(plan_ptr);
//    ASSERT_EQ(1, limit_plan->GetLimitCnt());
//}
//
// TEST_F(PlannerTest, FunDefForInPlanTest) {
//    const std::string sql_str =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=0\n"
//        "    for x in l\n"
//        "        if x > a\n"
//        "            sum = sum + x\n"
//        "    return sum\n"
//        "end\n"
//        "%%sql\n"
//        "select col1, test(col1, col2) from t1 limit 1;";
//    std::cout << sql_str;
//
//    node::NodePointVector list;
//    node::PlanNodeList trees;
//    base::Status status;
//    int ret = parser_->parse(sql_str, list, manager_, status);
//    ASSERT_EQ(0, ret);
//    ASSERT_EQ(2u, list.size());
//    std::cout << *(list[0]) << std::endl;
//    std::cout << *(list[1]) << std::endl;
//
//    Planner *planner_ptr = new SimplePlanner(manager_);
//    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
//    std::cout << status << std::endl;
//    ASSERT_EQ(2u, trees.size());
//    PlanNode *plan_ptr = trees[0];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//
//    // validate fundef plan
//    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
//    node::FuncDefPlanNode *plan =
//        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
//
//    ASSERT_TRUE(nullptr != plan->fn_def_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->header_);
//    ASSERT_TRUE(nullptr != plan->fn_def_->block_);
//    ASSERT_EQ(3u, plan->fn_def_->block_->children.size());
//
//    // validate udf plan
//    ASSERT_EQ(node::kFnAssignStmt,
//              plan->fn_def_->block_->children[0]->GetType());
//    ASSERT_EQ(node::kFnForInBlock,
//              plan->fn_def_->block_->children[1]->GetType());
//    // validate for in block
//    {
//        node::FnForInBlock *for_block = dynamic_cast<node::FnForInBlock *>(
//            plan->fn_def_->block_->children[1]);
//        ASSERT_EQ(1u, for_block->block_->children.size());
//        ASSERT_EQ(node::kFnIfElseBlock,
//                  for_block->block_->children[0]->GetType());
//    }
//    // validate select plan
//    plan_ptr = trees[1];
//    ASSERT_TRUE(NULL != plan_ptr);
//    std::cout << *plan_ptr << std::endl;
//    ASSERT_EQ(node::kPlanTypeQuery, plan_ptr->GetType());
//    plan_ptr = plan_ptr->GetChildren()[0];
//    ASSERT_EQ(node::kPlanTypeLimit, plan_ptr->GetType());
//    node::LimitPlanNode *limit_plan =
//        dynamic_cast<node::LimitPlanNode *>(plan_ptr);
//    ASSERT_EQ(1, limit_plan->GetLimitCnt());
//}

TEST_F(PlannerV2Test, MergeWindowsTest) {
    SimplePlannerV2 planner_ptr(manager_, false);
    auto partitions = manager_->MakeExprList(manager_->MakeColumnRefNode("col1", "t1"));

    auto orders = dynamic_cast<node::OrderByNode *>(
        manager_->MakeOrderByNode(manager_->MakeExprList(
                                      manager_->MakeOrderExpression(
                manager_->MakeColumnRefNode("ts", "t1"), false))));
    auto frame_1day = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1, node::kDay)),
                                  manager_->MakeFrameBound(node::kCurrent)));

    auto frame_30m = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(
            manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(30, node::kMinute)),
            manager_->MakeFrameBound(node::kCurrent)));

    auto frame_1hour = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1, node::kHour)),
                                  manager_->MakeFrameBound(node::kCurrent)));

    // window:col1,ts,[-1d, 0]
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1hour)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(3), false)));
        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(1u, windows.size());
        std::cout << *windows[0] << std::endl;
        ASSERT_EQ(-86400000, windows[0]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(0, windows[0]->GetFrame()->frame_maxsize());
    }

    // window:col2,ts,[-1d,0]
    // window:col1,ts,[-1h, 0]
    auto partitions2 = manager_->MakeExprList(manager_->MakeColumnRefNode("col2", "t1"));
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions2, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1hour)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(3), false)));
        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(2u, windows.size());

        std::cout << *(windows[0]) << std::endl;
        ASSERT_EQ(-86400000, windows[0]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(0, windows[0]->GetFrame()->frame_maxsize());

        std::cout << *(windows[1]) << std::endl;
        ASSERT_EQ(-3600000, windows[1]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[1]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(0, windows[1]->GetFrame()->frame_maxsize());
    }

    auto frame_100 = manager_->MakeFrameNode(
        node::kFrameRows,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(100)),
                                  manager_->MakeFrameBound(node::kCurrent)));
    auto frame_1000 = manager_->MakeFrameNode(
        node::kFrameRows,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1000)),
                                  manager_->MakeFrameBound(node::kCurrent)));
    // window:col1:range[-1d, 0] rows[-1000,0]
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1000)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(1u, windows.size());
        std::cout << *windows[0] << std::endl;
        ASSERT_EQ(-86400000, windows[0]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(-1000, windows[0]->GetFrame()->GetHistoryRowsStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->frame_maxsize());
    }

    // null window merge
    {
        const node::WindowDefNode *empty_w1 = nullptr;
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(empty_w1, manager_->MakeProjectListPlanNode(nullptr, false)));
        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));
        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(2u, windows.size());
        ASSERT_TRUE(nullptr == windows[0]);
        std::cout << *windows[1] << std::endl;
        ASSERT_EQ(-86400000, windows[1]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[1]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(0, windows[1]->GetFrame()->frame_maxsize());
    }

    // Merge Fail
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;
        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions2, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));
        ASSERT_FALSE(planner_ptr.MergeWindows(map, &windows));
    }
}

TEST_F(PlannerV2Test, MergeWindowsWithMaxSizeTest) {
    SimplePlannerV2 planner_ptr(manager_, false);
    auto partitions = manager_->MakeExprList(manager_->MakeColumnRefNode("col1", "t1"));

    auto orders = dynamic_cast<node::OrderByNode *>(
        manager_->MakeOrderByNode(manager_->MakeExprList(
                                      manager_->MakeOrderExpression(
                                       manager_->MakeColumnRefNode("ts", "t1"), false))));
    auto frame_1day = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1, node::kDay)),
                                  manager_->MakeFrameBound(node::kCurrent)));

    auto frame_30m = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(
            manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(30, node::kMinute)),
            manager_->MakeFrameBound(node::kCurrent)));

    auto frame_1day_masize_100 = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1, node::kDay)),
                                  manager_->MakeFrameBound(node::kCurrent)),
        100);
    auto frame_1day_masize_1000 = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1, node::kDay)),
                                  manager_->MakeFrameBound(node::kCurrent)),
        1000);

    auto frame_30m_maxsize_100 = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(
            manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(30, node::kMinute)),
            manager_->MakeFrameBound(node::kCurrent)),
        100);

    auto frame_1hour_maxsize_100 = manager_->MakeFrameNode(
        node::kFrameRowsRange,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1, node::kHour)),
                                  manager_->MakeFrameBound(node::kCurrent)),
        100);

    // window:col1,ts,[-1d, 0]
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day_masize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m_maxsize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        map.insert(std::make_pair(dynamic_cast<node::WindowDefNode *>(
                                      manager_->MakeWindowDefNode(partitions, orders, frame_1hour_maxsize_100)),
                                  manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(3), false)));
        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(1u, windows.size());
        std::cout << *windows[0] << std::endl;
        ASSERT_EQ(-86400000, windows[0]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(100, windows[0]->GetFrame()->frame_maxsize());
    }

    // window:col2,ts,[-1d,0]
    // window:col1,ts,[-1h, 0]
    auto partitions2 = manager_->MakeExprList(manager_->MakeColumnRefNode("col2", "t1"));
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(dynamic_cast<node::WindowDefNode *>(
                                      manager_->MakeWindowDefNode(partitions2, orders, frame_1day_masize_100)),
                                  manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m_maxsize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        map.insert(std::make_pair(dynamic_cast<node::WindowDefNode *>(
                                      manager_->MakeWindowDefNode(partitions, orders, frame_1hour_maxsize_100)),
                                  manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(3), false)));
        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(2u, windows.size());

        std::cout << *(windows[0]) << std::endl;
        ASSERT_EQ(-86400000, windows[0]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(100, windows[0]->GetFrame()->frame_maxsize());

        std::cout << *(windows[1]) << std::endl;
        ASSERT_EQ(-3600000, windows[1]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[1]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(100, windows[1]->GetFrame()->frame_maxsize());
    }

    auto frame_100 = manager_->MakeFrameNode(
        node::kFrameRows,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(100)),
                                  manager_->MakeFrameBound(node::kCurrent)));
    auto frame_1000 = manager_->MakeFrameNode(
        node::kFrameRows,
        manager_->MakeFrameExtent(manager_->MakeFrameBound(node::kPreceding, manager_->MakeConstNode(1000)),
                                  manager_->MakeFrameBound(node::kCurrent)));

    // window:col1:range[-1d, 0] rows[-1000,0]
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(dynamic_cast<node::WindowDefNode *>(
                                      manager_->MakeWindowDefNode(partitions, orders, frame_1day_masize_1000)),
                                  manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1000)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));

        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(1u, windows.size());
        std::cout << *windows[0] << std::endl;
        ASSERT_EQ(-86400000, windows[0]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[0]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(-1000, windows[0]->GetFrame()->GetHistoryRowsStart());
        ASSERT_EQ(1000, windows[0]->GetFrame()->frame_maxsize());
    }

    // null window merge
    {
        const node::WindowDefNode *empty_w1 = nullptr;
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;

        map.insert(std::make_pair(empty_w1, manager_->MakeProjectListPlanNode(nullptr, false)));
        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day_masize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m_maxsize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));
        ASSERT_TRUE(planner_ptr.MergeWindows(map, &windows));
        ASSERT_EQ(2u, windows.size());
        ASSERT_TRUE(nullptr == windows[0]);
        std::cout << *windows[1] << std::endl;
        ASSERT_EQ(-86400000, windows[1]->GetFrame()->GetHistoryRangeStart());
        ASSERT_EQ(0, windows[1]->GetFrame()->GetHistoryRangeEnd());
        ASSERT_EQ(100, windows[1]->GetFrame()->frame_maxsize());
    }

    // Merge Fail: can't merge windows with different partitions
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;
        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions2, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_30m)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));
        ASSERT_FALSE(planner_ptr.MergeWindows(map, &windows));
    }
    // Can't merge windows with different max_size
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;
        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day_masize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));
        ASSERT_FALSE(planner_ptr.MergeWindows(map, &windows));
    }
    // Can't merge windows with different max_size
    {
        std::map<const node::WindowDefNode *, node::ProjectListNode *> map;
        std::vector<const node::WindowDefNode *> windows;
        map.insert(std::make_pair(dynamic_cast<node::WindowDefNode *>(
                                      manager_->MakeWindowDefNode(partitions, orders, frame_1day_masize_1000)),
                                  manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(1), false)));

        map.insert(std::make_pair(
            dynamic_cast<node::WindowDefNode *>(manager_->MakeWindowDefNode(partitions, orders, frame_1day_masize_100)),
            manager_->MakeProjectListPlanNode(manager_->MakeWindowPlanNode(2), false)));
        ASSERT_FALSE(planner_ptr.MergeWindows(map, &windows));
    }
}
TEST_F(PlannerV2Test, WindowMergeOptTest) {
    const std::string sql =
        "      SELECT\n"
        "      sum(col1) OVER (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
        "BETWEEN 2d PRECEDING AND CURRENT ROW) as w_col1_sum,\n"
        "      sum(col2) OVER w1 as w1_col2_sum,\n"
        "      sum(col3) OVER (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN "
        "1000 PRECEDING AND CURRENT ROW) as w_col3_sum\n"
        "      FROM t1\n"
        "      WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
        "BETWEEN 1d PRECEDING AND CURRENT ROW) limit 10;";

    base::Status status;
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);
    LOG(INFO) << "logical plan:\n" << *plan_ptr << std::endl;

    auto project_plan_node = dynamic_cast<node::ProjectPlanNode *>(plan_ptr->GetChildren()[0]->GetChildren()[0]);
    ASSERT_EQ(node::kPlanTypeProject, project_plan_node->type_);
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    auto project_list = dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[0]);
    auto w = project_list->GetW();
    ASSERT_EQ("(col1)", node::ExprString(w->GetKeys()));
    ASSERT_EQ("(col5 ASC)", node::ExprString(w->GetOrders()));
    ASSERT_EQ("range[172800000 PRECEDING,0 CURRENT],rows[1000 PRECEDING,0 CURRENT]", w->frame_node()->GetExprString());
}
TEST_F(PlannerV2Test, RowsWindowExpandTest) {
    const std::string sql =
        "      SELECT\n"
        "      sum(col1) OVER (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
        "BETWEEN 2d PRECEDING AND 1d PRECEDING) as w_col1_sum,\n"
        "      sum(col2) OVER w1 as w1_col2_sum,\n"
        "      sum(col3) OVER (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN "
        "1000 PRECEDING AND 100 PRECEDING) as w_col3_sum\n"
        "      FROM t1\n"
        "      WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
        "BETWEEN 1d PRECEDING AND 6h PRECEDING) limit 10;";

    base::Status status;
    node::PlanNodeList plan_trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status));
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees[0];
    ASSERT_TRUE(NULL != plan_ptr);
    LOG(INFO) << "logical plan:\n" << *plan_ptr << std::endl;

    auto project_plan_node = dynamic_cast<node::ProjectPlanNode *>(plan_ptr->GetChildren()[0]->GetChildren()[0]);
    ASSERT_EQ(node::kPlanTypeProject, project_plan_node->type_);
    ASSERT_EQ(2u, project_plan_node->project_list_vec_.size());

    {
        auto project_list = dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[0]);
        auto w = project_list->GetW();
        ASSERT_EQ("(col1)", node::ExprString(w->GetKeys()));
        ASSERT_EQ("(col5 ASC)", node::ExprString(w->GetOrders()));
        ASSERT_EQ("rows[1000 PRECEDING,0 CURRENT]", w->frame_node()->GetExprString());
    }

    // Pure RowsRange Frame won't expand
    {
        auto project_list = dynamic_cast<node::ProjectListNode *>(project_plan_node->project_list_vec_[1]);
        auto w = project_list->GetW();
        ASSERT_EQ("(col1)", node::ExprString(w->GetKeys()));
        ASSERT_EQ("(col5 ASC)", node::ExprString(w->GetOrders()));
        ASSERT_EQ("range[172800000 PRECEDING,21600000 PRECEDING]", w->frame_node()->GetExprString());
    }
}

TEST_F(PlannerV2Test, CreatePlanLeakTest) {
    const std::string sql =
        "      SELECT\n"
        "      sum(col1) OVER (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
        "BETWEEN 2d PRECEDING AND 1d PRECEDING) as w_col1_sum,\n"
        "      sum(col2) OVER w1 as w1_col2_sum,\n"
        "      sum(col3) OVER (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN "
        "1000 PRECEDING AND 100 PRECEDING) as w_col3_sum\n"
        "      FROM t1\n"
        "      WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
        "BETWEEN 1d PRECEDING AND 6h PRECEDING) limit 10;";

    int64_t cnt = 0;
    while (true) {
        base::Status status;
        NodeManager nm;
        node::PlanNodeList plan_trees;
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &nm, status));
        if (cnt == 100) {
            LOG(INFO) << "process .......... " << cnt;
            break;
        }
        cnt++;
    }
}
TEST_F(PlannerV2Test, DeployPlanNodeTest) {
    const std::string sql = "DEPLOY IF NOT EXISTS foo SELECT col1 from t1;";
    node::PlanNodeList plan_trees;
    base::Status status;
    NodeManager nm;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &nm, status));
    ASSERT_EQ(1, plan_trees.size());
    EXPECT_STREQ(R"sql(+-[kPlanTypeDeploy]
  +-if_not_exists: true
  +-name: foo
  +-options: <nil>
  +-stmt:
    +-node[kQuery]: kQuerySelect
      +-distinct_opt: false
      +-where_expr: null
      +-group_expr_list: null
      +-having_expr: null
      +-order_expr_list: null
      +-limit: null
      +-select_list[list]:
      |  +-0:
      |    +-node[kResTarget]
      |      +-val:
      |      |  +-expr[column ref]
      |      |    +-relation_name: <nil>
      |      |    +-column_name: col1
      |      +-name: <nil>
      +-tableref_list[list]:
      |  +-0:
      |    +-node[kTableRef]: kTable
      |      +-table: t1
      |      +-alias: <nil>
      +-window_list: [])sql", plan_trees.front()->GetTreeString().c_str());
    auto deploy_stmt = dynamic_cast<node::DeployPlanNode*>(plan_trees.front());
    ASSERT_TRUE(deploy_stmt != nullptr);
    EXPECT_STREQ(R"sql(SELECT
  col1
FROM
  t1
)sql", deploy_stmt->StmtStr().c_str());
}

TEST_F(PlannerV2Test, DeployPlanNodeWithOptionsTest) {
    const std::string sql = "DEPLOY IF NOT EXISTS foo OPTIONS(long_windows='w1:100s') SELECT col1 from t1;";
    node::PlanNodeList plan_trees;
    base::Status status;
    NodeManager nm;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &nm, status));
    ASSERT_EQ(1, plan_trees.size());
    EXPECT_STREQ(R"sql(+-[kPlanTypeDeploy]
  +-if_not_exists: true
  +-name: foo
  +-options:
  |  +-long_windows:
  |    +-expr[primary]
  |      +-value: w1:100s
  |      +-type: string
  +-stmt:
    +-node[kQuery]: kQuerySelect
      +-distinct_opt: false
      +-where_expr: null
      +-group_expr_list: null
      +-having_expr: null
      +-order_expr_list: null
      +-limit: null
      +-select_list[list]:
      |  +-0:
      |    +-node[kResTarget]
      |      +-val:
      |      |  +-expr[column ref]
      |      |    +-relation_name: <nil>
      |      |    +-column_name: col1
      |      +-name: <nil>
      +-tableref_list[list]:
      |  +-0:
      |    +-node[kTableRef]: kTable
      |      +-table: t1
      |      +-alias: <nil>
      +-window_list: [])sql",
                 plan_trees.front()->GetTreeString().c_str());
    auto deploy_stmt = dynamic_cast<node::DeployPlanNode *>(plan_trees.front());
    ASSERT_TRUE(deploy_stmt != nullptr);
    EXPECT_STREQ(R"sql(SELECT
  col1
FROM
  t1
)sql",
                 deploy_stmt->StmtStr().c_str());
}

TEST_F(PlannerV2Test, LoadDataPlanNodeTest) {
    const std::string sql = "LOAD DATA INFILE 'hello.csv' INTO TABLE t1 OPTIONS (key = 'cat') CONFIG ( foo = 'bar' );";
    node::PlanNodeList plan_trees;
    base::Status status;
    NodeManager nm;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &nm, status));
    ASSERT_EQ(1, plan_trees.size());
    ASSERT_STREQ(R"sql(+-[kPlanTypeLoadData]
  +-file: hello.csv
  +-db: <nil>
  +-table: t1
  +-options:
  |  +-key:
  |    +-expr[primary]
  |      +-value: cat
  |      +-type: string
  +-config_options:
    +-foo:
      +-expr[primary]
        +-value: bar
        +-type: string)sql", plan_trees.front()->GetTreeString().c_str());
}

TEST_F(PlannerV2Test, SelectIntoPlanNodeTest) {
    const std::string sql = "SELECT c2 FROM t0 INTO OUTFILE 'm.txt' OPTIONS (key = 'cat') CONFIG (bar='foo');";
    node::PlanNodeList plan_trees;
    base::Status status;
    NodeManager nm;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &nm, status));
    ASSERT_EQ(1, plan_trees.size());
    EXPECT_STREQ(R"sql(+-[kPlanTypeSelectInto]
  +-out_file: m.txt
  +- query:
  |  +-[kQueryPlan]
  |    +-[kProjectPlan]
  |      +-table: t0
  |      +-project_list_vec[list]:
  |        +-[kProjectList]
  |          +-projects on table [list]:
  |            +-[kProjectNode]
  |              +-[0]c2: c2
  |      +-[kTablePlan]
  |        +-table: t0
  +-options:
  |  +-key:
  |    +-expr[primary]
  |      +-value: cat
  |      +-type: string
  +-config_options:
    +-bar:
      +-expr[primary]
        +-value: foo
        +-type: string)sql", plan_trees.front()->GetTreeString().c_str());

    const auto select_into = dynamic_cast<node::SelectIntoPlanNode*>(plan_trees.front());
    ASSERT_TRUE(select_into != nullptr);
    EXPECT_STREQ(R"sql(SELECT
  c2
FROM
  t0
)sql", select_into->QueryStr().c_str());
}

TEST_F(PlannerV2Test, SetPlanNodeTest) {
    const auto sql = "SET @@global.execute_mode = 'online'";
    node::PlanNodeList plan_trees;
    base::Status status;
    NodeManager nm;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &nm, status));
    ASSERT_EQ(1, plan_trees.size());
    EXPECT_STREQ(R"sql(+-[kPlanTypeSet]
  +-scope: GlobalSystemVariable
  +-key: execute_mode
  +-value:
    +-expr[primary]
      +-value: online
      +-type: string)sql", plan_trees.front()->GetTreeString().c_str());
}
//
// TEST_F(PlannerTest, CreateSpParseTest) {
//    std::string sql =
//        "create procedure sp1(const c1 string, const c3 int, c4 bigint,"
//        "c5 float, c6 double, c7 timestamp, c8 date) "
//        "begin "
//        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
//        "WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS "
//        "BETWEEN 2 PRECEDING AND CURRENT ROW);"
//        "end;";
//
//    base::Status status;
//    node::NodePointVector parser_trees;
//    int ret = parser_->parse(sql, parser_trees, manager_, status);
//    ASSERT_EQ(0, ret);
//
//    sql = "show procedure status;";
//    ret = parser_->parse(sql, parser_trees, manager_, status);
//    ASSERT_EQ(0, ret);
//    sql = "show procedure;";
//    ret = parser_->parse(sql, parser_trees, manager_, status);
//    ASSERT_EQ(1, ret);
//    sql = "show create procedure test.sp1;";
//    ret = parser_->parse(sql, parser_trees, manager_, status);
//    ASSERT_EQ(0, ret);
//    sql = "show create procedure sp1;";
//    ret = parser_->parse(sql, parser_trees, manager_, status);
//    ASSERT_EQ(0, ret);
//
//    sql =
//        "create procedure sp1(const c1 string, const c3 int, c4 bigint,"
//        "c5 float, c6 double, c7 timestamp, c8 date) "
//        "begin "
//        "insert into t1 (col1, col2, col3, col4) values(1, 2, 3.1, \"string\");"
//        "end;";
//    ret = parser_->parse(sql, parser_trees, manager_, status);
//    ASSERT_EQ(1, ret);
//}

class PlannerV2ErrorTest : public ::testing::TestWithParam<SqlCase> {
 public:
    PlannerV2ErrorTest() { manager_ = new NodeManager(); }

    ~PlannerV2ErrorTest() { delete manager_; }

 protected:
    NodeManager *manager_;
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(PlannerV2ErrorTest);

INSTANTIATE_TEST_SUITE_P(SqlErrorQuery, PlannerV2ErrorTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/error_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(SqlUnsupporQuery, PlannerV2ErrorTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/error_unsupport_sql.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(SqlErrorRequestQuery, PlannerV2ErrorTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/plan/error_request_query.yaml", FILTERS)));

TEST_P(PlannerV2ErrorTest, RequestModePlanErrorTest) {
    auto &sql_case = GetParam();
    auto &sqlstr = sql_case.sql_str();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "request-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode() << " for request mode error test";
        return;
    }
    base::Status status;
    node::PlanNodeList plan_trees;
    EXPECT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, manager_, status, false, false)) << status;
    if (!sql_case.expect_.msg_.empty()) {
        EXPECT_EQ(absl::StripAsciiWhitespace(sql_case.expect_.msg_), status.msg);
    }
}
TEST_P(PlannerV2ErrorTest, ClusterRequestModePlanErrorTest) {
    auto& sql_case = GetParam();
    auto& sqlstr = sql_case.sql_str();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode() << " for cluster request mode error test";
        return;
    }
    base::Status status;
    node::PlanNodeList plan_trees;
    EXPECT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, manager_, status, false, true)) << status;
    if (!sql_case.expect_.msg_.empty()) {
        EXPECT_EQ(absl::StripAsciiWhitespace(sql_case.expect_.msg_), status.msg);
    }
}
TEST_P(PlannerV2ErrorTest, BatchModePlanErrorTest) {
    auto& sql_case = GetParam();
    auto& sqlstr = sql_case.sql_str();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "batch-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode() << " for batch mode error test";
        return;
    }
    base::Status status;
    node::PlanNodeList plan_trees;
    EXPECT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, manager_, status, true)) << status;
    if (!sql_case.expect_.msg_.empty()) {
        EXPECT_EQ(absl::StripAsciiWhitespace(sql_case.expect_.msg_), status.msg);
    }
}

TEST_F(PlannerV2ErrorTest, SqlSyntaxErrorTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string &sql, const int code, const std::string &msg) {
        base::Status status;
        node::PlanNodeList plan_trees;
        ASSERT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status, true));
        EXPECT_EQ(code, status.code) << status;
        EXPECT_EQ(msg, status.msg) << status;
    };

    expect_converted("SELECT FROM t1;", common::kSyntaxError,
                     "Syntax error: SELECT list must not be empty [at 1:8]\n"
                     "SELECT FROM t1;\n"
                     "       ^");
    expect_converted("SELECT t1.col1, t2.col2 FROM t1 LAST JOIN t2 when t1.id=t2.id;", common::kSyntaxError,
                     "Syntax error: Expected keyword ON or keyword USING but got keyword WHEN [at 1:46]\n"
                     "SELECT t1.col1, t2.col2 FROM t1 LAST JOIN t2 when t1.id=t2.id;\n"
                     "                                             ^");

    // config clause: can't appear inside subquery
    expect_converted("select T1.a as a, T2.b as b from Table1 as T1 config (k='k') last join Table2 T2 using (c, d);",
                     common::kSyntaxError,
                     R"s(Syntax error: Expected end of input but got keyword LAST [at 1:62]
...a as a, T2.b as b from Table1 as T1 config (k='k') last join Table2 T2 usi...
                                                      ^)s");
}


TEST_F(PlannerV2ErrorTest, NonSupportSQL) {
    node::NodeManager node_manager;

    auto expect_converted = [&](const std::string &sql, const int code, const std::string &msg) {
      base::Status status;
      node::PlanNodeList plan_trees;
      ASSERT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status, true)) << status;
      EXPECT_EQ(code, status.code) << status;
      EXPECT_EQ(msg, status.msg) << status;
    };

    expect_converted(
        R"(
        SELECT SUM(COL2) over w1 from t1 GROUP BY COL1
        WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
        )",
        common::kPlanError, "Can't support group clause and window clause simultaneously");

    expect_converted(
        R"(
        SELECT SUM(COL2) over w1 from t1 HAVING SUM(COL2) >0
        WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
        )",
        common::kPlanError, "Can't support having clause and window clause simultaneously");

    expect_converted(
        R"(
        SELECT SUM(COL2) over w1,  SUM(COL3) from t1
        WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
        )",
        common::kPlanError, "Can't support table aggregation and window aggregation simultaneously");

    expect_converted(R"sql(select 'mike' like 'm%' escape '2c';)sql",
        common::kUnsupportSql, "escape value is not string or string size >= 2");

    expect_converted(R"sql(SELECT lag(COL2, lag(col1, 1)) over w1 from t1
                                 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);)sql",
                     common::kUnsupportSql, "INVALID_ARGUMENT: offset can only be constant");
}

TEST_F(PlannerV2ErrorTest, NonSupportOnlineServingSQL) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string &sql, const int code, const std::string &msg) {
      base::Status status;
      node::PlanNodeList plan_trees;
      // Generate SQL logical plan for online serving
      ASSERT_FALSE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status, false)) << status;
      ASSERT_EQ(code, status.code) << status;
      ASSERT_EQ(msg, status.msg) << status;
      std::cout << msg << std::endl;
    };


    expect_converted(
        R"(
        SELECT COL1 from t1 GROUP BY COL1;
        )",
        common::kPlanError, "Non-support kGroupPlan Op in online serving");

    expect_converted(
        R"(
        SELECT SUM(COL2) from t1 HAVING SUM(COL2) >0;
        )",
        common::kPlanError, "Non-support HAVING Op in online serving");
    expect_converted(
        R"(
        SELECT SUM(COL2) from t1;
        )",
        common::kPlanError, "Aggregate over a table cannot be supported in online serving");
    expect_converted(
        R"(
        SELECT COL1 FROM t1 order by COL1;
        )",
        common::kPlanError, "Non-support kSortPlan Op in online serving");

    expect_converted(
        R"(LOAD DATA INFILE 'a.csv' INTO TABLE t1 OPTIONS(foo='bar', num=1);)",
        common::kPlanError, "Non-support LOAD DATA Op in online serving");
}


TEST_F(PlannerV2ErrorTest, ClusterOnlineTrainingSQL) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string &sql, const int code, const std::string &msg) {
      base::Status status;
      node::PlanNodeList plan_trees;
      // Generate SQL logical plan for online serving
      auto ret = plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status, true, true);
      ASSERT_EQ(ret, code == common::kOk);
      ASSERT_EQ(code, status.code) << status;
      ASSERT_EQ(msg, status.msg) << status;
      std::cout << msg << std::endl;
    };

    expect_converted(
        R"(
        SELECT COL1 from t1 GROUP BY COL1;
        )",
        common::kOk, "ok");

    expect_converted(
        R"(
        SELECT SUM(COL2) from t1 HAVING SUM(COL2) >0;
        )",
        common::kOk, "ok");
    expect_converted(
        R"(
        SELECT SUM(COL2) from t1;
        )",
        common::kOk, "ok");
    expect_converted(
        R"(
        SELECT COL1 FROM t1 order by COL1;
        )",
        common::kPlanError, "Non-support kSortPlan Op in cluster online training");

    expect_converted(
        R"(
        SELECT SUM(COL2) over w1 FROM t1
        WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
        )",
        common::kOk, "ok");

    expect_converted(
        R"(
        SELECT COL1 FROM t1 LAST JOIN t2 order by COL5 on t1.COL1 = t2.COL1;
        )",
        common::kPlanError, "Non-support kJoinPlan Op in cluster online training");
}

TEST_F(PlannerV2Test, ClusterOnlinexTrainingSQLTest) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string &sql) {
      base::Status status;
      node::PlanNodeList plan_trees;
      // Generate SQL logical plan for online serving
      ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status, true, true)) << status;
    };
    expect_converted(
        R"(
        SELECT COL1 from t1 ;
        )");
    expect_converted(
        R"(
        SELECT COL1 from t1 LIMIT 10;
        )");

    expect_converted(
        R"(
        SELECT COL1 from t1 WHERE col2 = 1 LIMIT 10;
        )");

    expect_converted(
        R"(
        SELECT 1.0 as c1, 2 as c2;
        )");
}

TEST_F(PlannerV2Test, GetPlanLimitCnt) {
    node::NodeManager node_manager;
    auto expect_converted = [&](const std::string &sql, int limit_cnt) {
      base::Status status;
      node::PlanNodeList plan_trees;
      // Generate SQL logical plan for online serving
      ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, manager_, status, true, false)) << status;
      ASSERT_EQ(1, plan_trees.size());
      ASSERT_EQ(limit_cnt, plan::PlanAPI::GetPlanLimitCount(plan_trees[0]));
    };


    expect_converted(
        R"(
        SELECT COL1 from t1 LIMIT 10;
        )", 10);

    expect_converted(
        R"(
        SELECT COL1 from (SELECT COL1, COL2 FROM t1 LIMIT 5) as t11 LIMIT 10;
        )", 5);
    expect_converted(
        R"(
        SELECT COL1 from t1 last join (SELECT COL1, COL2 FROM t2 LIMIT 5) as t22 on t1.col1 = t22.col1 LIMIT 10;
        )", 10);
}
}  // namespace plan
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
