/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * planner_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/

#include "plan/planner.h"
#include <vector>
#include <utility>
#include "analyser/analyser.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
namespace fesql {
namespace plan {

using fesql::node::NodeManager;
using fesql::node::PlanNode;
using fesql::node::SQLNode;
using fesql::node::SQLNodeList;
// TODO(chenjing): add ut: 检查SQL的语法树节点预期 2019.10.23
class PlannerTest : public ::testing::Test {
 public:
    PlannerTest() {
        manager_ = new NodeManager();
        parser_ = new parser::FeSQLParser();
    }

    ~PlannerTest() {}

 protected:
    parser::FeSQLParser *parser_;
    NodeManager *manager_;
};

TEST_F(PlannerTest, SimplePlannerCreatePlanTest) {
    node::NodePointVector list;
    base::Status status;
    int ret = parser_->parse(
        "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 FROM t1 limit 10;",
        list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());

    Planner *planner_ptr = new SimplePlanner(manager_);
    node::PlanNodeList plan_trees;
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, plan_trees, status));
    ASSERT_EQ(1u, plan_trees.size());
    PlanNode *plan_ptr = plan_trees.front();
    std::cout << *(plan_ptr) << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *)plan_ptr;

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = select_ptr->GetChildren();
    ASSERT_EQ(1u, plan_vec.size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(
        3u,
        ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects().size());
    {
        node::ProjectPlanNode *project = dynamic_cast<node::ProjectPlanNode *>(
            ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects()[0]);
        ASSERT_EQ(0u, project->GetPos());
    }
    {
        node::ProjectPlanNode *project = dynamic_cast<node::ProjectPlanNode *>(
            ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects()[1]);
        ASSERT_EQ(1u, project->GetPos());
    }
    {
        node::ProjectPlanNode *project = dynamic_cast<node::ProjectPlanNode *>(
            ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects()[2]);
        ASSERT_EQ(2u, project->GetPos());
    }

    // validate limit 10
    ASSERT_EQ(node::kPlanTypeScan, plan_vec.at(0)->GetChildren()[0]->GetType());
    node::ScanPlanNode *scan_ptr = reinterpret_cast<node::ScanPlanNode *>(
        plan_vec.at(0)->GetChildren()[0]);
    ASSERT_EQ(10, scan_ptr->GetLimit());
    delete planner_ptr;
}

TEST_F(PlannerTest, SelectPlanWithWindowProjectTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    int ret = parser_->parse(
        "SELECT COL1, SUM(AMT) OVER w1 as w_amt_sum FROM t \n"
        "WINDOW w1 AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING) limit 10;",
        list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());

    std::cout << *(list[0]) << std::endl;
    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *)plan_ptr;

    // validate merge node
    ASSERT_EQ(node::kPlanTypeMerge, select_ptr->GetChildren()[0]->GetType());
    node::MergePlanNode *merge_ptr =
        (node::MergePlanNode *)select_ptr->GetChildren()[0];

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = merge_ptr->GetChildren();
    ASSERT_EQ(2u, plan_vec.size());

    // check projections 1: simple projection
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(
        1u,
        ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects().size());
    ASSERT_EQ(nullptr, ((node::ProjectListPlanNode *)plan_vec.at(0))->GetW());
    ASSERT_FALSE(((node::ProjectListPlanNode *)plan_vec.at(0))->IsWindowAgg());

    // check projections 2: window agg projection
    ASSERT_EQ(node::kProjectList, plan_vec.at(1)->GetType());
    ASSERT_EQ(
        1u,
        ((node::ProjectListPlanNode *)plan_vec.at(1))->GetProjects().size());
    ASSERT_TRUE(nullptr !=
                ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW());
    ASSERT_EQ(-3, ((node::ProjectListPlanNode *)plan_vec.at(1))
                      ->GetW()
                      ->GetStartOffset());
    ASSERT_EQ(
        3,
        ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW()->GetEndOffset());
    ASSERT_EQ(std::vector<std::string>({"COL2"}),
              ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW()->GetKeys());
    ASSERT_TRUE(((node::ProjectListPlanNode *)plan_vec.at(1))->IsWindowAgg());

    // validate scan node with limit 10
    ASSERT_EQ(plan_vec.at(0)->GetChildren()[0],
              plan_vec.at(1)->GetChildren()[0]);
    ASSERT_EQ(node::kPlanTypeScan, plan_vec.at(0)->GetChildren()[0]->GetType());
    node::ScanPlanNode *scan_ptr = reinterpret_cast<node::ScanPlanNode *>(
        plan_vec.at(0)->GetChildren()[0]);
    ASSERT_EQ(10, scan_ptr->GetLimit());
    delete planner_ptr;
}

TEST_F(PlannerTest, SelectPlanWithMultiWindowProjectTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    const std::string sql =
        "SELECT sum(col1) OVER w1 as w1_col1_sum, sum(col1) OVER w2 as "
        "w2_col1_sum FROM t1 "
        "WINDOW "
        "w1 AS (PARTITION BY col2 ORDER BY `TS` RANGE BETWEEN 1d PRECEDING AND "
        "1s PRECEDING), "
        "w2 AS (PARTITION BY col3 ORDER BY `TS` RANGE BETWEEN 2d PRECEDING AND "
        "1s PRECEDING) "
        "limit 10;";
    int ret = parser_->parse(sql, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());

    std::cout << *(list[0]) << std::endl;
    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *)plan_ptr;

    // validate merge node
    ASSERT_EQ(node::kPlanTypeMerge, select_ptr->GetChildren().at(0)->GetType());
    node::MergePlanNode *merge_node =
        (node::MergePlanNode *)select_ptr->GetChildren()[0];

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = merge_node->GetChildren();
    ASSERT_EQ(2u, merge_node->GetChildren().size());
    plan_vec = merge_node->GetChildren();

    // validate projection 1: window agg over w1
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(
        1u,
        ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects().size());
    ASSERT_TRUE(nullptr !=
                ((node::ProjectListPlanNode *)plan_vec.at(0))->GetW());
    ASSERT_TRUE(((node::ProjectListPlanNode *)plan_vec.at(0))->IsWindowAgg());
    ASSERT_EQ(-1 * 86400000, ((node::ProjectListPlanNode *)plan_vec.at(0))
                                 ->GetW()
                                 ->GetStartOffset());
    ASSERT_EQ(
        -1000,
        ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW()->GetEndOffset());

    // validate projection 1: window agg over w2
    ASSERT_EQ(node::kProjectList, plan_vec.at(1)->GetType());
    ASSERT_EQ(
        1u,
        ((node::ProjectListPlanNode *)plan_vec.at(1))->GetProjects().size());
    ASSERT_TRUE(nullptr !=
                ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW());
    ASSERT_EQ(-2 * 86400000, ((node::ProjectListPlanNode *)plan_vec.at(1))
                                 ->GetW()
                                 ->GetStartOffset());
    ASSERT_EQ(
        -1000,
        ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW()->GetEndOffset());
    ASSERT_EQ(std::vector<std::string>({"col3"}),
              ((node::ProjectListPlanNode *)plan_vec.at(1))->GetW()->GetKeys());
    ASSERT_TRUE(((node::ProjectListPlanNode *)plan_vec.at(1))->IsWindowAgg());

    ASSERT_EQ(node::kPlanTypeScan, plan_vec.at(0)->GetChildren()[0]->GetType());
    node::ScanPlanNode *scan_ptr = reinterpret_cast<node::ScanPlanNode *>(
        plan_vec.at(0)->GetChildren()[0]);
    ASSERT_EQ(10, scan_ptr->GetLimit());
    delete planner_ptr;
}

TEST_F(PlannerTest, MergeProjectListPlanPostTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    const std::string sql =
        "SELECT sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w2 as w2_col3_sum, "
        "sum(col4) OVER w2 as w2_col4_sum, "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "col2, "
        "sum(col1) OVER w2 as w2_col1_sum "
        "FROM t1 "
        "WINDOW "
        "w1 AS (PARTITION BY col2 ORDER BY `TS` RANGE BETWEEN 1d PRECEDING AND "
        "1s PRECEDING), "
        "w2 AS (PARTITION BY col3 ORDER BY `TS` RANGE BETWEEN 2d PRECEDING AND "
        "1s PRECEDING) "
        "limit 10;";
    int ret = parser_->parse(sql, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());

    std::cout << *(list[0]) << std::endl;
    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kPlanTypeSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *)plan_ptr;

    // validate merge node
    ASSERT_EQ(node::kPlanTypeMerge, select_ptr->GetChildren().at(0)->GetType());
    node::MergePlanNode *merge_node =
        (node::MergePlanNode *)select_ptr->GetChildren()[0];

    std::vector<std::pair<uint32_t, uint32_t>> pos_mapping =
        merge_node->GetPosMapping();
    ASSERT_EQ(7, pos_mapping.size());
    ASSERT_EQ(std::make_pair(1u, 0u), pos_mapping[0]);
    ASSERT_EQ(std::make_pair(2u, 0u), pos_mapping[1]);
    ASSERT_EQ(std::make_pair(2u, 1u), pos_mapping[2]);
    ASSERT_EQ(std::make_pair(0u, 0u), pos_mapping[3]);
    ASSERT_EQ(std::make_pair(1u, 1u), pos_mapping[4]);
    ASSERT_EQ(std::make_pair(0u, 1u), pos_mapping[5]);
    ASSERT_EQ(std::make_pair(2u, 2u), pos_mapping[6]);

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = merge_node->GetChildren();
    ASSERT_EQ(3u, merge_node->GetChildren().size());
    plan_vec = merge_node->GetChildren();

    // validate projection 1: window agg over w1
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    {
        node::ProjectListPlanNode *project_list =
            dynamic_cast<node::ProjectListPlanNode *>(plan_vec.at(0));

        ASSERT_EQ(2u, project_list->GetProjects().size());
        ASSERT_TRUE(nullptr == project_list->GetW());
        ASSERT_FALSE(project_list->IsWindowAgg());

        // validate col1 pos 3
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[0]);
            ASSERT_EQ(3u, project->GetPos());
        }
        // validate col2 pos 5
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[1]);
            ASSERT_EQ(5u, project->GetPos());
        }

        ASSERT_EQ(node::kPlanTypeScan,
                  project_list->GetChildren()[0]->GetType());
        node::ScanPlanNode *scan_ptr = reinterpret_cast<node::ScanPlanNode *>(
            project_list->GetChildren()[0]);
        ASSERT_EQ(10, scan_ptr->GetLimit());
    }
    {
        node::ProjectListPlanNode *project_list =
            dynamic_cast<node::ProjectListPlanNode *>(plan_vec.at(1));

        ASSERT_EQ(2u, project_list->GetProjects().size());
        ASSERT_FALSE(nullptr == project_list->GetW());
        ASSERT_EQ(-1 * 86400000, project_list->GetW()->GetStartOffset());
        ASSERT_EQ(-1000, project_list->GetW()->GetEndOffset());

        // validate w1_col1_sum pos 0
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[0]);
            ASSERT_EQ(0, project->GetPos());
        }
        // validate w1_col3_sum pos 0
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[1]);
            ASSERT_EQ(4u, project->GetPos());
        }

        ASSERT_EQ(node::kPlanTypeScan,
                  project_list->GetChildren()[0]->GetType());
        node::ScanPlanNode *scan_ptr = reinterpret_cast<node::ScanPlanNode *>(
            project_list->GetChildren()[0]);
        ASSERT_EQ(10, scan_ptr->GetLimit());
    }
    {
        // validate projection 1: window agg over w2
        ASSERT_EQ(node::kProjectList, plan_vec.at(2)->GetType());
        node::ProjectListPlanNode *project_list =
            dynamic_cast<node::ProjectListPlanNode *>(plan_vec.at(2));

        ASSERT_EQ(3u, project_list->GetProjects().size());
        ASSERT_TRUE(nullptr != project_list->GetW());
        ASSERT_EQ(-2 * 86400000, project_list->GetW()->GetStartOffset());
        ASSERT_EQ(-1000, project_list->GetW()->GetEndOffset());
        ASSERT_EQ(std::vector<std::string>({"col3"}),
                  project_list->GetW()->GetKeys());
        ASSERT_TRUE(project_list->IsWindowAgg());

        // validate w2_col3_sum pos 1
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[0]);
            ASSERT_EQ(1u, project->GetPos());
        }
        // validate w2_col4_sum pos 2
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[1]);
            ASSERT_EQ(2u, project->GetPos());
        }
        // validate w2_col1_sum pos 6
        {
            node::ProjectPlanNode *project =
                dynamic_cast<node::ProjectPlanNode *>(
                    project_list->GetProjects()[2]);
            ASSERT_EQ(6u, project->GetPos());
        }

        ASSERT_EQ(node::kPlanTypeScan,
                  project_list->GetChildren()[0]->GetType());
        node::ScanPlanNode *scan_ptr = reinterpret_cast<node::ScanPlanNode *>(
            project_list->GetChildren()[0]);
        ASSERT_EQ(10, scan_ptr->GetLimit());
    }

    delete planner_ptr;
}  // namespace plan

TEST_F(PlannerTest, CreateStmtPlanTest) {
    const std::string sql_str =
        "create table IF NOT EXISTS test(\n"
        "    column1 int NOT NULL,\n"
        "    column2 timestamp NOT NULL,\n"
        "    column3 int NOT NULL,\n"
        "    column4 string NOT NULL,\n"
        "    column5 int NOT NULL,\n"
        "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
        ");";

    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    int ret = parser_->parse(sql_str, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    std::cout << *(list[0]) << std::endl;

    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;

    // validate create plan
    ASSERT_EQ(node::kPlanTypeCreate, plan_ptr->GetType());
    node::CreatePlanNode *createStmt = (node::CreatePlanNode *)plan_ptr;

    type::TableDef table_def;
    TransformTableDef(createStmt->GetTableName(),
                      createStmt->GetColumnDescList(), &table_def, status);

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
    // TODO(chenjing): version and version count test
    //    ASSERT_EQ("column5", index_node->GetVersion());
    //    ASSERT_EQ(3, index_node->GetVersionCount());
}

TEST_F(PlannerTest, CmdStmtPlanTest) {
    const std::string sql_str = "show databases;";

    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    int ret = parser_->parse(sql_str, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    std::cout << *(list[0]) << std::endl;

    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;

    // validate create plan
    ASSERT_EQ(node::kPlanTypeCmd, plan_ptr->GetType());
    node::CmdPlanNode *cmd_plan = (node::CmdPlanNode *)plan_ptr;
    ASSERT_EQ(node::kCmdShowDatabases, cmd_plan->GetCmdType());
}

TEST_F(PlannerTest, FunDefPlanTest) {
    const std::string sql_str =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend";

    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    int ret = parser_->parse(sql_str, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    std::cout << *(list[0]) << std::endl;

    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    std::cout << status.msg << std::endl;
    ASSERT_EQ(1u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;

    // validate create plan
    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
    node::FuncDefPlanNode *plan =
        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
    ASSERT_TRUE(nullptr != plan->GetFnNodeList());
}

TEST_F(PlannerTest, FunDefAndSelectPlanTest) {
    const std::string sql_str =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nselect col1, test(col1, col2) from t1 limit 1;";

    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    int ret = parser_->parse(sql_str, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2u, list.size());
    std::cout << *(list[0]) << std::endl;
    std::cout << *(list[1]) << std::endl;

    Planner *planner_ptr = new SimplePlanner(manager_);
    ASSERT_EQ(0, planner_ptr->CreatePlanTree(list, trees, status));
    std::cout << status.msg << std::endl;
    ASSERT_EQ(2u, trees.size());
    PlanNode *plan_ptr = trees[0];
    ASSERT_TRUE(NULL != plan_ptr);
    std::cout << *plan_ptr << std::endl;

    // validate fundef plan
    ASSERT_EQ(node::kPlanTypeFuncDef, plan_ptr->GetType());
    node::FuncDefPlanNode *plan =
        dynamic_cast<node::FuncDefPlanNode *>(plan_ptr);
    ASSERT_TRUE(nullptr != plan->GetFnNodeList());

    // validate select plan
    plan_ptr = trees[1];
    ASSERT_TRUE(NULL != plan_ptr);
    std::cout << *plan_ptr << std::endl;
    // validate fundef plan
    ASSERT_EQ(node::kPlanTypeSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_plan =
        dynamic_cast<node::SelectPlanNode *>(plan_ptr);
    ASSERT_EQ(1, select_plan->GetChildrenSize());
}

}  // namespace plan
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
