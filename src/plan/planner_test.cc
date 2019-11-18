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
    // validate limit 10
    ASSERT_EQ(node::kPlanTypeLimit, select_ptr->GetChildren()[0]->GetType());
    node::LimitPlanNode *limit_node =
        (node::LimitPlanNode *)select_ptr->GetChildren()[0];
    ASSERT_EQ(10, limit_node->GetLimitCnt());

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = limit_node->GetChildren();
    ASSERT_EQ(1u, plan_vec.size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(
        3u, ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects().size());

    delete planner_ptr;
}

TEST_F(PlannerTest, SimplePlannerCreatePlanWithWindowProjectTest) {
    node::NodePointVector list;
    node::PlanNodeList trees;
    base::Status status;
    int ret = parser_->parse(
        "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 , max(t1.age) "
        "over w1 FROM t1 limit 10;",
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
    // validate limit 10
    // validate limit 10
    ASSERT_EQ(node::kPlanTypeLimit, select_ptr->GetChildren()[0]->GetType());
    node::LimitPlanNode *limit_node =
        (node::LimitPlanNode *)select_ptr->GetChildren()[0];
    ASSERT_EQ(10, limit_node->GetLimitCnt());

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = limit_node->GetChildren();
    ASSERT_EQ(2u, plan_vec.size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(
        3u, ((node::ProjectListPlanNode *)plan_vec.at(0))->GetProjects().size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(1)->GetType());
    ASSERT_EQ(
        1u, ((node::ProjectListPlanNode *)plan_vec.at(1))->GetProjects().size());
    delete planner_ptr;
}

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
    ASSERT_EQ(type::Type::kString, table->columns(3).type());
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

}  // namespace plan
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
