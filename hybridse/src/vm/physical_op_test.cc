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
#include "vm/physical_op.h"

#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "testing/test_base.h"
#include "udf/default_udf_library.h"
#include "vm/physical_plan_context.h"
namespace hybridse {
namespace vm {
class PhysicalOpTest : public ::testing::Test {
 public:
    PhysicalOpTest() {}
    ~PhysicalOpTest() {}
};
TEST_F(PhysicalOpTest, PhysicalAggregationNodeWithNewChildrenTest) {
    node::NodeManager nm;
    hybridse::type::Database db;
    db.set_name("db");
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    AddTable(db, table_def);
    auto catalog = BuildSimpleCatalog(db);

    vm::PhysicalPlanContext plan_ctx(&nm, udf::DefaultUdfLibrary::get(), "db", catalog, nullptr, false);
    vm::PhysicalAggregationNode* aggreration_node = nullptr;
    vm::PhysicalTableProviderNode* table_node;
    plan_ctx.CreateOp(&table_node, catalog->GetTable("db", "t1"));
    ColumnProjects projects;
    {
        node::ExprListNode args;
        args.AddChild(nm.MakeColumnRefNode("col1", "t1"));
        projects.Add("col1_sum", nm.MakeFuncNode("sum", &args, nullptr), nullptr);
    }
    {
        node::ExprListNode args;
        args.AddChild(nm.MakeColumnRefNode("col2", "t1"));
        projects.Add("col2_sum", nm.MakeFuncNode("sum", &args, nullptr), nullptr);
    }
    node::BinaryExpr* condition = nullptr;
    {
        node::ExprListNode args;
        args.AddChild(nm.MakeColumnRefNode("col2", "t1"));
        condition = nm.MakeBinaryExprNode(nm.MakeFuncNode("sum", &args, nullptr), nm.MakeConstNode(0), node::kFnOpGt);
    }
    plan_ctx.CreateOp(&aggreration_node, table_node, projects, condition);
    ASSERT_TRUE(nullptr != aggreration_node);
    ASSERT_EQ(
        "PROJECT(type=Aggregation, having_condition=sum(t1.col2) > 0)\n"
        "  DATA_PROVIDER(table=t1)",
        aggreration_node->GetTreeString());

    vm::PhysicalSimpleProjectNode* simple_project;
    {
        ColumnProjects projects;
        projects.Add("col1", nm.MakeColumnRefNode("col1", "t1", "db"), nullptr);
        projects.Add("col2", nm.MakeColumnRefNode("col2", "t1", "db"), nullptr);
        plan_ctx.CreateOp(&simple_project, table_node, projects);
    }
    vm::PhysicalOpNode* new_aggregation_node;
    std::cout << simple_project->GetTreeString();
    ASSERT_TRUE(aggreration_node->WithNewChildren(&nm, {simple_project}, &new_aggregation_node).isOK());
    ASSERT_EQ(
        "PROJECT(type=Aggregation, having_condition=sum(#3) > 0)\n"
        "  SIMPLE_PROJECT(sources=(db.t1.col1, db.t1.col2))\n"
        "    DATA_PROVIDER(table=t1)",
        new_aggregation_node->GetTreeString());
    // ASSERT having condition sum(#3) === sum(t1.col2)
    size_t col2_column_id = 0;
    ASSERT_TRUE(simple_project->schemas_ctx()->ResolveColumnID("db", "t1", "col2", &col2_column_id).isOK());
    ASSERT_EQ(3, col2_column_id);
}
TEST_F(PhysicalOpTest, PhysicalGroupAggrerationNodeWithNewChildrenTest) {
    node::NodeManager nm;
    hybridse::type::Database db;
    db.set_name("db");
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    AddTable(db, table_def);
    auto catalog = BuildSimpleCatalog(db);

    vm::PhysicalPlanContext plan_ctx(&nm, udf::DefaultUdfLibrary::get(), "db", catalog, nullptr, false);

    vm::PhysicalTableProviderNode* table_node;
    plan_ctx.CreateOp(&table_node, catalog->GetTable("db", "t1"));

    vm::PhysicalGroupNode* group_node = nullptr;
    auto group_keys = nm.MakeExprList();
    group_keys->AddChild(nm.MakeColumnRefNode("col0", "t1"));
    plan_ctx.CreateOp(&group_node, table_node, group_keys);

    ColumnProjects projects;
    {
        node::ExprListNode args;
        args.AddChild(nm.MakeColumnRefNode("col1", "t1"));
        projects.Add("col1_sum", nm.MakeFuncNode("sum", &args, nullptr), nullptr);
    }
    {
        node::ExprListNode args;
        args.AddChild(nm.MakeColumnRefNode("col2", "t1"));
        projects.Add("col2_sum", nm.MakeFuncNode("sum", &args, nullptr), nullptr);
    }
    node::BinaryExpr* condition = nullptr;
    {
        node::ExprListNode args;
        args.AddChild(nm.MakeColumnRefNode("col2", "t1"));
        condition = nm.MakeBinaryExprNode(nm.MakeFuncNode("sum", &args, nullptr), nm.MakeConstNode(0), node::kFnOpGt);
    }

    vm::PhysicalGroupAggrerationNode* group_aggreration_node = nullptr;
    plan_ctx.CreateOp(&group_aggreration_node, group_node, projects, condition, group_keys);
    ASSERT_TRUE(nullptr != group_aggreration_node);
    ASSERT_EQ(
        "PROJECT(type=GroupAggregation, group_keys=(t1.col0), having_condition=sum(t1.col2) > 0)\n"
        "  GROUP_BY(group_keys=(t1.col0))\n"
        "    DATA_PROVIDER(table=t1)",
        group_aggreration_node->GetTreeString());

    vm::PhysicalSimpleProjectNode* simple_project;
    {
        ColumnProjects projects;
        projects.Add("col0", nm.MakeColumnRefNode("col0", "t1", "db"), nullptr);
        projects.Add("col1", nm.MakeColumnRefNode("col1", "t1", "db"), nullptr);
        projects.Add("col2", nm.MakeColumnRefNode("col2", "t1", "db"), nullptr);
        plan_ctx.CreateOp(&simple_project, table_node, projects);
    }
    vm::PhysicalOpNode* new_group_node;
    vm::PhysicalOpNode* new_group_aggreration_node;
    std::cout << simple_project->GetTreeString();
    ASSERT_TRUE(group_node->WithNewChildren(&nm, {simple_project}, &new_group_node).isOK());
    ASSERT_TRUE(group_aggreration_node->WithNewChildren(&nm, {simple_project}, &new_group_aggreration_node).isOK());
    ASSERT_EQ(
        "PROJECT(type=GroupAggregation, group_keys=(#1), having_condition=sum(#3) > 0)\n"
        "  SIMPLE_PROJECT(sources=(db.t1.col0, db.t1.col1, db.t1.col2))\n"
        "    DATA_PROVIDER(table=t1)",
        new_group_aggreration_node->GetTreeString());
    // ASSERT group key (#1) === t1.cl0
    {
        size_t col2_column_id = 0;
        ASSERT_TRUE(simple_project->schemas_ctx()->ResolveColumnID("db", "t1", "col0", &col2_column_id).isOK());
        ASSERT_EQ(1, col2_column_id);
    }
    // ASSERT having condition sum(#3) === sum(t1.col2)
    {
        size_t col2_column_id = 0;
        ASSERT_TRUE(simple_project->schemas_ctx()->ResolveColumnID("db", "t1", "col2", &col2_column_id).isOK());
        ASSERT_EQ(3, col2_column_id);
    }
}

}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
