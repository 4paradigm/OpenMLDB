/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * resolve_udf_def_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/
#include "passes/resolve_udf_def.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
#include "plan/planner.h"

namespace fesql {
namespace passes {

class ResolveUdfDefTest : public ::testing::Test {};

TEST_F(ResolveUdfDefTest, TestResolve) {
    parser::FeSQLParser parser;
    Status status;
    node::NodeManager nm;
    plan::SimplePlanner planner(&nm);

    const std::string udf1 =
        "%%fun\n"
        "def test(x:i32, y:i32):i32\n"
        "    return x+y\n"
        "end\n";
    node::NodePointVector list1;
    int ok = parser.parse(udf1, list1, &nm, status);
    ASSERT_EQ(0, ok);

    node::PlanNodeList trees;
    planner.CreatePlanTree(list1, trees, status);
    ASSERT_EQ(1u, trees.size());

    auto def_plan = dynamic_cast<node::FuncDefPlanNode *>(trees[0]);
    ASSERT_TRUE(def_plan != nullptr);

    ResolveUdfDef resolver;
    status = resolver.Visit(def_plan->fn_def_);
    ASSERT_TRUE(status.isOK());
}

TEST_F(ResolveUdfDefTest, TestResolveFailed) {
    parser::FeSQLParser parser;
    Status status;
    node::NodeManager nm;
    plan::SimplePlanner planner(&nm);

    const std::string udf1 =
        "%%fun\n"
        "def test(x:i32, y:i32):i32\n"
        "    return x+z\n"
        "end\n";
    node::NodePointVector list1;
    int ok = parser.parse(udf1, list1, &nm, status);
    ASSERT_EQ(0, ok);

    node::PlanNodeList trees;
    planner.CreatePlanTree(list1, trees, status);
    ASSERT_EQ(1u, trees.size());

    auto def_plan = dynamic_cast<node::FuncDefPlanNode *>(trees[0]);
    ASSERT_TRUE(def_plan != nullptr);

    ResolveUdfDef resolver;
    status = resolver.Visit(def_plan->fn_def_);
    ASSERT_TRUE(!status.isOK());
}

}  // namespace passes
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
