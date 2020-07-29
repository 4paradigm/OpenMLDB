/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * lambdafy_projects_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/
#include "passes/lambdafy_projects.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"

namespace fesql {
namespace passes {

class LambdafyProjectsTest : public ::testing::Test {};

TEST_F(LambdafyProjectsTest, Test) {
    vm::SchemaSourceList input_schemas;
    auto schema = udf::MakeLiteralSchema<int32_t, float>();
    input_schemas.AddSchemaSource(&schema);

    parser::FeSQLParser parser;
    Status status;
    node::NodeManager nm;
    plan::SimplePlanner planner(&nm);

    const std::string udf1 =
        "select a, b, c, "
        "sum(d), "
        "count_where(x, y > 2), "
        "count(c) + g(sum(a + 1 + f(max(d)))) + 1,"
        "*, count(*),"
        "sum(slice(x, 1, 3))"
        "from t1;";
    node::NodePointVector list1;
    int ok = parser.parse(udf1, list1, &nm, status);
    ASSERT_EQ(0, ok);

    node::PlanNodeList trees;
    planner.CreatePlanTree(list1, trees, status);
    ASSERT_EQ(1u, trees.size());

    auto query_plan = dynamic_cast<node::QueryPlanNode *>(trees[0]);
    ASSERT_TRUE(query_plan != nullptr);

    auto project_plan =
        dynamic_cast<node::ProjectPlanNode *>(query_plan->GetChildren()[0]);
    ASSERT_TRUE(project_plan != nullptr);

    project_plan->Print(std::cerr, "");
    auto project_list_node = dynamic_cast<node::ProjectListNode *>(
        project_plan->project_list_vec_[0]);
    ASSERT_TRUE(project_list_node != nullptr);

    udf::DefaultUDFLibrary lib;
    lib.SetIsUDAF("count_where", 2);
    lib.SetIsUDAF("slice", 3);
    LambdafyProjects transformer(&nm, &lib, input_schemas);

    std::vector<int> is_agg_vec;
    node::LambdaNode *lambda;
    status = transformer.Transform(project_list_node->GetProjects(), &lambda,
                                   &is_agg_vec);
    ASSERT_TRUE(status.isOK());
    std::vector<int> expect_is_agg = {0, 0, 0, 1, 1, 1, 0, 0, 1, 1};
    ASSERT_TRUE(is_agg_vec.size() == expect_is_agg.size());
    for (size_t i = 0; i < expect_is_agg.size(); ++i) {
        ASSERT_EQ(expect_is_agg[i], is_agg_vec[i]);
    }
    lambda->Print(std::cerr, "");
}

}  // namespace passes
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
