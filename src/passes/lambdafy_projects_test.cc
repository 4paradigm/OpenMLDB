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

#include "passes/lambdafy_projects.h"
#include "gtest/gtest.h"
#include "plan/plan_api.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"

namespace hybridse {
namespace passes {

class LambdafyProjectsTest : public ::testing::Test {};

TEST_F(LambdafyProjectsTest, Test) {
    auto schema = udf::MakeLiteralSchema<int32_t, float, double>();
    vm::SchemasContext schemas_ctx;
    schemas_ctx.BuildTrivial({&schema});

    Status status;
    node::NodeManager nm;

    const std::string udf1 =
        "select "
        "    col_0, col_1 * col_2, "
        "    substring(\"hello\", 1, 3), "
        "    sum(col_0), "
        "    count_where(col_1, col_2 > 2), "
        "    count(col_0) + log(sum(col_1 + 1 + abs(max(col_2)))) + 1,"
        "    avg(col_0 - lead(col_0, 3)) "
        "from t1;";
    node::PlanNodeList trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(udf1, trees, &nm, status)) << status;
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

    std::vector<const node::ExprNode *> exprs;
    for (auto plan_node : project_list_node->GetProjects()) {
        auto pp_node = dynamic_cast<node::ProjectNode *>(plan_node);
        exprs.push_back(pp_node->GetExpression());
    }

    auto lib = udf::DefaultUdfLibrary::get();
    node::ExprAnalysisContext ctx(&nm, lib, &schemas_ctx);
    LambdafyProjects transformer(&ctx, false);

    std::vector<int> is_agg_vec;
    std::vector<std::string> names;
    std::vector<node::FrameNode *> frames;
    node::LambdaNode *lambda;
    status = transformer.Transform(exprs, &lambda, &is_agg_vec);
    LOG(WARNING) << status;
    ASSERT_TRUE(status.isOK());
    std::vector<int> expect_is_agg = {0, 0, 0, 1, 1, 1, 1};
    ASSERT_TRUE(is_agg_vec.size() == expect_is_agg.size());
    for (size_t i = 0; i < expect_is_agg.size(); ++i) {
        ASSERT_EQ(expect_is_agg[i], is_agg_vec[i]);
    }
    lambda->Print(std::cerr, "");
}

}  // namespace passes
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
