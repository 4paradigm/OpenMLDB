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

#ifndef SRC_PASSES_EXPRESSION_EXPR_PASS_TEST_H_
#define SRC_PASSES_EXPRESSION_EXPR_PASS_TEST_H_

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "passes/expression/expr_pass.h"
#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"
#include "plan/plan_api.h"
#include "udf/default_udf_library.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace passes {

void InitFunctionLet(const std::string& sql, node::ExprAnalysisContext* ctx,
                     node::LambdaNode** result) {
    Status status;
    node::PlanNodeList trees;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, trees, ctx->node_manager(), status)) << status;
    ASSERT_EQ(1u, trees.size());

    auto query_plan = dynamic_cast<node::QueryPlanNode*>(trees[0]);
    ASSERT_TRUE(query_plan != nullptr);

    auto project_plan =
        dynamic_cast<node::ProjectPlanNode*>(query_plan->GetChildren()[0]);
    ASSERT_TRUE(project_plan != nullptr);

    auto project_list_node = dynamic_cast<node::ProjectListNode*>(
        project_plan->project_list_vec_[0]);
    ASSERT_TRUE(project_list_node != nullptr);
    std::vector<const node::ExprNode*> exprs;
    for (auto pp : project_list_node->GetProjects()) {
        auto pp_node = dynamic_cast<node::ProjectNode*>(pp);
        exprs.push_back(pp_node->GetExpression());
    }

    LambdafyProjects transformer(ctx, false);
    std::vector<int> is_agg_vec;
    node::LambdaNode* lambda;
    status = transformer.Transform(exprs, &lambda, &is_agg_vec);
    ASSERT_TRUE(status.isOK()) << status;

    node::LambdaNode* resolved = nullptr;
    passes::ResolveFnAndAttrs resolver(ctx);
    status = resolver.VisitLambda(
        lambda, {lambda->GetArgType(0), lambda->GetArgType(1)}, &resolved);
    ASSERT_TRUE(status.isOK()) << status.str();
    *result = resolved;
}

class ExprPassTestBase : public ::testing::Test {
 public:
    ExprPassTestBase()
        : lib_(udf::DefaultUdfLibrary::get()),
          ctx_(&nm_, lib_, &schemas_ctx_) {}
    virtual ~ExprPassTestBase() {}
    node::ExprAnalysisContext* pass_ctx() { return &ctx_; }
    node::NodeManager* node_manager() { return &nm_; }
    const vm::SchemasContext* schemas_ctx() const { return &schemas_ctx_; }

    void InitFunctionLet(const std::string& sql, node::LambdaNode** result) {
        hybridse::passes::InitFunctionLet(sql, &ctx_, result);
    }

    Status ApplyPass(ExprPass* pass, node::LambdaNode* function_let,
                     node::ExprNode** output) {
        pass->SetRow(function_let->GetArg(0));
        pass->SetWindow(function_let->GetArg(1));
        return pass->Apply(pass_ctx(), function_let->body(), output);
    }

 protected:
    node::NodeManager nm_;
    vm::SchemasContext schemas_ctx_;
    const udf::UdfLibrary* lib_;
    node::ExprAnalysisContext ctx_;
};

}  // namespace passes
}  // namespace hybridse
#endif  // SRC_PASSES_EXPRESSION_EXPR_PASS_TEST_H_
