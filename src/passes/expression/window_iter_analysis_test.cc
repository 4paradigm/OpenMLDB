/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window_dep_analysis_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/
#include "passes/expression/window_iter_analysis.h"
#include <tuple>
#include "gtest/gtest.h"
#include "parser/parser.h"
#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"
#include "plan/planner.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"

namespace fesql {
namespace passes {

class WindowIterAnalysisTest : public ::testing::Test {};

void InitFunctionLet(const std::string& sql,
                     const vm::SchemasContext* schemas_ctx,
                     node::NodeManager* nm, node::LambdaNode** result) {
    parser::FeSQLParser parser;
    Status status;
    plan::SimplePlanner planner(nm);
    node::NodePointVector list1;
    int ok = parser.parse(sql, list1, nm, status);
    ASSERT_EQ(0, ok);

    node::PlanNodeList trees;
    planner.CreatePlanTree(list1, trees, status);
    ASSERT_EQ(1u, trees.size());

    auto query_plan = dynamic_cast<node::QueryPlanNode*>(trees[0]);
    ASSERT_TRUE(query_plan != nullptr);

    auto project_plan =
        dynamic_cast<node::ProjectPlanNode*>(query_plan->GetChildren()[0]);
    ASSERT_TRUE(project_plan != nullptr);

    project_plan->Print(std::cerr, "");
    auto project_list_node = dynamic_cast<node::ProjectListNode*>(
        project_plan->project_list_vec_[0]);
    ASSERT_TRUE(project_list_node != nullptr);
    std::vector<const node::ExprNode*> exprs;
    for (auto pp : project_list_node->GetProjects()) {
        auto pp_node = dynamic_cast<node::ProjectNode*>(pp);
        exprs.push_back(pp_node->GetExpression());
    }

    auto lib = udf::DefaultUDFLibrary::get();
    LambdafyProjects transformer(nm, lib, schemas_ctx, false);

    std::vector<int> is_agg_vec;
    node::LambdaNode* lambda;
    status = transformer.Transform(exprs, &lambda, &is_agg_vec);
    LOG(WARNING) << status;
    ASSERT_TRUE(status.isOK());
    *result = lambda;
}

TEST_F(WindowIterAnalysisTest, Test) {
    Status status;
    node::NodeManager nm;
    vm::SchemasContext schemas_ctx;
    auto schema = udf::MakeLiteralSchema<int32_t, float, double>();
    schemas_ctx.BuildTrivial({&schema});

    std::vector<std::tuple<std::string, size_t>> cases = {
        {"0", 0},
        {"col_0", 0},
        {"col_1 * col_2", 0},
        {"sum(col_0 + 1)", 1},
        {"sum(col_0 + sum(col_1))", 2},
        {"sum(col_0 + sum(col_1 + sum(col_2)))", 3},
        {"at(col_0, 1)", 1},
        {"at(col_0, min(col_0))", 1},
        {"count(fz_window_split(cast(col_0 as string), \",\"))", 1},
    };

    std::string sql = "select \n";
    for (size_t i = 0; i < cases.size(); ++i) {
        sql.append(std::get<0>(cases[i]));
        if (i < cases.size() - 1) {
            sql.append(",\n");
        }
    }
    sql.append("from t1;");

    node::LambdaNode* function_let = nullptr;
    InitFunctionLet(sql, &schemas_ctx, &nm, &function_let);
    auto row_type = function_let->GetArgType(0);
    auto window_type = function_let->GetArgType(1);

    auto lib = udf::DefaultUDFLibrary::get();

    node::LambdaNode* resolved_function_let = nullptr;
    passes::ResolveFnAndAttrs resolver(&nm, lib, &schemas_ctx);
    status = resolver.VisitLambda(function_let, {row_type, window_type},
                                  &resolved_function_let);
    ASSERT_TRUE(status.isOK()) << status.str();

    node::ExprAnalysisContext ctx(&nm, lib, &schemas_ctx);
    passes::WindowIterAnalysis window_dep_analyzer(&ctx);
    status = window_dep_analyzer.VisitFunctionLet(function_let);
    ASSERT_TRUE(status.isOK()) << status.str();

    auto expr_list = resolved_function_let->body();
    for (size_t i = 0; i < expr_list->GetChildNum(); ++i) {
        auto expr = expr_list->GetChild(i);
        WindowIterRank rank;
        window_dep_analyzer.GetRank(expr, &rank);
        LOG(INFO) << std::get<0>(cases[i]) << ": " << rank.rank;
        ASSERT_EQ(std::get<1>(cases[i]), rank.rank);
    }
}

}  // namespace passes
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
