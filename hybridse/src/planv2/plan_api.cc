/*
 * sql_plan_factory.cc.c
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "plan/plan_api.h"

#include "absl/strings/substitute.h"
#include "planv2/ast_node_converter.h"
#include "planv2/planner_v2.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"

namespace hybridse {
namespace plan {

base::Status PlanAPI::CreatePlanTreeFromScript(vm::SqlContext *ctx) {
    zetasql::ParserOptions parser_opts;
    zetasql::LanguageOptions language_opts;
    language_opts.EnableLanguageFeature(zetasql::FEATURE_V_1_3_COLUMN_DEFAULT_VALUE);
    parser_opts.set_language_options(&language_opts);
    // save parse result into SqlContext so SQL engine can reference fields inside ASTNode during whole compile stage
    auto zetasql_status =
        zetasql::ParseScript(ctx->sql, parser_opts, zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &ctx->ast_node);
    zetasql::ErrorLocation location;
    if (!zetasql_status.ok()) {
        zetasql::ErrorLocation location;
        GetErrorLocation(zetasql_status, &location);
        return {common::kSyntaxError, zetasql::FormatError(zetasql_status)};
    }

    DLOG(INFO) << "AST Node:\n" << ctx->ast_node->script()->DebugString();

    const zetasql::ASTScript *script = ctx->ast_node->script();
    auto planner_ptr =
        std::make_unique<SimplePlannerV2>(&ctx->nm, ctx->engine_mode == vm::kBatchMode, ctx->is_cluster_optimized,
                                          ctx->enable_batch_window_parallelization, ctx->options.get());
    return planner_ptr->CreateASTScriptPlan(script, ctx->logical_plan);
}

bool PlanAPI::CreatePlanTreeFromScript(const std::string &sql, PlanNodeList &plan_trees, NodeManager *node_manager,
                                       Status &status, bool is_batch_mode, bool is_cluster,
                                       bool enable_batch_window_parallelization,
                                       const std::unordered_map<std::string, std::string>* extra_options) {
    std::unique_ptr<zetasql::ParserOutput> parser_output;
    zetasql::ParserOptions parser_opts;
    zetasql::LanguageOptions language_opts;
    language_opts.EnableLanguageFeature(zetasql::FEATURE_V_1_3_COLUMN_DEFAULT_VALUE);
    parser_opts.set_language_options(&language_opts);
    auto zetasql_status = zetasql::ParseScript(sql, parser_opts,
                                               zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output);
    zetasql::ErrorLocation location;
    if (!zetasql_status.ok()) {
        zetasql::ErrorLocation location;
        GetErrorLocation(zetasql_status, &location);
        status.msg = zetasql::FormatError(zetasql_status);
        status.code = common::kSyntaxError;
        return false;
    }
    DLOG(INFO) << "AST Node:\n" << parser_output->script()->DebugString();
    const zetasql::ASTScript *script = parser_output->script();
    auto planner_ptr = std::make_unique<SimplePlannerV2>(node_manager, is_batch_mode, is_cluster,
                                                         enable_batch_window_parallelization, extra_options);
    status = planner_ptr->CreateASTScriptPlan(script, plan_trees);
    return status.isOK();
}

const int PlanAPI::GetPlanLimitCount(node::PlanNode* plan_tree) {
    if (nullptr == plan_tree) {
        return 0;
    }
    return Planner::GetPlanTreeLimitCount(plan_tree);
}
const std::string PlanAPI::GenerateName(const std::string prefix, int id) {
    time_t t;
    time(&t);
    std::string name = prefix + "_" + std::to_string(id) + "_" + std::to_string(t);
    return name;
}

absl::StatusOr<codec::Schema> ParseTableColumSchema(absl::string_view str) {
    zetasql::ParserOptions parser_opts;
    zetasql::LanguageOptions language_opts;
    language_opts.EnableLanguageFeature(zetasql::FEATURE_V_1_3_COLUMN_DEFAULT_VALUE);
    parser_opts.set_language_options(&language_opts);
    std::unique_ptr<zetasql::ParserOutput> parser_output;
    auto sql = absl::Substitute("CREATE TABLE t1 ($0)", str);
    auto zetasql_status = zetasql::ParseStatement(sql, parser_opts, &parser_output);
    if (!zetasql_status.ok()) {
        return zetasql_status;
    }

    node::SqlNode *sql_node = nullptr;
    node::NodeManager nm;
    CHECK_STATUS_TO_ABSL(ConvertStatement(parser_output->statement(), &nm, &sql_node));

    auto* create = sql_node->GetAsOrNull<node::CreateStmt>();
    if (create == nullptr) {
        return absl::FailedPreconditionError("not a create table statement");
    }

    return create->GetColumnDefListAsSchema();
}

}  // namespace plan
}  // namespace hybridse
