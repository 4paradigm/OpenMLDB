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
#include "planv2/planner_v2.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
namespace hybridse {
namespace plan {
using hybridse::plan::SimplePlannerV2;
bool PlanAPI::CreatePlanTreeFromScript(const std::string &sql, PlanNodeList &plan_trees, NodeManager *node_manager,
                                       Status &status, bool is_batch_mode, bool is_cluster,
                                       bool enable_batch_window_parallelization) {
    std::unique_ptr<zetasql::ParserOutput> parser_output;
    auto zetasql_status = zetasql::ParseScript(sql, zetasql::ParserOptions(),
                                               zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output);
    zetasql::ErrorLocation location;
    if (!zetasql_status.ok()) {
        zetasql::ErrorLocation location;
        GetErrorLocation(zetasql_status, &location);
        status.msg = zetasql::FormatError(zetasql_status);
        status.code = common::kSyntaxError;
        return false;
    }
    DLOG(INFO) << "\n" << parser_output->script()->DebugString();
    const zetasql::ASTScript *script = parser_output->script();
    SimplePlannerV2 *planner_ptr =
        new SimplePlannerV2(node_manager, is_batch_mode, is_cluster, enable_batch_window_parallelization);
    status = planner_ptr->CreateASTScriptPlan(script, plan_trees);
    return status.isOK();
}

const std::string PlanAPI::GenerateName(const std::string prefix, int id) {
    time_t t;
    time(&t);
    std::string name = prefix + "_" + std::to_string(id) + "_" + std::to_string(t);
    return name;
}

}  // namespace plan
}  // namespace hybridse
