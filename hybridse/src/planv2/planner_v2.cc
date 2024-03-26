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

#include "planv2/planner_v2.h"

#include "planv2/ast_node_converter.h"

namespace hybridse {
namespace plan {
base::Status SimplePlannerV2::CreateASTScriptPlan(const zetasql::ASTScript *script,
                                                  PlanNodeList &plan_trees) {  // NOLINT (runtime/references)
    Status status;
    if (nullptr == script) {
        status.msg = "fail to create plan tree: ASTScript is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status;
    }
    node::SqlNodeList *resolved_trees;
    CHECK_STATUS(ConvertASTScript(script, node_manager_, &resolved_trees));
    CHECK_TRUE(nullptr != resolved_trees && resolved_trees->GetSize() > 0, common::kPlanError,
               "fail to create plan, sql trees is null or empty");
    CHECK_STATUS(CreatePlanTree(resolved_trees->GetList(), plan_trees));
    DLOG(INFO) << "PlanNode:";
    for (decltype(plan_trees.size()) i = 0; i < plan_trees.size(); ++i) {
        DLOG(INFO) << i << "=>\n" << plan_trees[i]->GetTreeString();
    }
    return base::Status::OK();
}
}  // namespace plan
}  // namespace hybridse
