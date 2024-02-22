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

#ifndef HYBRIDSE_SRC_PLANV2_PLANNER_V2_H_
#define HYBRIDSE_SRC_PLANV2_PLANNER_V2_H_

#include <string>
#include <unordered_map>

#include "base/fe_status.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "plan/planner.h"
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace plan {

using base::Status;
using node::NodePointVector;
using node::PlanNodeList;

class SimplePlannerV2 : public SimplePlanner {
 public:
    SimplePlannerV2(node::NodeManager *manager, bool is_batch_mode, bool is_cluster_optimized = false,
                    bool enable_batch_window_parallelization = false,
                    const std::unordered_map<std::string, std::string> *extra_options = nullptr)
        : SimplePlanner(manager, is_batch_mode, is_cluster_optimized, enable_batch_window_parallelization,
                        extra_options) {}

    base::Status CreateASTScriptPlan(const zetasql::ASTScript *script,
                                     PlanNodeList &plan_trees);  // NOLINT (runtime/references)
};

}  // namespace plan
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PLANV2_PLANNER_V2_H_
