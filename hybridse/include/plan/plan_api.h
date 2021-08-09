/*
 * sql_plan_factory.h
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
#ifndef INCLUDE_PLAN_PLAN_API_H_
#define INCLUDE_PLAN_PLAN_API_H_
#include <string>
#include "node/node_manager.h"
namespace hybridse {
namespace plan {

using hybridse::base::Status;
using hybridse::node::NodeManager;
using hybridse::node::NodePointVector;
using hybridse::node::PlanNodeList;
class PlanAPI {
 public:
    static bool CreatePlanTreeFromScript(const std::string& sql,
                                         PlanNodeList& plan_trees,  // NOLINT
                                         NodeManager* node_manager,
                                         Status& status,  // NOLINT (runtime/references)
                                         bool is_batch_mode = true, bool is_cluster = false,
                                         bool enable_batch_window_parallelization = false);
    static const std::string GenerateName(const std::string prefix, int id);
};

}  // namespace plan
}  // namespace hybridse
#endif  // INCLUDE_PLAN_PLAN_API_H_
