/*
 * Copyright 2021 4paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_SIMPLE_PROJECT_OPTIMIZED_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_SIMPLE_PROJECT_OPTIMIZED_H_

#include "passes/physical/transform_up_physical_pass.h"

namespace hybridse {
namespace passes {

// Transform PhysicalSimpleProjectNode
// Rule 1, merge consecutive simple projects:
//   SimpleProject(project_list1)
//     SimpleProject(project_list2)
//       ...
//   ->
//   SimpleProject(merged_prject_list)
//     ...
class SimpleProjectOptimized : public TransformUpPysicalPass {
 public:
    explicit SimpleProjectOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}
    ~SimpleProjectOptimized() {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
};

}  // namespace passes
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_SIMPLE_PROJECT_OPTIMIZED_H_
