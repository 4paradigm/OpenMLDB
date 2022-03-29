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
#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_SPLIT_AGGREGATION_OPTIMIZED_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_SPLIT_AGGREGATION_OPTIMIZED_H_

#include <string>
#include <set>
#include "passes/physical/transform_up_physical_pass.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

class SplitAggregationOptimized : public TransformUpPysicalPass {
 public:
    explicit SplitAggregationOptimized(PhysicalPlanContext* plan_ctx);
    ~SplitAggregationOptimized() {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool SplitProjects(vm::PhysicalAggrerationNode* in, PhysicalOpNode** output);
    bool IsSplitable(vm::PhysicalAggrerationNode* op);

    std::set<std::string> long_windows_;
};
}  // namespace passes
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_SPLIT_AGGREGATION_OPTIMIZED_H_
