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
#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_LONG_WINDOW_OPTIMIZED_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_LONG_WINDOW_OPTIMIZED_H_

#include <set>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "passes/physical/transform_up_physical_pass.h"

namespace hybridse {
namespace passes {

class LongWindowOptimized : public TransformUpPysicalPass {
 public:
    explicit LongWindowOptimized(PhysicalPlanContext* plan_ctx);
    ~LongWindowOptimized() {}

 public:
    // e.g count_where(col1, col2 < 4)
    //  -> key_col_name = col1, filter_col_name = col2
    struct AggInfo {
        absl::string_view key_col_name;
        absl::string_view filter_col_name;
    };

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output) override;
    bool VerifySingleAggregation(vm::PhysicalProjectNode* op);
    bool OptimizeWithPreAggr(vm::PhysicalAggregationNode* in, int idx, PhysicalOpNode** output);

    static std::string ConcatExprList(std::vector<node::ExprNode*> exprs, const std::string& delimiter = ",");

    // Check supported ExprNode, return false if the call expr type is not implemented
    // otherwise, return ok status with the agg info
    static absl::StatusOr<AggInfo> CheckCallExpr(const node::CallExprNode* call);

    std::set<std::string> long_windows_;
};
}  // namespace passes
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_LONG_WINDOW_OPTIMIZED_H_
