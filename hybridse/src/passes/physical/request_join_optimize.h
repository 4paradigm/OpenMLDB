/**
 * Copyright (c) 2023 OpenMLDB Authors
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

#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_REQUEST_JOIN_OPTIMIZE_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_REQUEST_JOIN_OPTIMIZE_H_

#include <stack>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "passes/physical/transform_up_physical_pass.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

class RequestJoinOptimize : public TransformUpPysicalPass {
 public:
    explicit RequestJoinOptimize(PhysicalPlanContext* plan_ctx) ABSL_ATTRIBUTE_NONNULL();
    ~RequestJoinOptimize() override {}

    absl::Status Sucess() const;

 private:
    // right kids info of request joni
    struct RightParseInfo {
        vm::PhysicalDataProviderNode* data = nullptr;
    };

    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output) override;

    absl::Status CollectKidsInfo(PhysicalOpNode* kid, RightParseInfo* info) ABSL_ATTRIBUTE_NONNULL();

    absl::Status BuildNewExprList(const node::ExprNode* origin, const vm::SchemasContext* orgin_sc,
                                  const vm::SchemasContext* rebase_sc, node::ExprNode** out) ABSL_ATTRIBUTE_NONNULL();

    // mandatory optimizer, fail status results compile error
    absl::Status status_ = absl::OkStatus();
    std::stack<vm::PhysicalFilterNode*> filters_;
};
}  // namespace passes
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_REQUEST_JOIN_OPTIMIZE_H_
