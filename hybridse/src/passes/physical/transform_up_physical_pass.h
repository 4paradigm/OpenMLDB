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
#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_TRANSFORM_UP_PHYSICAL_PASS_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_TRANSFORM_UP_PHYSICAL_PASS_H_

#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "passes/physical/physical_pass.h"

namespace hybridse {
namespace passes {

using hybridse::base::Status;
using hybridse::vm::Catalog;
using hybridse::vm::PhysicalOpNode;
using hybridse::vm::PhysicalPlanContext;

enum PhysicalPlanPassType {
    kPassSimpleProjectsOptimized,
    kPassFilterOptimized,
    kPassGroupAndSortOptimized,
    kPassLeftJoinOptimized,
    kPassClusterOptimized,
    kPassLimitOptimized,
    kPassLongWindowOptimized,
    kPassSplitAggregationOptimized,
    kPassRequestJoinOptimze,
};

std::string PhysicalPlanPassTypeName(PhysicalPlanPassType type);

struct ExprPair {
    node::ExprNode* left_expr_ = nullptr;
    node::ExprNode* right_expr_ = nullptr;
};

Status CheckExprDependOnChildOnly(const node::ExprNode* expr,
                                  const vm::SchemasContext* child_schemas_ctx);

/**
 * Replaces `idx`th producer of PhysicalOpNode `op` and update `op`'s
 * SchemasContext if success. This will alter `op` if success, otherwise `op` is
 * revert to original state
 *
 * @plan_ctx: PhysicalPlanContext op inside
 * @op: physical node
 * @idx: `op`'s `idx`th producer
 * @child: physical node going to replace
 * @return true on success, false otherwise
 */
bool ResetProducer(PhysicalPlanContext* plan_ctx, PhysicalOpNode* op,
                   size_t idx, PhysicalOpNode* child);

class TransformUpPysicalPass : public PhysicalPass {
 public:
    explicit TransformUpPysicalPass(PhysicalPlanContext* plan_ctx)
        : plan_ctx_(plan_ctx),
          node_manager_(plan_ctx->node_manager()),
          db_(plan_ctx->db()),
          catalog_(plan_ctx->catalog()) {}
    ~TransformUpPysicalPass() {}

    /**
     * Applies a physical plan optimization strategy in a post-DFS style(aka
     * transform up), optimize every producer then current one.
     */
    bool Apply(PhysicalOpNode* in, PhysicalOpNode** out);

    /**
     * Transforms physical node `in` into `out` with a physical node
     * optimization strategy
     */
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** out) = 0;

    Status Apply(PhysicalPlanContext* ctx, PhysicalOpNode* in,
                 PhysicalOpNode** out) override {
        return base::Status(common::kPlanError, "Not implemented");
    }

 protected:
    PhysicalPlanContext* plan_ctx_;
    node::NodeManager* node_manager_;
    const std::string db_;
    std::shared_ptr<Catalog> catalog_;
    std::unordered_map<int, bool> visited_ids_;
};

}  // namespace passes
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_TRANSFORM_UP_PHYSICAL_PASS_H_
