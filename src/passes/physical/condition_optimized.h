/**
 * Copyright (c) 2021 4paradigm
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
#ifndef SRC_PASSES_PHYSICAL_CONDITION_OPTIMIZED_H_
#define SRC_PASSES_PHYSICAL_CONDITION_OPTIMIZED_H_

#include <utility>
#include "passes/physical/transform_up_physical_pass.h"

namespace fesql {
namespace passes {

using fesql::vm::Filter;
using fesql::vm::Join;
using fesql::vm::PhysicalBinaryNode;
using fesql::vm::SchemasContext;

// Optimize filter condition
// for FilterNode, JoinNode
class ConditionOptimized : public TransformUpPysicalPass {
 public:
    explicit ConditionOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

    static bool TransfromAndConditionList(
        const node::ExprNode* condition,
        node::ExprListNode* and_condition_list);
    static bool ExtractEqualExprPair(
        node::ExprNode* condition,
        std::pair<node::ExprNode*, node::ExprNode*>* expr_pair);
    static bool TransformJoinEqualExprPair(
        const SchemasContext* left_schemas_ctx,
        const SchemasContext* right_schemas_ctx,
        node::ExprListNode* and_conditions,
        node::ExprListNode* out_condition_list,
        std::vector<ExprPair>& condition_eq_pair);  // NOLINT
    static bool TransformConstEqualExprPair(
        node::ExprListNode* and_conditions,
        node::ExprListNode* out_condition_list,
        std::vector<ExprPair>& condition_eq_pair);  // NOLINT
    static bool MakeConstEqualExprPair(
        const std::pair<node::ExprNode*, node::ExprNode*> expr_pair,
        const SchemasContext* right_schemas_ctx, ExprPair* output);

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool JoinConditionOptimized(PhysicalBinaryNode* in, Join* join);
    void SkipConstExpression(node::ExprListNode input,
                             node::ExprListNode* output);
    bool FilterConditionOptimized(PhysicalOpNode* in, Filter* filter);
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_PHYSICAL_CONDITION_OPTIMIZED_H_
