/*
 * Copyright 2022 4Paradigm Authors
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


#ifndef HYBRIDSE_SRC_VM_INTERNAL_NODE_HELPER_H_
#define HYBRIDSE_SRC_VM_INTERNAL_NODE_HELPER_H_

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "vm/physical_op.h"
#include "vm/physical_plan_context.h"

namespace hybridse {
namespace vm {
namespace internal {

// apply the function(PhysicalOpNode* -> PhysicalOpNode*) to each node of the node tree
// result in the new node tree preserving the structure of node tree(number & relations unchanged)
template <typename Func>
Status MapNode(PhysicalPlanContext* plan_ctx, PhysicalOpNode* input, PhysicalOpNode** output, Func&& func) {
    std::vector<PhysicalOpNode*> children;
    children.reserve(input->GetProducerCnt());
    for (decltype(input->GetProducerCnt()) i = 0; i < input->GetProducerCnt(); ++i) {
        PhysicalOpNode* new_pro = nullptr;
        CHECK_STATUS(MapNode(plan_ctx, input->GetProducer(i), &new_pro, func));
        children.push_back(new_pro);
    }

    PhysicalOpNode* new_in = nullptr;
    CHECK_STATUS(std::forward<Func>(func)(input, &new_in));

    PhysicalOpNode* out = nullptr;
    CHECK_STATUS(plan_ctx->WithNewChildren(new_in, children, &out));

    *output = out;

    return Status::OK();
}

template <typename BinOp, typename GetKids, typename State>
State ReduceNode(const PhysicalOpNode* root, State state, BinOp&& op, GetKids&& get_kids) {
    state = std::forward<BinOp>(op)(std::move(state), root);

    auto kids = std::forward<GetKids>(get_kids)(root);
    for (auto& kid : kids) {
        state = ReduceNode(kid, std::move(state), op, get_kids);
    }

    return state;
}

// Get all dependent (db, table) info from physical plan
Status GetDependentTables(const PhysicalOpNode*, std::set<std::pair<std::string, std::string>>*);

}  // namespace internal
}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_INTERNAL_NODE_HELPER_H_
