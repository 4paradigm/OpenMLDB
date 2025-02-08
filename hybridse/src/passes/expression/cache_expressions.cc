/**
 * Copyright (c) 2025 Ace <teapot@aceforeverd.com>
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

#include "passes/expression/cache_expressions.h"

namespace hybridse {
namespace passes {

static std::string CallExprKey(const node::CallExprNode* call);

std::string CallExprKey(const node::CallExprNode* call) {
    std::string str = call->GetFnDef()->GetName();
    str.append("(");
    for (size_t i = 0; i < call->children_.size(); ++i) {
        str.append(call->children_[i]->GetExprString());
        if (i < call->children_.size() - 1) {
            str.append(", ");
        }
    }
    str.append(")");

    return str;
}

base::Status CacheExpressions::Apply(node::ExprAnalysisContext* ctx, node::ExprNode* expr, node::ExprNode** out) {
    *out = expr;
    for (int i = 0; i < expr->GetChildNum(); ++i) {
        node::ExprNode* co = nullptr;
        CHECK_STATUS(Apply(ctx, expr->GetChild(i), &co));
        if (co != nullptr && co != expr->GetChild(i)) {
            expr->SetChild(i, co);
        }
    }

    if (expr ->GetExprType() != node::kExprCall) {
        return {};
    }

    auto call = expr->GetAsOrNull<node::CallExprNode>();
    if (call == nullptr) {
        return {};
    }

    auto key = CallExprKey(call);

    auto it = expr_cache_.find(key);
    if (it != expr_cache_.end()) {
        *out = it->second;
    } else {
        expr_cache_.emplace(key, call);
    }

    return {};
}

}  // namespace passes
}  // namespace hybridse
