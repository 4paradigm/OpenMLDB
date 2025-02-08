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
#ifndef HYBRIDSE_SRC_PASSES_EXPRESSION_CACHE_EXPRESSIONS_H_
#define HYBRIDSE_SRC_PASSES_EXPRESSION_CACHE_EXPRESSIONS_H_

#include <absl/container/flat_hash_set.h>

#include <string>

#include "passes/expression/expr_pass.h"

namespace hybridse {
namespace passes {

// Cache same function calls in single transaction

// TODO(someone): for simplify, this passes currently only apply on
//  aggregate calls on single window group of single projection node
//
class CacheExpressions : public passes::ExprPass {
 public:
    base::Status Apply(node::ExprAnalysisContext* ctx, node::ExprNode* expr, node::ExprNode** out) override;

 private:
    absl::flat_hash_map<std::string, node::ExprNode*> expr_cache_;
};

}  // namespace passes
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_PASSES_EXPRESSION_CACHE_EXPRESSIONS_H_
