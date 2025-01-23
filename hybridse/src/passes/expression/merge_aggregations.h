/*
 * Copyright 2021 4Paradigm
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

#ifndef HYBRIDSE_SRC_PASSES_EXPRESSION_MERGE_AGGREGATIONS_H_
#define HYBRIDSE_SRC_PASSES_EXPRESSION_MERGE_AGGREGATIONS_H_

#include "passes/expression/expr_pass.h"

namespace hybridse {
namespace passes {

using base::Status;
using node::ExprAnalysisContext;
using node::ExprNode;

class MergeAggregations : public passes::ExprPass {
 public:
    Status Apply(ExprAnalysisContext* ctx, ExprNode* expr,
                 ExprNode** out) override;
};

}  // namespace passes
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_PASSES_EXPRESSION_MERGE_AGGREGATIONS_H_
