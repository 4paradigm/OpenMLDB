/*
 * src/passes/expression/default_passes.h
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * default_passes.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_EXPRESSION_DEFAULT_PASSES_H_
#define SRC_PASSES_EXPRESSION_DEFAULT_PASSES_H_

#include <memory>

#include "passes/expression/merge_aggregations.h"
#include "passes/expression/simplify.h"
#include "passes/resolve_fn_and_attrs.h"

namespace fesql {
namespace passes {

void AddDefaultExprOptPasses(node::ExprAnalysisContext* ctx,
                             ExprPassGroup* group) {
    group->AddPass(std::make_shared<passes::MergeAggregations>());
    group->AddPass(std::make_shared<passes::ExprSimplifier>());
    group->AddPass(std::make_shared<passes::ResolveFnAndAttrs>(ctx));
}

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_EXPRESSION_DEFAULT_PASSES_H_
