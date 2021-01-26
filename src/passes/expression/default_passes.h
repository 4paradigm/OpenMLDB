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
