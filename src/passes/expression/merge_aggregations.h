/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * merge_aggregations.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_EXPRESSION_MERGE_AGGREGATIONS_H_
#define SRC_PASSES_EXPRESSION_MERGE_AGGREGATIONS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "passes/expression/expr_pass.h"

namespace fesql {
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
}  // namespace fesql
#endif  // SRC_PASSES_EXPRESSION_MERGE_AGGREGATIONS_H_
