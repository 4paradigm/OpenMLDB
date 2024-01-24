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

#ifndef HYBRIDSE_SRC_PASSES_LAMBDAFY_PROJECTS_H_
#define HYBRIDSE_SRC_PASSES_LAMBDAFY_PROJECTS_H_

#include <string>
#include <unordered_set>
#include <vector>

#include "node/expr_node.h"
#include "node/sql_node.h"

namespace hybridse {
namespace passes {

using base::Status;

class LambdafyProjects {
 public:
    LambdafyProjects(node::ExprAnalysisContext* ctx, bool legacy_agg_opt)
        : ctx_(ctx), legacy_agg_opt_(legacy_agg_opt) {}
    /**
     * Create a virtual lambda representation for all project
     * expressions to codegen, which take signature: {
     *     @arg "row":    Current input row.
     *     @arg "window": Associating multi row list, avaliable in
     *                    window or agg compute mode.
     *     @return:       List of project expressions.
     *  }
     *
     * After transformation, a vector of flag are filled such that
     *   require_agg[k] = 1  => the kth output expression use window arg
     * thus subsequent steps can determine wheter the expressions
     * should be computed in aggregate node.
     *
     * "*" is expanded to all columns of input thus the final output
     * expressions num maybe larger than original projects num.
     */
    Status Transform(const std::vector<const node::ExprNode*>& exprs,
                     node::LambdaNode** out_lambda,
                     std::vector<int>* require_agg);

    /**
     * Transform original expression under lambda scope with arg
     *     @arg "row":    Current input row.
     *     @arg "window": Associating multi row list.
     *
     * Return transformed expression and fill two flags:
     *   "has_agg": Whether there exist agg expr node in output tree.
     */
    Status VisitExpr(node::ExprNode* expr, node::ExprIdNode* row_arg,
                     node::ExprIdNode* window_arg, node::ExprNode** out,
                     bool* has_agg);

    Status VisitLeafExpr(node::ExprNode* expr, node::ExprIdNode* row_arg,
                         node::ExprNode** out);

    Status VisitAggExpr(node::CallExprNode* call, node::ExprIdNode* row_arg,
                        node::ExprIdNode* window_arg, node::ExprNode** out,
                        bool* is_window_agg);

 private:
    node::ExprAnalysisContext* ctx_;

    // to make compatible with legacy agg builder
    bool FallBackToLegacyAgg(const node::ExprNode* expr);
    bool legacy_agg_opt_;
    std::unordered_set<std::string> agg_opt_fn_names_ = {"sum", "min", "max",
                                                         "count", "avg"};
};

}  // namespace passes
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_PASSES_LAMBDAFY_PROJECTS_H_
