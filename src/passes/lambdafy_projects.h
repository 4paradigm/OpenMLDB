/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * lambdafy_projects.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_LAMBDAFY_PROJECTS_H_
#define SRC_PASSES_LAMBDAFY_PROJECTS_H_

#include <string>
#include <vector>

#include "node/expr_node.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "udf/udf_library.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace passes {

using base::Status;

class LambdafyProjects {
 public:
    LambdafyProjects(node::NodeManager* nm, udf::UDFLibrary* library,
                     const vm::SchemaSourceList& input_schemas)
        : nm_(nm), library_(library), input_schemas_(input_schemas) {}

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
    Status Transform(const node::PlanNodeList& projects,
                     node::LambdaNode** out_lambda,
                     std::vector<int>* require_agg);

    /**
     * Transform original expression under lambda scope with arg
     *     @arg "row":    Current input row.
     *     @arg "window": Associating multi row list.
     *
     * Return transformed expression and fill two flags:
     *   "has_agg": Whether there exist agg expr node in output tree.
     *   "is_agg_root": Whether output root is itself agg expr node.
     */
    Status VisitExpr(node::ExprNode* expr, node::ExprIdNode* row_arg,
                     node::ExprIdNode* window_arg, node::ExprNode** out,
                     bool* has_agg, bool* is_agg_root);

    Status VisitLeafExpr(node::ExprNode* expr, node::ExprIdNode* row_arg,
                         node::ExprNode** out);

    Status VisitAggExpr(node::CallExprNode* call, node::ExprIdNode* row_arg,
                        node::ExprIdNode* window_arg, node::ExprNode** out);

 private:
    node::NodeManager* nm_;
    udf::UDFLibrary* library_;
    vm::SchemaSourceList input_schemas_;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_LAMBDAFY_PROJECTS_H_
