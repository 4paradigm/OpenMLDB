/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * resolve_fn_and_attrs.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_RESOLVE_FN_AND_ATTRS_H_
#define SRC_PASSES_RESOLVE_FN_AND_ATTRS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "node/expr_node.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "passes/expression/expr_pass.h"
#include "udf/udf_library.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace passes {

using base::Status;

class ResolveFnAndAttrs : public ExprPass {
 public:
    explicit ResolveFnAndAttrs(node::ExprAnalysisContext* ctx) : ctx_(ctx) {}

    Status Apply(node::ExprAnalysisContext* ctx, node::ExprNode* expr,
                 node::ExprNode** output) override;

    Status VisitFnDef(node::FnDefNode* fn,
                      const std::vector<const node::TypeNode*>& arg_types,
                      node::FnDefNode** output);

    Status VisitLambda(node::LambdaNode* lambda,
                       const std::vector<const node::TypeNode*>& arg_types,
                       node::LambdaNode** output);

    Status VisitUDFDef(node::UDFDefNode* lambda,
                       const std::vector<const node::TypeNode*>& arg_types,
                       node::UDFDefNode** output);

    Status VisitUDAFDef(node::UDAFDefNode* lambda,
                        const std::vector<const node::TypeNode*>& arg_types,
                        node::UDAFDefNode** output);

    Status VisitOneStep(node::ExprNode* expr, node::ExprNode** output);
    Status VisitExpr(node::ExprNode* expr, node::ExprNode** output);

 private:
    Status CheckSignature(node::FnDefNode* fn,
                          const std::vector<const node::TypeNode*>& arg_types);

    node::ExprAnalysisContext* ctx_;

    std::unordered_map<node::ExprNode*, node::ExprNode*> cache_;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_RESOLVE_FN_AND_ATTRS_H_
