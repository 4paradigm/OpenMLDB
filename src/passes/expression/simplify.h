/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * simplify.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_EXPRESSION_SIMPLIFY_H_
#define SRC_PASSES_EXPRESSION_SIMPLIFY_H_

#include <map>
#include <string>
#include <vector>

#include "passes/expression/expr_pass.h"

using fesql::base::Status;
using fesql::node::ExprAnalysisContext;
using fesql::node::ExprNode;

namespace fesql {
namespace passes {

class ExprInplaceTransformUp : public ExprPass {
 public:
    ExprInplaceTransformUp() {}
    virtual ~ExprInplaceTransformUp() {}

    Status Apply(ExprAnalysisContext* ctx, ExprNode* expr,
                 ExprNode** out) override;
    void Reset(ExprAnalysisContext* ctx);

    // expression transformers
    virtual Status DoApply(ExprNode* expr, ExprNode**);
    virtual Status VisitCall(node::CallExprNode*, ExprNode**);
    virtual Status VisitGetField(node::GetFieldExpr*, ExprNode**);
    virtual Status VisitDefault(ExprNode*, ExprNode**);

    // fn def transformers
    virtual Status VisitFnDef(node::FnDefNode*,
                              const std::vector<node::ExprAttrNode>&,
                              node::FnDefNode**);
    virtual Status VisitLambda(node::LambdaNode*,
                               const std::vector<node::ExprAttrNode>&,
                               node::FnDefNode**);
    virtual Status VisitUDAF(node::UDAFDefNode*,
                             const std::vector<node::ExprAttrNode>&,
                             node::FnDefNode**);

 private:
    ExprAnalysisContext* ctx_;
    std::map<size_t, ExprNode*> cache_;
};

class ExprSimplifier : public ExprInplaceTransformUp {
 public:
    Status VisitCall(node::CallExprNode*, ExprNode**) override;
    Status VisitGetField(node::GetFieldExpr*, ExprNode**) override;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_EXPRESSION_SIMPLIFY_H_
