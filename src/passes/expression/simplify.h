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

#ifndef SRC_PASSES_EXPRESSION_SIMPLIFY_H_
#define SRC_PASSES_EXPRESSION_SIMPLIFY_H_

#include <map>
#include <string>
#include <vector>

#include "passes/expression/expr_pass.h"

using hybridse::base::Status;
using hybridse::node::ExprAnalysisContext;
using hybridse::node::ExprNode;

namespace hybridse {
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
}  // namespace hybridse
#endif  // SRC_PASSES_EXPRESSION_SIMPLIFY_H_
