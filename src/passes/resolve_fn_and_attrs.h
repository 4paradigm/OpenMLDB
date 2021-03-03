/*
 * resolve_fn_and_attrs.h
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
