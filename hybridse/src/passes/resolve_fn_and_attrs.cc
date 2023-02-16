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

#include "passes/resolve_fn_and_attrs.h"
#include <utility>
#include "passes/resolve_udf_def.h"

using ::hybridse::common::kCodegenError;
using ::hybridse::node::TypeEquals;

namespace hybridse {
namespace passes {

Status ResolveFnAndAttrs::CheckSignature(
    node::FnDefNode* fn, const std::vector<const node::TypeNode*>& arg_types) {
    CHECK_TRUE(fn->GetArgSize() == arg_types.size(), kCodegenError,
               "Infer fn def failed, expect ", fn->GetArgSize(),
               " arguments but get ", arg_types.size());

    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        // we do not expect arg_types is always not null, sometimes null comes,
        // skipped the check so it won't stops compile.
        // However, it deserves a recheck later so we'll make arg_types always non-null.
        // The change is made due to #2974
        if (arg_types[i] == nullptr) {
            continue;
        }
        auto expect_type = fn->GetArgType(i);
        if (expect_type == nullptr) {
            continue;  // skip unknown type, it should be resolved in this pass
        }
        CHECK_TRUE(TypeEquals(expect_type, arg_types[i]), kCodegenError,
                   "Infer fn def failed, expect ", expect_type->GetName(),
                   " at ", i, "th argument, but get ", arg_types[i]->GetName());
    }
    return Status::OK();
}

Status ResolveFnAndAttrs::Apply(node::ExprAnalysisContext* ctx,
                                node::ExprNode* expr, node::ExprNode** output) {
    this->ctx_ = ctx;
    return VisitExpr(expr, output);
}

Status ResolveFnAndAttrs::VisitFnDef(
    node::FnDefNode* fn, const std::vector<const node::TypeNode*>& arg_types,
    node::FnDefNode** output) {
    switch (fn->GetType()) {
        case node::kLambdaDef: {
            node::LambdaNode* lambda = nullptr;
            CHECK_STATUS(VisitLambda(dynamic_cast<node::LambdaNode*>(fn),
                                     arg_types, &lambda));
            *output = lambda;
            break;
        }
        case node::kUdfDef: {
            node::UdfDefNode* udf_def = nullptr;
            CHECK_STATUS(VisitUdfDef(dynamic_cast<node::UdfDefNode*>(fn),
                                     arg_types, &udf_def));
            *output = udf_def;
            break;
        }
        case node::kUdafDef: {
            node::UdafDefNode* udaf_def = nullptr;
            CHECK_STATUS(VisitUdafDef(dynamic_cast<node::UdafDefNode*>(fn),
                                      arg_types, &udaf_def));
            *output = udaf_def;
            break;
        }
        default: {
            *output = fn;
        }
    }
    return Status::OK();
}

Status ResolveFnAndAttrs::VisitLambda(
    node::LambdaNode* lambda,
    const std::vector<const node::TypeNode*>& arg_types,
    node::LambdaNode** output) {
    // sanity checks
    CHECK_STATUS(CheckSignature(lambda, arg_types),
                 "Check lambda signature failed for\n",
                 lambda->GetTreeString());

    // bind lambda argument types
    for (size_t i = 0; i < arg_types.size(); ++i) {
        auto arg = lambda->GetArg(i);
        if (arg_types[i] != nullptr) {
            // set only if null, refer #2974
            arg->SetOutputType(arg_types[i]);
        }
        arg->SetNullable(true);
    }

    node::ExprNode* new_body = nullptr;
    CHECK_STATUS(VisitExpr(lambda->body(), &new_body),
                 "Resolve lambda body failed for\n", lambda->GetFlatString());
    lambda->SetBody(new_body);

    *output = lambda;
    return Status::OK();
}

Status ResolveFnAndAttrs::VisitUdfDef(
    node::UdfDefNode* udf_def,
    const std::vector<const node::TypeNode*>& arg_types,
    node::UdfDefNode** output) {
    // sanity checks
    CHECK_STATUS(CheckSignature(udf_def, arg_types),
                 "Check udf signature failed for\n", udf_def->GetTreeString());

    ResolveUdfDef udf_resolver;
    CHECK_STATUS(udf_resolver.Visit(udf_def->def()),
                 "Resolve udf definition failed for\n",
                 udf_def->GetTreeString());

    *output = udf_def;
    return Status::OK();
}

Status ResolveFnAndAttrs::VisitUdafDef(
    node::UdafDefNode* lambda,
    const std::vector<const node::TypeNode*>& arg_types,
    node::UdafDefNode** output) {
    // sanity checks
    CHECK_STATUS(CheckSignature(lambda, arg_types),
                 "Check udaf signature failed for\n", lambda->GetTreeString());

    // visit init
    node::ExprNode* resolved_init = nullptr;
    if (lambda->init_expr() != nullptr) {
        CHECK_STATUS(VisitExpr(lambda->init_expr(), &resolved_init),
                     "Resolve init expr failed for ", lambda->GetName(), ":\n",
                     lambda->GetTreeString());
    }

    // get state type
    const node::TypeNode* state_type;
    if (resolved_init != nullptr) {
        state_type = resolved_init->GetOutputType();
    } else {
        state_type = lambda->GetElementType(0);
    }
    CHECK_TRUE(state_type != nullptr, kCodegenError,
               "Fail to resolve state type of udaf ", lambda->GetName());

    // visit update
    std::vector<const node::TypeNode*> update_arg_types;
    update_arg_types.push_back(state_type);
    for (auto list_type : arg_types) {
        CHECK_TRUE(list_type->base() == node::kList, kCodegenError);
        update_arg_types.push_back(list_type->GetGenericType(0));
    }
    node::FnDefNode* resolved_update = nullptr;
    CHECK_TRUE(lambda->update_func() != nullptr, kCodegenError);
    CHECK_STATUS(
        VisitFnDef(lambda->update_func(), update_arg_types, &resolved_update),
        "Resolve update function of ", lambda->GetName(), " failed");
    state_type = resolved_update->GetReturnType();
    CHECK_TRUE(state_type != nullptr, kCodegenError,
               "Fail to resolve state type of udaf ", lambda->GetName());

    // visit merge
    node::FnDefNode* resolved_merge = nullptr;
    if (lambda->merge_func() != nullptr) {
        CHECK_STATUS(VisitFnDef(lambda->merge_func(), {state_type, state_type},
                                &resolved_merge),
                     "Resolve merge function of ", lambda->GetName(),
                     " failed");
    }

    // visit output
    node::FnDefNode* resolved_output = nullptr;
    if (lambda->output_func() != nullptr) {
        CHECK_STATUS(
            VisitFnDef(lambda->output_func(), {state_type}, &resolved_output),
            "Resolve output function of ", lambda->GetName(), " failed");
    }

    *output = ctx_->node_manager()->MakeUdafDefNode(
        lambda->GetName(), arg_types, resolved_init, resolved_update,
        resolved_merge, resolved_output);
    CHECK_STATUS((*output)->Validate(arg_types), "Illegal resolved udaf: \n",
                 (*output)->GetTreeString());
    return Status::OK();
}

Status ResolveFnAndAttrs::VisitOneStep(node::ExprNode* expr,
                                       node::ExprNode** output) {
    *output = expr;  // default
    switch (expr->GetExprType()) {
        case node::kExprCall: {
            auto call = dynamic_cast<node::CallExprNode*>(expr);
            auto external_fn =
                dynamic_cast<const node::ExternalFnDefNode*>(call->GetFnDef());
            if (external_fn == nullptr) {
                // non-external def
                node::FnDefNode* resolved_fn = nullptr;
                std::vector<const node::TypeNode*> arg_types;
                for (size_t i = 0; i < call->GetChildNum(); ++i) {
                    arg_types.push_back(call->GetChild(i)->GetOutputType());
                }
                CHECK_STATUS(
                    VisitFnDef(call->GetFnDef(), arg_types, &resolved_fn),
                    "Resolve function ", call->GetFnDef()->GetName(),
                    " failed");
                call->SetFnDef(resolved_fn);
                *output = call;

            } else {
                if (external_fn->IsResolved()) {
                    break;
                }
                node::ExprNode* result = nullptr;
                std::vector<node::ExprNode*> arg_list;
                for (size_t i = 0; i < call->GetChildNum(); ++i) {
                    arg_list.push_back(call->GetChild(i));
                }

                auto status = ctx_->library()->Transform(
                    external_fn->function_name(), arg_list,
                    ctx_->node_manager(), &result);
                if (status.isOK() && result != nullptr) {
                    node::ExprNode* resolved_result = nullptr;
                    CHECK_STATUS(VisitExpr(result, &resolved_result));
                    *output = resolved_result;
                } else {
                    // fallback to legacy fn gen with warning
                    DLOG(WARNING)
                        << "Resolve function '" << external_fn->function_name()
                        << "' failed, fallback to legacy: " << status;
                }
            }
            break;
        }
        default:
            break;
    }
    // Infer attr for non-group expr
    if ((*output)->GetExprType() != node::kExprList) {
        CHECK_STATUS((*output)->InferAttr(ctx_), "Fail to infer ",
                     (*output)->GetExprString());
    }
    return Status::OK();
}

Status ResolveFnAndAttrs::VisitExpr(node::ExprNode* expr,
                                    node::ExprNode** output) {
    auto iter = cache_.find(expr);
    if (iter != cache_.end()) {
        *output = iter->second;
        return Status::OK();
    }
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        node::ExprNode* old_child = expr->GetChild(i);
        node::ExprNode* new_child = nullptr;
        CHECK_STATUS(VisitExpr(old_child, &new_child), "Visit ", i,
                     "th child failed of\n", old_child->GetFlatString());
        if (new_child != nullptr && new_child != expr->GetChild(i)) {
            expr->SetChild(i, new_child);
        }
    }
    CHECK_STATUS(VisitOneStep(expr, output));
    cache_.insert(iter, std::make_pair(expr, *output));
    return Status::OK();
}

}  // namespace passes
}  // namespace hybridse
