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

#include "passes/expression/simplify.h"
#include "node/expr_node.h"
#include "node/node_manager.h"
#include "node/type_node.h"
#include "passes/resolve_fn_and_attrs.h"

namespace hybridse {
namespace passes {

using hybridse::node::ExprAttrNode;

Status ExprInplaceTransformUp::Apply(ExprAnalysisContext* ctx, ExprNode* expr,
                                     ExprNode** out) {
    Reset(ctx);
    return DoApply(expr, out);
}

void ExprInplaceTransformUp::Reset(ExprAnalysisContext* ctx) {
    ctx_ = ctx;
    cache_.clear();
}

Status ExprInplaceTransformUp::DoApply(ExprNode* expr, ExprNode** out) {
    size_t cache_key = expr->node_id();
    auto cache_iter = cache_.find(cache_key);
    if (cache_iter != cache_.end()) {
        *out = cache_iter->second;
        return Status::OK();
    }
    bool changed = false;
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        ExprNode* origin_child = expr->GetChild(i);
        auto origin_dtype = origin_child->GetOutputType();
        bool origin_nullable = origin_child->nullable();

        ExprNode* new_child = nullptr;
        CHECK_STATUS(DoApply(expr->GetChild(i), &new_child));
        if (new_child == nullptr) {
            LOG(WARNING) << "Optimized child is null";
            continue;
        } else if (new_child != expr->GetChild(i)) {
            expr->SetChild(i, new_child);
            changed = true;
        } else if (new_child->GetOutputType() != origin_dtype ||
                   new_child->nullable() != origin_nullable) {
            changed = true;
        }
    }
    if (changed) {
        node::ExprNode* update_expr = expr;
        ResolveFnAndAttrs resolver(ctx_);
        CHECK_STATUS(resolver.VisitOneStep(expr, &update_expr));
        expr = update_expr;
    }
    switch (expr->GetExprType()) {
        case node::kExprGetField: {
            CHECK_STATUS(
                VisitGetField(dynamic_cast<node::GetFieldExpr*>(expr), out));
            break;
        }
        case node::kExprCall: {
            auto call = dynamic_cast<node::CallExprNode*>(expr);
            auto fn = call->GetFnDef();
            node::FnDefNode* new_fn = fn;
            std::vector<ExprAttrNode> arg_attrs;
            for (size_t i = 0; i < call->GetChildNum(); ++i) {
                auto child = call->GetChild(i);
                arg_attrs.push_back(
                    ExprAttrNode(child->GetOutputType(), child->nullable()));
            }
            CHECK_STATUS(VisitFnDef(fn, arg_attrs, &new_fn));
            if (new_fn != fn) {
                node::FnDefNode* resolved_fn = new_fn;
                ResolveFnAndAttrs resolver(ctx_);
                std::vector<const node::TypeNode*> arg_types;
                for (size_t i = 0; i < call->GetChildNum(); ++i) {
                    arg_types.push_back(call->GetChild(i)->GetOutputType());
                }
                CHECK_STATUS(
                    resolver.VisitFnDef(new_fn, arg_types, &resolved_fn));
                call->SetFnDef(resolved_fn);

                node::ExprNode* update_expr = nullptr;
                CHECK_STATUS(resolver.VisitOneStep(call, &update_expr));
                if (update_expr->GetExprType() != node::kExprCall) {
                    return DoApply(update_expr, out);
                }
                call = dynamic_cast<node::CallExprNode*>(update_expr);
            }
            CHECK_STATUS(VisitCall(call, out));
            break;
        }
        default: {
            *out = expr;
            CHECK_STATUS(VisitDefault(expr, out));
            break;
        }
    }
    if (*out == nullptr) {
        LOG(WARNING) << "Optimized output is null";
        *out = expr;
    }
    cache_[cache_key] = *out;
    return Status::OK();
}

Status ExprInplaceTransformUp::VisitCall(node::CallExprNode* expr,
                                         ExprNode** out) {
    return VisitDefault(expr, out);
}

Status ExprInplaceTransformUp::VisitGetField(node::GetFieldExpr* expr,
                                             ExprNode** out) {
    return VisitDefault(expr, out);
}

Status ExprInplaceTransformUp::VisitDefault(ExprNode* expr, ExprNode** out) {
    *out = expr;
    return Status::OK();
}

Status ExprInplaceTransformUp::VisitFnDef(
    node::FnDefNode* fn, const std::vector<node::ExprAttrNode>& arg_attrs,
    node::FnDefNode** out) {
    *out = fn;
    switch (fn->GetType()) {
        case node::kLambdaDef: {
            return VisitLambda(dynamic_cast<node::LambdaNode*>(fn), arg_attrs,
                               out);
        }
        case node::kUdafDef: {
            return VisitUdaf(dynamic_cast<node::UdafDefNode*>(fn), arg_attrs,
                             out);
        }
        default: {
            return Status::OK();
        }
    }
}

Status ExprInplaceTransformUp::VisitLambda(
    node::LambdaNode* lambda, const std::vector<node::ExprAttrNode>& arg_attrs,
    node::FnDefNode** out) {
    CHECK_TRUE(arg_attrs.size() == lambda->GetArgSize(), common::kPlanError);
    node::ExprNode* new_body = nullptr;
    CHECK_STATUS(DoApply(lambda->body(), &new_body));
    if (new_body != lambda->body()) {
        lambda->SetBody(new_body);
    }
    *out = lambda;
    return Status::OK();
}

Status ExprInplaceTransformUp::VisitUdaf(
    node::UdafDefNode* udaf, const std::vector<node::ExprAttrNode>& arg_attrs,
    node::FnDefNode** out) {
    CHECK_TRUE(arg_attrs.size() == udaf->GetArgSize(), common::kPlanError);
    bool changed = false;

    // init
    node::ExprNode* init = udaf->init_expr();
    if (init != nullptr) {
        CHECK_STATUS(DoApply(udaf->init_expr(), &init));
        if (init != udaf->init_expr()) {
            changed = true;
        }
    }

    // update
    node::FnDefNode* update = udaf->update_func();
    if (update != nullptr) {
        std::vector<ExprAttrNode> update_args;
        update_args.push_back(ExprAttrNode(udaf->GetStateType(), false));
        for (auto& arg : arg_attrs) {
            const node::TypeNode* dtype = arg.type();
            if (dtype != nullptr && dtype->generics_.size() == 1) {
                // list<T>
                dtype = dtype->generics_[0];
            }
            update_args.push_back(ExprAttrNode(dtype, false));
        }
        CHECK_STATUS(VisitFnDef(udaf->update_func(), update_args, &update));
        if (update != udaf->update_func()) {
            changed = true;
        }
    }

    // merge
    node::FnDefNode* merge = udaf->merge_func();
    if (merge != nullptr) {
        std::vector<ExprAttrNode> merge_args;
        merge_args.push_back(ExprAttrNode(udaf->GetStateType(), false));
        merge_args.push_back(ExprAttrNode(udaf->GetStateType(), false));
        CHECK_STATUS(VisitFnDef(udaf->merge_func(), merge_args, &merge));
        if (merge != udaf->merge_func()) {
            changed = true;
        }
    }

    // output
    node::FnDefNode* output_fn = udaf->output_func();
    if (output_fn != nullptr) {
        std::vector<ExprAttrNode> output_args;
        output_args.push_back(ExprAttrNode(udaf->GetStateType(), false));
        CHECK_STATUS(VisitFnDef(udaf->output_func(), output_args, &output_fn));
        if (output_fn != udaf->output_func()) {
            changed = true;
        }
    }

    if (changed) {
        *out = ctx_->node_manager()->MakeUdafDefNode(
            udaf->GetName(), udaf->GetArgTypeList(), init, update, merge,
            output_fn);
    } else {
        *out = udaf;
    }
    return Status::OK();
}

/**
 * GetField(MakeTuple(e1, e2, ..., ek, ...), k) => ek
 */
Status ExprSimplifier::VisitGetField(node::GetFieldExpr* get_field,
                                     ExprNode** out) {
    *out = get_field;
    auto input = get_field->GetChild(0);
    auto input_type = input->GetOutputType();
    if (input_type == nullptr || input_type->base() != node::kTuple) {
        return Status::OK();
    }
    auto call = dynamic_cast<node::CallExprNode*>(input);
    if (call == nullptr) {
        return Status::OK();
    }
    std::string funcname = call->GetFnDef()->GetName();
    if (funcname != "make_tuple") {
        return Status::OK();
    }
    size_t index = get_field->GetColumnID();
    if (call->GetChildNum() <= index) {
        return Status::OK();
    }
    *out = call->GetChild(index);
    return Status::OK();
}

/**
 * (lambda x => body(x)) . y => body(y)
 */
Status ExprSimplifier::VisitCall(node::CallExprNode* call, ExprNode** out) {
    *out = call;
    auto lambda = dynamic_cast<node::LambdaNode*>(call->GetFnDef());
    if (lambda == nullptr) {
        return Status::OK();
    }
    if (lambda->GetArgSize() != call->GetChildNum()) {
        return Status::OK();
    }
    ExprReplacer replacer;
    for (size_t i = 0; i < lambda->GetArgSize(); ++i) {
        replacer.AddReplacement(lambda->GetArg(i), call->GetChild(i));
    }
    ExprNode* new_body = nullptr;
    CHECK_STATUS(replacer.Replace(lambda->body(), &new_body));

    ExprNode* simplify_body = nullptr;
    CHECK_STATUS(DoApply(new_body, &simplify_body));

    *out = simplify_body;
    return Status::OK();
}

}  // namespace passes
}  // namespace hybridse
