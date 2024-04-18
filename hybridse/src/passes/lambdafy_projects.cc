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

#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"

namespace hybridse {
namespace passes {

using ::hybridse::common::kCodegenError;

Status LambdafyProjects::Transform(
    const std::vector<const node::ExprNode*>& exprs,
    node::LambdaNode** out_lambda, std::vector<int>* require_agg_vec) {
    // arg1: current input row
    auto nm = ctx_->node_manager();
    auto schemas_ctx = ctx_->schemas_context();
    auto row_type = nm->MakeRowType(schemas_ctx);
    auto row_arg = nm->MakeExprIdNode("row");
    row_arg->SetOutputType(row_type);

    // arg2: optional row list for agg
    auto window_type = nm->MakeTypeNode(node::kList, row_type);
    auto window_arg = nm->MakeExprIdNode("window");
    window_arg->SetOutputType(window_type);

    // iterate project exprs
    auto out_list = nm->MakeExprList();
    require_agg_vec->clear();
    for (auto origin_expr : exprs) {
        CHECK_TRUE(origin_expr != nullptr, kCodegenError);
        if (origin_expr->GetExprType() == node::kExprAll) {
            // expand *
            for (size_t slice = 0; slice < schemas_ctx->GetSchemaSourceSize();
                 ++slice) {
                auto schema_source = schemas_ctx->GetSchemaSource(slice);
                for (size_t k = 0; k < schema_source->size(); ++k) {
                    auto col_name = schema_source->GetSchema()->Get(k).name();
                    size_t col_id = schema_source->GetColumnID(k);
                    auto get_col =
                        nm->MakeGetFieldExpr(row_arg, col_name, col_id);
                    out_list->AddChild(get_col);
                    require_agg_vec->push_back(false);
                }
            }
        } else if (legacy_agg_opt_ && FallBackToLegacyAgg(origin_expr)) {
            auto expr = origin_expr->DeepCopy(nm);
            CHECK_TRUE(expr != nullptr, kCodegenError);
            out_list->AddChild(expr);
            require_agg_vec->push_back(true);
        } else {
            auto expr = origin_expr->DeepCopy(nm);
            CHECK_TRUE(expr != nullptr, kCodegenError);
            bool has_agg;
            node::ExprNode* transformed = nullptr;
            CHECK_STATUS(
                VisitExpr(expr, row_arg, window_arg, &transformed, &has_agg),
                "Lambdafy ", expr->GetExprString(), " failed");
            out_list->AddChild(transformed);
            require_agg_vec->push_back(has_agg);
        }
    }

    *out_lambda = nm->MakeLambdaNode({row_arg, window_arg}, out_list);
    return Status::OK();
}

Status LambdafyProjects::VisitExpr(node::ExprNode* expr,
                                   node::ExprIdNode* row_arg,
                                   node::ExprIdNode* window_arg,
                                   node::ExprNode** out, bool* has_agg) {
    // determine whether an agg call
    size_t child_num = expr->GetChildNum();
    if (expr->GetExprType() == node::kExprCall) {
        auto call = dynamic_cast<node::CallExprNode*>(expr);
        auto library = ctx_->library();
        auto fn =
            dynamic_cast<const node::ExternalFnDefNode*>(call->GetFnDef());
        if (fn != nullptr && !fn->IsResolved()) {
            if (!library->HasFunction(fn->function_name())) {
                // not a registered udf, maybe user defined script function
                // do not transform child if has over clause
                // this only aims to pass existing cases
                if (call->GetOver() != nullptr) {
                    *out = expr;
                    *has_agg = false;
                    return Status::OK();
                }
            } else if (library->IsUdaf(fn->function_name(), child_num)) {
                CHECK_STATUS(VisitAggExpr(call, row_arg, window_arg, out, has_agg));
                return base::Status::OK();
            }
        }
    }
    *has_agg = false;

    // count(*)
    if (expr->GetExprType() == node::kExprAll) {
        *out = row_arg;
        return Status::OK();
    }

    // determine whether a leaf
    if (child_num == 0) {
        CHECK_STATUS(VisitLeafExpr(expr, row_arg, out));
        return Status::OK();
    }

    // recursive visit children
    std::vector<node::ExprNode*> transformed_children(child_num);
    for (size_t i = 0; i < child_num; ++i) {
        bool child_has_agg = false;

        auto child = expr->GetChild(i);
        if (expr->RequireListAt(ctx_, i)) {
            bool child_is_col = child->GetExprType() == node::kExprColumnRef;
            if (child_is_col) {
                transformed_children[i] = child;
                *has_agg = true;
                continue;
            }
            // Expression require list type input at current position
            // but child can not ensure that.
            // TODO(bxq): support map expression
            // Visit child with new row arg, and produce new_child(row),
            // then wrap with map(window, row => new_child(row))
            // For example:
            // (1) slice(x > y, 1, 3)  ->
            //         slice(map(window, row => row.x > row.y), 1, 3)
            // (2) at(x, 2)  ->
            //         at(map(window, row => row.x), 2)
            CHECK_TRUE(child->IsListReturn(ctx_) && !child_is_col,
                       kCodegenError, "Can not lift child at ", i,
                       " to list for ", expr->GetExprString());
        }

        CHECK_STATUS(VisitExpr(child, row_arg, window_arg,
                               &transformed_children[i], &child_has_agg));
        *has_agg |= child_has_agg;
    }

    // root(c1, c2 ...) -> root(transform(c1), transform(c2), ...)
    for (size_t i = 0; i < child_num; ++i) {
        expr->SetChild(i, transformed_children[i]);
    }
    *out = expr;
    return Status::OK();
}

Status LambdafyProjects::VisitLeafExpr(node::ExprNode* expr,
                                       node::ExprIdNode* row_arg,
                                       node::ExprNode** out) {
    auto nm = ctx_->node_manager();
    auto schemas_ctx = ctx_->schemas_context();
    switch (expr->GetExprType()) {
        case node::kExprParameter: {
            // ?1 -> row => ?1
            *out = expr;
            break;
        }
        case node::kExprPrimary: {
            // 1 -> row => 1
            *out = expr;
            break;
        }
        case node::kExprColumnRef: {
            // column ref -> row => row.c
            auto column_ref = dynamic_cast<node::ColumnRefNode*>(expr);
            size_t schema_idx;
            size_t col_idx;

            CHECK_STATUS(schemas_ctx->ResolveColumnRefIndex(
                column_ref, &schema_idx, &col_idx));
            size_t column_id =
                schemas_ctx->GetSchemaSource(schema_idx)->GetColumnID(col_idx);
            *out = nm->MakeGetFieldExpr(row_arg, column_ref->GetColumnName(),
                                        column_id);
            break;
        }
        case node::kExprColumnId: {
            // column ref -> row => row.c
            size_t column_id =
                dynamic_cast<node::ColumnIdNode*>(expr)->GetColumnID();
            size_t schema_idx;
            size_t col_idx;

            CHECK_STATUS(schemas_ctx->ResolveColumnIndexByID(
                column_id, &schema_idx, &col_idx));
            *out = nm->MakeGetFieldExpr(
                row_arg, "#" + std::to_string(column_id), column_id);
            break;
        }
        case node::kExprArray: {
            // empty array: [] or ARRAY<T>[]
            *out = expr;
            break;
        }
        case node::kExprCall: {
            // fn with empty args
            *out = expr;
            break;
        }
        default:
            FAIL_STATUS(common::kCodegenError, "Unknown leaf expr type: " + ExprTypeName(expr->GetExprType()))
    }
    return Status::OK();
}

Status LambdafyProjects::VisitAggExpr(node::CallExprNode* call,
                                      node::ExprIdNode* row_arg,
                                      node::ExprIdNode* window_arg,
                                      node::ExprNode** out,
                                      bool* is_window_agg) {
    auto nm = ctx_->node_manager();
    auto fn = dynamic_cast<const node::ExternalFnDefNode*>(call->GetFnDef());
    CHECK_TRUE(fn != nullptr, kCodegenError, "Fail to visit agg expression with null function definition node");

    // represent row argument in window iteration
    node::ExprIdNode* iter_row = nullptr;

    auto agg_arg_num = call->GetChildNum();
    std::vector<node::ExprNode*> transformed_child(agg_arg_num);

    std::vector<int> args_require_iter;
    std::vector<int> args_require_window_iter;

    // collect original udaf argument types
    std::vector<node::ExprNode*> agg_original_args;

    bool has_window_iter = false;
    for (size_t i = 0; i < agg_arg_num; ++i) {
        auto child = call->GetChild(i);

        // if child alway produce list results, do not transform
        // it to window iteration form, except for column reference
        bool child_is_list = child->IsListReturn(ctx_) &&
                             child->GetExprType() != node::kExprColumnRef;

        // TODO(bxq): udaf require information about const argument positions
        bool child_is_const = child->GetExprType() == node::kExprPrimary;
        bool child_require_iter =
            call->RequireListAt(ctx_, i) && !child_is_const;
        bool child_require_window_iter = child_require_iter && !child_is_list;
        args_require_iter.push_back(child_require_iter);
        args_require_window_iter.push_back(child_require_window_iter);

        // if current argument position require list input,
        // the child should be computed on udaf's iteration row,
        // else it is not an iterative input.
        node::ExprIdNode* child_row_arg;
        if (child_require_window_iter) {
            has_window_iter = true;
            if (iter_row == nullptr) {
                iter_row = nm->MakeExprIdNode("iter_row");
                iter_row->SetOutputType(row_arg->GetOutputType());
                iter_row->SetNullable(false);
            }
            child_row_arg = iter_row;
        } else {
            child_row_arg = row_arg;
        }

        bool child_has_agg = false;
        CHECK_STATUS(VisitExpr(child, child_row_arg, window_arg,
                               &transformed_child[i], &child_has_agg));

        // resolve update arg
        node::ExprNode* resolved_arg = nullptr;
        ResolveFnAndAttrs resolver(ctx_);
        CHECK_STATUS(resolver.VisitExpr(transformed_child[i], &resolved_arg),
                     "Resolve transformed udaf argument at ", i,
                     " failed: ", transformed_child[i]->GetTreeString());
        CHECK_TRUE(resolved_arg->GetOutputType() != nullptr, kCodegenError);

        // collect original udaf info
        transformed_child[i] = resolved_arg;
        auto original_arg =
            nm->MakeExprIdNode("udaf_list_arg_" + std::to_string(i));
        auto resolved_type = resolved_arg->GetOutputType();
        if (child_require_window_iter) {
            original_arg->SetOutputType(
                nm->MakeTypeNode(node::kList, resolved_type));
            original_arg->SetNullable(false);
        } else {
            if (child_require_iter) {
                CHECK_TRUE(resolved_type->base() == node::kList, kCodegenError,
                           "UDAF require list type at position ", i,
                           " but get ", resolved_type->GetName());
            }
            if (child_is_const) {
                original_arg->SetOutputType(
                    nm->MakeTypeNode(node::kList, resolved_type));
                original_arg->SetNullable(false);
            } else {
                original_arg->SetOutputType(resolved_type);
                original_arg->SetNullable(resolved_arg->nullable());
            }
        }
        agg_original_args.push_back(original_arg);
    }
    *is_window_agg = has_window_iter;

    // resolve original udaf
    node::FnDefNode* fn_def = nullptr;
    CHECK_STATUS(ctx_->library()->ResolveFunction(
                     fn->function_name(), agg_original_args, nm, &fn_def),
                 "Resolve original udaf for ", fn->function_name(), " failed");
    auto origin_udaf = dynamic_cast<node::UdafDefNode*>(fn_def);
    CHECK_TRUE(origin_udaf != nullptr, kCodegenError, fn->function_name(),
               " is not an udaf");

    // refer to original udaf's functionalities
    auto ori_update_fn = origin_udaf->update_func();
    auto ori_merge_fn = origin_udaf->merge_func();
    auto ori_output_fn = origin_udaf->output_func();
    auto ori_init = origin_udaf->init_expr();
    CHECK_TRUE(
        ori_init != nullptr, kCodegenError,
        "Do not support use first element as init state for lambdafy udaf");

    // build new udaf update function
    std::vector<node::ExprNode*> actual_update_args;
    std::vector<node::ExprIdNode*> proxy_update_args;
    std::vector<node::ExprNode*> proxy_udaf_args;
    std::vector<const node::TypeNode*> proxy_udaf_arg_types;

    // state argument is first argument of update function
    auto state_arg = nm->MakeExprIdNode("state");
    actual_update_args.push_back(state_arg);
    proxy_update_args.push_back(state_arg);

    // new udaf may iterate on window rows
    if (has_window_iter) {
        proxy_update_args.push_back(iter_row);
        proxy_udaf_args.push_back(window_arg);
        proxy_udaf_arg_types.push_back(window_arg->GetOutputType());
    }

    // fill other update arguments
    for (size_t i = 0; i < agg_arg_num; ++i) {
        if (args_require_window_iter[i]) {
            // use transformed child (produced by new row arg)
            actual_update_args.push_back(transformed_child[i]);
        } else if (args_require_iter[i]) {
            // use proxy lambda argument
            auto arg = nm->MakeExprIdNode("iter_arg_" + std::to_string(i));
            auto child_type = transformed_child[i]->GetOutputType();
            CHECK_TRUE(child_type->base() == node::kList, kCodegenError);
            arg->SetOutputType(child_type->GetGenericType(0));
            arg->SetNullable(child_type->IsGenericNullable(0));

            proxy_update_args.push_back(arg);
            actual_update_args.push_back(arg);
            proxy_udaf_args.push_back(transformed_child[i]);
            proxy_udaf_arg_types.push_back(
                transformed_child[i]->GetOutputType());
        } else {
            // non-iter argument
            actual_update_args.push_back(transformed_child[i]);
        }
    }

    // wrap actual update call into proxy update function
    auto update_body =
        nm->MakeFuncNode(ori_update_fn, actual_update_args, nullptr);
    auto update_func = nm->MakeLambdaNode(proxy_update_args, update_body);

    std::string new_udaf_name = "window_agg_$";
    new_udaf_name.append(fn->function_name());
    new_udaf_name.append("<");
    for (size_t i = 0; i < agg_original_args.size(); ++i) {
        new_udaf_name.append(agg_original_args[i]->GetOutputType()->GetName());
        if (i < agg_original_args.size() - 1) {
            new_udaf_name.append(", ");
        }
    }
    new_udaf_name.append(">");

    auto new_udaf =
        nm->MakeUdafDefNode(new_udaf_name, proxy_udaf_arg_types, ori_init,
                            update_func, ori_merge_fn, ori_output_fn);
    *out = nm->MakeFuncNode(new_udaf, proxy_udaf_args, nullptr);
    return Status::OK();
}

bool LambdafyProjects::FallBackToLegacyAgg(const node::ExprNode* expr) {
    switch (expr->expr_type_) {
        case node::kExprCall: {
            auto call = dynamic_cast<const node::CallExprNode*>(expr);
            std::string agg_func_name = "";
            switch (call->GetFnDef()->GetType()) {
                case node::kExternalFnDef: {
                    agg_func_name =
                        dynamic_cast<const node::ExternalFnDefNode*>(
                            call->GetFnDef())
                            ->function_name();
                    break;
                }
                default:
                    return false;
            }
            if (agg_opt_fn_names_.find(agg_func_name) ==
                agg_opt_fn_names_.end()) {
                return false;
            }
            if (call->GetChildNum() != 1) {
                return false;
            }
            auto input_expr = call->GetChild(0);
            if (input_expr->expr_type_ != node::kExprColumnRef) {
                return false;
            }
            auto col = dynamic_cast<node::ColumnRefNode*>(
                const_cast<node::ExprNode*>(input_expr));
            const std::string& rel_name = col->GetRelationName();
            const std::string& col_name = col->GetColumnName();
            size_t schema_idx;
            size_t col_idx;
            auto schemas_ctx = ctx_->schemas_context();
            auto status =
                schemas_ctx->ResolveColumnRefIndex(col, &schema_idx, &col_idx);
            if (!status.isOK()) {
                LOG(WARNING)
                    << "fail to resolve column " << rel_name + "." + col_name;
                return false;
            }
            switch (schemas_ctx->GetSchema(schema_idx)->Get(col_idx).type()) {
                case hybridse::type::kInt16:
                case hybridse::type::kInt32:
                case hybridse::type::kInt64:
                case hybridse::type::kFloat:
                case hybridse::type::kDouble:
                    break;
                default:
                    return false;
            }
            break;
        }
        default:
            return false;
    }
    return true;
}

}  // namespace passes
}  // namespace hybridse
