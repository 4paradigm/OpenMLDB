/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * lambdafy_projects.cc
 *--------------------------------------------------------------------------
 **/
#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"

namespace fesql {
namespace passes {

Status LambdafyProjects::Transform(const node::PlanNodeList& projects,
                                   node::LambdaNode** out_lambda,
                                   std::vector<int>* require_agg_vec,
                                   std::vector<std::string>* out_names,
                                   std::vector<node::FrameNode*>* out_frames) {
    // arg1: current input row
    auto row_type = nm_->MakeRowType(input_schemas_);
    auto row_arg = nm_->MakeExprIdNode("row", node::ExprIdNode::GetNewId());
    row_arg->SetOutputType(row_type);

    // arg2: optional row list for agg
    auto window_type = nm_->MakeTypeNode(node::kList, row_type);
    auto window_arg =
        nm_->MakeExprIdNode("window", node::ExprIdNode::GetNewId());
    window_arg->SetOutputType(window_type);

    // iterate project exprs
    auto out_list = nm_->MakeExprList();
    require_agg_vec->clear();
    for (node::PlanNode* plan_node : projects) {
        auto pp_node = dynamic_cast<node::ProjectNode*>(plan_node);
        CHECK_TRUE(pp_node != nullptr);
        auto expr = pp_node->GetExpression();
        if (expr->GetExprType() == node::kExprAll) {
            // expand *
            for (size_t slice = 0;
                 slice < input_schemas_.GetSchemaSourceListSize(); ++slice) {
                auto schema_slice = input_schemas_.GetSchemaSourceSlice(slice);
                std::string rel_name = schema_slice.table_name_;
                for (int k = 0; k < schema_slice.schema_->size(); ++k) {
                    auto col_name = schema_slice.schema_->Get(k).name();

                    auto get_col =
                        nm_->MakeGetFieldExpr(row_arg, col_name, rel_name);
                    out_list->AddChild(get_col);
                    require_agg_vec->push_back(false);
                    out_frames->push_back(nullptr);
                    out_names->push_back(col_name);
                }
            }
        } else if (legacy_agg_opt_ && FallBackToLegacyAgg(expr)) {
            DLOG(INFO) << "Use agg opt for " << expr->GetExprString();
            out_list->AddChild(expr);
            require_agg_vec->push_back(true);
            out_frames->push_back(pp_node->frame());
            out_names->push_back(pp_node->GetName());

        } else {
            bool has_agg;
            bool is_root_agg;
            node::ExprNode* transformed = nullptr;
            CHECK_STATUS(VisitExpr(expr, row_arg, window_arg, &transformed,
                                   &has_agg, &is_root_agg),
                         "Lambdafy ", expr->GetExprString(), " failed");
            out_list->AddChild(transformed);
            require_agg_vec->push_back(has_agg);
            out_frames->push_back(pp_node->frame());
            out_names->push_back(pp_node->GetName());
        }
    }

    *out_lambda = nm_->MakeLambdaNode({row_arg, window_arg}, out_list);
    return Status::OK();
}

Status LambdafyProjects::VisitExpr(node::ExprNode* expr,
                                   node::ExprIdNode* row_arg,
                                   node::ExprIdNode* window_arg,
                                   node::ExprNode** out, bool* has_agg,
                                   bool* is_agg_root) {
    // determine whether an agg call
    size_t child_num = expr->GetChildNum();
    if (expr->GetExprType() == node::kExprCall) {
        auto call = dynamic_cast<node::CallExprNode*>(expr);
        auto fn =
            dynamic_cast<const node::ExternalFnDefNode*>(call->GetFnDef());
        if (fn != nullptr && !fn->IsResolved()) {
            if (library_->FindAll(fn->function_name()) == nullptr) {
                // not a registered udf, maybe user defined script function
                *out = expr;
                *has_agg = false;
                *is_agg_root = false;
                return Status::OK();
            } else if (library_->IsUDAF(fn->function_name(), child_num)) {
                *has_agg = true;
                *is_agg_root = true;
                return VisitAggExpr(call, row_arg, window_arg, out);
            }
        }
    }
    *is_agg_root = false;
    *has_agg = false;

    // count(*)
    if (expr->GetExprType() == node::kExprAll) {
        *out = row_arg;
        return Status::OK();
    }

    // determine whether a leaf
    if (child_num == 0) {
        return VisitLeafExpr(expr, row_arg, out);
    }

    // recursive visit children
    std::vector<node::ExprNode*> transformed_children(child_num);
    for (size_t i = 0; i < child_num; ++i) {
        bool child_has_agg;
        bool child_is_root_agg;
        CHECK_STATUS(VisitExpr(expr->GetChild(i), row_arg, window_arg,
                               &transformed_children[i], &child_has_agg,
                               &child_is_root_agg));
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
    switch (expr->GetExprType()) {
        case node::kExprPrimary: {
            // 1 -> row => 1
            *out = expr;
            break;
        }
        case node::kExprColumnRef: {
            // column ref -> row => row.c
            auto column_ref = dynamic_cast<node::ColumnRefNode*>(expr);
            *out = nm_->MakeGetFieldExpr(row_arg, column_ref->GetColumnName(),
                                         column_ref->GetRelationName());
            break;
        }
        default:
            return Status(
                common::kCodegenError,
                "Unknown left expr type: " + ExprTypeName(expr->GetExprType()));
    }
    return Status::OK();
}

Status LambdafyProjects::VisitAggExpr(node::CallExprNode* call,
                                      node::ExprIdNode* row_arg,
                                      node::ExprIdNode* window_arg,
                                      node::ExprNode** out) {
    auto fn = dynamic_cast<const node::ExternalFnDefNode*>(call->GetFnDef());
    CHECK_TRUE(fn != nullptr);

    // build update function
    auto state_arg = nm_->MakeExprIdNode("state", node::ExprIdNode::GetNewId());
    auto new_row_arg = nm_->MakeExprIdNode("row", node::ExprIdNode::GetNewId());
    new_row_arg->SetOutputType(row_arg->GetOutputType());

    // update function args: [state, transform(c1), transform(c2), ...]
    auto agg_col_num = call->GetChildNum();
    std::vector<node::ExprNode*> update_args(1 + agg_col_num);
    update_args[0] = state_arg;

    // original udaf argument types
    std::vector<node::ExprNode*> agg_original_args;
    std::vector<const node::TypeNode*> agg_original_list_types;

    bool all_agg_child = true;
    for (size_t i = 0; i < agg_col_num; ++i) {
        bool child_has_agg;
        bool child_is_root_agg;
        CHECK_STATUS(VisitExpr(call->GetChild(i), new_row_arg, window_arg,
                               &update_args[i + 1], &child_has_agg,
                               &child_is_root_agg));

        // resolve update arg
        node::ExprNode* resolved_arg = nullptr;
        ResolveFnAndAttrs resolver(nm_, library_, input_schemas_);
        auto status = resolver.VisitExpr(update_args[i + 1], &resolved_arg);
        if (status.isOK() && resolved_arg->GetOutputType() != nullptr) {
            update_args[i + 1] = resolved_arg;

            // collect original udaf info
            auto original_arg =
                nm_->MakeExprIdNode("udaf_list_arg_" + std::to_string(i),
                                    node::ExprIdNode::GetNewId());
            auto original_type =
                nm_->MakeTypeNode(node::kList, resolved_arg->GetOutputType());
            original_arg->SetOutputType(original_type);
            original_arg->SetNullable(false);
            agg_original_args.push_back(original_arg);
            agg_original_list_types.push_back(original_type);

        } else {
            LOG(WARNING) << "Resolve " << i
                         << "th udaf argument failed: " << status.msg;
        }

        // if all args agg
        all_agg_child &= child_is_root_agg;
    }

    if (all_agg_child && agg_col_num == 1) {
        // eg. sum(slice(col1), 1, 4)
        for (size_t i = 0; i < agg_col_num; ++i) {
            call->SetChild(i, update_args[i + 1]);
        }
        *out = call;
        return Status::OK();
    }

    // resolve original udaf
    node::FnDefNode* fn_def = nullptr;
    vm::SchemasContext schemas_ctx(input_schemas_);
    node::ExprAnalysisContext expr_analysis_ctx(nm_, &schemas_ctx);
    CHECK_STATUS(library_->ResolveFunction(fn->function_name(),
                                           agg_original_args, nm_, &fn_def),
                 "Resolve original udaf for ", fn->function_name(), " failed");
    auto origin_udaf = dynamic_cast<node::UDAFDefNode*>(fn_def);
    CHECK_TRUE(origin_udaf != nullptr, fn->function_name(), " is not an udaf");

    // convention to refer unresolved udaf's update function
    auto ori_update_fn = origin_udaf->update_func();
    auto ori_merge_fn = origin_udaf->merge_func();
    auto ori_output_fn = origin_udaf->output_func();
    auto ori_init = origin_udaf->init_expr();
    CHECK_TRUE(
        ori_init != nullptr,
        "Do not support use first element as init state for lambdafy udaf");

    auto update_body = nm_->MakeFuncNode(ori_update_fn, update_args, nullptr);
    auto update_func =
        nm_->MakeLambdaNode({state_arg, new_row_arg}, update_body);

    // build new udaf call
    std::string new_udaf_name = fn->function_name();
    new_udaf_name.append("<");
    for (size_t i = 0; i < agg_original_list_types.size(); ++i) {
        new_udaf_name.append(agg_original_list_types[i]->GetName());
        if (i < agg_original_list_types.size() - 1) {
            new_udaf_name.append(", ");
        }
    }
    new_udaf_name.append(">");

    auto new_udaf = nm_->MakeUDAFDefNode(
        new_udaf_name, {window_arg->GetOutputType()}, ori_init, update_func,
        ori_merge_fn, ori_output_fn);
    *out = nm_->MakeFuncNode(new_udaf, {window_arg}, nullptr);
    return Status::OK();
}

bool LambdafyProjects::FallBackToLegacyAgg(node::ExprNode* expr) {
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
            const vm::RowSchemaInfo* info;
            vm::SchemasContext schema_context(input_schemas_);
            if (!schema_context.ColumnRefResolved(rel_name, col_name, &info)) {
                LOG(WARNING)
                    << "fail to resolve column " << rel_name + "." + col_name;
                return false;
            }
            codec::RowDecoder decoder(*info->schema_);
            codec::ColInfo col_info;
            if (!decoder.ResolveColumn(col_name, &col_info)) {
                LOG(WARNING)
                    << "fail to resolve column " << rel_name + "." + col_name;
                return false;
            }
            switch (col_info.type) {
                case fesql::type::kInt16:
                case fesql::type::kInt32:
                case fesql::type::kInt64:
                case fesql::type::kFloat:
                case fesql::type::kDouble:
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
}  // namespace fesql
