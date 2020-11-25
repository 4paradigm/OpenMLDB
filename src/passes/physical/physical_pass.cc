/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_pass.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "passes/physical/physical_pass.h"

#include <string>
#include <vector>

#include "codegen/ir_base_builder.h"
#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"

namespace fesql {
namespace vm {

using fesql::base::Status;
using fesql::common::kPlanError;

Status PhysicalPlanContext::InitFnDef(const node::ExprListNode* exprs,
                                      const SchemasContext* schemas_ctx,
                                      bool is_row_project,
                                      FnComponent* fn_component) {
    ColumnProjects projects;
    for (size_t i = 0; i < exprs->GetChildNum(); ++i) {
        const node::ExprNode* expr = exprs->GetChild(i);
        CHECK_TRUE(expr != nullptr, kPlanError);
        projects.Add(expr->GetExprString(), expr, nullptr);
    }
    return InitFnDef(projects, schemas_ctx, is_row_project, fn_component);
}

Status PhysicalPlanContext::InitFnDef(const ColumnProjects& projects,
                                      const SchemasContext* schemas_ctx,
                                      bool is_row_project,
                                      FnComponent* fn_component) {
    // lambdafy project expressions
    std::vector<const node::ExprNode*> exprs;
    for (size_t i = 0; i < projects.size(); ++i) {
        exprs.push_back(projects.GetExpr(i));
    }
    const bool enable_legacy_agg_opt = true;
    passes::LambdafyProjects lambdafy_pass(node_manager(), library(),
                                           schemas_ctx,
                                           &node_id_to_column_id_,
                                           enable_legacy_agg_opt);
    node::LambdaNode* lambdafy_func = nullptr;
    std::vector<int> require_agg;
    CHECK_STATUS(lambdafy_pass.Transform(exprs, &lambdafy_func, &require_agg));

    // check whether agg exists in row mode
    bool has_agg = false;
    for (size_t i = 0; i < require_agg.size(); ++i) {
        if (require_agg[i]) {
            has_agg = true;
            CHECK_TRUE(
                !is_row_project, kPlanError,
                "Can not gen agg project in row project node, ", i,
                "th expression is: ", projects.GetExpr(i)->GetExprString());
        }
    }

    // type inference and resolve udf function
    std::vector<const node::TypeNode*> global_arg_types = {
        lambdafy_func->GetArgType(0), lambdafy_func->GetArgType(1)};
    node::LambdaNode* resolved_func = nullptr;
    passes::ResolveFnAndAttrs resolve_pass(node_manager(), library(),
                                           schemas_ctx);
    CHECK_STATUS(resolve_pass.VisitLambda(lambdafy_func, global_arg_types,
                                          &resolved_func));

    FnInfo* output_fn = fn_component->mutable_fn_info();
    output_fn->Clear();

    // set output schema
    auto expr_list = resolved_func->body();
    CHECK_TRUE(projects.size() == expr_list->GetChildNum(), kPlanError);
    for (size_t i = 0; i < projects.size(); ++i) {
        type::ColumnDef column_def;
        column_def.set_name(projects.GetName(i));
        column_def.set_is_not_null(false);

        type::Type column_type;
        auto resolved_expr = expr_list->GetChild(i);

        // TODO(xxx): legacy udf type infer
        if (resolved_expr->GetOutputType() == nullptr) {
            auto call = dynamic_cast<node::CallExprNode*>(resolved_expr);
            if (call != nullptr) {
                auto fname = call->GetFnDef()->GetName();
                auto fiter = legacy_udf_dict_.find(fname);
                if (fiter != legacy_udf_dict_.end()) {
                    column_def.set_type(fiter->second);
                    auto frame = has_agg ? projects.GetFrame(i) : nullptr;
                    output_fn->AddOutputColumn(column_def, frame);
                    continue;
                }
            }
        }

        CHECK_TRUE(resolved_expr->GetOutputType() != nullptr, kPlanError, i,
                   "th output project expression type "
                   "is unknown: ",
                   resolved_expr->GetExprString());
        CHECK_TRUE(codegen::DataType2SchemaType(*resolved_expr->GetOutputType(),
                                                &column_type),
                   kPlanError, i, "th output project expression type illegal: ",
                   resolved_expr->GetOutputType()->GetName());
        column_def.set_type(column_type);

        auto frame = has_agg ? projects.GetFrame(i) : nullptr;
        output_fn->AddOutputColumn(column_def, frame);
    }

    // set output function info
    std::string fn_name =
        "__internal_sql_codegen_" + std::to_string(codegen_func_id_counter_++);
    output_fn->SetFn(fn_name, resolved_func, schemas_ctx);
    if (has_agg) {
        output_fn->SetPrimaryFrame(projects.GetPrimaryFrame());
    }
    return Status::OK();
}

Status PhysicalPlanContext::GetSourceID(const std::string& table_name,
                                        const std::string& column_name,
                                        size_t* column_id) {
    CHECK_STATUS(InitializeSourceIdMappings(table_name));
    auto tbl_iter = table_column_id_map_.find(table_name);
    CHECK_TRUE(tbl_iter != table_column_id_map_.end(), kPlanError,
               "Fail to find source table name ", table_name);
    auto& dict = tbl_iter->second;
    auto col_iter = dict.find(column_name);
    CHECK_TRUE(col_iter != dict.end(), kPlanError, "Fail to find column ",
               column_name, " in source table ", table_name);
    *column_id = col_iter->second;
    return Status::OK();
}

Status PhysicalPlanContext::GetRequestSourceID(const std::string& table_name,
                                               const std::string& column_name,
                                               size_t* column_id) {
    CHECK_STATUS(InitializeSourceIdMappings(table_name));
    auto tbl_iter = request_column_id_map_.find(table_name);
    CHECK_TRUE(tbl_iter != request_column_id_map_.end(), kPlanError,
               "Fail to find source table name ", table_name);
    auto& dict = tbl_iter->second;
    auto col_iter = dict.find(column_name);
    CHECK_TRUE(col_iter != dict.end(), kPlanError, "Fail to find column \"",
               column_name, "\" in source table ", table_name);
    *column_id = col_iter->second;
    return Status::OK();
}

Status PhysicalPlanContext::InitializeSourceIdMappings(
    const std::string& table_name) {
    if (table_column_id_map_.find(table_name) != table_column_id_map_.end()) {
        return Status::OK();
    }
    auto table = catalog_->GetTable(db(), table_name);
    CHECK_TRUE(table != nullptr, kPlanError,
               "Fail to find source table name: ", table_name);

    const codec::Schema& schema = *table->GetSchema();
    auto& table_dict = table_column_id_map_[table_name];
    for (auto j = 0; j < schema.size(); ++j) {
        const auto& col_def = schema.Get(j);
        const std::string& column_name = col_def.name();

        size_t source_id = GetNewColumnID();
        table_dict[column_name] = source_id;
        column_id_to_name_[source_id] = {table_name, column_name};
    }

    auto& request_dict = request_column_id_map_[table_name];
    for (auto j = 0; j < schema.size(); ++j) {
        const auto& col_def = schema.Get(j);
        const std::string& column_name = col_def.name();

        auto request_id = GetNewColumnID();
        request_dict[column_name] = request_id;
        column_id_to_name_[request_id] = {table_name, column_name};
        request_column_id_to_source_id_[request_id] =
            table_dict[col_def.name()];
    }
    return Status::OK();
}

size_t PhysicalPlanContext::GetNewColumnID() {
    return this->column_id_counter_++;
}

}  // namespace vm
}  // namespace fesql
