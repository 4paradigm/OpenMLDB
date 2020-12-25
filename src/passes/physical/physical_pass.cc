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
                                           schemas_ctx, enable_legacy_agg_opt);
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

Status BuildColumnReplacement(const node::ExprNode* expr,
                              const SchemasContext* origin_schema,
                              const SchemasContext* rebase_schema,
                              node::NodeManager* nm,
                              passes::ExprReplacer* replacer) {
    // Find all column expressions the expr depend on
    std::vector<const node::ExprNode*> origin_columns;
    CHECK_STATUS(
        origin_schema->ResolveExprDependentColumns(expr, &origin_columns));

    // Build possible replacement
    for (auto col_expr : origin_columns) {
        if (col_expr->GetExprType() == node::kExprColumnRef) {
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(col_expr);
            size_t origin_schema_idx;
            size_t origin_col_idx;
            CHECK_STATUS(origin_schema->ResolveColumnRefIndex(
                col_ref, &origin_schema_idx, &origin_col_idx));
            size_t column_id = origin_schema->GetSchemaSource(origin_schema_idx)
                                   ->GetColumnID(origin_col_idx);

            size_t new_schema_idx;
            size_t new_col_idx;
            Status status = rebase_schema->ResolveColumnIndexByID(
                column_id, &new_schema_idx, &new_col_idx);

            // (1) the column is inherited with same column id
            if (status.isOK()) {
                replacer->AddReplacement(col_ref->GetRelationName(),
                                         col_ref->GetColumnName(),
                                         nm->MakeColumnIdNode(column_id));
                continue;
            }

            // (2) the column is with same name
            status = rebase_schema->ResolveColumnRefIndex(
                col_ref, &new_schema_idx, &new_col_idx);
            if (status.isOK()) {
                size_t new_column_id =
                    rebase_schema->GetSchemaSource(new_schema_idx)
                        ->GetColumnID(new_col_idx);
                replacer->AddReplacement(col_ref->GetRelationName(),
                                         col_ref->GetColumnName(),
                                         nm->MakeColumnIdNode(new_column_id));
                continue;
            }

            // (3) pick the column at the same index
            size_t total_idx = origin_col_idx;
            for (size_t i = 0; i < origin_schema_idx; ++i) {
                total_idx += origin_schema->GetSchemaSource(i)->size();
            }
            bool index_is_valid = false;
            for (size_t i = 0; i < rebase_schema->GetSchemaSourceSize(); ++i) {
                auto source = rebase_schema->GetSchemaSource(i);
                if (total_idx < source->size()) {
                    auto col_id_node =
                        nm->MakeColumnIdNode(source->GetColumnID(total_idx));
                    replacer->AddReplacement(col_ref->GetRelationName(),
                                             col_ref->GetColumnName(),
                                             col_id_node);
                    replacer->AddReplacement(column_id, col_id_node);
                    index_is_valid = true;
                    break;
                }
                total_idx -= source->size();
            }

            // (3) can not build replacement
            CHECK_TRUE(index_is_valid, common::kPlanError,
                       "Build replacement failed: " + col_ref->GetExprString());

        } else if (col_expr->GetExprType() == node::kExprColumnId) {
            auto column_id = dynamic_cast<const node::ColumnIdNode*>(col_expr)
                                 ->GetColumnID();
            size_t origin_schema_idx;
            size_t origin_col_idx;
            CHECK_STATUS(origin_schema->ResolveColumnIndexByID(
                column_id, &origin_schema_idx, &origin_col_idx));

            size_t new_schema_idx;
            size_t new_col_idx;
            Status status = rebase_schema->ResolveColumnIndexByID(
                column_id, &new_schema_idx, &new_col_idx);

            // (1) the column is inherited with same column id
            if (status.isOK()) {
                continue;
            }

            // (2) pick the column at the same index
            size_t total_idx = origin_col_idx;
            for (size_t i = 0; i < origin_col_idx; ++i) {
                total_idx += origin_schema->GetSchemaSource(i)->size();
            }
            bool index_is_valid = false;
            for (size_t i = 0; i < rebase_schema->GetSchemaSourceSize(); ++i) {
                auto source = rebase_schema->GetSchemaSource(i);
                if (total_idx < source->size()) {
                    replacer->AddReplacement(
                        column_id,
                        nm->MakeColumnIdNode(source->GetColumnID(total_idx)));
                    index_is_valid = true;
                    break;
                }
                total_idx -= source->size();
            }

            // (3) can not build replacement
            CHECK_TRUE(
                index_is_valid, common::kPlanError,
                "Build replacement failed: " + col_expr->GetExprString());
        } else {
            return Status(common::kPlanError, "Invalid column expression type");
        }
    }
    return Status::OK();
}

}  // namespace vm
}  // namespace fesql
