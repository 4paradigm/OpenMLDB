/**
 * Copyright 2021 4paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "vm/physical_plan_context.h"

#include "codegen/ir_base_builder.h"
#include "passes/expression/default_passes.h"
#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"

namespace hybridse {
namespace vm {

using hybridse::common::kPlanError;

Status OptimizeFunctionLet(const ColumnProjects& projects,
                           node::ExprAnalysisContext* ctx,
                           node::LambdaNode* func);

Status PhysicalPlanContext::InitFnDef(const node::ExprListNode* exprs, const SchemasContext* schemas_ctx,
                                      bool is_row_project, FnComponent* fn_component) {
    ColumnProjects projects;
    for (size_t i = 0; i < exprs->GetChildNum(); ++i) {
        const node::ExprNode* expr = exprs->GetChild(i);
        CHECK_TRUE(expr != nullptr, kPlanError, "Can not init fn def with null expression");
        projects.Add(expr->GetExprString(), expr, nullptr);
    }
    return InitFnDef(projects, schemas_ctx, is_row_project, fn_component);
}
Status PhysicalPlanContext::InitFnDef(const ColumnProjects& projects, const SchemasContext* schemas_ctx,
                                      bool is_row_project, FnComponent* fn_component) {
    // lambdafy project expressions
    std::vector<const node::ExprNode*> exprs;
    for (size_t i = 0; i < projects.size(); ++i) {
        exprs.push_back(projects.GetExpr(i));
    }

    node::ExprAnalysisContext expr_pass_ctx(node_manager(), library(), schemas_ctx, parameter_types_);
    const bool enable_legacy_agg_opt = true;
    passes::LambdafyProjects lambdafy_pass(&expr_pass_ctx, enable_legacy_agg_opt);
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
    passes::ResolveFnAndAttrs resolve_pass(&expr_pass_ctx);
    CHECK_STATUS(resolve_pass.VisitLambda(lambdafy_func, global_arg_types,
                                          &resolved_func));

    // expression optimization
    if (enable_expr_opt_) {
        base::Status optimized_status = OptimizeFunctionLet(projects, &expr_pass_ctx, resolved_func);
        if (!optimized_status.isOK()) {
            DLOG(WARNING) << optimized_status;
        }
     }

    FnInfo* output_fn = fn_component->mutable_fn_info();
    output_fn->Clear();

    // set output schema
    auto expr_list = resolved_func->body();
    CHECK_TRUE(projects.size() == expr_list->GetChildNum(), kPlanError);
    for (size_t i = 0; i < projects.size(); ++i) {
        type::ColumnDef column_def;
        column_def.set_name(projects.GetName(i));
        column_def.set_is_not_null(false);

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

        CHECK_TRUE(resolved_expr->GetOutputType() != nullptr, kPlanError, "Fail to resolve expression: ",
                   resolved_expr->GetExprString());
        auto* mut_col_schema = column_def.mutable_schema();
        auto as = codegen::Type2ColumnSchema(resolved_expr->GetOutputType(), mut_col_schema);
        if (mut_col_schema->has_base_type()) {
            // backwards compatibility to types field
            column_def.set_type(mut_col_schema->base_type());
        }
        CHECK_TRUE(as.ok(), kPlanError, as.ToString(), " for expression ", resolved_expr->GetExprString());

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

Status PhysicalPlanContext::GetSourceID(const std::string& db_name,
                                        const std::string& table_name,
                                        const std::string& column_name,
                                        size_t* column_id) {
    std::string dbstr = db_name.empty() ? db() : db_name;
    CHECK_STATUS(InitializeSourceIdMappings(dbstr, table_name));
    CHECK_TRUE(db_table_column_id_map_.find(dbstr) != db_table_column_id_map_.end(), kPlanError,
               "Fail to find database ", dbstr);
    auto tbl_iter = db_table_column_id_map_[dbstr].find(table_name);
    CHECK_TRUE(tbl_iter != db_table_column_id_map_[dbstr].end(), kPlanError,
               "Fail to find source table name ", table_name);
    auto& dict = tbl_iter->second;
    auto col_iter = dict.find(column_name);
    CHECK_TRUE(col_iter != dict.end(), kPlanError, "Fail to find column ",
               column_name, " in source table ", table_name);
    *column_id = col_iter->second;
    return Status::OK();
}

Status PhysicalPlanContext::GetRequestSourceID(const std::string& db_name,
                                               const std::string& table_name,
                                               const std::string& column_name,
                                               size_t* column_id) {
    CHECK_STATUS(InitializeSourceIdMappings(db_name.empty() ? db() : db_name, table_name));
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
    const std::string& db_name,
    const std::string& table_name) {
    if (db_table_column_id_map_.find(db_name) != db_table_column_id_map_.end()) {
        if (db_table_column_id_map_[db_name].find(table_name) != db_table_column_id_map_[db_name].end()) {
            return Status::OK();
        }
    } else {
        db_table_column_id_map_.insert(std::make_pair(db_name, std::map<std::string, std::map<std::string, size_t>>()));
    }
    auto &table_column_id_map = db_table_column_id_map_[db_name];

    auto table = catalog_->GetTable(db_name, table_name);
    CHECK_TRUE(table != nullptr, kPlanError,
               "Fail to find source table name: ", table_name);

    const codec::Schema& schema = *table->GetSchema();
    auto& table_dict = table_column_id_map[table_name];
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

Status OptimizeFunctionLet(const ColumnProjects& projects,
                           node::ExprAnalysisContext* ctx,
                           node::LambdaNode* func) {
    CHECK_TRUE(func->GetArgSize() == 2, kPlanError);
    CHECK_TRUE(projects.size() == func->body()->GetChildNum(), kPlanError);

    // group idx -> (idx in group -> output idx)
    std::vector<std::vector<size_t>> pos_mappings;

    // frame name -> group idx
    std::map<std::string, size_t> frame_mappings;

    // expr groups
    std::vector<node::ExprListNode*> groups;

    // split expressions by frame
    for (size_t i = 0; i < projects.size(); ++i) {
        auto expr = func->body()->GetChild(i);
        auto frame = projects.GetFrame(i);
        std::string key = frame ? frame->GetExprString() : "";
        auto iter = frame_mappings.find(key);
        size_t group_idx;
        if (iter == frame_mappings.end()) {
            group_idx = groups.size();
            frame_mappings.insert(iter, std::make_pair(key, group_idx));
            pos_mappings.push_back(std::vector<size_t>());
            groups.push_back(ctx->node_manager()->MakeExprList());
        } else {
            group_idx = iter->second;
        }
        pos_mappings[group_idx].push_back(i);
        groups[group_idx]->AddChild(expr);
    }

    for (size_t i = 0; i < groups.size(); ++i) {
        passes::ExprPassGroup pass_group;
        pass_group.SetRow(func->GetArg(0));
        pass_group.SetWindow(func->GetArg(1));
        AddDefaultExprOptPasses(ctx, &pass_group);

        node::ExprNode* optimized = nullptr;
        CHECK_STATUS(pass_group.Apply(ctx, groups[i], &optimized));

        CHECK_TRUE(optimized != nullptr &&
                       optimized->GetChildNum() == pos_mappings[i].size(),
                   kPlanError);
        for (size_t j = 0; j < optimized->GetChildNum(); ++j) {
            size_t output_idx = pos_mappings[i][j];
            func->body()->SetChild(output_idx, optimized->GetChild(j));
        }
    }
    return base::Status::OK();
}
}  // namespace vm
}  // namespace hybridse
