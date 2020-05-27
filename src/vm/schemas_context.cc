/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * schema.cc
 *
 * Author: chenjing
 * Date: 2020/4/20
 *--------------------------------------------------------------------------
 **/
#include "vm/schemas_context.h"
#include <set>
namespace fesql {
namespace vm {
vm::SchemasContext::SchemasContext(
    const vm::SchemaSourceList& table_schema_list) {
    uint32_t idx = 0;
    for (auto iter = table_schema_list.schema_source_list_.cbegin();
         iter != table_schema_list.schema_source_list_.cend(); iter++) {
        RowSchemaInfo info(idx, iter->table_name_, iter->schema_,
                           iter->sources_);
        row_schema_info_list_.push_back(info);
        // init table -> context idx map
        if (!info.table_name_.empty()) {
            table_context_id_map_.insert(std::make_pair(info.table_name_, idx));
        }

        // init col -> context idx map
        auto schema = info.schema_;
        for (auto col_iter = schema->begin(); col_iter != schema->end();
             col_iter++) {
            auto map_iter = col_context_id_map_.find(col_iter->name());
            if (map_iter == col_context_id_map_.cend()) {
                col_context_id_map_.insert(
                    std::make_pair(col_iter->name(), std::vector<uint32_t>()));
                map_iter = col_context_id_map_.find(col_iter->name());
            }
            map_iter->second.push_back(idx);
        }
        idx++;
    }
}

// Return true if expression list resolved from context
// Store row info into {infos}
bool SchemasContext::ExprListResolved(
    const std::vector<node::ExprNode*>& expr_list,
    std::set<const RowSchemaInfo*>& infos) const {  // NOLINT
    infos.clear();
    for (auto expr : expr_list) {
        const RowSchemaInfo* info = nullptr;
        if (!ExprRefResolved(expr, &info)) {
            return false;
        }
        if (nullptr != info) {
            infos.insert(info);
        }
    }
    return true;
}

// Return true if only expression list resolved from same schema
// Store schema info into {info}
bool SchemasContext::ExprListResolvedFromSchema(
    const std::vector<node::ExprNode*>& expr_list,
    const RowSchemaInfo** info) const {
    if (expr_list.empty()) {
        *info = nullptr;
        return true;
    }
    std::set<const RowSchemaInfo*> infos;
    if (!ExprListResolved(expr_list, infos)) {
        return false;
    }
    if (infos.size() > 1) {
        LOG(WARNING) << "Expression based on difference table";
        return false;
    }
    if (infos.empty()) {
        *info = nullptr;
        return true;
    }

    *info = *infos.cbegin();
    return true;
}
bool SchemasContext::ExprRefResolved(const node::ExprNode* expr,
                                     const RowSchemaInfo** info) const {
    if (nullptr == expr) {
        *info = nullptr;
        return true;
    }
    switch (expr->expr_type_) {
        case node::kExprId:
        case node::kExprPrimary: {
            *info = nullptr;
            return true;
        }
        case node::kExprAll: {
            auto all_expr = dynamic_cast<const node::AllNode*>(expr);
            return AllRefResolved(all_expr->GetRelationName(), info);
        }
        case node::kExprColumnRef: {
            auto column_expr = dynamic_cast<const node::ColumnRefNode*>(expr);
            return ColumnRefResolved(column_expr->GetRelationName(),
                                     column_expr->GetColumnName(), info);
        }
        case node::kExprBetween: {
            std::vector<node::ExprNode*> expr_list;
            auto between_expr = dynamic_cast<const node::BetweenExpr*>(expr);
            expr_list.push_back(between_expr->left_);
            expr_list.push_back(between_expr->right_);
            expr_list.push_back(between_expr->expr_);
            return ExprListResolvedFromSchema(expr_list, info);
        }
        case node::kExprCall: {
            std::vector<node::ExprNode*> expr_list;
            auto call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            if (!node::ExprListNullOrEmpty(call_expr->GetArgs())) {
                for (auto expr : call_expr->GetArgs()->children_) {
                    expr_list.push_back(expr);
                }
            }
            if (nullptr != call_expr->GetOver()) {
                if (nullptr != call_expr->GetOver()->GetOrders()) {
                    expr_list.push_back(call_expr->GetOver()->GetOrders());
                }
                if (nullptr != call_expr->GetOver()->GetPartitions()) {
                    for (auto expr :
                         call_expr->GetOver()->GetPartitions()->children_) {
                        expr_list.push_back(expr);
                    }
                }
            }
            return ExprListResolvedFromSchema(expr_list, info);
        }
        default: {
            return ExprListResolvedFromSchema(expr->children_, info);
        }
    }
}
bool SchemasContext::AllRefResolved(const std::string& relation_name,
                                    const RowSchemaInfo** info) const {
    if (relation_name.empty()) {
        LOG(WARNING) << "fail to find column: relation and col is empty";
        return false;
    }

    uint32_t table_ctx_id = -1;
    if (!relation_name.empty()) {
        auto table_map_iter = table_context_id_map_.find(relation_name);

        if (table_map_iter == table_context_id_map_.cend()) {
            LOG(WARNING) << "Unknow Table: ' " + relation_name + "'  in DB";
            return false;
        }
        table_ctx_id = table_map_iter->second;
    }

    if (table_context_id_map_.size() > 1) {
        LOG(WARNING) << "'*':  in field list is ambiguous";
        return false;
    }
    *info = &row_schema_info_list_[table_ctx_id];
    return true;
}
bool SchemasContext::ColumnRefResolved(const std::string& relation_name,
                                       const std::string& col_name,
                                       const RowSchemaInfo** info) const {
    if (relation_name.empty() && col_name.empty()) {
        LOG(WARNING) << "fail to find column: relation and col is empty";
        return false;
    }

    uint32_t table_ctx_id = -1;
    if (!relation_name.empty()) {
        auto table_map_iter = table_context_id_map_.find(relation_name);
        if (table_map_iter == table_context_id_map_.cend()) {
            LOG(WARNING) << "Unknow Column: '" + col_name + "'  in '" +
                                relation_name + "'";
            return false;
        }
        table_ctx_id = table_map_iter->second;
    }

    auto iter = col_context_id_map_.find(col_name);
    if (iter == col_context_id_map_.end()) {
        LOG(WARNING) << "fail to find column";
        return false;
    }

    if (iter->second.size() > 1) {
        if (relation_name.empty()) {
            LOG(WARNING) << "Column: '" + col_name +
                                "' in field list is ambiguous";
            return false;
        } else {
            *info = nullptr;
            for (auto col_ctx_id : iter->second) {
                if (col_ctx_id == table_ctx_id) {
                    *info = &row_schema_info_list_[col_ctx_id];
                    return true;
                }
            }
            LOG(WARNING) << "Unknow Column: ' " + col_name + "'  in '" +
                                relation_name + "'";
            return false;
        }
    } else {
        uint32_t col_context_id = iter->second[0];
        if (!relation_name.empty()) {
            if (table_ctx_id != col_context_id) {
                LOG(WARNING) << "Unknow Column: ' " + col_name + "'  in '" +
                                    relation_name + "'";
                return false;
            }
        }
        *info = &row_schema_info_list_[col_context_id];
        return true;
    }
}

const std::string SchemasContext::SourceColumnNameResolved(
    node::ColumnRefNode* column) {
    std::string column_name = column->GetColumnName();
    if (!Empty()) {
        auto source = ColumnSourceResolved(column->GetRelationName(),
                                           column->GetColumnName());
        if (vm::kSourceColumn == source.type()) {
            column_name = row_schema_info_list_[source.schema_idx()]
                              .sources_->at(source.column_idx())
                              .column_name();
        }
    }
    return column_name;
}
vm::ColumnSource SchemasContext::ColumnSourceResolved(
    const std::string& relation_name, const std::string& col_name) const {
    const RowSchemaInfo* row_schema_info;
    if (!ColumnRefResolved(relation_name, col_name, &row_schema_info)) {
        LOG(WARNING) << "Resolve column expression failed";
        return ColumnSource();
    }
    int32_t column_idx =
        ColumnIndexResolved(col_name, row_schema_info->schema_);
    if (-1 == column_idx) {
        return ColumnSource();
    }
    return ColumnSource(row_schema_info->idx_, column_idx, col_name);
}

int32_t SchemasContext::ColumnIndexResolved(const std::string& column,
                                            const Schema* schema) const {
    int32_t column_idx = -1;
    for (int i = 0; i < schema->size(); ++i) {
        if (schema->Get(i).name() == column) {
            column_idx = i;
            break;
        }
    }
    return column_idx;
}

int32_t SchemasContext::ColumnOffsetResolved(
    const std::string& relation_name, const std::string& col_name) const {
    const RowSchemaInfo* row_schema_info;
    if (!ColumnRefResolved(relation_name, col_name, &row_schema_info)) {
        LOG(WARNING) << "Resolve column expression failed";
        return -1;
    }

    int32_t column_index =
        ColumnIndexResolved(col_name, row_schema_info->schema_);
    if (-1 == column_index) {
        return -1;
    }
    int offset = column_index;
    for (uint32_t i = 0; i < row_schema_info->idx_; ++i) {
        offset += this->row_schema_info_list_[i].schema_->size();
    }
    return offset;
}

}  // namespace vm
}  // namespace fesql
