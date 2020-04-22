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
    const vm::NameSchemaList& table_schema_list) {
    uint32_t idx = 0;
    for (auto iter = table_schema_list.cbegin();
         iter != table_schema_list.cend(); iter++) {
        RowSchemaInfo info = {
            .idx_ = idx, .table_name_ = iter->first, .schema_ = iter->second};
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
bool SchemasContext::ExprListResolved(std::vector<node::ExprNode*> expr_list,
                                      const RowSchemaInfo** info) const {
    if (expr_list.empty()) {
        *info = nullptr;
        return true;
    }
    std::set<const RowSchemaInfo*> infos;
    for (auto expr : expr_list) {
        const RowSchemaInfo* info = nullptr;
        if (!ExprRefResolved(expr, &info)) {
            return false;
        }
        if (nullptr != info) {
            infos.insert(info);
        }
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
            return ExprListResolved(expr_list, info);
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
            return ExprListResolved(expr_list, info);
        }
        default: {
            return ExprListResolved(expr->children_, info);
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

}  // namespace vm
}  // namespace fesql
