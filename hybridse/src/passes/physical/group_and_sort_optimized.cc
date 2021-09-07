/*
 * Copyright 021 4paradigm
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
#include "passes/physical/group_and_sort_optimized.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

using hybridse::vm::DataProviderType;
using hybridse::vm::INVALID_POS;
using hybridse::vm::PhysicalDataProviderNode;
using hybridse::vm::PhysicalFilterNode;
using hybridse::vm::PhysicalGroupNode;
using hybridse::vm::PhysicalJoinNode;
using hybridse::vm::PhysicalOpType;
using hybridse::vm::PhysicalPartitionProviderNode;
using hybridse::vm::PhysicalProjectNode;
using hybridse::vm::PhysicalRenameNode;
using hybridse::vm::PhysicalRequestJoinNode;
using hybridse::vm::PhysicalRequestUnionNode;
using hybridse::vm::PhysicalSimpleProjectNode;
using hybridse::vm::PhysicalWindowAggrerationNode;
using hybridse::vm::ProjectType;

static bool ResolveColumnToSourceColumnName(const node::ColumnRefNode* col,
                                            const SchemasContext* schemas_ctx,
                                            std::string* source_name);

bool GroupAndSortOptimized::Transform(PhysicalOpNode* in,
                                      PhysicalOpNode** output) {
    *output = in;
    switch (in->GetOpType()) {
        case PhysicalOpType::kPhysicalOpGroupBy: {
            PhysicalGroupNode* group_op = dynamic_cast<PhysicalGroupNode*>(in);
            PhysicalOpNode* new_producer;
            if (!GroupOptimized(group_op->schemas_ctx(),
                                group_op->GetProducer(0), &group_op->group_,
                                &new_producer)) {
                return false;
            }
            if (!ResetProducer(plan_ctx_, group_op, 0, new_producer)) {
                return false;
            }
            if (!group_op->Valid()) {
                *output = group_op->producers()[0];
            }
            return true;
        }
        case PhysicalOpType::kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
            if (ProjectType::kWindowAggregation == project_op->project_type_) {
                auto window_agg_op =
                    dynamic_cast<PhysicalWindowAggrerationNode*>(project_op);
                PhysicalOpNode* input = window_agg_op->GetProducer(0);

                PhysicalOpNode* new_producer;
                if (!window_agg_op->instance_not_in_window()) {
                    if (KeyAndOrderOptimized(input->schemas_ctx(), input,
                                             &window_agg_op->window_.partition_,
                                             &window_agg_op->window_.sort_,
                                             &new_producer)) {
                        input = new_producer;
                        if (!ResetProducer(plan_ctx_, window_agg_op, 0,
                                           input)) {
                            return false;
                        }
                    }
                }
                // must prepare for window join column infer
                auto& window_joins = window_agg_op->window_joins();
                auto& window_unions = window_agg_op->window_unions();
                window_agg_op->InitJoinList(plan_ctx_);
                auto& joined_op_list_ = window_agg_op->joined_op_list_;
                if (!window_joins.Empty()) {
                    size_t join_idx = 0;
                    for (auto& window_join : window_joins.window_joins()) {
                        PhysicalOpNode* cur_joined = joined_op_list_[join_idx];

                        PhysicalOpNode* new_join_right;
                        if (JoinKeysOptimized(
                                cur_joined->schemas_ctx(), window_join.first,
                                &window_join.second, &new_join_right)) {
                            window_join.first = new_join_right;
                        }
                        join_idx += 1;
                    }
                }
                if (!window_unions.Empty()) {
                    for (auto& window_union : window_unions.window_unions_) {
                        PhysicalOpNode* new_producer;
                        if (KeyAndOrderOptimized(
                                window_union.first->schemas_ctx(),
                                window_union.first,
                                &window_union.second.partition_,
                                &window_union.second.sort_, &new_producer)) {
                            window_union.first = new_producer;
                        }
                    }
                }
                return true;
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpRequestUnion: {
            PhysicalRequestUnionNode* union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(in);
            PhysicalOpNode* new_producer;

            if (!union_op->instance_not_in_window()) {
                if (KeysAndOrderFilterOptimized(
                        union_op->schemas_ctx(), union_op->GetProducer(1),
                        &union_op->window_.partition_,
                        &union_op->window_.index_key_, &union_op->window_.sort_,
                        &new_producer)) {
                    if (!ResetProducer(plan_ctx_, union_op, 1, new_producer)) {
                        return false;
                    }
                }
            }

            if (!union_op->window_unions().Empty()) {
                for (auto& window_union :
                     union_op->window_unions_.window_unions_) {
                    PhysicalOpNode* new_producer;
                    auto& window = window_union.second;
                    if (KeysAndOrderFilterOptimized(
                            window_union.first->schemas_ctx(),
                            window_union.first, &window.partition_,
                            &window.index_key_, &window.sort_, &new_producer)) {
                        window_union.first = new_producer;
                    }
                }
            }
            return true;
        }
        case PhysicalOpType::kPhysicalOpRequestJoin: {
            PhysicalRequestJoinNode* join_op =
                dynamic_cast<PhysicalRequestJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (!JoinKeysOptimized(join_op->schemas_ctx(),
                                   join_op->GetProducer(1), &join_op->join_,
                                   &new_producer)) {
                return false;
            }
            if (!ResetProducer(plan_ctx_, join_op, 1, new_producer)) {
                return false;
            }

            return true;
        }
        case PhysicalOpType::kPhysicalOpJoin: {
            PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (!JoinKeysOptimized(join_op->schemas_ctx(),
                                   join_op->GetProducer(1), &join_op->join_,
                                   &new_producer)) {
                return false;
            }
            if (!ResetProducer(plan_ctx_, join_op, 1, new_producer)) {
                return false;
            }
            return true;
        }
        case PhysicalOpType::kPhysicalOpFilter: {
            PhysicalFilterNode* filter_op =
                dynamic_cast<PhysicalFilterNode*>(in);
            PhysicalOpNode* new_producer;
            if (FilterOptimized(filter_op->schemas_ctx(),
                                filter_op->GetProducer(0), &filter_op->filter_,
                                &new_producer)) {
                if (!ResetProducer(plan_ctx_, filter_op, 0, new_producer)) {
                    return false;
                }
            }
        }
        default: {
            return false;
        }
    }
    return false;
}

/**
 * optimize keys on condition. Remove keys from upper node if key match indexes
 * defined in table schema `left_key` & `index_key` is required, `right_key` is
 * optional
 * if `right_key` is not nullptr:
 *   - `left_key`, `index_key`, `right_key` corresponding to
 *     `Filter::left_key_`, `Filter::index_key_`, `Filter::right_key_`
 * otherwise:
 *   - `left_key`, `index_key` corresponding to Key group & Key hash
 */
bool GroupAndSortOptimized::KeysOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* left_key,
    Key* index_key, Key* right_key, Sort* sort, PhysicalOpNode** new_in) {
    if (nullptr == left_key || nullptr == index_key || !left_key->ValidKey()) {
        return false;
    }

    if (right_key != nullptr && !right_key->ValidKey()) {
        return false;
    }

    if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        // Do not optimized with Request DataProvider (no index has been provided)
        if (DataProviderType::kProviderTypeRequest == scan_op->provider_type_) {
            return false;
        }

        if (DataProviderType::kProviderTypeTable == scan_op->provider_type_ ||
            DataProviderType::kProviderTypePartition == scan_op->provider_type_) {
            const node::ExprListNode* right_partition =
                right_key == nullptr ? left_key->keys() : right_key->keys();

            size_t key_num = right_partition->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            node::ExprListNode order_values;

            PhysicalPartitionProviderNode* partition_op = nullptr;
            std::string index_name;
            if (DataProviderType::kProviderTypeTable == scan_op->provider_type_) {
                // Apply key columns and order column optimization with all indexes binding to scan_op->table_handler_
                // Return false if fail to find an appropriate index
                if (!TransformKeysAndOrderExpr(root_schemas_ctx, right_partition,
                                               nullptr == sort ? nullptr : sort->orders_, scan_op->table_handler_,
                                               &index_name, &bitmap)) {
                    return false;
                }
                Status status = plan_ctx_->CreateOp<PhysicalPartitionProviderNode>(&partition_op, scan_op, index_name);
                if (!status.isOK()) {
                    LOG(WARNING) << "Fail to create partition op: " << status;
                    return false;
                }
            } else {
                partition_op = dynamic_cast<PhysicalPartitionProviderNode*>(scan_op);
                index_name = partition_op->index_name_;
                // Apply key columns and order column optimization with given index name
                // Return false if given index do not match the keys and order column
                if (!TransformKeysAndOrderExpr(root_schemas_ctx, right_partition,
                                               nullptr == sort ? nullptr : sort->orders_, scan_op->table_handler_,
                                               &index_name, &bitmap)) {
                    return false;
                }
            }


            auto new_left_keys = node_manager_->MakeExprList();
            auto new_right_keys = node_manager_->MakeExprList();
            auto new_index_keys = node_manager_->MakeExprList();
            for (size_t i = 0; i < bitmap.size(); ++i) {
                auto left = left_key->keys()->GetChild(i);
                if (bitmap[i]) {
                    new_index_keys->AddChild(left);
                } else {
                    new_left_keys->AddChild(left);
                    if (right_key != nullptr) {
                        new_right_keys->AddChild(
                            right_key->keys()->GetChild(i));
                    }
                }
            }
            if (right_key != nullptr) {
                right_key->set_keys(new_right_keys);
            }
            index_key->set_keys(new_index_keys);
            left_key->set_keys(new_left_keys);
            // Clear order expr list if we optimized orders
            if (nullptr != sort && nullptr != sort->orders_ && nullptr != sort->orders_->GetOrderExpression(0)) {
                auto first_order_expression = sort->orders_->GetOrderExpression(0);
                sort->set_orders(dynamic_cast<node::OrderByNode*>(
                    node_manager_->MakeOrderByNode(
                        node_manager_->MakeExprList(
                        node_manager_->MakeOrderExpression(nullptr, first_order_expression->is_asc())))));
            }
            *new_in = partition_op;
            return true;
        }
    } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!KeysOptimized(root_schemas_ctx, simple_project->producers()[0],
                           left_key, index_key, right_key, sort, &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* new_simple_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(
            &new_simple_op, new_depend, simple_project->project());
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create simple project op: " << status;
            return false;
        }
        *new_in = new_simple_op;
        return true;
    } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        if (!KeysOptimized(root_schemas_ctx, in->producers()[0], left_key,
                           index_key, right_key, sort, &new_depend)) {
            return false;
        }
        PhysicalRenameNode* new_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalRenameNode>(
            &new_op, new_depend, dynamic_cast<PhysicalRenameNode*>(in)->name_);
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create rename op: " << status;
            return false;
        }
        *new_in = new_op;
        return true;
    }
    return false;
}

bool GroupAndSortOptimized::KeysFilterOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
    Key* hash, PhysicalOpNode** new_in) {
    return KeysAndOrderFilterOptimized(root_schemas_ctx, in, group, hash,
                                       nullptr, new_in);
}
bool GroupAndSortOptimized::KeysAndOrderFilterOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
    Key* hash, Sort* sort, PhysicalOpNode** new_in) {
    return KeysOptimized(root_schemas_ctx, in, group, hash, nullptr, sort,
                         new_in);
}

bool GroupAndSortOptimized::JoinKeysOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Join* join,
    PhysicalOpNode** new_in) {
    if (nullptr == join) {
        return false;
    }
    return FilterAndOrderOptimized(root_schemas_ctx, in, join,
                                   &join->right_sort_, new_in);
}

bool GroupAndSortOptimized::FilterOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
    PhysicalOpNode** new_in) {
    return FilterAndOrderOptimized(root_schemas_ctx, in, filter, nullptr,
                                   new_in);
}
bool GroupAndSortOptimized::FilterAndOrderOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
    Sort* sort, PhysicalOpNode** new_in) {
    return KeysOptimized(root_schemas_ctx, in, &filter->left_key_,
                         &filter->index_key_, &filter->right_key_, sort,
                         new_in);
}

bool GroupAndSortOptimized::GroupOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
    PhysicalOpNode** new_in) {
    return KeyAndOrderOptimized(root_schemas_ctx, in, group, nullptr, new_in);
}
bool GroupAndSortOptimized::KeyAndOrderOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
    Sort* sort, PhysicalOpNode** new_in) {
    Key mock_key;
    return KeysAndOrderFilterOptimized(root_schemas_ctx, in, group, &mock_key,
                                       sort, new_in);
}

bool GroupAndSortOptimized::SortOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Sort* sort) {
    if (nullptr == sort) {
        return false;
    }
    if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (DataProviderType::kProviderTypePartition !=
            scan_op->provider_type_) {
            return false;
        }
        auto partition_provider =
            dynamic_cast<PhysicalPartitionProviderNode*>(scan_op);
        const node::OrderByNode* new_orders = nullptr;

        auto& index_hint = partition_provider->table_handler_->GetIndex();
        std::string index_name = partition_provider->index_name_;
        auto index_st = index_hint.at(index_name);
        TransformOrderExpr(root_schemas_ctx, sort->orders(),
                           *(scan_op->table_handler_->GetSchema()), index_st,
                           &new_orders);
        sort->set_orders(new_orders);
        return true;
    } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        return SortOptimized(root_schemas_ctx, simple_project->producers()[0],
                             sort);
    } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
        return SortOptimized(root_schemas_ctx, in->producers()[0], sort);
    }
    return false;
}

bool GroupAndSortOptimized::TransformGroupExpr(
    const SchemasContext* root_schemas_ctx, const node::ExprListNode* groups,
    std::shared_ptr<TableHandler> table_handler, std::string* index_name,
    std::vector<bool>* output_bitmap) {
    return TransformKeysAndOrderExpr(root_schemas_ctx, groups, nullptr,
                                     table_handler, index_name, output_bitmap);
}
bool GroupAndSortOptimized::TransformKeysAndOrderExpr(
    const SchemasContext* root_schemas_ctx, const node::ExprListNode* groups,
    const node::OrderByNode* order, std::shared_ptr<TableHandler> table_handler,
    std::string* index_name, std::vector<bool>* output_bitmap) {
    if (nullptr == groups || nullptr == output_bitmap ||
        nullptr == index_name) {
        DLOG(WARNING) << "fail to transform keys expr : key expr or output "
                         "or index_name ptr is null";
        return false;
    }

    if (nullptr == order) {
        DLOG(INFO) << "keys optimized: " << node::ExprString(groups);
    } else {
        DLOG(INFO) << "keys and order optimized: keys="
                   << node::ExprString(groups)
                   << ", order=" << node::ExprString(order);
    }
    std::vector<std::string> columns;
    std::vector<std::string> order_columns;
    std::map<size_t, size_t> result_bitmap_mapping;

    for (size_t i = 0; i < groups->children_.size(); ++i) {
        auto group = groups->children_[i];
        switch (group->expr_type_) {
            case node::kExprColumnRef: {
                auto column = dynamic_cast<node::ColumnRefNode*>(group);
                std::string source_column_name;
                if (!ResolveColumnToSourceColumnName(column, root_schemas_ctx,
                                                     &source_column_name)) {
                    return false;
                }
                result_bitmap_mapping[columns.size()] = i;
                columns.push_back(source_column_name);
                break;
            }
            default: {
                break;
            }
        }
    }

    if (nullptr != order) {
        for (size_t i = 0; i < order->order_expressions()->GetChildNum(); ++i) {
            auto expr = order->GetOrderExpressionExpr(i);
            if (nullptr != expr && expr->GetExprType() == node::kExprColumnRef) {
                auto column = dynamic_cast<const node::ColumnRefNode*>(expr);
                std::string source_column_name;
                if (!ResolveColumnToSourceColumnName(column, root_schemas_ctx,
                                                     &source_column_name)) {
                    return false;
                }
                order_columns.push_back(source_column_name);
            }
        }
    }
    if (columns.empty()) {
        return false;
    }

    std::vector<bool> match_bitmap;
    std::vector<bool> state_bitmap(columns.size(), true);
    if (!MatchBestIndex(columns, order_columns, table_handler, &state_bitmap,
                        index_name, &match_bitmap)) {
        return false;
    }
    if (match_bitmap.size() != columns.size()) {
        return false;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        if (match_bitmap[i]) {
            size_t origin_idx = result_bitmap_mapping[i];
            (*output_bitmap)[origin_idx] = true;
        }
    }
    return true;
}

// When *index_name is empty, return true if we can find the best index for key columns and order column
// When *index_name isn't empty, return true if the given index_name match key columns and order column
bool GroupAndSortOptimized::MatchBestIndex(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& order_columns,
    std::shared_ptr<TableHandler> table_handler, std::vector<bool>* bitmap_ptr,
    std::string* index_name, std::vector<bool>* index_bitmap) {
    if (nullptr == bitmap_ptr || nullptr == index_name) {
        LOG(WARNING)
            << "fail to match best index: bitmap or index_name ptr is null";
        return false;
    }

    if (!table_handler) {
        LOG(WARNING) << "fail to match best index: table is null";
        return false;
    }
    auto& index_hint = table_handler->GetIndex();
    auto schema = table_handler->GetSchema();
    if (nullptr == schema) {
        LOG(WARNING) << "fail to match best index: table schema null";
        return false;
    }

    std::set<std::string> column_set;
    auto& bitmap = *bitmap_ptr;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (bitmap[i]) {
            column_set.insert(columns[i]);
        }
    }

    if (order_columns.size() > 1) {
        LOG(WARNING) << "fail to match best index: non-support multi ts index";
        return false;
    }
    // Go through the all indexs to find out index meet the requirements.
    // Notice: only deal with index specific by given index name when (*index_name) is non-emtpy
    for (auto iter = index_hint.cbegin(); iter != index_hint.cend(); iter++) {
        IndexSt index = iter->second;

        if (!(*index_name).empty() && index.name != *index_name) {
            // if (*index_name) isn't empty
            // skip index whose index.name != given index_name
            continue;
        }
        if (!order_columns.empty()) {
            if (index.ts_pos == INVALID_POS) {
                continue;
            }
            auto& ts_column = schema->Get(index.ts_pos);
            if (ts_column.name() != order_columns[0]) {
                continue;
            }
        }
        std::set<std::string> keys;
        for (auto key_iter = index.keys.cbegin(); key_iter != index.keys.cend();
             key_iter++) {
            keys.insert(key_iter->name);
        }
        if (column_set == keys) {
            *index_name = index.name;
            *index_bitmap = bitmap;
            return true;
        }
    }

    std::string best_index_name;
    std::vector<bool> best_index_bitmap;

    bool succ = false;
    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (bitmap[i]) {
            bitmap[i] = false;
            std::string name;
            std::vector<bool> sub_best_bitmap;
            if (MatchBestIndex(columns, order_columns, table_handler,
                               bitmap_ptr, &name, &sub_best_bitmap)) {
                succ = true;
                if (best_index_name.empty()) {
                    best_index_name = name;
                    best_index_bitmap = sub_best_bitmap;
                } else {
                    auto org_index = index_hint.at(best_index_name);
                    auto new_index = index_hint.at(name);
                    if (org_index.keys.size() < new_index.keys.size()) {
                        best_index_name = name;
                        best_index_bitmap = sub_best_bitmap;
                    }
                }
            }
            bitmap[i] = true;
        }
    }
    *index_name = best_index_name;
    *index_bitmap = best_index_bitmap;
    return succ;
}

bool GroupAndSortOptimized::TransformOrderExpr(
    const SchemasContext* schemas_ctx, const node::OrderByNode* order,
    const Schema& schema, const IndexSt& index_st,
    const node::OrderByNode** output) {
    *output = order;
    if (nullptr == order || nullptr == output) {
        DLOG(WARNING)
            << "fail to optimize order expr : order expr or output is null";
        return false;
    }
    if (index_st.ts_pos == INVALID_POS) {
        DLOG(WARNING) << "not set ts col";
        return false;
    }
    auto& ts_column = schema.Get(index_st.ts_pos);
    *output = order;
    int succ_match = -1;
    for (size_t i = 0; i < order->order_expressions()->GetChildNum(); ++i) {
        auto expr = order->GetOrderExpressionExpr(i);
        if (nullptr != expr && expr->GetExprType() == node::kExprColumnRef) {
            auto column = dynamic_cast<const node::ColumnRefNode*>(expr);
            std::string source_column_name;
            if (ResolveColumnToSourceColumnName(column, schemas_ctx,
                                                &source_column_name)) {
                if (ts_column.name() == source_column_name) {
                    succ_match = i;
                    break;
                }
            }
        }
    }
    if (succ_match >= 0) {
        node::ExprListNode* expr_list = node_manager_->MakeExprList();
        for (size_t i = 0; i < order->order_expressions()->GetChildNum(); ++i) {
            if (static_cast<size_t>(succ_match) != i) {
                expr_list->AddChild(order->order_expressions()->GetChild(i));
            }
        }
        *output = dynamic_cast<node::OrderByNode*>(
            node_manager_->MakeOrderByNode(expr_list));
        return true;
    } else {
        return false;
    }
}

/**
 * Resolve column reference to possible source table's column name
 */
static bool ResolveColumnToSourceColumnName(const node::ColumnRefNode* col,
                                            const SchemasContext* schemas_ctx,
                                            std::string* source_name) {
    // use detailed column resolve utility
    size_t column_id;
    int path_idx;
    size_t child_column_id;
    size_t source_column_id;
    const PhysicalOpNode* source;
    Status status = schemas_ctx->ResolveColumnID(
        col->GetRelationName(), col->GetColumnName(), &column_id, &path_idx,
        &child_column_id, &source_column_id, &source);

    // try loose the relation
    if (!status.isOK() && !col->GetRelationName().empty()) {
        status = schemas_ctx->ResolveColumnID(
            "", col->GetColumnName(), &column_id, &path_idx, &child_column_id,
            &source_column_id, &source);
    }

    if (!status.isOK()) {
        LOG(WARNING) << "Illegal index column: " << col->GetExprString();
        return false;
    }
    if (source == nullptr ||
        source->GetOpType() != PhysicalOpType::kPhysicalOpDataProvider) {
        LOG(WARNING) << "Index column is not from any source table: "
                     << col->GetExprString();
        return false;
    }
    status = source->schemas_ctx()->ResolveColumnNameByID(source_column_id,
                                                          source_name);
    if (!status.isOK()) {
        LOG(WARNING) << "Illegal source column id #" << source_column_id
                     << " for index column " << col->GetExprString();
        return false;
    }
    return true;
}

}  // namespace passes
}  // namespace hybridse
