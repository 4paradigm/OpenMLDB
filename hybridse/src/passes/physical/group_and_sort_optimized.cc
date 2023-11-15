/*
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
#include "passes/physical/group_and_sort_optimized.h"

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "node/node_enum.h"
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
using hybridse::vm::PhysicalRequestAggUnionNode;
using hybridse::vm::PhysicalSimpleProjectNode;
using hybridse::vm::PhysicalWindowAggrerationNode;
using hybridse::vm::ProjectType;

template <typename T>
T* ShadowCopy(T* in, std::vector<T>* ct) {
    if (in != nullptr) {
        auto copy = in->ShadowCopy();
        ct->push_back(copy);
        return &ct->back();
    }
    return nullptr;
}

// ExprNode may be resolving under different SchemasContext later (say one of its descendants context),
// with column name etc it may not able to resvole since a column rename may happen in SimpleProject node.
// With the column id hint written to corresponding ColumnRefNode earlier, resolving issue can be mitigated.
absl::Status GroupAndSortOptimized::BuildExprCache(const node::ExprNode* node, const SchemasContext* sc) {
    if (node == nullptr) {
        return {};
    }

    switch (node->GetExprType()) {
        case node::kExprColumnRef: {
            auto ref = dynamic_cast<const node::ColumnRefNode*>(node);

            auto& ref_map = expr_cache_[ref];

            auto db = ref->GetDBName();
            auto rel = ref->GetRelationName();
            auto col_name = ref->GetColumnName();
            size_t column_id;
            Status status = sc->ResolveColumnID(db, rel, col_name, &column_id);
            if (!status.isOK()) {
                return absl::NotFoundError(absl::StrCat("Illegal index column: ", ref->GetExprString()));
            }

            auto descendants = sc->GetRoot()->TraceLastDescendants(column_id);
            if (!descendants.ok()) {
                return descendants.status();
            }

            for (auto& entry : descendants.value()) {
                if (entry.first->GetOpType() != vm::kPhysicalOpDataProvider) {
                    continue;
                }
                auto* data_node = dynamic_cast<const vm::PhysicalDataProviderNode*>(entry.first);
                std::string source_col, source_table, source_db;
                auto s = data_node->schemas_ctx()->ResolveDbTableColumnByID(entry.second, &source_db, &source_table,
                                                                              &source_col);
                if (!s.isOK()) {
                    return absl::NotFoundError(s.msg);
                }
                if (ref_map.find(data_node) != ref_map.end()) {
                    return absl::AlreadyExistsError(
                        absl::StrCat("node ", entry.first->GetTreeString(), " already exists"));
                }
                ref_map[data_node] = {source_col, source_table, source_db};
            }
            break;
        }
        default:
            break;
    }

    for (uint32_t i = 0; i < node->GetChildNum(); ++i) {
        auto s = BuildExprCache(node->GetChild(i), sc);
        if (!s.ok()) {
            return s;
        }
    }
    return {};
}

bool GroupAndSortOptimized::Transform(PhysicalOpNode* in,
                                      PhysicalOpNode** output) {
    *output = in;
    TransformCxtGuard<decltype(ctx_)> guard(&ctx_, KeysInfo(in->GetOpType(), nullptr, nullptr, nullptr, nullptr));
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
            PhysicalRequestUnionNode* union_op = dynamic_cast<PhysicalRequestUnionNode*>(in);
            PhysicalOpNode* new_producer;

            if (!union_op->instance_not_in_window()) {
                if (KeysAndOrderFilterOptimized(union_op->GetProducer(1)->schemas_ctx(), union_op->GetProducer(1),
                                                &union_op->window_.partition_, &union_op->window_.index_key_,
                                                &union_op->window_.sort_, &new_producer)) {
                    if (!ResetProducer(plan_ctx_, union_op, 1, new_producer)) {
                        return false;
                    }
                }
            }

            if (!union_op->window_unions().Empty()) {
                for (auto& window_union :
                     union_op->window_unions_.window_unions_) {
                    PhysicalOpNode* new_producer = nullptr;
                    // 1. optimize it self (e.g Join(t1, t2) can optimize t2 based on join condition)
                    if (Apply(window_union.first, &new_producer) && new_producer != nullptr) {
                        window_union.first = new_producer;
                    }
                    // 2. optimize based on window definition
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
        case PhysicalOpType::kPhysicalOpRequestAggUnion: {
            PhysicalRequestAggUnionNode* union_op = dynamic_cast<PhysicalRequestAggUnionNode*>(in);
            PhysicalOpNode* new_producer;

            if (!union_op->instance_not_in_window()) {
                if (KeysAndOrderFilterOptimized(
                        union_op->GetProducer(1)->schemas_ctx(), union_op->GetProducer(1),
                        &union_op->window_.partition_,
                        &union_op->window_.index_key_, &union_op->window_.sort_,
                        &new_producer)) {
                    if (!ResetProducer(plan_ctx_, union_op, 1, new_producer)) {
                        return false;
                    }
                }

                if (KeysAndOrderFilterOptimized(
                        union_op->GetProducer(2)->schemas_ctx(), union_op->GetProducer(2),
                        &union_op->agg_window_.partition_,
                        &union_op->agg_window_.index_key_, &union_op->agg_window_.sort_,
                        &new_producer)) {
                    if (!ResetProducer(plan_ctx_, union_op, 2, new_producer)) {
                        return false;
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

bool GroupAndSortOptimized::KeysOptimized(const vm::SchemasContext* root_schemas_ctx,
                                          PhysicalOpNode* in,
                                          Key* left_key,
                                          Key* index_key,
                                          Key* right_key,
                                          Sort* sort,
                                          PhysicalOpNode** new_in) {
    if (nullptr == left_key || nullptr == index_key || !left_key->ValidKey()) {
        return false;
    }

    if (right_key != nullptr && !right_key->ValidKey()) {
        return false;
    }

    absl::Cleanup clean = [&]() {
        expr_cache_.clear();
        optimize_info_ = nullptr;
    };

    auto s = BuildExprCache(index_key->keys(), root_schemas_ctx);
    if (!s.ok()) {
        LOG(WARNING) << s;
        return false;
    }
    if (right_key != nullptr) {
        s = BuildExprCache(right_key->keys(), root_schemas_ctx);
        if (!s.ok()) {
            LOG(WARNING) << s;
            return false;
        }
    } else {
        // build cache from left only right_key is empty
        auto s = BuildExprCache(left_key->keys(), root_schemas_ctx);
        if (!s.ok()) {
            LOG(WARNING) << s;
            return false;
        }
    }
    if (sort != nullptr) {
        s = BuildExprCache(sort->orders(), root_schemas_ctx);
        if (!s.ok()) {
            LOG(WARNING) << s;
            return false;
        }
    }
    return KeysOptimizedImpl(root_schemas_ctx, in, left_key, index_key, right_key, sort, new_in);
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
bool GroupAndSortOptimized::KeysOptimizedImpl(const SchemasContext* root_schemas_ctx,
                                          PhysicalOpNode* in,
                                          Key* left_key,
                                          Key* index_key,
                                          Key* right_key,
                                          Sort* sort,
                                          PhysicalOpNode** new_in) {
    TransformCxtGuard<decltype(ctx_)> guard(&ctx_, KeysInfo(in->GetOpType(), left_key, right_key, index_key, sort));

    if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        // Do not optimize with Request DataProvider (no index has been provided)
        if (DataProviderType::kProviderTypeRequest == scan_op->provider_type_) {
            return false;
        }

        if (DataProviderType::kProviderTypeTable == scan_op->provider_type_ ||
            DataProviderType::kProviderTypePartition == scan_op->provider_type_) {
            if (optimize_info_) {
                if (optimize_info_->left_key == left_key && optimize_info_->index_key == index_key &&
                    optimize_info_->right_key == right_key && optimize_info_->sort_key == sort) {
                    if (optimize_info_->optimized != nullptr &&
                        scan_op->GetDb() == optimize_info_->optimized->GetDb() &&
                        scan_op->GetName() == optimize_info_->optimized->GetName()) {
                        *new_in = optimize_info_->optimized;
                        return true;
                    }
                }
            }
            const node::ExprListNode* right_partition =
                right_key == nullptr ? left_key->keys() : right_key->keys();

            IndexBitMap bitmap((std::vector<std::optional<ColIndexInfo>>(right_partition->GetChildNum())));
            PhysicalPartitionProviderNode* partition_op = nullptr;
            std::string index_name;

            if (DataProviderType::kProviderTypeTable == scan_op->provider_type_) {
                // Apply key columns and order column optimization with all indexes binding to scan_op->table_handler_
                // Return false if fail to find an appropriate index
                if (!TransformKeysAndOrderExpr( right_partition,
                                               nullptr == sort ? nullptr : sort->orders_, scan_op,
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
                if (!TransformKeysAndOrderExpr( right_partition,
                                               nullptr == sort ? nullptr : sort->orders_, scan_op,
                                               &index_name, &bitmap)) {
                    return false;
                }
            }

            auto new_left_keys = node_manager_->MakeExprList();
            auto new_right_keys = node_manager_->MakeExprList();
            auto new_index_keys = node_manager_->MakeExprList();

            new_index_keys->children_.resize(bitmap.refered_index_key_count);
            for (size_t i = 0; i < bitmap.bitmap.size(); ++i) {
                auto left = left_key->keys()->GetChild(i);
                if (bitmap.bitmap[i].has_value()) {
                    // reorder index keys to the index definition order, for the runner to correctly filter rows if
                    // condtion keys has different order to index keys
                    new_index_keys->SetChild(bitmap.bitmap[i].value().index, left);
                } else {
                    new_left_keys->AddChild(left);
                    if (right_key != nullptr) {
                        new_right_keys->AddChild(
                            right_key->keys()->GetChild(i));
                    }
                }
            }

            for (auto expr : new_index_keys->children_) {
                DCHECK(expr != nullptr);
            }

            // write new keys
            // FIXME:(#2457) last join (filter op<optimized>) not supported in iterator
            bool has_filter = false;
            bool has_join = false;
            Sort* join_sort = nullptr;
            for (auto it = ctx_.rbegin(); it != ctx_.rend(); ++it) {
                switch (it->type) {
                    case vm::kPhysicalOpJoin:
                    case vm::kPhysicalOpRequestJoin: {
                        if (has_filter) {
                            has_join = true;
                            join_sort = it->right_sort;
                        }
                        break;
                    }
                    case vm::kPhysicalOpFilter: {
                        has_filter = true;
                        break;
                    }
                    default:
                        break;
                }
            }
            bool support_opt = !(has_filter && has_join);
            if (scan_op->provider_type_ == vm::kProviderTypeTable || support_opt) {
                // key update is skipped if optimized scan op already and ctx size >= 2

                // consider the case when a REQUEST_JOIN(, FILTER(DATA)) tree, both REQUEST_JOIN and FILTER node
                // can optimize DATA node, if the FILTER node optimzed data node already, request join node
                // should be aware of the optimization

                if (right_key != nullptr) {
                    right_key->set_keys(new_right_keys);
                }
                index_key->set_keys(new_index_keys);
                left_key->set_keys(new_left_keys);
            }

            // Clear order expr list if we optimized orders
            auto* mut_sort = sort;
            if (mut_sort == nullptr) {
                mut_sort = join_sort;
            }
            if (nullptr != mut_sort && nullptr != mut_sort->orders_ &&
                nullptr != mut_sort->orders_->GetOrderExpression(0)) {
                auto first_order_expression = mut_sort->orders_->GetOrderExpression(0);
                mut_sort->set_orders(
                    dynamic_cast<node::OrderByNode*>(node_manager_->MakeOrderByNode(node_manager_->MakeExprList(
                        node_manager_->MakeOrderExpression(nullptr, first_order_expression->is_asc())))));
            }

            optimize_info_.reset(new OptimizeInfo(left_key, index_key, right_key, sort, partition_op));
            *new_in = partition_op;
            return true;
        }
    } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        if (!KeysOptimizedImpl(in->GetProducer(0)->schemas_ctx(), in->GetProducer(0), left_key, index_key, right_key,
                               sort, &new_depend)) {
            return false;
        }

        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalSimpleProjectNode* new_simple_op = nullptr;
        Status status =
            plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(&new_simple_op, new_depend, simple_project->project());
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create simple project op: " << status;
            return false;
        }
        *new_in = new_simple_op;
        return true;
    } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        if (!KeysOptimizedImpl(in->GetProducer(0)->schemas_ctx(), in->producers()[0], left_key,
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
    } else if (PhysicalOpType::kPhysicalOpFilter == in->GetOpType()) {
        // respect filter's optimize result, try optimize only if not optimized
        PhysicalFilterNode* filter_op = dynamic_cast<PhysicalFilterNode*>(in);

        PhysicalOpNode* new_depend;
        if (!KeysOptimizedImpl(root_schemas_ctx, in->producers()[0], left_key, index_key, right_key, sort,
                               &new_depend)) {
            return false;
        }
        PhysicalFilterNode* new_filter = nullptr;
        auto status = plan_ctx_->CreateOp<PhysicalFilterNode>(&new_filter, new_depend,
                                                              filter_op->filter());
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create filter op: " << status;
            return false;
        }
        *new_in = new_filter;
        return true;
    } else if (PhysicalOpType::kPhysicalOpRequestJoin == in->GetOpType()) {
        PhysicalRequestJoinNode* request_join = dynamic_cast<PhysicalRequestJoinNode*>(in);
        // try optimze left source of request join with window definition
        // window partition by and order by columns must refer to the left most table only
        PhysicalOpNode* new_depend = nullptr;
        auto* rebase_sc = in->GetProducer(0)->schemas_ctx();
        if (!KeysOptimizedImpl(rebase_sc, in->GetProducer(0), left_key, index_key, right_key, sort,
                           &new_depend)) {
            return false;
        }
        PhysicalOpNode* new_right = in->GetProducer(1);
        if (request_join->join_.join_type_ == node::kJoinTypeConcat) {
            // for concat join, only acceptable if the two inputs (of course same table) optimized by the same index
            auto* rebase_sc = in->GetProducer(1)->schemas_ctx();
            if (!KeysOptimizedImpl(rebase_sc, in->GetProducer(1), left_key, index_key, right_key, sort, &new_right)) {
                return false;
            }
        }
        PhysicalRequestJoinNode* new_join = nullptr;
        auto s = plan_ctx_->CreateOp<PhysicalRequestJoinNode>(&new_join, new_depend, new_right,
                                                              request_join->join(), request_join->output_right_only());
        if (!s.isOK()) {
            LOG(WARNING) << "Fail to create new request join op: " << s;
            return false;
        }

        *new_in = new_join;
        return true;
    } else if (PhysicalOpType::kPhysicalOpJoin == in->GetOpType()) {
        auto* join = dynamic_cast<PhysicalJoinNode*>(in);
        // try optimze left source of request join with window definition
        // window partition by and order by columns must refer to the left most table only
        PhysicalOpNode* new_depend = nullptr;
        auto* rebase_sc = in->GetProducer(0)->schemas_ctx();
        if (!KeysOptimizedImpl(rebase_sc, in->GetProducer(0), left_key, index_key, right_key, sort,
                           &new_depend)) {
            return false;
        }
        PhysicalJoinNode* new_join = nullptr;
        auto s = plan_ctx_->CreateOp<PhysicalJoinNode>(&new_join, new_depend, join->GetProducer(1),
                                                       join->join(), join->output_right_only());
        if (!s.isOK()) {
            LOG(WARNING) << "Fail to create new join op: " << s;
            return false;
        }

        *new_in = new_join;
        return true;
    } else if (PhysicalOpType::kPhysicalOpProject == in->GetOpType()) {
        auto * project = dynamic_cast<PhysicalProjectNode*>(in);
        if (project == nullptr || project->project_type_ != vm::kAggregation) {
            return false;
        }

        auto * agg_project = dynamic_cast<vm::PhysicalAggregationNode*>(in);

        PhysicalOpNode* new_depend = nullptr;
        auto* rebase_sc = in->GetProducer(0)->schemas_ctx();
        if (!KeysOptimizedImpl(rebase_sc, in->GetProducer(0), left_key, index_key, right_key, sort,
                           &new_depend)) {
            return false;
        }

        vm::PhysicalAggregationNode* new_agg = nullptr;
        if (!plan_ctx_
                 ->CreateOp<vm::PhysicalAggregationNode>(&new_agg, new_depend, agg_project->project(),
                                                         agg_project->having_condition_.condition())
                 .isOK()) {
            return false;
        }
        *new_in = new_agg;
        return true;
    } else if (PhysicalOpType::kPhysicalOpRequestUnion == in->GetOpType()) {
        // JOIN (..., AGG(REQUEST_UNION(left, ...))): JOIN condition optimizing left
        PhysicalOpNode* new_left_depend = nullptr;
        auto* rebase_sc = in->GetProducer(0)->schemas_ctx();
        if (!KeysOptimizedImpl(rebase_sc, in->GetProducer(0), left_key, index_key, right_key, sort,
                           &new_left_depend)) {
            return false;
        }

        auto * request_union = dynamic_cast<vm::PhysicalRequestUnionNode*>(in);

        vm::PhysicalRequestUnionNode* new_union = nullptr;
        if (!plan_ctx_
                 ->CreateOp<vm::PhysicalRequestUnionNode>(
                     &new_union, new_left_depend, in->GetProducer(1), request_union->window(),
                     request_union->instance_not_in_window(), request_union->exclude_current_time(),
                     request_union->output_request_row())
                 .isOK()) {
            return false;
        }
        for (auto& pair : request_union->window_unions().window_unions_) {
            if (!new_union->AddWindowUnion(pair.first, pair.second)) {
                return false;
            }
        }
        *new_in = new_union;
        return true;
    } else if (PhysicalOpType::kPhysicalOpSetOperation == in->GetOpType()) {
        auto set_op = dynamic_cast<vm::PhysicalSetOperationNode*>(in);
        // keys optimize for each inputs for set operation
        std::vector<PhysicalOpNode*> opt_inputs;
        opt_inputs.reserve(in->GetProducerCnt());
        bool opt_all = true;
        Key* left_key_opt = nullptr;
        Key* index_key_opt = nullptr;
        Key* right_key_opt = nullptr;
        Sort* sort_opt = nullptr;
        std::vector<Key> alloca_keys;
        alloca_keys.reserve(3 * in->GetProducerCnt());
        std::vector<Sort> alloca_sort;
        alloca_sort.reserve(in->GetProducerCnt());

        for (size_t i = 0; i < in->GetProducerCnt(); i++) {
            auto n = in->GetProducer(i);
            // expr_cache_.clear();
            // optimize_info_ = nullptr;
            PhysicalOpNode* optimized = nullptr;
            // copy keys
            auto left_key_cp = ShadowCopy(left_key, &alloca_keys);
            auto index_key_cp = ShadowCopy(index_key, &alloca_keys);
            auto right_key_cp = ShadowCopy(right_key, &alloca_keys);
            auto sort_cp = ShadowCopy(sort, &alloca_sort);

            if (!KeysOptimizedImpl(n->schemas_ctx(), n, left_key_cp, index_key_cp, right_key_cp, sort_cp, &optimized)) {
                LOG(WARNING) << "unable to optimize operation set input: " << n->GetTreeString();
                opt_all = false;
            }
            opt_inputs.push_back(optimized == nullptr ? n : optimized);

            if (i == 0) {
                left_key_opt = left_key_cp;
                index_key_opt = index_key_cp;
                right_key_opt = right_key_cp;
                sort_opt = sort_cp;
            } else {
                // check all optimized keys equals
                if (!node::SqlEquals(left_key_opt->keys(), left_key_cp->keys())) {
                    LOG(WARNING) << "[optimizing set operation] optimized left keys not equal: "
                                 << node::ExprString(left_key_opt->keys()) << " vs "
                                 << node::ExprString(left_key_cp->keys());
                    return false;
                }
                if (!node::SqlEquals(index_key_opt->keys(), index_key_cp->keys())) {
                    LOG(WARNING) << "[optimizing set operation] optimized index keys not equal: "
                                 << node::ExprString(index_key_opt->keys()) << " vs "
                                 << node::ExprString(index_key_cp->keys());
                    return false;
                }
                if (right_key_opt && !node::SqlEquals(right_key_opt->keys(), right_key_cp->keys())) {
                    LOG(WARNING) << "[optimizing set operation] optimized right keys not equal: "
                                 << node::ExprString(right_key_opt->keys()) << " vs "
                                 << node::ExprString(right_key_cp->keys());
                    return false;
                }
                if (sort_opt && !node::SqlEquals(sort_opt->orders(), sort_cp->orders())) {
                    LOG(WARNING) << "[optimizing set operation] optimized order keys not equal: "
                                 << node::ExprString(sort_opt->orders()) << " vs "
                                 << node::ExprString(sort_cp->orders());
                    return false;
                }
            }
        }
        if (opt_all) {
            // write keys
            left_key->set_keys(left_key_opt->keys());
            index_key->set_keys(index_key_opt->keys());
            if (right_key && right_key_opt) {
                right_key->set_keys(right_key_opt->keys());
            }
            if (sort && sort_opt) {
                sort->set_orders(sort_opt->orders());
            }
            vm::PhysicalSetOperationNode* opt_set = nullptr;
            if (!plan_ctx_
                     ->CreateOp<vm::PhysicalSetOperationNode>(&opt_set, set_op->op_type_, opt_inputs, set_op->distinct_)
                     .isOK()) {
                return false;
            }
            *new_in = opt_set;
        }

        return opt_all;
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
    bool hasOptimized = FilterAndOrderOptimized(root_schemas_ctx, in, filter, nullptr,
                                   new_in);
    // construct filter condition after keys optimization
    if (filter->left_key().ValidKey() && filter->right_key().ValidKey()) {
        auto condition_list = node_manager_->MakeExprList();
        if (nullptr != filter->condition_.condition()) {
            condition_list->AddChild(const_cast<node::ExprNode*>(filter->condition_.condition()));
        }
        for (size_t i = 0; i < filter->left_key().keys()->GetChildNum(); i++) {
            condition_list->AddChild(node_manager_->MakeBinaryExprNode(
                filter->left_key().keys()->GetChild(i), filter->right_key().keys()->GetChild(i), node::kFnOpEq));
        }
        filter->right_key_.set_keys(node_manager_->MakeExprList());
        filter->left_key_.set_keys(node_manager_->MakeExprList());
        filter->condition_.set_condition(node_manager_->MakeAndExpr(condition_list));
    }
    return hasOptimized;
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

bool GroupAndSortOptimized::TransformKeysAndOrderExpr(const node::ExprListNode* groups, const node::OrderByNode* order,
                                                      vm::PhysicalDataProviderNode* data_node, std::string* index_name,
                                                      IndexBitMap* output_bitmap) {
    if (nullptr == groups || nullptr == output_bitmap || nullptr == index_name) {
        DLOG(WARNING) << "fail to transform keys expr : key expr or output "
                         "or index_name ptr is null";
        return false;
    }

    if (nullptr == order) {
        DLOG(INFO) << "keys optimized: " << node::ExprString(groups);
    } else {
        DLOG(INFO) << "keys and order optimized: keys=" << node::ExprString(groups)
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
                auto oop = expr_cache_.find(column);
                if (oop == expr_cache_.end()) {
                    return false;
                }

                auto op = oop->second.find(data_node);
                if (op == oop->second.end()) {
                    return false;
                }

                if (data_node-> table_handler_->GetName() != op->second.tb_name ||
                    data_node->table_handler_->GetDatabase() != op->second.db_name) {
                    return false;
                }

                result_bitmap_mapping[columns.size()] = i;
                columns.emplace_back(op->second.col_name);
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
                auto oop = expr_cache_.find(column);
                if (oop == expr_cache_.end()) {
                    return false;
                }

                auto op = oop->second.find(data_node);
                if (op == oop->second.end()) {
                    return false;
                }

                if (data_node->table_handler_->GetName() != op->second.tb_name ||
                    data_node->table_handler_->GetDatabase() != op->second.db_name) {
                    return false;
                }

                order_columns.emplace_back(op->second.col_name);
            }
        }
    }
    if (columns.empty()) {
        return false;
    }

    IndexBitMap match_bitmap;
    // internal structure for MatchBestIndex, initially turn every bit true
    IndexBitMap state_bitmap(std::vector<std::optional<ColIndexInfo>>(columns.size(), std::make_optional(0)));
    if (!MatchBestIndex(columns, order_columns, data_node->table_handler_, &state_bitmap, index_name, &match_bitmap)) {
        return false;
    }
    if (match_bitmap.bitmap.size() != columns.size()) {
        return false;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        if (match_bitmap.bitmap[i].has_value()) {
            size_t origin_idx = result_bitmap_mapping[i];
            output_bitmap->bitmap.at(origin_idx) = match_bitmap.bitmap[i].value();
        }
    }
    output_bitmap->refered_index_key_count = match_bitmap.refered_index_key_count;
    return true;
}

// When *index_name is empty, return true if we can find the best index for key columns and order column
// When *index_name isn't empty, return true if the given index_name match key columns and order column
bool GroupAndSortOptimized::MatchBestIndex(const std::vector<std::string>& columns,
                                           const std::vector<std::string>& order_columns,
                                           std::shared_ptr<TableHandler> table_handler,
                                           IndexBitMap* bitmap_ptr,
                                           std::string* index_name,
                                           IndexBitMap* index_bitmap) {
    if (nullptr == bitmap_ptr || nullptr == index_name) {
        LOG(WARNING)
            << "fail to match best index: bitmap or index_name ptr is null";
        return false;
    }

    if (!table_handler) {
        LOG(WARNING) << "fail to match best index: table is null";
        return false;
    }
    const auto& index_hint = table_handler->GetIndex();
    auto* schema = table_handler->GetSchema();
    if (nullptr == schema) {
        LOG(WARNING) << "fail to match best index: table schema null";
        return false;
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

        // key column name -> (idx of index definition, whether hitted by one of columns)
        std::unordered_map<absl::string_view, std::pair<uint32_t, bool>> key_name_idx_map;
        for (uint32_t i = 0; i < index.keys.size(); ++i) {
            key_name_idx_map[index.keys[i].name] = std::make_pair(i, false);
        }

        // flag whether all the true bitted columns matches key in the same index
        bool exact_match_all = true;
        // construct a copy of IndexBitMap for matching
        IndexBitMap matching = *bitmap_ptr;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (matching.bitmap[i].has_value()) {
                auto it = key_name_idx_map.find(columns[i]);
                if (it == key_name_idx_map.end()) {
                    exact_match_all = false;
                    break;
                }
                it->second.second = true;
                // reset to the correct index of matched Index definition
                matching.bitmap[i].emplace(it->second.first);
            }
        }
        for (auto & kv : key_name_idx_map) {
            exact_match_all &= kv.second.second;
        }

        if (exact_match_all) {
            // index absolute match
            matching.refered_index_key_count = index.keys.size();
            *index_name = index.name;
            *index_bitmap = matching;
            return true;
        }
    }

    // try match best index
    std::string best_index_name;
    IndexBitMap best_index_bitmap;

    bool succ = false;
    for (size_t i = 0; i < bitmap_ptr->bitmap.size(); ++i) {
        // find solutions recursively by flip one of the hitted bit
        // then choose the best one among those
        if (bitmap_ptr->bitmap[i].has_value()) {
            auto val = bitmap_ptr->bitmap[i].value();
            bitmap_ptr->bitmap[i] = {};
            std::string name;
            IndexBitMap sub_best_bitmap;
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
                        // override with better index
                        best_index_name = name;
                        best_index_bitmap = sub_best_bitmap;
                    }
                }
            }
            bitmap_ptr->bitmap[i] = val;
        }
    }
    *index_name = best_index_name;
    *index_bitmap = best_index_bitmap;
    return succ;
}

}  // namespace passes
}  // namespace hybridse
