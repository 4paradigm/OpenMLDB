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

#include "base/ddl_parser.h"

#include <algorithm>
#include <vector>

namespace openmldb::base {

// Ref hybridse/src/passes/physical/group_and_sort_optimized.cc:651
// // TODO(hw): hybridse should open this method
bool ResolveColumnToSourceColumnName(const hybridse::node::ColumnRefNode* col, const SchemasContext* schemas_ctx,
        std::string* source_name);

bool IndexMapBuilder::CreateIndex(const std::string& table, const hybridse::node::ExprListNode* keys,
                                  const hybridse::node::OrderByNode* ts, const SchemasContext* ctx) {
    // we encode table, keys and ts to one string
    auto index = Encode(table, keys, ts, ctx);
    if (index.empty()) {
        LOG(WARNING) << "index encode failed for table " << table;
        return false;
    }

    if (index_map_.find(index) != index_map_.end()) {
        // TODO(hw): ttl merge
        LOG(DFATAL) << "index existed in cache, can't handle it now";
        return false;
    }
    LOG(INFO) << "create index with unset ttl: " << index;

    // default TTLSt is abs and ttl=0, rows will never expire.
    common::TTLSt ttl_st;
    index_map_[index] = ttl_st;
    latest_record_ = index;
    return true;
}

bool IndexMapBuilder::UpdateIndex(const hybridse::vm::Range& range) {
    if (latest_record_.empty() || index_map_.find(latest_record_) == index_map_.end()) {
        LOG(DFATAL) << "want to update ttl status, but index is not created before";
        return false;
    }
    // TODO(hw): it's better to check the ts col name
    //  but range's column names may be renamed, needs schema context

    if (!range.Valid()) {
        LOG(INFO) << "range is invalid, can't update ttl, still use the default ttl";
        return true;
    }

    auto frame = range.frame();
    auto start = frame->GetHistoryRangeStart();
    auto rows_start = frame->GetHistoryRowsStart();

    DLOG_ASSERT(start <= 0 && rows_start <= 0);

    std::stringstream ss;
    range.frame()->Print(ss, "");
    LOG(INFO) << "frame info: " << ss.str() << ", get start points: " << start << ", " << rows_start;

    common::TTLSt ttl_st;

    auto type = frame->frame_type();
    if (type == hybridse::node::kFrameRows) {
        // frame_rows is valid
        DLOG_ASSERT(frame->frame_range() == nullptr && frame->GetHistoryRowsStartPreceding() > 0);
        ttl_st.set_lat_ttl(frame->GetHistoryRowsStartPreceding());
        ttl_st.set_ttl_type(type::TTLType::kLatestTime);
    } else {
        // frame_range is valid
        DLOG_ASSERT(type != hybridse::node::kFrameRowsMergeRowsRange) << "merge type, how to parse?";
        DLOG_ASSERT(frame->frame_rows() == nullptr && frame->GetHistoryRangeStart() < 0);
        // GetHistoryRangeStart is negative, ttl needs uint64
        ttl_st.set_abs_ttl(std::max(MIN_TIME, -1 * frame->GetHistoryRangeStart()));
        ttl_st.set_ttl_type(type::TTLType::kAbsoluteTime);
    }

    index_map_[latest_record_] = ttl_st;
    LOG(INFO) << latest_record_ << " update ttl " << index_map_[latest_record_].DebugString();

    // to avoid double update
    latest_record_.clear();
    return true;
}

IndexMap IndexMapBuilder::ToMap() {
    IndexMap result;
    for (auto& pair : index_map_) {
        auto dec = Decode(pair.first);
        result[dec.first].emplace_back(dec.second);
    }

    return result;
}

std::string IndexMapBuilder::Encode(const std::string& table, const hybridse::node::ExprListNode* keys,
                                    const hybridse::node::OrderByNode* ts, const SchemasContext* ctx) {
    // children are ColumnRefNode
    auto cols = NormalizeColumns(table, keys->children_, ctx);
    if (cols.empty()) {
        return {};
    }

    std::stringstream ss;
    ss << table << ":";
    auto iter = cols.begin();
    ss << (*iter);
    iter++;
    for (; iter != cols.end(); iter++) {
        ss << "," << (*iter);
    }
    ss << ";";

    if (ts != nullptr && ts->order_expressions_ != nullptr) {
        for (auto order : ts->order_expressions_->children_) {
            auto cast = dynamic_cast<hybridse::node::OrderExpression*>(order);
            if (cast->expr() != nullptr) {
                auto res = NormalizeColumns(table, {const_cast<hybridse::node::ExprNode*>(cast->expr())}, ctx);
                if (res.size() != 1 || res[0].empty()) {
                    LOG(DFATAL) << "parse ts col from order node failed, " << cast->GetExprString();
                }
                ss << res[0];
            }
        }
    }
    return ss.str();
}

std::vector<std::string> IndexMapBuilder::NormalizeColumns(const std::string& table,
                                                           const std::vector<hybridse::node::ExprNode*>& nodes,
                                                           const SchemasContext* ctx) {
    if (table.empty() || nodes.empty()) {
        return {};
    }
    std::vector<std::string> result;
    for (auto& node : nodes) {
        auto cast = hybridse::node::ColumnRefNode::CastFrom(node);
        std::string name;
        if (!ResolveColumnToSourceColumnName(cast, ctx, &name)) {
            return {};
        }
        result.emplace_back(name);
    }
    // sort to avoid dup index
    std::sort(result.begin(), result.end());
    return result;
}

std::pair<std::string, common::ColumnKey> IndexMapBuilder::Decode(const std::string& index_str) {
    if (index_str.empty()) {
        return {};
    }

    auto key_sep = index_str.find(KEY_MARK);
    auto table_name = index_str.substr(0, key_sep);

    common::ColumnKey column_key;
    auto ts_sep = index_str.find(TS_MARK);
    auto keys_str = index_str.substr(key_sep + 1, ts_sep - key_sep - 1);
    // split keys
    std::vector<std::string> keys;
    boost::split(keys, keys_str, boost::is_any_of(std::string(1, KEY_SEP)));
    for (auto& key : keys) {
        DLOG_ASSERT(!key.empty());
        column_key.add_col_name(key);
    }
    // if no ts hint, do not set. No ts in index is OK
    auto ts_col = GetTsCol(index_str);
    if (!ts_col.empty()) {
        column_key.set_ts_name(ts_col);
    }
    return std::make_pair(table_name, column_key);
}

bool GroupAndSortOptimizedParser::KeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in,
                                                     Key* left_key, Key* index_key, Key* right_key, Sort* sort,
                                                     PhysicalOpNode**) {
    if (nullptr == left_key || nullptr == index_key || !left_key->ValidKey()) {
        return false;
    }

    if (right_key != nullptr && !right_key->ValidKey()) {
        return false;
    }

    if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<hybridse::vm::PhysicalDataProviderNode*>(in);
        // Do not optimize with Request DataProvider (no index has been provided)
        if (DataProviderType::kProviderTypeRequest == scan_op->provider_type_) {
            return false;
        }

        if (DataProviderType::kProviderTypeTable == scan_op->provider_type_ ||
            DataProviderType::kProviderTypePartition == scan_op->provider_type_) {
            const hybridse::node::ExprListNode* right_partition =
                right_key == nullptr ? left_key->keys() : right_key->keys();

            size_t key_num = right_partition->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            hybridse::node::ExprListNode order_values;

            if (DataProviderType::kProviderTypeTable == scan_op->provider_type_) {
                // Apply key columns and order column optimization with all indexes binding to
                // scan_op->table_handler_ Return false if fail to find an appropriate index
                auto groups = right_partition;
                auto order = (nullptr == sort ? nullptr : sort->orders_);
                DLOG(INFO) << "keys and order optimized: keys=" << hybridse::node::ExprString(groups)
                           << ", order=" << (order == nullptr ? "null" : hybridse::node::ExprString(order))
                           << " for table " << scan_op->table_handler_->GetName();

                // columns in groups or order, may be renamed

                index_map_builder_.CreateIndex(scan_op->table_handler_->GetName(), groups, order, root_schemas_ctx);
                // parser won't create partition_op
                return true;
            } else {
                auto partition_op = dynamic_cast<hybridse::vm::PhysicalPartitionProviderNode*>(scan_op);
                DLOG_ASSERT(partition_op != nullptr);
                auto index_name = partition_op->index_name_;
                // Apply key columns and order column optimization with given index name
                // Return false if given index do not match the keys and order column
                // -- return false won't change index_name
                LOG(WARNING) << "What if the index is not best index? Do we need to adjust index?";
                return false;
            }
        }
    } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<hybridse::vm::PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        return KeysOptimizedParse(root_schemas_ctx, simple_project->producers()[0], left_key, index_key, right_key,
                                  sort, &new_depend);

    } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        return KeysOptimizedParse(root_schemas_ctx, in->producers()[0], left_key, index_key, right_key, sort,
                                  &new_depend);
    }
    return false;
}

std::vector<PhysicalOpNode*> GroupAndSortOptimizedParser::InitJoinList(
    hybridse::vm::PhysicalWindowAggrerationNode* op) {
    std::vector<PhysicalOpNode*> joined_op_list;
    auto& window_joins = op->window_joins_.window_joins();
    PhysicalOpNode* cur = op->GetProducer(0);
    for (auto& pair : window_joins) {
        auto joined = new hybridse::vm::PhysicalJoinNode(cur, pair.first, pair.second);
        joined_op_list.push_back(joined);
        cur = joined;
    }
    return joined_op_list;
}

void GroupAndSortOptimizedParser::TransformParse(PhysicalOpNode* in) {
    switch (in->GetOpType()) {
        case PhysicalOpType::kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<hybridse::vm::PhysicalGroupNode*>(in);
            PhysicalOpNode* new_producer;
            if (GroupOptimizedParse(group_op->schemas_ctx(), group_op->GetProducer(0), &group_op->group_,
                                    &new_producer)) {
                // no orders->no sort->no ttl info
                DLOG(INFO) << "ttl won't update by node:\n" << group_op->GetTreeString();
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpProject: {
            auto project_op = dynamic_cast<hybridse::vm::PhysicalProjectNode*>(in);
            if (hybridse::vm::ProjectType::kWindowAggregation == project_op->project_type_) {
                auto window_agg_op = dynamic_cast<hybridse::vm::PhysicalWindowAggrerationNode*>(project_op);
                CHECK_NOTNULL(window_agg_op);
                PhysicalOpNode* input = window_agg_op->GetProducer(0);

                PhysicalOpNode* new_producer;
                if (!window_agg_op->instance_not_in_window()) {
                    if (KeyAndOrderOptimizedParse(input->schemas_ctx(), input, &window_agg_op->window_.partition_,
                                                  &window_agg_op->window_.sort_, &new_producer)) {
                        index_map_builder_.UpdateIndex(window_agg_op->window_.range());
                    }
                }
                // must prepare for window join column infer
                auto& window_joins = window_agg_op->window_joins();
                auto& window_unions = window_agg_op->window_unions();
                auto joined_op_list = InitJoinList(window_agg_op);
                if (!window_joins.Empty()) {
                    size_t join_idx = 0;
                    for (auto& window_join : window_joins.window_joins()) {
                        PhysicalOpNode* cur_joined = joined_op_list[join_idx];

                        PhysicalOpNode* new_join_right;
                        if (JoinKeysOptimizedParse(cur_joined->schemas_ctx(), window_join.first, &window_join.second,
                                                   &new_join_right)) {
                            // no range info
                            DLOG(INFO) << "ttl won't update by node:\n" << window_agg_op->GetTreeString();
                        }
                        join_idx += 1;
                    }
                }
                // joined_op_list need to be deleted
                for (auto& op : joined_op_list) {
                    delete op;
                }

                if (!window_unions.Empty()) {
                    for (auto& window_union : window_unions.window_unions_) {
                        PhysicalOpNode* new_producer1;
                        if (KeyAndOrderOptimizedParse(window_union.first->schemas_ctx(), window_union.first,
                                                      &window_union.second.partition_, &window_union.second.sort_,
                                                      &new_producer1)) {
                            index_map_builder_.UpdateIndex(window_union.second.range());
                        }
                    }
                }
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpRequestUnion: {
            auto union_op = dynamic_cast<hybridse::vm::PhysicalRequestUnionNode*>(in);

            PhysicalOpNode* new_producer;
            if (!union_op->instance_not_in_window()) {
                if (KeysAndOrderFilterOptimizedParse(union_op->schemas_ctx(), union_op->GetProducer(1),
                                                     &union_op->window_.partition_, &union_op->window_.index_key_,
                                                     &union_op->window_.sort_, &new_producer)) {
                    index_map_builder_.UpdateIndex(union_op->window().range());
                }
            }

            if (!union_op->window_unions().Empty()) {
                for (auto& window_union : union_op->window_unions_.window_unions_) {
                    PhysicalOpNode* new_producer1;
                    auto& window = window_union.second;
                    if (KeysAndOrderFilterOptimizedParse(window_union.first->schemas_ctx(), window_union.first,
                                                         &window.partition_, &window.index_key_, &window.sort_,
                                                         &new_producer1)) {
                        index_map_builder_.UpdateIndex(window.range());
                    }
                }
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpRequestJoin: {
            auto* join_op = dynamic_cast<hybridse::vm::PhysicalRequestJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (JoinKeysOptimizedParse(join_op->schemas_ctx(), join_op->GetProducer(1), &join_op->join_,
                                       &new_producer)) {
                // no range info
                DLOG(INFO) << "ttl won't update by node:\n" << join_op->GetTreeString();
            }

            break;
        }
        case PhysicalOpType::kPhysicalOpJoin: {
            auto* join_op = dynamic_cast<hybridse::vm::PhysicalRequestJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (JoinKeysOptimizedParse(join_op->schemas_ctx(), join_op->GetProducer(1), &join_op->join_,
                                       &new_producer)) {
                // no range info
                DLOG(INFO) << "ttl won't update by node:\n" << join_op->GetTreeString();
            }

            break;
        }
        case PhysicalOpType::kPhysicalOpFilter: {
            auto* filter_op = dynamic_cast<hybridse::vm::PhysicalFilterNode*>(in);
            PhysicalOpNode* new_producer;
            if (FilterOptimizedParse(filter_op->schemas_ctx(), filter_op->GetProducer(0), &filter_op->filter_,
                                     &new_producer)) {
                // no range info
                DLOG(INFO) << "ttl won't update by node:\n" << filter_op->GetTreeString();
            }
        }
        default: {
            break;
        }
    }
}

std::ostream& operator<<(std::ostream& os, IndexMap& index_map) {
    for (auto& indexes : index_map) {
        os << " {" << indexes.first << "[";
        for (auto& ck : indexes.second) {
            os << ck.ShortDebugString() << ", ";
        }
        os << "]} ";
    }
    return os;
}

bool ResolveColumnToSourceColumnName(const hybridse::node::ColumnRefNode* col, const SchemasContext* schemas_ctx,
                                     std::string* source_name) {
    // use detailed column resolve utility
    size_t column_id;
    int path_idx;
    size_t child_column_id;
    size_t source_column_id;
    const PhysicalOpNode* source;
    hybridse::base::Status status =
        schemas_ctx->ResolveColumnID(col->GetDBName(), col->GetRelationName(), col->GetColumnName(), &column_id,
                                     &path_idx,
                                     &child_column_id, &source_column_id, &source);

    // try loose the relation
    if (!status.isOK() && !col->GetRelationName().empty()) {
        status = schemas_ctx->ResolveColumnID("", "", col->GetColumnName(), &column_id, &path_idx, &child_column_id,
                                              &source_column_id, &source);
    }

    if (!status.isOK()) {
        LOG(WARNING) << "Illegal index column: " << col->GetExprString();
        return false;
    }
    if (source == nullptr || source->GetOpType() != PhysicalOpType::kPhysicalOpDataProvider) {
        LOG(WARNING) << "Index column is not from any source table: " << col->GetExprString();
        return false;
    }
    status = source->schemas_ctx()->ResolveColumnNameByID(source_column_id, source_name);
    if (!status.isOK()) {
        LOG(WARNING) << "Illegal source column id #" << source_column_id << " for index column "
                     << col->GetExprString();
        return false;
    }
    return true;
}
}  // namespace openmldb::base
