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
#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_GROUP_AND_SORT_OPTIMIZED_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_GROUP_AND_SORT_OPTIMIZED_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <list>
#include <utility>

#include "passes/physical/transform_up_physical_pass.h"

namespace hybridse {
namespace passes {

using codec::Schema;
using hybridse::vm::Filter;
using hybridse::vm::IndexSt;
using hybridse::vm::Join;
using hybridse::vm::Key;
using hybridse::vm::SchemasContext;
using hybridse::vm::Sort;
using hybridse::vm::TableHandler;

class GroupAndSortOptimized : public TransformUpPysicalPass {
 public:
    explicit GroupAndSortOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

    ~GroupAndSortOptimized() {}

 private:
    struct ColIndexInfo {
        ColIndexInfo(uint32_t idx) : index(idx) {}  // NOLINT

        // the vector index for column reference of the index definition, if column hit one of indexes
        uint32_t index;
    };
    struct IndexBitMap {
        IndexBitMap() {}
        IndexBitMap(std::vector<std::optional<ColIndexInfo>> mp) : bitmap(mp) {}  // NOLINT

        std::vector<std::optional<ColIndexInfo>> bitmap;
        // total size of keys from the best matched index
        uint32_t refered_index_key_count = 0;
    };

    struct KeysInfo {
        KeysInfo(vm::PhysicalOpType type, vm::Key* left_key, vm::Key* right_key, vm::Key* index_key, vm::Sort* sort)
            : type(type), left_key(left_key), right_key(right_key), index_key(index_key), right_sort(sort) {}
        vm::PhysicalOpType type;
        Key* left_key;
        Key* right_key;
        Key* index_key;
        Sort* right_sort;
    };

    template <typename Container>
    struct TransformCxtGuard {
        explicit TransformCxtGuard(Container* c, KeysInfo&& info) : c_(c) {
            c_->push_back(std::forward<KeysInfo>(info));
        }
        ~TransformCxtGuard() { c_->pop_back(); }
        TransformCxtGuard(const TransformCxtGuard&) = delete;
        TransformCxtGuard(TransformCxtGuard&&) = delete;
        TransformCxtGuard& operator=(const TransformCxtGuard&) = delete;
        TransformCxtGuard& operator=(TransformCxtGuard&&) = delete;

     private:
        Container* c_;
    };

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);

    bool KeysOptimized(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* left_key, Key* index_key,
                       Key* right_key, Sort* sort, PhysicalOpNode** new_in);

    bool FilterAndOrderOptimized(const SchemasContext* root_schemas_ctx,
                                 PhysicalOpNode* in, Filter* filter, Sort* sort,
                                 PhysicalOpNode** new_in);

    bool KeyAndOrderOptimized(const SchemasContext* root_schemas_ctx,
                              PhysicalOpNode* in, Key* group, Sort* sort,
                              PhysicalOpNode** new_in);

    bool KeysAndOrderFilterOptimized(const SchemasContext* root_schemas_ctx,
                                     PhysicalOpNode* in, Key* group, Key* hash,
                                     Sort* sort, PhysicalOpNode** new_in);

    bool FilterOptimized(const SchemasContext* root_schemas_ctx,
                         PhysicalOpNode* in, Filter* filter,
                         PhysicalOpNode** new_in);
    bool JoinKeysOptimized(const SchemasContext* schemas_ctx,
                           PhysicalOpNode* in, Join* join,
                           PhysicalOpNode** new_in);
    bool KeysFilterOptimized(const SchemasContext* root_schemas_ctx,
                             PhysicalOpNode* in, Key* group, Key* hash,
                             PhysicalOpNode** new_in);
    bool GroupOptimized(const SchemasContext* root_schemas_ctx,
                        PhysicalOpNode* in, Key* group,
                        PhysicalOpNode** new_in);
    bool SortOptimized(const SchemasContext* root_schemas_ctx,
                       PhysicalOpNode* in, Sort* sort);
    bool TransformOrderExpr(const SchemasContext* schemas_ctx,
                            const node::OrderByNode* order,
                            const Schema& schema, const IndexSt& index_st,
                            const node::OrderByNode** output);
    bool TransformKeysAndOrderExpr(const SchemasContext* schemas_ctx,
                                   const node::ExprListNode* groups,
                                   const node::OrderByNode* order,
                                   std::shared_ptr<TableHandler> table_handler,
                                   std::string* index,
                                   IndexBitMap* best_bitmap);
    bool MatchBestIndex(const std::vector<std::string>& columns,
                        const std::vector<std::string>& order_columns,
                        std::shared_ptr<TableHandler> table_handler,
                        IndexBitMap* bitmap,
                        std::string* index_name,
                        IndexBitMap* best_bitmap);

 private:
    std::list<KeysInfo> ctx_;
};
}  // namespace passes
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_GROUP_AND_SORT_OPTIMIZED_H_
