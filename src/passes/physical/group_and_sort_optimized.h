/*
 * Copyright (c) 2021 4paradigm
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
#ifndef SRC_PASSES_PHYSICAL_GROUP_AND_SORT_OPTIMIZED_H_
#define SRC_PASSES_PHYSICAL_GROUP_AND_SORT_OPTIMIZED_H_

#include <memory>
#include <string>
#include "passes/physical/transform_up_physical_pass.h"

namespace fesql {
namespace passes {

using codec::Schema;
using fesql::vm::Filter;
using fesql::vm::IndexSt;
using fesql::vm::Join;
using fesql::vm::Key;
using fesql::vm::SchemasContext;
using fesql::vm::Sort;
using fesql::vm::TableHandler;

class GroupAndSortOptimized : public TransformUpPysicalPass {
 public:
    explicit GroupAndSortOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

    ~GroupAndSortOptimized() {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);

    bool KeysOptimized(
            const SchemasContext* root_schemas_ctx, PhysicalOpNode* in,
            Key* left_key, Key* index_key, Key* right_key,
            Sort* sort, PhysicalOpNode** new_in);

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
    bool TransformGroupExpr(const SchemasContext* schemas_ctx,
                            const node::ExprListNode* group,
                            std::shared_ptr<TableHandler> table_handler,
                            std::string* index, std::vector<bool>* best_bitmap);
    bool TransformOrderExpr(const SchemasContext* schemas_ctx,
                            const node::OrderByNode* order,
                            const Schema& schema, const IndexSt& index_st,
                            const node::OrderByNode** output);
    bool TransformKeysAndOrderExpr(const SchemasContext* schemas_ctx,
                                   const node::ExprListNode* groups,
                                   const node::OrderByNode* order,
                                   std::shared_ptr<TableHandler> table_handler,
                                   std::string* index,
                                   std::vector<bool>* best_bitmap);
    bool MatchBestIndex(const std::vector<std::string>& columns,
                        const std::vector<std::string>& order_columns,
                        std::shared_ptr<TableHandler> table_handler,
                        std::vector<bool>* bitmap, std::string* index_name,
                        std::vector<bool>* best_bitmap);  // NOLINT
};
}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_PHYSICAL_GROUP_AND_SORT_OPTIMIZED_H_
