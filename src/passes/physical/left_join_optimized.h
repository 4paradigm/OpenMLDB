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
#ifndef SRC_PASSES_PHYSICAL_LEFT_JOIN_OPTIMIZED_H_
#define SRC_PASSES_PHYSICAL_LEFT_JOIN_OPTIMIZED_H_

#include <string>
#include "passes/physical/transform_up_physical_pass.h"

namespace fesql {
namespace passes {

using fesql::codec::Schema;

class LeftJoinOptimized : public TransformUpPysicalPass {
 public:
    explicit LeftJoinOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool ColumnExist(const Schema& schema, const std::string& column);
    bool CheckExprListFromSchema(const node::ExprListNode* expr_list,
                                 const Schema* schema);
};
}  // namespace passes
}  // namespace fesql

#endif  // SRC_PASSES_PHYSICAL_LEFT_JOIN_OPTIMIZED_H_
