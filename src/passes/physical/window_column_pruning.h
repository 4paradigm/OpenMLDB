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

#include <map>

#include "passes/physical/physical_pass.h"
#include "vm/physical_op.h"

#ifndef SRC_PASSES_PHYSICAL_WINDOW_COLUMN_PRUNING_H_
#define SRC_PASSES_PHYSICAL_WINDOW_COLUMN_PRUNING_H_

namespace fesql {
namespace passes {

using fesql::base::Status;
using fesql::vm::PhysicalWindowAggrerationNode;

class WindowColumnPruning : public PhysicalPass {
 public:
    Status Apply(PhysicalPlanContext* ctx, PhysicalOpNode* input,
                 PhysicalOpNode** out) override;

 private:
    Status DoApply(PhysicalPlanContext* ctx, PhysicalOpNode* input,
                   PhysicalOpNode** out);
    Status ProcessWindow(PhysicalPlanContext* ctx,
                         PhysicalWindowAggrerationNode* input,
                         PhysicalOpNode** out);

    std::map<size_t, PhysicalOpNode*> cache_;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_PHYSICAL_WINDOW_COLUMN_PRUNING_H_
