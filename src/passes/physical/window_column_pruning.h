/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
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
