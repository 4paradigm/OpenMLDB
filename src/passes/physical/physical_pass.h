/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_pass.h
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/

#include <vector>

#include "base/fe_object.h"
#include "base/fe_status.h"
#include "node/node_manager.h"
#include "passes/pass_base.h"
#include "vm/physical_op.h"
#include "vm/physical_plan_context.h"

#ifndef SRC_PASSES_PHYSICAL_PHYSICAL_PASS_H_
#define SRC_PASSES_PHYSICAL_PHYSICAL_PASS_H_

namespace fesql {
namespace passes {

using fesql::vm::PhysicalOpNode;
using fesql::vm::PhysicalPlanContext;

class PhysicalPass : public PassBase<PhysicalOpNode, PhysicalPlanContext> {
 public:
    PhysicalPass() = default;
    virtual ~PhysicalPass() {}
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_PHYSICAL_PHYSICAL_PASS_H_
