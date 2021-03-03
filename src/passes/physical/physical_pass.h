/*
 * physical_pass.h
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
