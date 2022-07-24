// Copyright 2022 4Paradigm Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: eval.h
// -----------------------------------------------------------------------------
//
// Defines some runner evaluation related helper functions.
// Used by 'vm/runner.{h, cc}' where codegen evaluation is skiped,
// likely in long window runner nodes
//
// -----------------------------------------------------------------------------

#ifndef HYBRIDSE_SRC_VM_INTERNAL_EVAL_H_
#define HYBRIDSE_SRC_VM_INTERNAL_EVAL_H_

#include "node/node_enum.h"

namespace hybridse {
namespace vm {

template <typename T>
bool EvalSimpleBinaryExpr(node::FnOperator op, const T& lhs, const T& rhs) {
    switch (op) {
        case node::FnOperator::kFnOpLt:
            return lhs < rhs;
        case node::FnOperator::kFnOpLe:
            return lhs <= rhs;
        case node::FnOperator::kFnOpGt:
            return lhs > rhs;
        case node::FnOperator::kFnOpGe:
            return lhs >= rhs;
        case node::FnOperator::kFnOpEq:
            return lhs == rhs;
        case node::FnOperator::kFnOpNeq:
            return lhs != rhs;
        default:
            break;
    }

    return false;
}

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_INTERNAL_EVAL_H_
