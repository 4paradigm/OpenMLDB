/**
 * Copyright (c) 2023 OpenMLDB authors.
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

// Definitions for expression registered builtin functions.
// You may register expr udfs via any of:
// - RegisterExprUdf
// - RegisterExprUdfTemplate

#ifndef HYBRIDSE_SRC_UDF_DEFAULT_DEFS_EXPR_DEF_H_
#define HYBRIDSE_SRC_UDF_DEFAULT_DEFS_EXPR_DEF_H_

#include <tuple>
#include "udf/udf_library.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {
namespace container {

// radians
template <typename T>
struct RadiansDef {
    using Args = std::tuple<T>;

    node::ExprNode* operator()(::hybridse::udf::UdfResolveContext* ctx, node::ExprNode* operand) {
        auto* nm = ctx->node_manager();
        // operand / 180.0 * M_PI;
        return nm->MakeBinaryExprNode(
            nm->MakeConstNode(M_PI),
            nm->MakeBinaryExprNode(operand, nm->MakeConstNode(180.0), node::FnOperator::kFnOpFDiv),
            node::FnOperator::kFnOpMulti);
    }
};

}  // namespace container
}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_DEFAULT_DEFS_EXPR_DEF_H_
