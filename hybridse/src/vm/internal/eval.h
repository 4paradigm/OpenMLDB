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

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "codec/row.h"
#include "node/expr_node.h"
#include "node/node_enum.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace vm {
namespace internal {

// extract value from expr node
// limited implementation since it only expect node one of
// * ColumnRefNode
// * ConstNode
template <typename T>
absl::StatusOr<std::optional<T>> ExtractValue(const RowParser* parser, const codec::Row& row,
                                              const node::ExprNode* node) {
    if (node->GetExprType() == node::ExprType::kExprPrimary) {
        const auto* const_node = dynamic_cast<const node::ConstNode*>(node);
        return const_node->GetAs<T>();
    }

    if (node->GetExprType() == node::ExprType::kExprColumnRef) {
        const auto* column_ref = dynamic_cast<const node::ColumnRefNode*>(node);
        if (parser->IsNull(row, *column_ref)) {
            std::optional<T> r = {};
            return r;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            std::string data;
            if (0 == parser->GetString(row, *column_ref, &data)) {
                return data;
            }
        } else if constexpr (std::is_same_v<T, bool>) {
            bool v = false;
            if (0 == parser->GetValue(row, *column_ref, type::kBool, &v)) {
                return v;
            }
        } else if constexpr (std::is_same_v<T, int16_t>) {
            int16_t v = 0;
            if (0 == parser->GetValue(row, *column_ref, type::kInt16, &v)) {
                return v;
            }
        } else if constexpr (std::is_same_v<T, int32_t>) {
            int32_t v = 0;
            if (0 == parser->GetValue(row, *column_ref, type::kInt32, &v)) {
                return v;
            }
        } else if constexpr (std::is_same_v<T, int64_t>) {
            int64_t v = 0;
            if (0 == parser->GetValue(row, *column_ref, type::kInt64, &v)) {
                return v;
            }
        } else if constexpr (std::is_same_v<T, float>) {
            float v = 0.0;
            if (0 == parser->GetValue(row, *column_ref, type::kFloat, &v)) {
                return v;
            }
        } else if constexpr (std::is_same_v<T, double>) {
            double v = 0.0;
            if (0 == parser->GetValue(row, *column_ref, type::kDouble, &v)) {
                return v;
            }
        }
    }

    return absl::UnimplementedError(absl::StrCat("invalid node: ", node->GetExprString()));
}

template <typename T>
std::optional<bool> EvalSimpleBinaryExpr(node::FnOperator op, const std::optional<T>& lhs,
                                         const std::optional<T>& rhs) {
    if (!lhs.has_value() || !rhs.has_value()) {
        std::optional<bool> r = {};
        return r;
    }

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

    std::optional<bool> r = {};
    return r;
}

template <typename T>
absl::StatusOr<std::optional<bool>> EvalBinaryExpr(const RowParser* parser, const codec::Row& row, node::FnOperator op,
                                                   const node::ExprNode* lhs, const node::ExprNode* rhs) {
    absl::Status ret = absl::OkStatus();
    auto ls = ExtractValue<T>(parser, row, lhs);
    auto rs = ExtractValue<T>(parser, row, rhs);
    ret.Update(ls.status());
    ret.Update(rs.status());
    if (ret.ok()) {
        return EvalSimpleBinaryExpr<T>(op, ls.value(), rs.value());
    }

    return ret;
}

// evaluate the condition expr node
//
// implementation is limited
// * only assume `cond` as `BinaryExprNode`, and supports six basic compassion operators
//
// returns compassion result
// * true/false/NULL
// * invalid input -> InvalidStatus
absl::StatusOr<std::optional<bool>> EvalCond(const RowParser* parser, const codec::Row& row,
                                             const node::ExprNode* cond);

// evaluate the condition expr same as `EvalCond`
// but inputed `row` and schema is from pre-agg table.
// The expr is also only supported as Binary Expr as 'col < constant', but col name to the
// pre-agg table is already defined as 'filter_key', instead taken from ColumnRefNode kid of binary expr node
//
// * type infer is not smart as `EvalCond` because it just use type of const node
absl::StatusOr<std::optional<bool>> EvalCondWithAggRow(const RowParser* parser, const codec::Row& row,
                                                       const node::ExprNode* cond, absl::string_view filter_col_name);

// extract type of the expr node
//
// why not use ExprNode::GetOutputType ?
// usually it will be null because the infered type node is saved in other place
// (FnComponent) that can't access from input expr node
node::TypeNode* ExtractType(node::NodeManager* nm, const RowParser* parser, const codec::Row& row,
                            const node::ExprNode* node);

}  // namespace internal
}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_INTERNAL_EVAL_H_
