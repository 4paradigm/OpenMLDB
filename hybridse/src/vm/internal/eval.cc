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

#include "vm/internal/eval.h"

#include <string>

#include "codegen/ir_base_builder.h"
#include "node/node_manager.h"

namespace hybridse {
namespace vm {
namespace internal {

absl::StatusOr<std::optional<bool>> EvalCond(const RowParser* parser, const codec::Row& row,
                                             const node::ExprNode* cond) {
    const auto* bin_expr = dynamic_cast<const node::BinaryExpr*>(cond);
    if (bin_expr == nullptr) {
        return absl::InvalidArgumentError("can't evaluate expr other than binary expr");
    }

    auto tp = ExtractCompareType(parser, bin_expr);
    if (!tp.ok()) {
        return tp.status();
    }

    const auto* left = bin_expr->GetChild(0);
    const auto* right = bin_expr->GetChild(1);

    switch (tp.value()) {
        case type::kBool: {
            return EvalBinaryExpr<bool>(parser, row, bin_expr->GetOp(), left, right);
        }
        case type::kInt16: {
            return EvalBinaryExpr<int16_t>(parser, row, bin_expr->GetOp(), left, right);
        }
        case type::kInt32:
        case type::kDate: {
            return EvalBinaryExpr<int32_t>(parser, row, bin_expr->GetOp(), left, right);
        }
        case type::kTimestamp:
        case type::kInt64: {
            return EvalBinaryExpr<int64_t>(parser, row, bin_expr->GetOp(), left, right);
        }
        case type::kFloat: {
            return EvalBinaryExpr<float>(parser, row, bin_expr->GetOp(), left, right);
        }
        case type::kDouble: {
            return EvalBinaryExpr<double>(parser, row, bin_expr->GetOp(), left, right);
        }
        case type::kVarchar: {
            return EvalBinaryExpr<std::string>(parser, row, bin_expr->GetOp(), left, right);
        }
        default:
            break;
    }

    return absl::UnimplementedError(cond->GetExprString());
}

absl::StatusOr<std::optional<bool>> EvalCondWithAggRow(const RowParser* parser, const codec::Row& row,
                                                       const node::ExprNode* cond, absl::string_view filter_col_name) {
    const auto* bin_expr = dynamic_cast<const node::BinaryExpr*>(cond);
    if (bin_expr == nullptr) {
        return absl::InvalidArgumentError("can't evaluate expr other than binary expr");
    }

    std::string filter = std::string(filter_col_name);

    // if value of filter_col_name is NULL
    if (parser->IsNull(row, filter)) {
        return std::nullopt;
    }

    std::string filter_val;
    parser->GetString(row, filter, &filter_val);

    const auto* left = bin_expr->GetChild(0);
    const auto* right = bin_expr->GetChild(1);
    node::DataType op_type;

    if (left->GetExprType() == node::kExprColumnRef) {
        auto* const_node = dynamic_cast<const node::ConstNode*>(right);
        if (const_node == nullptr) {
            return absl::InvalidArgumentError("expect right node as const node for evaluation");
        }
        op_type = const_node->GetDataType();
        if (const_node->IsNull()) {
            return std::nullopt;
        }

        switch (op_type) {
            case node::DataType::kBool: {
                bool v;
                if (!absl::SimpleAtob(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to bool"));
                }
                return EvalSimpleBinaryExpr<bool>(bin_expr->GetOp(), v,
                                                  const_node->GetAs<bool>().value_or(std::nullopt));
            }
            case node::DataType::kInt16: {
                int32_t v;
                if (!absl::SimpleAtoi<int32_t>(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to int32_t"));
                }
                return EvalSimpleBinaryExpr<int16_t>(bin_expr->GetOp(), static_cast<int16_t>(v),
                                                     const_node->GetAs<int16_t>().value_or(std::nullopt));
            }
            case node::DataType::kInt32:
            case node::DataType::kDate: {
                int32_t v;
                if (!absl::SimpleAtoi<int32_t>(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to int32_t"));
                }
                return EvalSimpleBinaryExpr<int32_t>(bin_expr->GetOp(), v,
                                                     const_node->GetAs<int32_t>().value_or(std::nullopt));
            }
            case node::DataType::kTimestamp:
            case node::DataType::kInt64: {
                int64_t v;
                if (!absl::SimpleAtoi<int64_t>(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to int64_t"));
                }
                return EvalSimpleBinaryExpr<int64_t>(bin_expr->GetOp(), v,
                                                     const_node->GetAs<int64_t>().value_or(std::nullopt));
            }
            case node::DataType::kFloat: {
                float v;
                if (!absl::SimpleAtof(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to flat"));
                }
                return EvalSimpleBinaryExpr<float>(bin_expr->GetOp(), v,
                                                   const_node->GetAs<float>().value_or(std::nullopt));
            }
            case node::DataType::kDouble: {
                double v;
                if (!absl::SimpleAtod(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to double"));
                }
                return EvalSimpleBinaryExpr<double>(bin_expr->GetOp(), v,
                                                    const_node->GetAs<double>().value_or(std::nullopt));
            }
            case node::DataType::kVarchar: {
                return EvalSimpleBinaryExpr<std::string>(bin_expr->GetOp(), filter_val,
                                                         const_node->GetAs<std::string>().value_or(std::nullopt));
            }
            default:
                break;
        }
    } else if (right->GetExprType() == node::kExprColumnRef) {
        auto* const_node = dynamic_cast<const node::ConstNode*>(left);
        if (const_node == nullptr) {
            return absl::InvalidArgumentError("expect left node as const node for evaluation");
        }
        op_type = const_node->GetDataType();

        if (const_node->IsNull()) {
            return std::nullopt;
        }

        switch (op_type) {
            case node::DataType::kBool: {
                bool v;
                if (!absl::SimpleAtob(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to bool"));
                }
                return EvalSimpleBinaryExpr<bool>(bin_expr->GetOp(), const_node->GetAs<bool>().value_or(std::nullopt),
                                                  v);
            }
            case node::DataType::kInt16: {
                int32_t v;
                if (!absl::SimpleAtoi<int32_t>(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to int32_t"));
                }
                return EvalSimpleBinaryExpr<int16_t>(
                    bin_expr->GetOp(), const_node->GetAs<int16_t>().value_or(std::nullopt), static_cast<int16_t>(v));
            }
            case node::DataType::kInt32:
            case node::DataType::kDate: {
                int32_t v;
                if (!absl::SimpleAtoi<int32_t>(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to int32_t"));
                }
                return EvalSimpleBinaryExpr<int32_t>(bin_expr->GetOp(),
                                                     const_node->GetAs<int32_t>().value_or(std::nullopt), v);
            }
            case node::DataType::kTimestamp:
            case node::DataType::kInt64: {
                int64_t v;
                if (!absl::SimpleAtoi<int64_t>(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to int64_t"));
                }
                return EvalSimpleBinaryExpr<int64_t>(bin_expr->GetOp(),
                                                     const_node->GetAs<int64_t>().value_or(std::nullopt), v);
            }
            case node::DataType::kFloat: {
                float v;
                if (!absl::SimpleAtof(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to flat"));
                }
                return EvalSimpleBinaryExpr<float>(bin_expr->GetOp(), const_node->GetAs<float>().value_or(std::nullopt),
                                                   v);
            }
            case node::DataType::kDouble: {
                double v;
                if (!absl::SimpleAtod(filter_val, &v)) {
                    return absl::InvalidArgumentError(absl::StrCat("can't cast ", filter_val, " to double"));
                }
                return EvalSimpleBinaryExpr<double>(bin_expr->GetOp(),
                                                    const_node->GetAs<double>().value_or(std::nullopt), v);
            }
            case node::DataType::kVarchar: {
                return EvalSimpleBinaryExpr<std::string>(
                    bin_expr->GetOp(), const_node->GetAs<std::string>().value_or(std::nullopt), filter_val);
            }
            default:
                break;
        }
    }

    return absl::InvalidArgumentError(absl::StrCat("unsupport binary op: ", cond->GetExprString()));
}

absl::StatusOr<type::Type> ExtractCompareType(const RowParser* parser, const node::BinaryExpr* node) {
    if (node->GetChild(0)->GetExprType() == node::kExprColumnRef &&
        node->GetChild(1)->GetExprType() == node::kExprPrimary) {
        return parser->GetType(*dynamic_cast<const node::ColumnRefNode*>(node->GetChild(0)));
    }
    if (node->GetChild(1)->GetExprType() == node::kExprColumnRef &&
        node->GetChild(0)->GetExprType() == node::kExprPrimary) {
        return parser->GetType(*dynamic_cast<const node::ColumnRefNode*>(node->GetChild(1)));
    }

    return absl::UnimplementedError(absl::StrCat("Evaluating type for binary expr '", node->GetExprString()));
}


}  // namespace internal
}  // namespace vm
}  // namespace hybridse
