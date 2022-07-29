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

    const auto* left = bin_expr->GetChild(0);
    const auto* right = bin_expr->GetChild(1);

    node::NodeManager nm;
    auto* left_type = ExtractType(&nm, parser, row, left);
    auto* right_type = ExtractType(&nm, parser, row, right);
    if (left_type == nullptr || right_type == nullptr) {
        return absl::InvalidArgumentError("operand type is null");
    }

    const node::TypeNode* compare_type = nullptr;
    if (left_type->base() == right_type->base()) {
        compare_type = left_type;
    } else {
        auto s = node::ExprNode::InferNumberCastTypes(&nm, left_type, right_type, &compare_type);
        if (!s.isOK()) {
            return absl::InvalidArgumentError(s.GetMsg());
        }
    }

    switch (compare_type->base()) {
        case node::DataType::kBool: {
            return EvalBinaryExpr<bool>(parser, row, bin_expr->GetOp(), left, right);
        }
        case node::DataType::kInt16: {
            return EvalBinaryExpr<int16_t>(parser, row, bin_expr->GetOp(), left, right);
        }
        case node::DataType::kInt32:
        case node::DataType::kDate: {
            return EvalBinaryExpr<int32_t>(parser, row, bin_expr->GetOp(), left, right);
        }
        case node::DataType::kTimestamp:
        case node::DataType::kInt64: {
            return EvalBinaryExpr<int64_t>(parser, row, bin_expr->GetOp(), left, right);
        }
        case node::DataType::kFloat: {
            return EvalBinaryExpr<float>(parser, row, bin_expr->GetOp(), left, right);
        }
        case node::DataType::kDouble: {
            return EvalBinaryExpr<double>(parser, row, bin_expr->GetOp(), left, right);
        }
        case node::DataType::kVarchar: {
            return EvalBinaryExpr<std::string>(parser, row, bin_expr->GetOp(), left, right);
        }
        default:
            break;
    }

    return absl::UnimplementedError(cond->GetExprString());
}

node::TypeNode* ExtractType(node::NodeManager* nm, const RowParser* parser, const codec::Row& row,
                            const node::ExprNode* node) {
    if (node->GetExprType() == node::ExprType::kExprPrimary) {
        const auto* const_node = dynamic_cast<const node::ConstNode*>(node);
        return nm->MakeTypeNode(const_node->GetDataType());
    }
    if (node->GetExprType() == node::ExprType::kExprColumnRef) {
        const auto* column_ref = dynamic_cast<const node::ColumnRefNode*>(node);
        auto* type = nm->MakeTypeNode(node::DataType::kNull);
        if (!codegen::SchemaType2DataType(parser->GetType(*column_ref), type)) {
            LOG(ERROR) << "failed to convert data type";
            return nullptr;
        }
        return type;
    }

    return nullptr;
}

}  // namespace internal
}  // namespace vm
}  // namespace hybridse
