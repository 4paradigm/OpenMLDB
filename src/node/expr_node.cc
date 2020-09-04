/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * expr_node_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/11
 *--------------------------------------------------------------------------
 **/

#include "node/expr_node.h"
#include "codec/fe_row_codec.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "vm/schemas_context.h"
#include "vm/transform.h"

using ::fesql::common::kTypeError;

namespace fesql {
namespace node {

std::atomic<int64_t> ExprIdNode::expr_id_cnt_(0);

Status ColumnRefNode::InferAttr(ExprAnalysisContext* ctx) {
    LOG(WARNING) << "ColumnRef should be transformed out before infer pass";

    const vm::RowSchemaInfo* schema_info = nullptr;
    bool ok = ctx->schemas_context()->ColumnRefResolved(
        relation_name_, column_name_, &schema_info);
    CHECK_TRUE(ok, kTypeError, "Fail to resolve column ", GetExprString());

    codec::RowDecoder decoder(*schema_info->schema_);
    codec::ColInfo col_info;
    CHECK_TRUE(decoder.ResolveColumn(column_name_, &col_info), kTypeError,
               "Fail to resolve column ", GetExprString());

    node::DataType dtype;
    CHECK_TRUE(vm::SchemaType2DataType(col_info.type, &dtype), kTypeError,
               "Fail to convert type: ", col_info.type);

    auto nm = ctx->node_manager();
    SetOutputType(nm->MakeTypeNode(dtype));
    return Status::OK();
}

Status ConstNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(ctx->node_manager()->MakeTypeNode(data_type_));
    if (kNull == data_type_) {
        SetNullable(true);
    } else {
        SetNullable(false);
    }
    return Status::OK();
}

Status CallExprNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(GetFnDef()->GetReturnType());
    return Status::OK();
}

Status ExprIdNode::InferAttr(ExprAnalysisContext* ctx) {
    // var node should be bind outside
    return Status::OK();
}

Status CastExprNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(ctx->node_manager()->MakeTypeNode(cast_type_));
    return Status::OK();
}

Status GetFieldExpr::InferAttr(ExprAnalysisContext* ctx) {
    auto input_type = GetRow()->GetOutputType();
    if (input_type->base() == node::kTuple) {
        try {
            size_t idx = std::stoi(this->GetColumnName());
            CHECK_TRUE(0 <= idx && idx < input_type->GetGenericSize(),
                       kTypeError, "Tuple idx out of range: ", idx);
            SetOutputType(input_type->GetGenericType(idx));
            SetNullable(input_type->IsGenericNullable(idx));
        } catch (std::invalid_argument err) {
            return Status(common::kTypeError,
                          "Invalid Tuple index: " + this->GetColumnName());
        }

    } else if (input_type->base() == node::kRow) {
        auto row_type = dynamic_cast<const RowTypeNode*>(input_type);
        vm::SchemasContext schemas_context(row_type->schema_source());

        const vm::RowSchemaInfo* schema_info = nullptr;
        bool ok = schemas_context.ColumnRefResolved(relation_name_,
                                                    column_name_, &schema_info);
        CHECK_TRUE(ok, kTypeError, "Fail to resolve column ", GetExprString());

        codec::RowDecoder decoder(*schema_info->schema_);
        codec::ColInfo col_info;
        CHECK_TRUE(decoder.ResolveColumn(column_name_, &col_info), kTypeError,
                   "Fail to resolve column ", GetExprString());

        node::DataType dtype;
        CHECK_TRUE(vm::SchemaType2DataType(col_info.type, &dtype), kTypeError,
                   "Fail to convert type: ", col_info.type);

        auto nm = ctx->node_manager();
        SetOutputType(nm->MakeTypeNode(dtype));
    } else {
        return Status(common::kTypeError,
                      "Get field's input is neither tuple nor row");
    }
    return Status::OK();
}

Status WhenExprNode::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 2, kTypeError);
    SetOutputType(then_expr()->GetOutputType());
    SetNullable(false);
    return Status::OK();
}

// Case when 返回类型推断，目前要求所有的then/else的输出类型都一致
// TODO(chenjing, xinqi): case when output type需要作类型兼容
Status CaseWhenExprNode::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 2, kTypeError);
    CHECK_TRUE(when_expr_list()->GetChildNum() > 0, kTypeError);
    const TypeNode* type = nullptr;
    for (auto expr : when_expr_list()->children_) {
        auto expr_type = expr->GetOutputType();
        if (nullptr == type) {
            type = expr_type;
        } else if (expr_type->base() != node::kNull &&
                   type->base() != node::kNull) {
            CHECK_TRUE(
                type->Equals(expr_type), kTypeError,
                "fail infer case when expr attr: then return types and else "
                "return type aren't compatible");
        }
    }
    CHECK_TRUE(nullptr != else_expr(), kTypeError,
               "fail infer case when expr attr: else expr is nullptr");
    CHECK_TRUE(node::IsNullPrimary(else_expr()) ||
                   type->Equals(else_expr()->GetOutputType()),
               kTypeError,
               "fail infer case when expr attr: then return types and else "
               "return type aren't compatible");

    CHECK_TRUE(nullptr != type, kTypeError,
               "fail infer case when expr: output type is null");
    SetOutputType(type);
    SetNullable(true);
    return Status::OK();
}

bool IsSafeCast(const TypeNode* from_type, const TypeNode* target_type) {
    auto from_base = from_type->base();
    auto target_base = target_type->base();
    switch (target_base) {
        case kBool:
            return from_base == kBool;
        case kInt16:
            return from_base == kBool || from_base == kInt16;
        case kInt32:
            return from_base == kBool || from_base == kInt16 ||
                   from_base == kInt32;
        case kInt64:
            return from_base == kBool || from_type->IsInteger();
        case kFloat:
            return from_base == kBool || from_type->IsInteger() ||
                   from_base == kFloat;
        case kDouble:
            return from_base == kBool || from_type->IsArithmetic();
        case kTimestamp:
            return from_type->IsInteger();
        default:
            return false;
    }
}

Status InferBinaryArithmeticType(const TypeNode* left, const TypeNode* right,
                                 const TypeNode** output) {
    CHECK_TRUE(left != nullptr && right != nullptr, kTypeError);
    if (TypeEquals(left, right)) {
        *output = left;
    } else if (IsSafeCast(left, right)) {
        *output = right;
    } else if (IsSafeCast(right, left)) {
        *output = left;
    } else if (left->IsFloating() && right->IsInteger()) {
        *output = left;
    } else if (left->IsInteger() && right->IsFloating()) {
        *output = right;
    } else if (left->base() == node::kNull) {
        *output = right;
    } else if (right->base() == node::kNull) {
        *output = left;
    } else {
        return Status(common::kTypeError,
                      "Incompatible lhs and rhs type: " + left->GetName() +
                          ", " + right->GetName());
    }
    return Status::OK();
}

Status InferBinaryComparisionType(const TypeNode* left, const TypeNode* right,
                                  const TypeNode** output) {
    CHECK_TRUE(left != nullptr && right != nullptr, kTypeError);
    if (TypeEquals(left, right)) {
        *output = left;
    } else if (IsSafeCast(left, right)) {
        *output = right;
    } else if (IsSafeCast(right, left)) {
        *output = left;
    } else if (left->IsFloating() && right->IsInteger()) {
        *output = left;
    } else if (left->IsInteger() && right->IsFloating()) {
        *output = right;
    } else if (left->base() == node::kVarchar) {
        *output = left;
    } else if (right->base() == node::kVarchar) {
        *output = right;
    } else if (left->base() == node::kNull) {
        *output = right;
    } else if (right->base() == node::kNull) {
        *output = left;
    } else {
        return Status(common::kTypeError,
                      "Incompatible lhs and rhs type: " + left->GetName() +
                          ", " + right->GetName());
    }
    return Status::OK();
}

Status BinaryExpr::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 2, kTypeError);
    auto left_type = GetChild(0)->GetOutputType();
    auto right_type = GetChild(1)->GetOutputType();
    bool nullable = GetChild(0)->nullable() || GetChild(1)->nullable();
    CHECK_TRUE(left_type != nullptr && right_type != nullptr, kTypeError);
    switch (GetOp()) {
        case kFnOpAdd:
        case kFnOpMinus:
        case kFnOpMulti:
        case kFnOpMod:
        case kFnOpDiv: {
            const TypeNode* output_type = nullptr;
            CHECK_STATUS(
                InferBinaryArithmeticType(left_type, right_type, &output_type));
            SetOutputType(output_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpFDiv: {
            const TypeNode* output_type = nullptr;
            CHECK_STATUS(
                InferBinaryArithmeticType(left_type, right_type, &output_type));
            SetOutputType(ctx->node_manager()->MakeTypeNode(kDouble));
            SetNullable(nullable);
            break;
        }
        case kFnOpEq:
        case kFnOpNeq:
        case kFnOpLt:
        case kFnOpLe:
        case kFnOpGt:
        case kFnOpGe: {
            const TypeNode* top_type = nullptr;
            CHECK_STATUS(
                InferBinaryComparisionType(left_type, right_type, &top_type));
            SetOutputType(ctx->node_manager()->MakeTypeNode(node::kBool));
            SetNullable(nullable);
            break;
        }
        case kFnOpOr:
        case kFnOpXor:
        case kFnOpAnd: {
            const TypeNode* top_type = nullptr;
            CHECK_STATUS(
                InferBinaryArithmeticType(left_type, right_type, &top_type));
            SetOutputType(ctx->node_manager()->MakeTypeNode(node::kBool));
            SetNullable(nullable);
            break;
        }
        case kFnOpAt: {
            CHECK_TRUE(left_type->base() == kList, kTypeError,
                       "At op should take list input");
            CHECK_TRUE(right_type->IsInteger(), kTypeError,
                       "At index should be integer");
            SetOutputType(left_type->GetGenericType(0));
            SetNullable(left_type->IsGenericNullable(0) || nullable);
            break;
        }
        default:
            return Status(common::kTypeError,
                          "Unknown binary op type: " + ExprOpTypeName(GetOp()));
    }
    return Status::OK();
}

Status UnaryExpr::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 1, kTypeError);
    auto dtype = GetChild(0)->GetOutputType();
    bool nullable = GetChild(0)->nullable();
    CHECK_TRUE(dtype != nullptr, kTypeError);
    switch (GetOp()) {
        case kFnOpNot: {
            CHECK_TRUE(dtype->IsArithmetic() || dtype->base() == kBool,
                       kTypeError,
                       "Invalid unary predicate type: ", dtype->GetName());
            SetOutputType(ctx->node_manager()->MakeTypeNode(node::kBool));
            SetNullable(nullable);
            break;
        }
        case kFnOpMinus: {
            CHECK_TRUE(dtype->IsArithmetic(), kTypeError,
                       "Invalid unary type for minus: ", dtype->GetName());
            SetOutputType(dtype);
            SetNullable(nullable);
            break;
        }
        case kFnOpBracket: {
            SetOutputType(dtype);
            SetNullable(nullable);
            break;
        }
        case kFnOpIsNull: {
            SetOutputType(ctx->node_manager()->MakeTypeNode(node::kBool));
            SetNullable(false);
            break;
        }
        case kFnOpNonNull: {
            SetOutputType(dtype);
            SetNullable(false);
            break;
        }
        default:
            return Status(common::kTypeError,
                          "Unknown unary op type: " + ExprOpTypeName(GetOp()));
    }
    return Status::OK();
}

Status CondExpr::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetCondition() != nullptr &&
                   GetCondition()->GetOutputType() != nullptr &&
                   GetCondition()->GetOutputType()->base() == node::kBool,
               kTypeError, "Condition must be boolean type");
    CHECK_TRUE(GetLeft() != nullptr && GetRight() != nullptr, kTypeError);
    auto left_type = GetLeft()->GetOutputType();
    auto right_type = GetRight()->GetOutputType();
    CHECK_TRUE(left_type != nullptr, kTypeError, kTypeError,
               "Unknown cond left type");
    CHECK_TRUE(right_type != nullptr, kTypeError, kTypeError,
               "Unknown cond right type");
    CHECK_TRUE(TypeEquals(left_type, right_type), kTypeError,
               "Condition's left and right type do not match: ",
               left_type->GetName(), " : ", right_type->GetName());
    this->SetOutputType(left_type);
    this->SetNullable(GetLeft()->nullable() || GetRight()->nullable());
    return Status::OK();
}

}  // namespace node
}  // namespace fesql
