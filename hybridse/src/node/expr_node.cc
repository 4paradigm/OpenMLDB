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

#include "node/expr_node.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "codec/fe_row_codec.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "passes/expression/expr_pass.h"
#include "passes/resolve_fn_and_attrs.h"
#include "vm/schemas_context.h"
#include "codegen/ir_base_builder.h"

using ::hybridse::common::kTypeError;

namespace hybridse {
namespace node {

Status ColumnRefNode::InferAttr(ExprAnalysisContext* ctx) {
    DLOG(WARNING) << "ColumnRef should be transformed out before infer pass";
    auto schemas_ctx = ctx->schemas_context();
    size_t schema_idx;
    size_t col_idx;
    CHECK_STATUS(
        schemas_ctx->ResolveColumnRefIndex(this, &schema_idx, &col_idx),
        "Fail to resolve column ", GetExprString());
    auto& col = schemas_ctx->GetSchema(schema_idx)->Get(col_idx);
    type::ColumnSchema sc;
    if (col.has_schema()) {
        sc = col.schema();
    } else {
        sc.set_base_type(col.type());
    }
    auto nm = ctx->node_manager();
    auto s = codegen::ColumnSchema2Type(sc, nm);
    CHECK_TRUE(s.ok(), kTypeError, s.status());
    SetOutputType(nm->MakeNode<node::TypeNode>(node::kList, s.value()));
    return Status::OK();
}

Status ColumnIdNode::InferAttr(ExprAnalysisContext* ctx) {
    auto schemas_ctx = ctx->schemas_context();
    size_t schema_idx;
    size_t col_idx;
    CHECK_STATUS(schemas_ctx->ResolveColumnIndexByID(this->GetColumnID(),
                                                     &schema_idx, &col_idx),
                 "Fail to resolve column ", GetExprString());
    auto& col = schemas_ctx->GetSchema(schema_idx)->Get(col_idx);
    type::ColumnSchema sc;
    if (col.has_schema()) {
        sc = col.schema();
    } else {
        sc.set_base_type(col.type());
    }
    auto nm = ctx->node_manager();
    auto s = codegen::ColumnSchema2Type(sc, nm);
    CHECK_TRUE(s.ok(), kTypeError, s.status());
    SetOutputType(nm->MakeNode<node::TypeNode>(node::kList, s.value()));
    return Status::OK();
}
Status ParameterExpr::InferAttr(ExprAnalysisContext *ctx) {
    CHECK_TRUE(nullptr != ctx->parameter_types(), common::kTypeError,
               "Fail to get parameter type with NULL parameter types")
    CHECK_TRUE(position() > 0 &&  position() <= ctx->parameter_types()->size(), common::kTypeError,
               "Fail to get parameter type with position ", position())
    auto& col = ctx->parameter_types()->Get(position()-1);
    type::ColumnSchema sc;
    if (col.has_schema()) {
        sc = col.schema();
    } else {
        sc.set_base_type(col.type());
    }
    auto s = codegen::ColumnSchema2Type(sc, ctx->node_manager());
    CHECK_TRUE(s.ok(), kTypeError, s.status());
    SetOutputType(s.value());
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

bool CallExprNode::RequireListAt(ExprAnalysisContext* ctx, size_t index) const {
    return GetFnDef()->RequireListAt(ctx, index);
}

bool CallExprNode::IsListReturn(ExprAnalysisContext* ctx) const {
    return GetFnDef()->IsListReturn(ctx);
}
Status ExprIdNode::InferAttr(ExprAnalysisContext* ctx) {
    // var node should be bind outside
    CHECK_TRUE(this->GetOutputType() != nullptr, kTypeError,
               this->GetExprString(), "  should get type binding before infer");
    return Status::OK();
}

node::DataType CastExprNode::base_cast_type() const { return cast_type_->base(); }
Status CastExprNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(cast_type_);
    return Status::OK();
}

Status GetFieldExpr::InferAttr(ExprAnalysisContext* ctx) {
    auto input_type = GetRow()->GetOutputType();
    if (input_type->base() == node::kTuple) {
        try {
            size_t idx = GetColumnID();
            CHECK_TRUE(0 <= idx && idx < input_type->GetGenericSize(),
                       kTypeError, "Tuple idx out of range: ", idx);
            SetOutputType(input_type->GetGenericType(idx));
            SetNullable(input_type->IsGenericNullable(idx));
        } catch (std::invalid_argument& err) {
            return Status(common::kTypeError,
                          "Invalid Tuple index: " + this->GetColumnName());
        }

    } else if (input_type->base() == node::kRow) {
        auto row_type = input_type->GetAsOrNull<RowTypeNode>();
        const auto schemas_context = row_type->schemas_ctx();

        size_t schema_idx;
        size_t col_idx;
        CHECK_STATUS(schemas_context->ResolveColumnIndexByID(
                         GetColumnID(), &schema_idx, &col_idx),
                     "Fail to resolve column ", GetExprString());

        auto& col = schemas_context->GetSchema(schema_idx)->Get(col_idx);
        type::ColumnSchema sc;
        if (col.has_schema()) {
            sc = col.schema();
        } else {
            sc.set_base_type(col.type());
        }
        auto s = codegen::ColumnSchema2Type(sc, ctx->node_manager());
        CHECK_TRUE(s.ok(), kTypeError, s.status());

        SetOutputType(s.value());
        SetNullable(true);
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
Status CaseWhenExprNode::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 2, kTypeError);
    CHECK_TRUE(when_expr_list()->GetChildNum() > 0, kTypeError);

    // get compatiable type for [when list] and else
    auto* nm = ctx->node_manager();
    const TypeNode* out_type = nm->MakeTypeNode(DataType::kNull);
    for (auto expr : when_expr_list()->children_) {
        auto expr_type = expr->GetOutputType();
        auto res = CompatibleType(nm, out_type, expr_type);
        CHECK_TRUE(res.ok(), kTypeError, res.status());
        out_type = res.value();
    }
    CHECK_TRUE(nullptr != else_expr(), kTypeError,
               "fail infer case when expr attr: else expr is nullptr");
    auto res = CompatibleType(nm, out_type, else_expr()->GetOutputType());
    CHECK_TRUE(res.ok(), kTypeError, res.status());
    out_type = res.value();

    SetOutputType(out_type);
    SetNullable(true);
    return Status::OK();
}

Status ExprNode::IsCastAccept(node::NodeManager* nm, const TypeNode* src,
                              const TypeNode* dist, const TypeNode** output) {
    CHECK_TRUE(src != nullptr && dist != nullptr, kTypeError);
    if (TypeEquals(src, dist) || IsSafeCast(src, dist)) {
        *output = dist;
        return Status::OK();
    }

    if (src->IsDate() && dist->IsNumber() && !dist->IsBool()) {
        return Status(
            common::kCodegenError,
            "incastable from " + src->GetName() + " to " + dist->GetName());
    }

    if (src->IsNumber() && dist->IsDate()) {
        return Status(
            common::kCodegenError,
            "incastable from " + src->GetName() + " to " + dist->GetName());
    }
    *output = dist;
    return Status::OK();
}

// this handles compatible type when both lhs and rhs are basic types
// composited types like array, list, tuple are not handled correctly, so do not expect the function to handle those
absl::StatusOr<const TypeNode*> ExprNode::CompatibleType(NodeManager* nm, const TypeNode* lhs, const TypeNode* rhs) {
    if (*lhs == *rhs) {
        // include Null = Null
        return rhs;
    }

    if (lhs->base() == kVoid && rhs->base() == kNull) {
        return lhs;
    }

    if (lhs->base() == kNull && rhs->base() == kVoid) {
        return rhs;
    }

    if (lhs->IsNull()) {
        // NULL/VOID + T -> T
        return rhs;
    }
    if (rhs->IsNull()) {
        // T + NULL/VOID -> T
        return lhs;
    }

    if (IsSafeCast(lhs, rhs)) {
        return rhs;
    }
    if (IsSafeCast(rhs, lhs)) {
        return lhs;
    }
    if (IsIntFloat2PointerCast(lhs, rhs)) {
        // rhs is float while lhs is 64bit
        if (rhs->base() == kFloat && (lhs->base() == kInt64 || lhs->base() == kDouble)) {
            return nm->MakeTypeNode(kDouble);
        }

        return rhs;
    }

    if (IsIntFloat2PointerCast(rhs, lhs)) {
        if ((rhs->base() == kInt64 || rhs->base() == kDouble) && lhs->base() == kFloat) {
            return nm->MakeTypeNode(kDouble);
        }
        return lhs;
    }

    if (lhs->IsBaseOrNullType() && rhs->IsBaseOrNullType()) {
        // both casting to string as a fallback
        return nm->MakeTypeNode(kVarchar);
    }

    // for composited types, there is no fallback type, requires exact match
    return absl::InvalidArgumentError(absl::Substitute(
        "no compatiable type: composited type $0 and $1 requires exact match", lhs->DebugString(), rhs->DebugString()));
}

/**
* support rules:
* 1. case target_type
*    bool            -> from_type is bool
*    intXX           -> from_type is bool or from_type is equal/smaller integral type
*    float | double  -> from_type is bool or equal/smaller float type
*    timestamp       -> from_type is timestamp or integral type
*    string | date   -> not convertible from other type
*    MAP<KEY, VALUE> ->
*      from_type: MAP<VOID, VOID> (consturct by map()) -> OK
*      from_type: MAP<K, V> -> SafeCast(K -> KEY) && SafeCast(V -> VALUE)
*
* 2. from_type of NOT_NULL = false can not cast to target_type of NOT_NULL = True
*    TODO(someone): TypeNode should contains NOT_NULL ATtribute.
*/
bool ExprNode::IsSafeCast(const TypeNode* from_type,
                          const TypeNode* target_type) {
    if (from_type == nullptr || target_type == nullptr) {
        return false;
    }
    if (from_type->IsNull()) {
        // VOID -> T
        return true;
    }
    if (TypeEquals(from_type, target_type)) {
        return true;
    }
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
            return from_base == kBool || from_base == kFloat;
        case kDouble:
            return from_base == kBool || from_type->IsFloating();
        case kTimestamp:
            return from_base == kTimestamp || from_type->IsInteger();
        default:
            break;
    }
    return false;
}

bool ExprNode::IsIntFloat2PointerCast(const TypeNode* lhs,
                                      const TypeNode* rhs) {
    return lhs->IsNumber() && rhs->IsFloating();
}
Status ExprNode::InferNumberCastTypes(node::NodeManager* nm,
                                      const TypeNode* left_type,
                                      const TypeNode* right_type,
                                      const TypeNode** output_type) {
    CHECK_TRUE(left_type->IsNumber() && right_type->IsNumber(), kTypeError,
               "Fail to infer number types: invalid types ",
               left_type->GetName(), ", ", right_type->GetName())

    if (IsSafeCast(left_type, right_type)) {
        *output_type = right_type;
    } else if (IsSafeCast(right_type, left_type)) {
        *output_type = left_type;
    } else if (IsIntFloat2PointerCast(left_type, right_type)) {
        *output_type = right_type;
    } else if (IsIntFloat2PointerCast(right_type, left_type)) {
        *output_type = left_type;
    } else {
        return base::Status(kTypeError,
                            "Fail cast numbers, types aren't compatible:" +
                                left_type->GetName() + ", " +
                                right_type->GetName());
    }
    return Status::OK();
}
Status ExprNode::AndTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                               const TypeNode* rhs,
                               const TypeNode** output_type) {
    CHECK_TRUE(lhs->IsInteger() && rhs->IsInteger(), kTypeError,
               "Invalid Bit-And type: lhs ", lhs->GetName(), " rhs ",
               rhs->GetName())

    CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    return Status::OK();
}
Status ExprNode::LShiftTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                                  const TypeNode* rhs,
                                  const TypeNode** output_type) {
    CHECK_TRUE(lhs->IsInteger() && rhs->IsInteger(), kTypeError,
               "Invalid lshift type: lhs ", lhs->GetName(), " rhs ",
               rhs->GetName())

    CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    return Status::OK();
}

// Accept rules:
// 1. timestamp + timestamp
// 2. interger + timestamp
// 3. timestamp + integer
// 4. number + number
// 5. same tuple<number, number, ..> types can be added together
Status ExprNode::AddTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                               const TypeNode* rhs,
                               const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE(!lhs->IsTuple() && !rhs->IsTuple(), kTypeError);
    CHECK_TRUE(
        (lhs->IsNull() || lhs->IsNumber() || lhs->IsTimestamp()) &&
            (rhs->IsNull() || rhs->IsNumber() || rhs->IsTimestamp()),
        kTypeError,
        "Invalid Sub Op type: lhs " + lhs->GetName() + " rhs " + rhs->GetName())
    if (lhs->IsTupleNumbers() || rhs->IsTupleNumbers()) {
        CHECK_TRUE(TypeEquals(lhs, rhs), kTypeError,
                   "Invalid Add Op type: lhs " + lhs->GetName() + " rhs " +
                       rhs->GetName())
        *output_type = lhs;
    } else if (lhs->IsNull()) {
        *output_type = rhs;
    } else if (rhs->IsNull()) {
        *output_type = lhs;
    } else if (lhs->IsTimestamp() && rhs->IsTimestamp()) {
        *output_type = lhs;
    } else if (lhs->IsTimestamp() && rhs->IsInteger()) {
        *output_type = lhs;
    } else if (lhs->IsInteger() && rhs->IsTimestamp()) {
        *output_type = rhs;
    } else if (lhs->IsNumber() && rhs->IsNumber()) {
        CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    } else {
        return Status(kTypeError, "Invalid Add Op type: lhs " + lhs->GetName() +
                                      " rhs " + rhs->GetName());
    }
    return Status::OK();
}
// Accept rules:
// 1. timestamp - interger
// 2. number - number
Status ExprNode::SubTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                               const TypeNode* rhs,
                               const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE(
        (lhs->IsNull() || lhs->IsNumber() || lhs->IsTimestamp()) &&
            (rhs->IsNull() || rhs->IsNumber() || rhs->IsTimestamp()),
        kTypeError,
        "Invalid Sub Op type: lhs " + lhs->GetName() + " rhs " + rhs->GetName())
    if (lhs->IsNull()) {
        *output_type = rhs;
    } else if (rhs->IsNull()) {
        *output_type = lhs;
    } else if (lhs->IsTimestamp() && rhs->IsTimestamp()) {
        *output_type = lhs;
    } else if (lhs->IsTimestamp() && rhs->IsInteger()) {
        *output_type = lhs;
    } else if (lhs->IsNumber() && rhs->IsNumber()) {
        CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    } else {
        return Status(kTypeError, "Invalid Sub Op type: lhs " + lhs->GetName() +
                                      " rhs " + rhs->GetName());
    }
    return Status::OK();
}
Status ExprNode::MultiTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                                 const TypeNode* rhs,
                                 const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE((lhs->IsNull() || lhs->IsNumber()) &&
                   (rhs->IsNull() || rhs->IsNumber()),
               kTypeError,
               "Invalid Multi Op type: lhs " + lhs->GetName() + " rhs " +
                   rhs->GetName())
    if (lhs->IsNull()) {
        *output_type = rhs;
    } else if (rhs->IsNull()) {
        *output_type = lhs;
    } else {
        CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    }
    return Status::OK();
}
Status ExprNode::FDivTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                                const TypeNode* rhs,
                                const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE((lhs->IsNull() || lhs->IsNumber() || lhs->IsTimestamp()) &&
                   (rhs->IsNull() || rhs->IsNumber()),
               kTypeError,
               "Invalid FDiv Op type: lhs " + lhs->GetName() + " rhs " +
                   rhs->GetName())
    *output_type = nm->MakeTypeNode(kDouble);
    return Status::OK();
}
Status ExprNode::SDivTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                                const TypeNode* rhs,
                                const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE((lhs->IsNull() || lhs->IsInteger()) &&
                   (rhs->IsNull() || rhs->IsInteger()),
               kTypeError, "Invalid SDiv type: lhs ", lhs->GetName(), " rhs ",
               rhs->GetName())
    if (lhs->IsNull()) {
        *output_type = rhs;
    } else if (rhs->IsNull()) {
        *output_type = lhs;
    } else {
        CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    }
    return Status::OK();
}
Status ExprNode::ModTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                               const TypeNode* rhs,
                               const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE((lhs->IsNull() || lhs->IsNumber()) &&
                   (rhs->IsNull() || rhs->IsNumber()),
               kTypeError, "Invalid Mod type: lhs ", lhs->GetName(), " rhs ",
               rhs->GetName())
    if (lhs->IsNull()) {
        *output_type = rhs;
    } else if (rhs->IsNull()) {
        *output_type = lhs;
    } else {
        CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type))
    }
    return Status::OK();
}

Status ExprNode::NotTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                               const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr, kTypeError);
    CHECK_TRUE(lhs->IsNull() || lhs->IsBaseType(), kTypeError,
               "Invalid Mod type: lhs ", lhs->GetName())

    *output_type = nm->MakeTypeNode(kBool);
    return Status::OK();
}

Status ExprNode::CompareTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                                   const TypeNode* rhs,
                                   const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE(!lhs->IsTuple() && !rhs->IsTuple(), kTypeError);
    CHECK_TRUE((lhs->IsNull() || lhs->IsBaseType()) &&
                   (rhs->IsNull() || rhs->IsBaseType()),
               kTypeError, "Invalid Compare Op type: lhs ", lhs->GetName(),
               " rhs ", rhs->GetName())

    if (lhs->IsNull() || rhs->IsNull()) {
        *output_type = nm->MakeTypeNode(kBool);
    } else if (lhs->IsNumber() && rhs->IsNumber()) {
        *output_type = nm->MakeTypeNode(kBool);
    } else if (lhs->IsString() || rhs->IsString()) {
        *output_type = nm->MakeTypeNode(kBool);
    } else if (TypeEquals(lhs, rhs)) {
        *output_type = nm->MakeTypeNode(kBool);
    } else {
        return Status(kTypeError, "Invalid Compare Op type: lhs " +
                                      lhs->GetName() + " rhs " +
                                      rhs->GetName());
    }
    return Status::OK();
}
Status ExprNode::LogicalOpTypeAccept(node::NodeManager* nm, const TypeNode* lhs,
                                     const TypeNode* rhs,
                                     const TypeNode** output_type) {
    CHECK_TRUE((lhs->IsNull() || lhs->IsBaseType()) &&
                   (rhs->IsNull() || rhs->IsBaseType()),
               kTypeError, "Invalid Logical Op type: lhs ", lhs->GetName(),
               " rhs ", rhs->GetName())
    *output_type = nm->MakeTypeNode(kBool);
    return Status::OK();
}

/**
 * Bitwise Logical Operators, which is
 *   - bitwise NOT: `~ rhs`
 *   - bitwise AND: `lhs & rhs`
 *   - bitwise OR:  `lhs & rhs`
 *   - bitwise XOR: `lhs ^ rhs`
 * Rules:
 *  only accept NULL, int64, int32, int16
 */
Status ExprNode::BitwiseLogicalTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* rhs,
                                          const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError, "lhs and rhs must not null");
    CHECK_TRUE((lhs->IsNull() || lhs->IsIntegral()) && (rhs->IsNull() || rhs->IsIntegral()), kTypeError,
               "Invalid Bitwise Op type, not integral type: lhs ", lhs->GetName(), ", rhs ", rhs->GetName());
    if (lhs->IsNull()) {
        *output_type = rhs;
    } else if (rhs->IsNull()) {
        *output_type = lhs;
    } else {
        CHECK_STATUS(InferNumberCastTypes(nm, lhs, rhs, output_type));
    }
    return Status::OK();
}

Status ExprNode::BitwiseNotTypeAccept(node::NodeManager* nm, const TypeNode* rhs, const TypeNode** output_type) {
    CHECK_TRUE(rhs != nullptr, kTypeError, "value for bitwise NOT must not null");
    CHECK_TRUE(rhs->IsNull() || rhs->IsIntegral(), kTypeError,
               "value for bitwise NOT must be integral type, but get ", rhs->GetName());
    *output_type = rhs;
    return Status::OK();
}

Status ExprNode::BetweenTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* low,
                                   const TypeNode* high, const TypeNode** output_type) {
    CHECK_TRUE(lhs != nullptr && low != nullptr && high != nullptr, kTypeError);

    const TypeNode* cond_1 = nullptr;
    CHECK_STATUS(CompareTypeAccept(nm, lhs, low, &cond_1));

    const TypeNode* cond_2 = nullptr;
    CHECK_STATUS(CompareTypeAccept(nm, lhs, high, &cond_2));

    CHECK_STATUS(LogicalOpTypeAccept(nm, cond_1, cond_2, output_type));
    return Status::OK();
}

// MC LIKE PC ESCAPE EC
// rules:
// 1. MC & PC is string or null
// 2. EC is string
Status ExprNode::LikeTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* rhs,
                                const TypeNode** output) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE(lhs->IsNull() || lhs->IsString(), kTypeError, "invalid 'LIKE' lhs: ", lhs->GetName());
    if (rhs->IsTuple()) {
        CHECK_TRUE(rhs->GetGenericSize() == 2, kTypeError, "'LIKE' with ESCAPE have invalid size");
    } else {
        CHECK_TRUE(rhs->IsNull() || rhs->IsString(), kTypeError, "invalid 'LIKE' rhs: ", rhs->GetName());
    }
    *output = nm->MakeTypeNode(kBool);
    return Status::OK();
}

// MC RlIKE PC
// rules:
// 1. MC & PC is string or null
Status ExprNode::RlikeTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* rhs,
                                const TypeNode** output) {
    CHECK_TRUE(lhs != nullptr && rhs != nullptr, kTypeError);
    CHECK_TRUE(lhs->IsNull() || lhs->IsString(), kTypeError, "invalid 'RlIKE' lhs: ", lhs->GetName());
    CHECK_TRUE(rhs->IsNull() || rhs->IsString(), kTypeError, "invalid 'RlIKE' rhs: ", rhs->GetName());
    *output = nm->MakeTypeNode(kBool);
    return Status::OK();
}

Status BinaryExpr::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 2, kTypeError);
    auto left_type = GetChild(0)->GetOutputType();
    auto right_type = GetChild(1)->GetOutputType();
    bool nullable = GetChild(0)->nullable() || GetChild(1)->nullable();
    CHECK_TRUE(left_type != nullptr && right_type != nullptr, kTypeError);
    switch (GetOp()) {
        case kFnOpAdd: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(AddTypeAccept(ctx->node_manager(), left_type,
                                       right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpMinus: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(SubTypeAccept(ctx->node_manager(), left_type,
                                       right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpMulti: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(MultiTypeAccept(ctx->node_manager(), left_type,
                                         right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpMod: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(ModTypeAccept(ctx->node_manager(), left_type,
                                       right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpDiv: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(SDivTypeAccept(ctx->node_manager(), left_type,
                                        right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpFDiv: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(FDivTypeAccept(ctx->node_manager(), left_type,
                                        right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpEq:
        case kFnOpNeq:
        case kFnOpLt:
        case kFnOpLe:
        case kFnOpGt:
        case kFnOpGe: {
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(CompareTypeAccept(ctx->node_manager(), left_type,
                                           right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpOr:
        case kFnOpXor:
        case kFnOpAnd: {
            // all type accept
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(LogicalOpTypeAccept(ctx->node_manager(), left_type,
                                             right_type, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpBitwiseAnd:
        case kFnOpBitwiseOr:
        case kFnOpBitwiseXor: {
            const TypeNode* top_type = nullptr;
            CHECK_STATUS(BitwiseLogicalTypeAccept(ctx->node_manager(), left_type, right_type, &top_type));
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpAt: {
            return ctx->InferAsUdf(this, "at");
            break;
        }
        case kFnOpILike:
        case kFnOpLike: {
            const TypeNode* top_type = nullptr;
            CHECK_STATUS(LikeTypeAccept(ctx->node_manager(), left_type, right_type, &top_type));
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
            }
        case kFnOpRLike: {
            const TypeNode* top_type = nullptr;
            CHECK_STATUS(RlikeTypeAccept(ctx->node_manager(), left_type, right_type, &top_type));
            SetOutputType(top_type);
            SetNullable(nullable);
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
            // all type accept
            const TypeNode* top_type = ctx->node_manager()->MakeTypeNode(kBool);
            CHECK_STATUS(NotTypeAccept(ctx->node_manager(), dtype, &top_type))
            SetOutputType(top_type);
            SetNullable(nullable);
            break;
        }
        case kFnOpMinus: {
            CHECK_TRUE(dtype->IsNumber(), kTypeError,
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
        case kFnOpBitwiseNot: {
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
    CHECK_TRUE(GetCondition() != nullptr && GetCondition()->GetOutputType() != nullptr &&
                   GetCondition()->GetOutputType()->base() == node::kBool,
               kTypeError, "Condition must be boolean type");
    CHECK_TRUE(GetLeft() != nullptr && GetRight() != nullptr, kTypeError);
    auto left_type = GetLeft()->GetOutputType();
    auto right_type = GetRight()->GetOutputType();
    CHECK_TRUE(left_type != nullptr, kTypeError, kTypeError, "Unknown cond left type");
    CHECK_TRUE(right_type != nullptr, kTypeError, kTypeError, "Unknown cond right type");

    auto out_res = CompatibleType(ctx->node_manager(), left_type, right_type);
    CHECK_TRUE(out_res.ok(), kTypeError, out_res.status());
    SetOutputType(out_res.value());

    SetNullable(GetLeft()->nullable() || GetRight()->nullable());
    return Status::OK();
}

Status ExprAnalysisContext::InferAsUdf(node::ExprNode* expr,
                                       const std::string& name) {
    auto nm = this->node_manager();
    std::vector<node::ExprNode*> proxy_args;
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        auto child = expr->GetChild(i);
        auto arg =
            node_manager()->MakeExprIdNode("proxy_arg_" + std::to_string(i));
        arg->SetOutputType(child->GetOutputType());
        arg->SetNullable(child->nullable());
        proxy_args.push_back(arg);
    }
    node::ExprNode* transformed = nullptr;
    CHECK_STATUS(library()->Transform(name, proxy_args, nm, &transformed),
                 "Resolve ", expr->GetExprString(), " as \"", name,
                 "\" failed");

    node::ExprNode* target_expr = nullptr;
    passes::ResolveFnAndAttrs resolver(this);
    CHECK_STATUS(resolver.VisitExpr(transformed, &target_expr), "Infer ",
                 expr->GetExprString(), " as \"", name, "\" failed");

    expr->SetOutputType(target_expr->GetOutputType());
    expr->SetNullable(target_expr->nullable());
    return Status::OK();
}

UnaryExpr* UnaryExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeUnaryExprNode(GetChild(0), GetOp());
}

BinaryExpr* BinaryExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeBinaryExprNode(GetChild(0), GetChild(1), GetOp());
}

ExprListNode* ExprListNode::ShadowCopy(NodeManager* nm) const {
    auto list = nm->MakeExprList();
    list->children_ = this->children_;
    return list;
}

Status ExprListNode::InferAttr(ExprAnalysisContext* ctx) {
    auto top_type = ctx->node_manager()->MakeTypeNode(kTuple);
    for (const auto& ele : children_) {
        top_type->AddGeneric(ele->GetOutputType(), ele->nullable());
    }
    SetOutputType(top_type);
    SetNullable(false);
    return Status::OK();
}

ArrayExpr* ArrayExpr::ShadowCopy(NodeManager* nm) const {
    auto array = nm->MakeArrayExpr();
    array->children_ = children_;
    array->specific_type_ = specific_type_;
    return array;
}

Status ArrayExpr::InferAttr(ExprAnalysisContext* ctx) {
    // if specific_type_ exists, and has the array element type, take the type directly
    // whether the specific type is castable from should checked during codegen
    if (specific_type_ != nullptr && !specific_type_->generics().empty()) {
        SetOutputType(specific_type_);
        SetNullable(true);
        return Status::OK();
    }

    TypeNode* top_type = nullptr;
    auto nm = ctx->node_manager();
    const TypeNode* ele_type = nm->MakeNode<TypeNode>();  // void type
    for (size_t i = 0; i < children_.size(); ++i) {
        auto res = CompatibleType(ctx->node_manager(), ele_type, children_[i]->GetOutputType());
        CHECK_TRUE(res.ok(), kTypeError, res.status());
        ele_type = res.value();
    }
    top_type = nm->MakeArrayType(ele_type, children_.size());
    SetOutputType(top_type);
    // array is nullable
    SetNullable(true);
    return Status::OK();
}

OrderByNode* OrderByNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeOrderByNode(order_expressions_);
}
OrderExpression *OrderExpression::ShadowCopy(NodeManager *nm) const {
    return nm->MakeOrderExpression(expr_, is_asc_);
}
ExprIdNode* ExprIdNode::ShadowCopy(NodeManager* nm) const {
    auto expr_id = nm->MakeUnresolvedExprId(GetName());
    expr_id->SetId(GetId());
    return expr_id;
}

CastExprNode* CastExprNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeNode<CastExprNode>(cast_type_, GetChild(0));
}

WhenExprNode* WhenExprNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeWhenNode(when_expr(), then_expr());
}

CaseWhenExprNode* CaseWhenExprNode::ShadowCopy(NodeManager* nm) const {
    auto node = new CaseWhenExprNode(when_expr_list(), else_expr());
    return nm->RegisterNode(node);
}

AllNode* AllNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeAllNode(GetRelationName(), GetDBName());
}

BetweenExpr* BetweenExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeBetweenExpr(GetLhs(), GetLow(), GetHigh(), is_not_between());
}
Status BetweenExpr::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(GetChildNum() == 3, kTypeError);

    const TypeNode* top_type = nullptr;
    CHECK_STATUS(BetweenTypeAccept(ctx->node_manager(), GetLhs()->GetOutputType(), GetLow()->GetOutputType(),
                                   GetHigh()->GetOutputType(), &top_type));

    SetOutputType(top_type);
    SetNullable(GetLhs()->nullable() || GetLow()->nullable() || GetHigh()->nullable());
    return Status::OK();
}

InExpr* InExpr::ShadowCopy(NodeManager *nm) const {
    return nm->MakeInExpr(GetLhs(), GetInList(), IsNot());
}

Status InExpr::InferAttr(ExprAnalysisContext* ctx) {
    CHECK_TRUE(kExprList == GetInList()->GetExprType(), kTypeError,
               absl::StrCat("Un-support in_list type in In Expression, expect ExprList, but got ",
                            ExprTypeName(GetInList()->GetExprType())));
    bool nullable = GetLhs()->nullable();
    const auto in_list = dynamic_cast<const ExprListNode*>(GetInList());
    for (const auto& ele : in_list->children_) {
        const TypeNode* cmp_type = nullptr;
        CHECK_STATUS(
            CompareTypeAccept(ctx->node_manager(), GetLhs()->GetOutputType(), ele->GetOutputType(), &cmp_type));
        nullable |= ele->nullable();
    }
    SetOutputType(ctx->node_manager()->MakeTypeNode(kBool));
    SetNullable(nullable);
    return Status::OK();
}

EscapedExpr* EscapedExpr::ShadowCopy(NodeManager * nm) const {
    return nm->MakeEscapeExpr(GetPattern(), GetEscape());
}

// EscapedExpr output is only meaningful when using together with BinaryExpr[LIKE]
// - output: tuple of (pattern, escape)
// - nullable: pattern's nullable
Status EscapedExpr::InferAttr(ExprAnalysisContext* ctx) {
    TypeNode* top_type = nullptr;

    CHECK_TRUE(GetPattern()->GetOutputType()->IsString() || GetPattern()->GetOutputType()->IsNull(), kTypeError,
               "invalid 'LIKE' rhs: ", GetPattern()->GetOutputType()->GetName())
    CHECK_TRUE(GetEscape()->GetOutputType()->IsString(), kTypeError,
               "invalid 'LIKE' ESCAPE clause: ", GetEscape()->GetOutputType()->GetName())

    top_type = ctx->node_manager()->MakeTypeNode(node::kTuple);
    top_type->AddGeneric(GetPattern()->GetOutputType(), GetPattern()->nullable());
    top_type->AddGeneric(GetEscape()->GetOutputType(), GetEscape()->nullable());
    SetOutputType(top_type);
    SetNullable(GetPattern()->nullable());
    return Status::OK();
}

QueryExpr* QueryExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeQueryExprNode(query_);
}

CondExpr* CondExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeCondExpr(GetCondition(), GetLeft(), GetRight());
}

CallExprNode* CallExprNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeFuncNode(GetFnDef(), children_, GetOver());
}

CallExprNode* CallExprNode::DeepCopy(NodeManager* nm) const {
    std::vector<ExprNode*> new_args;
    FnDefNode* new_fn = GetFnDef()->DeepCopy(nm);
    for (auto child : children_) {
        new_args.push_back(child->DeepCopy(nm));
    }
    return nm->MakeFuncNode(new_fn, new_args, GetOver());
}

ParameterExpr *ParameterExpr::ShadowCopy(NodeManager *nm) const {
    return nm->MakeParameterExpr(position());
}
ConstNode* ConstNode::ShadowCopy(NodeManager* nm) const {
    switch (GetDataType()) {
        case DataType::kBool:
            return nm->MakeConstNode(GetBool());
        case DataType::kInt16:
            return nm->MakeConstNode(GetSmallInt());
        case DataType::kInt32:
            return nm->MakeConstNode(GetInt());
        case DataType::kFloat:
            return nm->MakeConstNode(GetFloat());
        case DataType::kDouble:
            return nm->MakeConstNode(GetDouble());
        case DataType::kVarchar:
            return nm->MakeConstNode(std::string(GetStr()));

        case DataType::kInt64:
        case DataType::kDate:
        case DataType::kTimestamp:
        case DataType::kDay:
        case DataType::kHour:
        case DataType::kMinute:
        case DataType::kSecond:
            return nm->MakeConstNode(GetLong(), GetDataType());
        case DataType::kNull:
            return nm->MakeConstNode();

        case DataType::kList:
        case DataType::kIterator:
        case DataType::kMap:
        case DataType::kRow:
        case DataType::kInt8Ptr:
        case DataType::kTuple:
        case DataType::kOpaque:
        case DataType::kPlaceholder:
        case DataType::kVoid: {
            LOG(WARNING) << "Fail to copy primary expr of type " << node::DataTypeName(GetDataType());
            return nm->MakeConstNode(GetDataType());
        }
        default: {
            LOG(ERROR) << "Unsupported Data type " << node::DataTypeName(GetDataType());
            return nullptr;
        }
    }
}

ColumnRefNode* ColumnRefNode::ShadowCopy(NodeManager* nm) const {
    auto col =
        nm->MakeColumnRefNode(GetColumnName(), GetRelationName(), GetDBName());
    return col;
}

ColumnIdNode* ColumnIdNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeColumnIdNode(this->GetColumnID());
}

GetFieldExpr* GetFieldExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeGetFieldExpr(GetChild(0), GetColumnName(), GetColumnID());
}

StructExpr* StructExpr::ShadowCopy(NodeManager* nm) const {
    auto node = new StructExpr(GetName());
    node->SetFileds(fileds_);
    node->SetMethod(methods_);
    return nm->RegisterNode(node);
}

LambdaNode* LambdaNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeLambdaNode(args_, body_);
}

LambdaNode* LambdaNode::DeepCopy(NodeManager* nm) const {
    std::vector<ExprIdNode*> new_args;
    passes::ExprReplacer replacer;
    for (auto origin_arg : args_) {
        auto new_arg = nm->MakeExprIdNode(origin_arg->GetName());
        if (origin_arg->IsResolved()) {
            replacer.AddReplacement(origin_arg, new_arg);
        }
        new_args.push_back(new_arg);
    }
    auto cloned_body = body()->DeepCopy(nm);
    node::ExprNode* new_body = nullptr;
    auto status = replacer.Replace(cloned_body, &new_body);
    if (status.isOK()) {
        return nm->MakeLambdaNode(new_args, new_body);
    } else {
        LOG(WARNING) << "Deep copy lambda body failed: " << status.msg;
        return nm->MakeLambdaNode(new_args, cloned_body);
    }
}

ExternalFnDefNode* ExternalFnDefNode::ShadowCopy(NodeManager* nm) const {
    return DeepCopy(nm);
}

ExternalFnDefNode* ExternalFnDefNode::DeepCopy(NodeManager* nm) const {
    if (IsResolved()) {
        return nm->MakeExternalFnDefNode(function_name(), function_ptr(),
                                         GetReturnType(), IsReturnNullable(),
                                         arg_types_, arg_nullable_,
                                         variadic_pos(), return_by_arg());
    } else {
        return nm->MakeUnresolvedFnDefNode(function_name());
    }
}

DynamicUdfFnDefNode* DynamicUdfFnDefNode::ShadowCopy(NodeManager* nm) const {
    return DeepCopy(nm);
}

DynamicUdfFnDefNode* DynamicUdfFnDefNode::DeepCopy(NodeManager* nm) const {
    if (IsResolved()) {
        return nm->MakeDynamicUdfFnDefNode(GetName(), function_ptr(),
                                         GetReturnType(), IsReturnNullable(),
                                         arg_types_, arg_nullable_,
                                         return_by_arg(),
                                         init_context_node_ == nullptr ? nullptr : init_context_node_->DeepCopy(nm));
    } else {
        return nm->MakeDynamicUdfFnDefNode(GetName(), nullptr, nullptr, true, {}, {}, false, nullptr);
    }
}

UdfDefNode* UdfDefNode::ShadowCopy(NodeManager* nm) const {
    return DeepCopy(nm);
}

UdfDefNode* UdfDefNode::DeepCopy(NodeManager* nm) const {
    return nm->MakeUdfDefNode(def_);
}

UdfByCodeGenDefNode* UdfByCodeGenDefNode::ShadowCopy(NodeManager* nm) const {
    return DeepCopy(nm);
}

UdfByCodeGenDefNode* UdfByCodeGenDefNode::DeepCopy(NodeManager* nm) const {
    auto def_node = nm->MakeUdfByCodeGenDefNode(
        name_, arg_types_, arg_nullable_, ret_type_, ret_nullable_);
    def_node->SetGenImpl(this->GetGenImpl());
    return def_node;
}

UdafDefNode* UdafDefNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeUdafDefNode(name_, arg_types_, init_expr_, update_, merge_,
                               output_);
}

UdafDefNode* UdafDefNode::DeepCopy(NodeManager* nm) const {
    ExprNode* new_init = init_expr_ ? init_expr_->DeepCopy(nm) : nullptr;
    FnDefNode* new_update = update_ ? update_->DeepCopy(nm) : nullptr;
    FnDefNode* new_merge = merge_ ? merge_->DeepCopy(nm) : nullptr;
    FnDefNode* new_output = output_ ? output_->DeepCopy(nm) : nullptr;
    return nm->MakeUdafDefNode(name_, arg_types_, new_init, new_update,
                               new_merge, new_output);
}

VariadicUdfDefNode* VariadicUdfDefNode::ShadowCopy(NodeManager* nm) const {
    return nm->MakeNode<VariadicUdfDefNode>(name_, init_, update_, output_);
}

VariadicUdfDefNode* VariadicUdfDefNode::DeepCopy(NodeManager* nm) const {
    FnDefNode* new_init = init_ ? init_->DeepCopy(nm) : nullptr;
    std::vector<FnDefNode*> new_update;
    for (FnDefNode* update_func : update_) {
        new_update.push_back(update_func ? update_func->DeepCopy(nm): nullptr);
    }
    FnDefNode* new_output = output_ ? output_->DeepCopy(nm) : nullptr;
    return nm->MakeNode<VariadicUdfDefNode>(name_, new_init, new_update, new_output);
}

// Default expr deep copy: shadow copy self and deep copy children
ExprNode* ExprNode::DeepCopy(NodeManager* nm) const {
    auto root = this->ShadowCopy(nm);
    for (size_t i = 0; i < this->GetChildNum(); ++i) {
        root->SetChild(i, root->GetChild(i)->DeepCopy(nm));
    }
    return root;
}

ArrayElementExpr::ArrayElementExpr(ExprNode* array, ExprNode* pos) : ExprNode(kExprArrayElement) {
    AddChild(array);
    AddChild(pos);
}

void ArrayElementExpr::Print(std::ostream& output, const std::string& org_tab) const {
    // Print for ExprNode just talk too much, I don't intend impl that
    // GetExprString is much simpler
    output << org_tab << GetExprString();
}

const std::string ArrayElementExpr::GetExprString() const {
    return absl::StrCat(array()->GetExprString(), "[", position()->GetExprString(), "]");
}

ArrayElementExpr* ArrayElementExpr::ShadowCopy(NodeManager* nm) const {
    return nm->MakeNode<ArrayElementExpr>(array(), position());
}

Status ArrayElementExpr::InferAttr(ExprAnalysisContext* ctx) {
    auto* arr_type = array()->GetOutputType();
    auto* pos_type = position()->GetOutputType();

    if (arr_type->IsMap()) {
        auto map_type = arr_type->GetAsOrNull<MapType>();
        CHECK_TRUE(node::ExprNode::IsSafeCast(pos_type, map_type->key_type()), common::kTypeError,
                   "incompatiable key type for ArrayElement, expect ", map_type->key_type()->DebugString(), ", got ",
                   pos_type->DebugString());

        SetOutputType(map_type->value_type());
        SetNullable(map_type->value_nullable());
    } else if (arr_type->IsArray()) {
        CHECK_TRUE(pos_type->IsInteger(), common::kTypeError,
                   "index type mismatch for ArrayElement, expect integer, got ", pos_type->DebugString());
        CHECK_TRUE(arr_type->GetGenericSize() == 1, common::kTypeError, "internal error: array of empty T");

        SetOutputType(arr_type->GetGenericType(0));
        SetNullable(arr_type->IsGenericNullable(0));
    } else {
        FAIL_STATUS(common::kTypeError, "can't get element from ", arr_type->DebugString(), ", expect map or array");
    }
    return {};
}
ExprNode *ArrayElementExpr::array() const { return GetChild(0); }
ExprNode *ArrayElementExpr::position() const { return GetChild(1); }

StructCtorWithParens* StructCtorWithParens::ShadowCopy(NodeManager* nm) const {
    return nm->MakeNode<StructCtorWithParens>(fields());
}
const std::string StructCtorWithParens::GetExprString() const {
    return absl::StrCat(
        "(",
        absl::StrJoin(fields(), ", ",
                      [](std::string* out, const ExprNode* e) { absl::StrAppend(out, e->GetExprString()); }),
        ")");
}

Status StructCtorWithParens::InferAttr(ExprAnalysisContext* ctx) {
    // TODO
    return {};
}
}  // namespace node
}  // namespace hybridse
