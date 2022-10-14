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

#include "codegen/null_ir_builder.h"
#include "codegen/predicate_expr_ir_builder.h"

namespace hybridse {
namespace codegen {

using ::hybridse::common::kCodegenError;

NullIRBuilder::NullIRBuilder() {}
NullIRBuilder::~NullIRBuilder() {}

base::Status NullIRBuilder::SafeNullBinaryExpr(
    ::llvm::BasicBlock* block, const NativeValue& value_left,
    const NativeValue& value_right,
    const std::function<bool(::llvm::BasicBlock* block, ::llvm::Value*,
                             ::llvm::Value*, ::llvm::Value**, base::Status&)>
        expr_func,
    NativeValue* value_output) {
    base::Status status;
    ::llvm::IRBuilder<> builder(block);

    NativeValue left = value_left;
    NativeValue right = value_right;
    if (value_left.IsConstNull()) {
        *value_output = NativeValue::CreateNull(right.GetType());
        return base::Status::OK();
    }
    if (value_right.IsConstNull()) {
        *value_output = NativeValue::CreateNull(left.GetType());
        return base::Status::OK();
    }
    ::llvm::Value* raw_left = left.GetValue(&builder);
    ::llvm::Value* raw_right = right.GetValue(&builder);
    ::llvm::Value* output = nullptr;

    NullIRBuilder null_ir_builder;
    ::llvm::Value* should_ret_null = nullptr;
    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, left, &should_ret_null));
    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, right, &should_ret_null));
    CHECK_TRUE(expr_func(block, raw_left, raw_right, &output, status),
               kCodegenError, status.msg);
    if (nullptr != should_ret_null) {
        *value_output = NativeValue::CreateWithFlag(output, should_ret_null);
    } else {
        *value_output = NativeValue::Create(output);
    }
    return status;
}
base::Status NullIRBuilder::SafeNullCastExpr(
    ::llvm::BasicBlock* block, const NativeValue& value_left,
    ::llvm::Type* cast_type,
    const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*,
                             ::llvm::Type* type, ::llvm::Value**,
                             base::Status&)>
        expr_func,
    NativeValue* value_output) {
    base::Status status;
    ::llvm::IRBuilder<> builder(block);

    NativeValue left = value_left;
    if (value_left.IsConstNull()) {
        *value_output = NativeValue::CreateNull(left.GetType());
        return base::Status::OK();
    }
    ::llvm::Value* raw_left = left.GetValue(&builder);
    ::llvm::Value* output = nullptr;

    NullIRBuilder null_ir_builder;
    ::llvm::Value* should_ret_null = nullptr;
    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, left, &should_ret_null));
    CHECK_TRUE(expr_func(block, raw_left, cast_type, &output, status),
               kCodegenError, status.msg);
    if (nullptr != should_ret_null) {
        *value_output = NativeValue::CreateWithFlag(output, should_ret_null);
    } else {
        *value_output = NativeValue::Create(output);
    }
    return status;
}
base::Status NullIRBuilder::SafeNullUnaryExpr(
    ::llvm::BasicBlock* block, const NativeValue& value_left,
    const std::function<bool(::llvm::BasicBlock* block, ::llvm::Value*,
                             ::llvm::Value**, base::Status&)>
        expr_func,
    NativeValue* value_output) {
    base::Status status;
    ::llvm::IRBuilder<> builder(block);

    NativeValue left = value_left;
    if (value_left.IsConstNull()) {
        *value_output = NativeValue::CreateNull(left.GetType());
        return base::Status::OK();
    }
    ::llvm::Value* raw_left = left.GetValue(&builder);
    ::llvm::Value* output = nullptr;

    NullIRBuilder null_ir_builder;
    ::llvm::Value* should_ret_null = nullptr;
    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, left, &should_ret_null));
    CHECK_TRUE(expr_func(block, raw_left, &output, status), kCodegenError,
               status.msg);
    if (nullptr != should_ret_null) {
        *value_output = NativeValue::CreateWithFlag(output, should_ret_null);
    } else {
        *value_output = NativeValue::Create(output);
    }
    return status;
}
base::Status NullIRBuilder::CheckAnyNull(::llvm::BasicBlock* block,
                                         const NativeValue& value,
                                         ::llvm::Value** should_ret_null) {
    CHECK_TRUE(nullptr != should_ret_null, kCodegenError,
               "fail to check any null: should ret null llvm value is null");
    ::llvm::IRBuilder<> builder(block);
    if (value.IsNullable()) {
        if (*should_ret_null == nullptr) {
            *should_ret_null = value.GetIsNull(&builder);
        } else {
            *should_ret_null =
                builder.CreateOr(*should_ret_null, value.GetIsNull(&builder));
        }
    }
    return base::Status::OK();
}

base::Status NullIRBuilder::CheckAllNull(::llvm::BasicBlock* block,
                                         const NativeValue& value,
                                         ::llvm::Value** should_ret_null) {
    CHECK_TRUE(nullptr != should_ret_null, kCodegenError,
               "fail to check all null: should ret null llvm value is null");
    ::llvm::IRBuilder<> builder(block);
    if (value.IsNullable()) {
        if (*should_ret_null == nullptr) {
            *should_ret_null = value.GetIsNull(&builder);
        } else {
            *should_ret_null =
                builder.CreateAnd(*should_ret_null, value.GetIsNull(&builder));
        }
    }
    return base::Status::OK();
}
base::Status NullIRBuilder::SafeNullDivExpr(
    ::llvm::BasicBlock* block, const NativeValue& left, const NativeValue& right,
    const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*, ::llvm::Value*, ::llvm::Value**, base::Status&)>
        expr_func,
    NativeValue* output) {
    NativeValue rhs_eq_zero;
    PredicateIRBuilder predicate_builder(block);
    CHECK_STATUS(
        predicate_builder.BuildEqExpr(
            right, NativeValue::Create(::llvm::ConstantInt::get(::llvm::Type::getInt32Ty(block->getContext()), 0)),
            &rhs_eq_zero),
        "failed to build equal expr for rhs of div");

    NativeValue safe_null_value;
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(block, left, right, expr_func, &safe_null_value));

    CondSelectIRBuilder select_builder;

    return select_builder.Select(
        block, rhs_eq_zero,
        NativeValue::CreateNull(safe_null_value.GetType()),
        safe_null_value, output);
}
}  // namespace codegen
}  // namespace hybridse
