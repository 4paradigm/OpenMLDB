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

#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/null_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "codegen/type_ir_builder.h"

using hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {

PredicateIRBuilder::PredicateIRBuilder(::llvm::BasicBlock* block)
    : block_(block), cast_expr_ir_builder_(block) {}
PredicateIRBuilder::~PredicateIRBuilder() {}

/**
 * Fano graph for nullable and:
 * - value                      - null flag
 *     | 00 | 01 | 11 | 10 |        | 00 | 01 | 11 | 10 |
 * ----+----+----+----+----+    ----+----+----+----+----|
 *  00 |    |    |    |    |     00 |    |    |    |    |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  01 |    | 1  | *  | *  |     01 |    |    | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  11 |    | *  | *  | *  |     11 |    | 1  | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  10 |    | *  | *  | *  |     10 |    | 1  | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 */
Status PredicateIRBuilder::BuildAndExpr(NativeValue left, NativeValue right,
                                        NativeValue* output) {
    CHECK_STATUS(TypeIRBuilder::BinaryOpTypeInfer(
        node::ExprNode::LogicalOpTypeAccept, left.GetType(), right.GetType()));
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* raw_left = left.GetValue(&builder);
    ::llvm::Value* raw_right = right.GetValue(&builder);
    ::llvm::Value* left_is_null = left.GetIsNull(&builder);
    ::llvm::Value* right_is_null = right.GetIsNull(&builder);

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;
    Status status;
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw_left, &casted_left, status),
               kCodegenError,
               "Infer and cast lhs type of and(&&) failed: ", status.msg);
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw_right, &casted_right, status),
               kCodegenError,
               "Infer and cast rhs type of and(&&) failed: ", status.msg);
    CHECK_TRUE(casted_left->getType()->isIntegerTy(1) &&
                   casted_right->getType()->isIntegerTy(1),
               kCodegenError,
               "Fail to codegen &&(and) expr: value types are invalid");

    ::llvm::Value* result_val = builder.CreateAnd(casted_left, casted_right);

    ::llvm::Value* result_is_null = builder.CreateAnd(
        left_is_null, builder.CreateOr(right_is_null, casted_right));
    result_is_null = builder.CreateOr(
        result_is_null, builder.CreateAnd(casted_left, right_is_null));

    *output = NativeValue::CreateWithFlag(result_val, result_is_null);
    return Status::OK();
}

/**
 * Fano graph for nullable or:
 * - value                      - null flag
 *     | 00 | 01 | 11 | 10 |        | 00 | 01 | 11 | 10 |
 * ----+----+----+----+----+    ----+----+----+----+----|
 *  00 |    | 1  | *  | *  |     00 |    |    | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  01 | 1  | 1  | 1  | 1  |     01 |    |    |    |    |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  11 | *  | 1  | *  | *  |     11 | 1  |    | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  10 | *  | 1  | *  | *  |     10 | 1  |    | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 */
Status PredicateIRBuilder::BuildOrExpr(NativeValue left, NativeValue right,
                                       NativeValue* output) {
    CHECK_STATUS(TypeIRBuilder::BinaryOpTypeInfer(
        node::ExprNode::LogicalOpTypeAccept, left.GetType(), right.GetType()));
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* raw_left = left.GetValue(&builder);
    ::llvm::Value* raw_right = right.GetValue(&builder);
    ::llvm::Value* left_is_null = left.GetIsNull(&builder);
    ::llvm::Value* right_is_null = right.GetIsNull(&builder);

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;
    Status status;
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw_left, &casted_left, status),
               kCodegenError,
               "Infer and cast lhs type of or(||) failed: ", status.msg);
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw_right, &casted_right, status),
               kCodegenError,
               "Infer and cast rhs type of or(||) failed: ", status.msg);
    CHECK_TRUE(casted_left->getType()->isIntegerTy(1) &&
                   casted_right->getType()->isIntegerTy(1),
               kCodegenError,
               "Fail to codegen &&(and) expr: value types are invalid");

    ::llvm::Value* result_val = builder.CreateOr(casted_left, casted_right);

    ::llvm::Value* result_is_null = builder.CreateAnd(
        left_is_null,
        builder.CreateOr(right_is_null, builder.CreateNot(casted_right)));
    result_is_null = builder.CreateOr(
        result_is_null,
        builder.CreateAnd(builder.CreateNot(casted_left), right_is_null));

    *output = NativeValue::CreateWithFlag(result_val, result_is_null);
    return Status::OK();
}

/**
 * Fano graph for nullable xor:
 * - value                      - null flag
 *     | 00 | 01 | 11 | 10 |        | 00 | 01 | 11 | 10 |
 * ----+----+----+----+----+    ----+----+----+----+----|
 *  00 |    | 1  | *  | *  |     00 |    |    | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  01 | 1  |    | *  | *  |     01 |    |    | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  11 | *  | *  | *  | *  |     11 | 1  | 1  | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 *  10 | *  | *  | *  | *  |     10 | 1  | 1  | 1  | 1  |
 * ----+----+----+----+----+    ----+----+----+----+----+
 */
Status PredicateIRBuilder::BuildXorExpr(NativeValue left, NativeValue right,
                                        NativeValue* output) {
    CHECK_STATUS(TypeIRBuilder::BinaryOpTypeInfer(
        node::ExprNode::LogicalOpTypeAccept, left.GetType(), right.GetType()));
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* raw_left = left.GetValue(&builder);
    ::llvm::Value* raw_right = right.GetValue(&builder);
    ::llvm::Value* left_is_null = left.GetIsNull(&builder);
    ::llvm::Value* right_is_null = right.GetIsNull(&builder);

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;
    Status status;
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw_left, &casted_left, status),
               kCodegenError,
               "Infer and cast lhs type of and(&&) failed: ", status.msg);
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw_right, &casted_right, status),
               kCodegenError,
               "Infer and cast rhs type of and(&&) failed: ", status.msg);
    CHECK_TRUE(casted_left->getType()->isIntegerTy(1) &&
                   casted_right->getType()->isIntegerTy(1),
               kCodegenError,
               "Fail to codegen &&(and) expr: value types are invalid");

    ::llvm::Value* result_val = builder.CreateXor(casted_left, casted_right);
    ::llvm::Value* result_is_null =
        builder.CreateOr(left_is_null, right_is_null);
    *output = NativeValue::CreateWithFlag(result_val, result_is_null);
    return Status::OK();
}

Status PredicateIRBuilder::BuildNotExpr(NativeValue input,
                                        NativeValue* output) {
    CHECK_STATUS(TypeIRBuilder::UnaryOpTypeInfer(node::ExprNode::NotTypeAccept,
                                                 input.GetType()));
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* raw = input.GetValue(&builder);
    ::llvm::Value* is_null = input.GetIsNull(&builder);

    ::llvm::Value* casted_raw = nullptr;
    Status status;
    CHECK_TRUE(InferAndCastBoolTypes(block_, raw, &casted_raw, status),
               kCodegenError, status.msg);
    CHECK_TRUE(casted_raw->getType()->isIntegerTy(1), kCodegenError,
               "Fail to codegen !(not) expr: value types are invalid");

    *output =
        NativeValue::CreateWithFlag(builder.CreateNot(casted_raw), is_null);
    return Status::OK();
}
Status PredicateIRBuilder::BuildEqExpr(NativeValue left, NativeValue right,
                                       NativeValue* output) {
    CHECK_STATUS(CompareTypeAccept(left.GetType(), right.GetType()))
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildEqExpr(block, lhs, rhs, output, status);
        },
        output));
    if (output->IsConstNull()) {
        output->SetType(BoolIRBuilder::GetType(block_->getModule()));
    }
    return Status::OK();
}
Status PredicateIRBuilder::BuildNeqExpr(NativeValue left, NativeValue right,
                                        NativeValue* output) {
    CHECK_STATUS(CompareTypeAccept(left.GetType(), right.GetType()))
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildNeqExpr(block, lhs, rhs, output, status);
        },
        output));
    if (output->IsConstNull()) {
        output->SetType(BoolIRBuilder::GetType(block_->getModule()));
    }
    return Status::OK();
}
Status PredicateIRBuilder::BuildGtExpr(NativeValue left, NativeValue right,
                                       NativeValue* output) {
    CHECK_STATUS(CompareTypeAccept(left.GetType(), right.GetType()))
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildGtExpr(block, lhs, rhs, output, status);
        },
        output));
    if (output->IsConstNull()) {
        output->SetType(BoolIRBuilder::GetType(block_->getModule()));
    }
    return Status::OK();
}
Status PredicateIRBuilder::BuildGeExpr(NativeValue left, NativeValue right,
                                       NativeValue* output) {
    CHECK_STATUS(CompareTypeAccept(left.GetType(), right.GetType()))
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildGeExpr(block, lhs, rhs, output, status);
        },
        output));
    if (output->IsConstNull()) {
        output->SetType(BoolIRBuilder::GetType(block_->getModule()));
    }
    return Status::OK();
}
Status PredicateIRBuilder::BuildLeExpr(NativeValue left, NativeValue right,
                                       NativeValue* output) {
    CHECK_STATUS(CompareTypeAccept(left.GetType(), right.GetType()))
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildLeExpr(block, lhs, rhs, output, status);
        },
        output));
    if (output->IsConstNull()) {
        output->SetType(BoolIRBuilder::GetType(block_->getModule()));
    }
    return Status::OK();
}
Status PredicateIRBuilder::BuildLtExpr(NativeValue left, NativeValue right,
                                       NativeValue* output) {
    CHECK_STATUS(CompareTypeAccept(left.GetType(), right.GetType()))
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildLtExpr(block, lhs, rhs, output, status);
        },
        output));
    if (output->IsConstNull()) {
        output->SetType(BoolIRBuilder::GetType(block_->getModule()));
    }
    return Status::OK();
}

bool PredicateIRBuilder::BuildEqExpr(::llvm::BasicBlock* block,
                                     ::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;
    if (false == InferAndCastTypes(block, left, right, &casted_left,
                                   &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpEQ(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOEQ(casted_left, casted_right);
    } else if (TypeIRBuilder::IsTimestampPtr(casted_left->getType()) &&
               TypeIRBuilder::IsTimestampPtr(casted_right->getType())) {
        llvm::Value* left_days;
        llvm::Value* right_days;
        TimestampIRBuilder timestamp_ir_builder(block->getModule());
        timestamp_ir_builder.GetTs(block, casted_left, &left_days);
        timestamp_ir_builder.GetTs(block, casted_right, &right_days);
        return BuildEqExpr(block, left_days, right_days, output, status);
    } else if (TypeIRBuilder::IsDatePtr(casted_left->getType()) &&
               TypeIRBuilder::IsDatePtr(casted_right->getType())) {
        llvm::Value* left_days;
        llvm::Value* right_days;
        DateIRBuilder date_ir_builder(block->getModule());
        date_ir_builder.GetDate(block, casted_left, &left_days);
        date_ir_builder.GetDate(block, casted_right, &right_days);
        return BuildEqExpr(block, left_days, right_days, output, status);
    } else if (TypeIRBuilder::IsStringPtr(casted_left->getType()) &&
               TypeIRBuilder::IsStringPtr(casted_right->getType())) {
        StringIRBuilder string_ir_builder(block->getModule());
        NativeValue compare_value;
        status = string_ir_builder.Compare(
            block, NativeValue::Create(casted_left),
            NativeValue::Create(casted_right), &compare_value);
        if (!status.isOK()) {
            return false;
        }
        return BuildEqExpr(block, compare_value.GetValue(&builder),
                           builder.getInt32(0), output, status);

    } else {
        status.msg = "fail to codegen == expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen == expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildNeqExpr(::llvm::BasicBlock* block,
                                      ::llvm::Value* left, ::llvm::Value* right,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferAndCastTypes(block, left, right, &casted_left,
                                   &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpNE(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpUNE(casted_left, casted_right);
    } else if (TypeIRBuilder::IsStringPtr(casted_left->getType()) &&
               TypeIRBuilder::IsStringPtr(casted_right->getType())) {
        StringIRBuilder string_ir_builder(block->getModule());
        NativeValue compare_value;
        status = string_ir_builder.Compare(
            block, NativeValue::Create(casted_left),
            NativeValue::Create(casted_right), &compare_value);
        if (!status.isOK()) {
            return false;
        }
        return BuildNeqExpr(block, compare_value.GetValue(&builder),
                            builder.getInt32(0), output, status);

    } else {
        status.msg = "fail to codegen neq expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen == expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildGtExpr(::llvm::BasicBlock* block,
                                     ::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferAndCastTypes(block, left, right, &casted_left,
                                   &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType() == builder.getInt1Ty()) {
        *output = builder.CreateICmpUGT(casted_left, casted_right);
    } else if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSGT(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOGT(casted_left, casted_right);
    } else if (TypeIRBuilder::IsStringPtr(casted_left->getType()) &&
               TypeIRBuilder::IsStringPtr(casted_right->getType())) {
        StringIRBuilder string_ir_builder(block->getModule());
        NativeValue compare_value;
        status = string_ir_builder.Compare(
            block, NativeValue::Create(casted_left),
            NativeValue::Create(casted_right), &compare_value);
        if (!status.isOK()) {
            return false;
        }
        return BuildGtExpr(block, compare_value.GetValue(&builder),
                           builder.getInt32(0), output, status);

    } else {
        status.msg = "fail to codegen > expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen > expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildGeExpr(::llvm::BasicBlock* block,
                                     ::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferAndCastTypes(block, left, right, &casted_left,
                                   &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType() == builder.getInt1Ty()) {
        *output = builder.CreateICmpUGE(casted_left, casted_right);
    } else if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSGE(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOGE(casted_left, casted_right);
    } else if (TypeIRBuilder::IsStringPtr(casted_left->getType()) &&
               TypeIRBuilder::IsStringPtr(casted_right->getType())) {
        StringIRBuilder string_ir_builder(block->getModule());
        NativeValue compare_value;
        status = string_ir_builder.Compare(
            block, NativeValue::Create(casted_left),
            NativeValue::Create(casted_right), &compare_value);
        if (!status.isOK()) {
            return false;
        }
        return BuildGeExpr(block, compare_value.GetValue(&builder),
                           builder.getInt32(0), output, status);

    } else {
        status.msg = "fail to codegen >= expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen >= expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildLtExpr(::llvm::BasicBlock* block,
                                     ::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferAndCastTypes(block, left, right, &casted_left,
                                   &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType() == builder.getInt1Ty()) {
        *output = builder.CreateICmpULT(casted_left, casted_right);
    } else if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSLT(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOLT(casted_left, casted_right);
    } else if (TypeIRBuilder::IsStringPtr(casted_left->getType()) &&
               TypeIRBuilder::IsStringPtr(casted_right->getType())) {
        StringIRBuilder string_ir_builder(block->getModule());
        NativeValue compare_value;
        status = string_ir_builder.Compare(
            block, NativeValue::Create(casted_left),
            NativeValue::Create(casted_right), &compare_value);
        if (!status.isOK()) {
            return false;
        }
        return BuildLtExpr(block, compare_value.GetValue(&builder),
                           builder.getInt32(0), output, status);

    } else {
        status.msg = "fail to codegen < expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen < expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildLeExpr(::llvm::BasicBlock* block,
                                     ::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferAndCastTypes(block, left, right, &casted_left,
                                   &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType() == builder.getInt1Ty()) {
        *output = builder.CreateICmpULE(casted_left, casted_right);
    } else if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSLE(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOLE(casted_left, casted_right);
    } else if (TypeIRBuilder::IsStringPtr(casted_left->getType()) &&
               TypeIRBuilder::IsStringPtr(casted_right->getType())) {
        StringIRBuilder string_ir_builder(block->getModule());
        NativeValue compare_value;
        status = string_ir_builder.Compare(
            block, NativeValue::Create(casted_left),
            NativeValue::Create(casted_right), &compare_value);
        if (!status.isOK()) {
            return false;
        }
        return BuildLeExpr(block, compare_value.GetValue(&builder),
                           builder.getInt32(0), output, status);

    } else {
        status.msg = "fail to codegen <= expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen <= expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::IsAcceptType(::llvm::Type* type) {
    if (nullptr == type) {
        return false;
    }

    ::hybridse::node::DataType hybridse_type;
    if (false == GetBaseType(type, &hybridse_type)) {
        return false;
    }
    switch (hybridse_type) {
        case ::hybridse::node::kVoid:
        case ::hybridse::node::kList:
            return false;
        default: {
            return true;
        }
    }
}

Status PredicateIRBuilder::CompareTypeAccept(::llvm::Type* lhs,
                                             ::llvm::Type* rhs) {
    CHECK_STATUS(TypeIRBuilder::BinaryOpTypeInfer(
        node::ExprNode::CompareTypeAccept, lhs, rhs));
    return Status::OK();
}
bool PredicateIRBuilder::InferAndCastBoolTypes(
    ::llvm::BasicBlock* block, ::llvm::Value* value,
    ::llvm::Value** casted_value, ::hybridse::base::Status& status) {
    if (NULL == value) {
        status.msg = "value is null";
        status.code = common::kCodegenError;
        return false;
    }

    ::llvm::Type* type = value->getType();
    if (!IsAcceptType(type)) {
        status.msg = "invalid type for bool expression";
        status.code = common::kCodegenError;
        return false;
    }
    *casted_value = value;
    CastExprIRBuilder cast_expr_ir_builder(block);
    ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(block->getContext());
    if (type != bool_ty) {
        if (!cast_expr_ir_builder.BoolCast(value, casted_value, status)) {
            status.msg = "fail to codegen add expr: " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}
bool PredicateIRBuilder::InferAndCastTypes(::llvm::BasicBlock* block,
                                           ::llvm::Value* left,
                                           ::llvm::Value* right,
                                           ::llvm::Value** casted_left,
                                           ::llvm::Value** casted_right,
                                           ::hybridse::base::Status& status) {
    if (NULL == left || NULL == right) {
        status.msg = "left or right value is null";
        status.code = common::kCodegenError;
        return false;
    }

    ::llvm::Type* left_type = left->getType();
    ::llvm::Type* right_type = right->getType();

    if (!IsAcceptType(left_type) || !IsAcceptType(right_type)) {
        status.msg = "invalid type for arithmetic expression";
        status.code = common::kCodegenError;
        return false;
    }

    *casted_left = left;
    *casted_right = right;

    StringIRBuilder string_builder(block->getModule());
    if (TypeIRBuilder::IsStringPtr(left_type)) {
        status = string_builder.CastFrom(block, right, casted_right);
        return status.isOK();
    }

    if (TypeIRBuilder::IsStringPtr(right_type)) {
        status = string_builder.CastFrom(block, left, casted_left);
        return status.isOK();
    }

    TimestampIRBuilder timestamp_builder(block->getModule());
    if (TypeIRBuilder::IsTimestampPtr(left_type)) {
        if (false == timestamp_builder.GetTs(block, left, casted_left)) {
            status.msg = "fail to get ts";
            LOG(WARNING) << status;
            return false;
        }
        left_type = (*casted_left)->getType();
    }

    if (TypeIRBuilder::IsTimestampPtr(right_type)) {
        if (false == timestamp_builder.GetTs(block, right, casted_right)) {
            status.msg = "fail to get ts";
            LOG(WARNING) << status;
            return false;
        }
        right_type = (*casted_right)->getType();
    }

    CastExprIRBuilder cast_expr_ir_builder(block);
    if (left_type != right_type) {
        if (cast_expr_ir_builder.IsSafeCast(left_type, right_type)) {
            if (!cast_expr_ir_builder.SafeCastNumber(left, right_type,
                                                     casted_left, status)) {
                status.msg = "fail to codegen expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsSafeCast(right_type, left_type)) {
            if (!cast_expr_ir_builder.SafeCastNumber(right, left_type,
                                                     casted_right, status)) {
                status.msg = "fail to codegen expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsIntFloat2PointerCast(left_type,
                                                               right_type)) {
            if (!cast_expr_ir_builder.UnSafeCastNumber(left, right_type,
                                                       casted_left, status)) {
                status.msg = "fail to codegen expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsIntFloat2PointerCast(right_type,
                                                               left_type)) {
            if (!cast_expr_ir_builder.UnSafeCastNumber(right, left_type,
                                                       casted_right, status)) {
                status.msg = "fail to codegen expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else {
            status.msg =
                "fail to codegen add expr: value type isn't compatible";
            status.code = common::kCodegenError;
            LOG(WARNING) << status;
            return false;
        }
    }
    if (TypeIRBuilder::IsDatePtr((*casted_left)->getType()) &&
        TypeIRBuilder::IsDatePtr((*casted_right)->getType())) {
        DateIRBuilder date_ir_builder(block->getModule());
        date_ir_builder.GetDate(block, *casted_left, casted_left);
        date_ir_builder.GetDate(block, *casted_right, casted_right);
    }
    return true;
}

Status PredicateIRBuilder::BuildIsNullExpr(NativeValue input,
                                           NativeValue* output) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* is_null = input.GetIsNull(&builder);
    *output = NativeValue::Create(is_null);
    return Status::OK();
}

Status PredicateIRBuilder::BuildBetweenExpr(const NativeValue& lhs, const NativeValue& low, const NativeValue& high,
                                            bool is_not_between, NativeValue* output) {
    NativeValue first_condition;
    NativeValue second_condition;
    if (is_not_between) {
        CHECK_STATUS(BuildLtExpr(lhs, low, &first_condition));
        CHECK_STATUS(BuildGtExpr(lhs, high, &second_condition));
        CHECK_STATUS(BuildOrExpr(first_condition, second_condition, output));
    } else {
        CHECK_STATUS(BuildGeExpr(lhs, low, &first_condition));
        CHECK_STATUS(BuildLeExpr(lhs, high, &second_condition));
        CHECK_STATUS(BuildAndExpr(first_condition, second_condition, output));
    }
    return Status::OK();
}

Status PredicateIRBuilder::BuildInExpr(const NativeValue& lhs, const NativeValue& in_list, bool is_not,
                                       NativeValue* output) {
    ::llvm::IRBuilder<> builder(block_);
    NativeValue default_value = NativeValue::Create(builder.getInt1(false));
    // PERF: preliminary implementation, expect to be slow
    if (in_list.IsTuple()) {
        for (size_t i = 0; i < in_list.GetFieldNum(); ++i) {
            const auto& expr = in_list.GetField(i);

            NativeValue eq_value;
            CHECK_STATUS(BuildEqExpr(lhs, expr, &eq_value));

            CHECK_STATUS(BuildOrExpr(default_value, eq_value, &default_value));
        }
    } else {
        CHECK_TRUE(false, kCodegenError, "Un-supported: in_list value of IN predicate is not tuple");
    }
    if (is_not) {
        CHECK_STATUS(BuildNotExpr(default_value, &default_value));
    }
    *output = default_value;
    return Status::OK();
}

}  // namespace codegen
}  // namespace hybridse
