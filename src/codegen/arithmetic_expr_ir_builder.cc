/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * arithmetic_expr_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/cond_select_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/null_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "codegen/type_ir_builder.h"
namespace fesql {
namespace codegen {

ArithmeticIRBuilder::ArithmeticIRBuilder(::llvm::BasicBlock* block)
    : block_(block), cast_expr_ir_builder_(block) {}
ArithmeticIRBuilder::~ArithmeticIRBuilder() {}

bool ArithmeticIRBuilder::IsAcceptType(::llvm::Type* type) {
    return nullptr != type &&
           (type->isIntegerTy() || type->isFloatTy() || type->isDoubleTy() ||
            TypeIRBuilder::IsTimestampPtr(type));
}

bool ArithmeticIRBuilder::InferBaseTypes(::llvm::BasicBlock* block,
                                         ::llvm::Value* left,
                                         ::llvm::Value* right,
                                         ::llvm::Value** casted_left,
                                         ::llvm::Value** casted_right,
                                         ::fesql::base::Status& status) {
    if (NULL == left || NULL == right) {
        status.msg = "left or right value is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::Type* left_type = left->getType();
    ::llvm::Type* right_type = right->getType();

    if (!IsAcceptType(left_type) || !IsAcceptType(right_type)) {
        status.msg = "invalid type for arithmetic expression";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    *casted_left = left;
    *casted_right = right;
    CastExprIRBuilder cast_expr_ir_builder(block);
    if (left_type != right_type) {
        if (cast_expr_ir_builder.IsSafeCast(left_type, right_type)) {
            if (!cast_expr_ir_builder.SafeCast(left, right_type, casted_left,
                                               status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsSafeCast(right_type, left_type)) {
            if (!cast_expr_ir_builder.SafeCast(right, left_type, casted_right,
                                               status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsIntFloat2PointerCast(left_type,
                                                               right_type)) {
            if (!cast_expr_ir_builder.UnSafeCast(left, right_type, casted_left,
                                                 status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsIntFloat2PointerCast(right_type,
                                                               left_type)) {
            if (!cast_expr_ir_builder.UnSafeCast(right, left_type, casted_right,
                                                 status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else {
            status.msg =
                "fail to codegen add expr: value type isn't compatible: " +
                TypeIRBuilder::TypeName(left_type) + " and  " +
                TypeIRBuilder::TypeName(right_type);
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}
bool ArithmeticIRBuilder::InferBaseIntegerTypes(::llvm::BasicBlock* block,
                                                ::llvm::Value* left,
                                                ::llvm::Value* right,
                                                ::llvm::Value** casted_left,
                                                ::llvm::Value** casted_right,
                                                ::fesql::base::Status& status) {
    if (NULL == left || NULL == right) {
        status.msg = "left or right value is null";
        status.code = common::kCodegenError;
        return false;
    }

    ::llvm::Type* left_type = left->getType();
    ::llvm::Type* right_type = right->getType();

    if (!left_type->isIntegerTy() || !right_type->isIntegerTy()) {
        status.msg = "invalid type for integer expression";
        status.code = common::kCodegenError;
        return false;
    }
    *casted_left = left;
    *casted_right = right;
    CastExprIRBuilder cast_expr_ir_builder(block);
    if (left_type != right_type) {
        if (cast_expr_ir_builder.IsSafeCast(left_type, right_type)) {
            if (!cast_expr_ir_builder.SafeCast(left, right_type, casted_left,
                                               status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder.IsSafeCast(right_type, left_type)) {
            if (!cast_expr_ir_builder.SafeCast(right, left_type, casted_right,
                                               status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else {
            status.msg =
                "fail to codegen add expr: value type isn't compatible: " +
                TypeIRBuilder::TypeName(left_type) + " and  " +
                TypeIRBuilder::TypeName(right_type);
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}

bool ArithmeticIRBuilder::InferBaseDoubleTypes(::llvm::BasicBlock* block,
                                               ::llvm::Value* left,
                                               ::llvm::Value* right,
                                               ::llvm::Value** casted_left,
                                               ::llvm::Value** casted_right,
                                               ::fesql::base::Status& status) {
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
    CastExprIRBuilder cast_expr_ir_builder(block);
    if (!left_type->isDoubleTy()) {
        if (!cast_expr_ir_builder.UnSafeCast(
                left, ::llvm::Type::getDoubleTy(block->getContext()),
                casted_left, status)) {
            status.msg = "fail to codegen add expr: " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }
    }

    if (!right_type->isDoubleTy()) {
        if (!cast_expr_ir_builder.UnSafeCast(
                right, ::llvm::Type::getDoubleTy(block->getContext()),
                casted_right, status)) {
            status.msg = "fail to codegen add expr: " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}

// Return left & right
bool ArithmeticIRBuilder::BuildAnd(::llvm::BasicBlock* block,
                                   ::llvm::Value* left, ::llvm::Value* right,
                                   ::llvm::Value** output,
                                   base::Status& status) {
    if (!left->getType()->isIntegerTy() || !right->getType()->isIntegerTy()) {
        status.msg =
            "fail to codegen arithmetic and expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    *output = builder.CreateAnd(left, right);
    return true;
}
bool ArithmeticIRBuilder::BuildLShiftLeft(::llvm::BasicBlock* block,
                                          ::llvm::Value* left,
                                          ::llvm::Value* right,
                                          ::llvm::Value** output,
                                          base::Status& status) {
    if (!left->getType()->isIntegerTy() || !right->getType()->isIntegerTy()) {
        status.msg =
            "fail to codegen logical shift left expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    *output = builder.CreateShl(left, right);
    return true;
}
bool ArithmeticIRBuilder::BuildLShiftRight(::llvm::BasicBlock* block,
                                           ::llvm::Value* left,
                                           ::llvm::Value* right,
                                           ::llvm::Value** output,
                                           base::Status& status) {
    if (!left->getType()->isIntegerTy() || !right->getType()->isIntegerTy()) {
        status.msg =
            "fail to codegen logical shift right expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    *output = builder.CreateLShr(left, right);
    return true;
}
bool ArithmeticIRBuilder::BuildAddExpr(
    ::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
    ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseTypes(block, left, right, &casted_left, &casted_right,
                                status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateAdd(casted_left, casted_right, "expr_add");
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFAdd(casted_left, casted_right, "expr_add");
    } else if (TypeIRBuilder::IsTimestampPtr(casted_left->getType()) &&
               TypeIRBuilder::IsTimestampPtr(casted_right->getType())) {
        ::llvm::Value* ts1;
        ::llvm::Value* ts2;
        ::llvm::Value* ts_add;
        TimestampIRBuilder ts_builder(block->getModule());
        if (!ts_builder.GetTs(block, casted_left, &ts1)) {
            return false;
        }
        if (!ts_builder.GetTs(block, casted_right, &ts2)) {
            return false;
        }
        BuildAddExpr(block, ts1, ts2, &ts_add, status);
        if (!ts_builder.NewTimestamp(block, ts_add, output)) {
            return false;
        }
    } else {
        status.msg = "fail to codegen add expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}


Status ArithmeticIRBuilder::BuildAnd(const NativeValue& left,
                                     const NativeValue& right,
                                     NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildAnd(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildLShiftLeft(
    const NativeValue& left, const NativeValue& right,
    NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildLShiftLeft(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildLShiftRight(
    const NativeValue& left, const NativeValue& right,
    NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildLShiftRight(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildAddExpr(const NativeValue& left,
                                         const NativeValue& right,
                                         NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildAddExpr(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildSubExpr(const NativeValue& left,
                                         const NativeValue& right,
                                         NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildSubExpr(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildMultiExpr(
    const NativeValue& left, const NativeValue& right,
    NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildMultiExpr(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildFDivExpr(
    const NativeValue& left, const NativeValue& right,
    NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildFDivExpr(block, lhs, rhs, output, status);
        },
        value_output));
    if (value_output->IsConstNull()) {
        value_output->SetType(::llvm::Type::getDoubleTy(block_->getContext()));
    }
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildSDivExpr(
    const NativeValue& left, const NativeValue& right,
    NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildSDivExpr(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
Status ArithmeticIRBuilder::BuildModExpr(const NativeValue& left,
                                         const NativeValue& right,
                                         NativeValue* value_output) {  // NOLINT
    CHECK_STATUS(NullIRBuilder::SafeNullBinaryExpr(
        block_, left, right,
        [](::llvm::BasicBlock* block, ::llvm::Value* lhs, ::llvm::Value* rhs,
           ::llvm::Value** output, Status& status) {
            return BuildModExpr(block, lhs, rhs, output, status);
        },
        value_output));
    return Status::OK();
}
bool ArithmeticIRBuilder::BuildSubExpr(
    ::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
    ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseTypes(block, left, right, &casted_left, &casted_right,
                                status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateSub(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFSub(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen sub expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}

bool ArithmeticIRBuilder::BuildMultiExpr(
    ::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
    ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseTypes(block, left, right, &casted_left, &casted_right,
                                status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateMul(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFMul(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen mul expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}

bool ArithmeticIRBuilder::BuildFDivExpr(::llvm::BasicBlock* block,
                                        ::llvm::Value* left,
                                        ::llvm::Value* right,
                                        ::llvm::Value** output,
                                        base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseDoubleTypes(block, left, right, &casted_left,
                                      &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isFloatingPointTy()) {
        *output = builder.CreateFDiv(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen fdiv expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}
bool ArithmeticIRBuilder::BuildSDivExpr(::llvm::BasicBlock* block,
                                        ::llvm::Value* left,
                                        ::llvm::Value* right,
                                        ::llvm::Value** output,
                                        base::Status& status) {
    if (!left->getType()->isIntegerTy() || !right->getType()->isIntegerTy()) {
        status.msg =
            "fail to codegen integer sdiv expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseIntegerTypes(block, left, right, &casted_left,
                                       &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);

    // TODO(someone): fully and correctly handle arithmetic exception
    ::llvm::Type* llvm_ty = casted_right->getType();
    ::llvm::Value* zero = ::llvm::ConstantInt::get(llvm_ty, 0);
    ::llvm::Value* div_is_zero = builder.CreateICmpEQ(casted_right, zero);
    casted_right = builder.CreateSelect(
        div_is_zero, ::llvm::ConstantInt::get(llvm_ty, 1), casted_right);
    ::llvm::Value* div_result = builder.CreateSDiv(casted_left, casted_right);
    div_result = builder.CreateSelect(div_is_zero, zero, div_result);

    *output = div_result;
    return true;
}
bool ArithmeticIRBuilder::BuildModExpr(::llvm::BasicBlock* block,
                                       llvm::Value* left, llvm::Value* right,
                                       llvm::Value** output,
                                       base::Status status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseTypes(block, left, right, &casted_left, &casted_right,
                                status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    if (casted_left->getType()->isIntegerTy()) {
        // TODO(someone): fully and correctly handle arithmetic exception
        ::llvm::Value* zero =
            ::llvm::ConstantInt::get(casted_right->getType(), 0);
        ::llvm::Value* rem_is_zero = builder.CreateICmpEQ(casted_right, zero);
        casted_right = builder.CreateSelect(
            rem_is_zero, ::llvm::ConstantInt::get(casted_right->getType(), 1),
            casted_right);
        ::llvm::Value* srem_result =
            builder.CreateSRem(casted_left, casted_right);
        srem_result = builder.CreateSelect(rem_is_zero, zero, srem_result);
        *output = srem_result;
    } else if (casted_left->getType()->isFloatingPointTy()) {
        *output = builder.CreateFRem(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen mul expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
