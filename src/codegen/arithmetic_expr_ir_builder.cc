/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * arithmetic_expr_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
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

bool ArithmeticIRBuilder::InferBaseTypes(::llvm::Value* left,
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

    if (left_type != right_type) {
        if (cast_expr_ir_builder_.IsSafeCast(left_type, right_type)) {
            if (!cast_expr_ir_builder_.SafeCast(left, right_type, casted_left,
                                                status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder_.IsSafeCast(right_type, left_type)) {
            if (!cast_expr_ir_builder_.SafeCast(right, left_type, casted_right,
                                                status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder_.IsIntFloat2PointerCast(left_type,
                                                                right_type)) {
            if (!cast_expr_ir_builder_.UnSafeCast(left, right_type, casted_left,
                                                  status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (cast_expr_ir_builder_.IsIntFloat2PointerCast(right_type,
                                                                left_type)) {
            if (!cast_expr_ir_builder_.UnSafeCast(right, left_type,
                                                  casted_right, status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else {
            status.msg =
                "fail to codegen add expr: value type isn't compatible";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}

bool ArithmeticIRBuilder::InferBaseDoubleTypes(::llvm::Value* left,
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

    if (!left_type->isDoubleTy()) {
        if (!cast_expr_ir_builder_.UnSafeCast(
                left, ::llvm::Type::getDoubleTy(this->block_->getContext()),
                casted_left, status)) {
            status.msg = "fail to codegen add expr: " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }
    }

    if (!right_type->isDoubleTy()) {
        if (!cast_expr_ir_builder_.UnSafeCast(
                right, ::llvm::Type::getDoubleTy(this->block_->getContext()),
                casted_right, status)) {
            status.msg = "fail to codegen add expr: " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}
// Return left & right
bool ArithmeticIRBuilder::BuildAnd(::llvm::Value* left, ::llvm::Value* right,
                                   ::llvm::Value** output,
                                   base::Status& status) {
    if (!left->getType()->isIntegerTy() || !right->getType()->isIntegerTy()) {
        status.msg =
            "fail to codegen arithmetic and expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    *output = builder.CreateAnd(left, right);
    return true;
}
bool ArithmeticIRBuilder::BuildLShiftLeft(::llvm::Value* left,
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
    ::llvm::IRBuilder<> builder(block_);
    *output = builder.CreateShl(left, right);
    return true;
}
bool ArithmeticIRBuilder::BuildLShiftRight(::llvm::Value* left,
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
    ::llvm::IRBuilder<> builder(block_);
    *output = builder.CreateLShr(left, right);
    return true;
}
bool ArithmeticIRBuilder::BuildAddExpr(
    ::llvm::Value* left, ::llvm::Value* right, ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
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
        TimestampIRBuilder ts_builder(block_->getModule());
        if (!ts_builder.GetTs(block_, casted_left, &ts1)) {
            return false;
        }
        if (!ts_builder.GetTs(block_, casted_right, &ts2)) {
            return false;
        }
        BuildAddExpr(ts1, ts2, &ts_add, status);
        if (!ts_builder.NewTimestamp(block_, ts_add, output)) {
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

bool ArithmeticIRBuilder::BuildSubExpr(
    ::llvm::Value* left, ::llvm::Value* right, ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateSub(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFSub(casted_left, casted_right);
    } else if (TypeIRBuilder::IsTimestampPtr(casted_left->getType()) &&
               TypeIRBuilder::IsTimestampPtr(casted_right->getType())) {
        ::llvm::Value* ts1;
        ::llvm::Value* ts2;
        ::llvm::Value* ts_add;
        TimestampIRBuilder ts_builder(block_->getModule());
        if (!ts_builder.GetTs(block_, casted_left, &ts1)) {
            return false;
        }
        if (!ts_builder.GetTs(block_, casted_right, &ts2)) {
            return false;
        }
        BuildSubExpr(ts1, ts2, &ts_add, status);
        if (!ts_builder.NewTimestamp(block_, ts_add, output)) {
            return false;
        }
    } else {
        status.msg = "fail to codegen sub expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}

bool ArithmeticIRBuilder::BuildMultiExpr(
    ::llvm::Value* left, ::llvm::Value* right, ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
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

bool ArithmeticIRBuilder::BuildFDivExpr(::llvm::Value* left,
                                        ::llvm::Value* right,
                                        ::llvm::Value** output,
                                        base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBaseDoubleTypes(left, right, &casted_left, &casted_right,
                                      status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
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
bool ArithmeticIRBuilder::BuildModExpr(llvm::Value* left, llvm::Value* right,
                                       llvm::Value** output,
                                       base::Status status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateSRem(casted_left, casted_right);
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
