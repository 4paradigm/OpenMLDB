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
namespace fesql {
namespace codegen {

ArithmeticIRBuilder::ArithmeticIRBuilder(::llvm::BasicBlock* block,
                                         ScopeVar* scope_var)
    : block_(block), sv_(scope_var), _cast_expr_ir_builder(block, scope_var) {}
ArithmeticIRBuilder::~ArithmeticIRBuilder() {}

bool ArithmeticIRBuilder::CastTypes(::llvm::Value* left, ::llvm::Value* right,
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
    *casted_left = left;
    *casted_right = right;

    if (casted_left != casted_right) {
        if (_cast_expr_ir_builder.isSafeCast(left_type, right_type)) {
            if (!_cast_expr_ir_builder.SafeCast(left, right_type, casted_left,
                                                status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (_cast_expr_ir_builder.isSafeCast(right_type, left_type)) {
            if (!_cast_expr_ir_builder.SafeCast(right, left_type, casted_right,
                                                status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (_cast_expr_ir_builder.isIFCast(left_type, right_type)) {
            if (!_cast_expr_ir_builder.UnSafeCast(left, right_type, casted_left,
                                                  status)) {
                status.msg = "fail to codegen add expr: " + status.msg;
                LOG(WARNING) << status.msg;
                return false;
            }
        } else if (_cast_expr_ir_builder.isIFCast(right_type, left_type)) {
            if (!_cast_expr_ir_builder.UnSafeCast(right, left_type,
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

bool ArithmeticIRBuilder::BuildAddExpr(
    ::llvm::Value* left, ::llvm::Value* right, ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == CastTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        builder.CreateAdd(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy()) {
        builder.CreateFAdd(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen add expr: value type isn't compatible";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return false;
}

bool ArithmeticIRBuilder::BuildSubExpr(
    ::llvm::Value* left, ::llvm::Value* right, ::llvm::Value** output,
    ::fesql::base::Status& status) {  // NOLINT

    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == CastTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        builder.CreateSub(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy()) {
        builder.CreateFSub(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen sub expr: value type isn't compatible";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return false;
}

}  // namespace codegen
}  // namespace fesql
