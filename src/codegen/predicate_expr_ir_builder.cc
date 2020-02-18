/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * predicate_expr_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/1/9
 *--------------------------------------------------------------------------
 **/

#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
namespace fesql {
namespace codegen {

PredicateIRBuilder::PredicateIRBuilder(::llvm::BasicBlock* block)
    : block_(block), cast_expr_ir_builder_(block) {}
PredicateIRBuilder::~PredicateIRBuilder() {}
bool PredicateIRBuilder::BuildAndExpr(::llvm::Value* left, ::llvm::Value* right,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBoolTypes(left, &casted_left, status)) {
        return false;
    }
    if (false == InferBoolTypes(right, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy(1)) {
        *output = builder.CreateAnd(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen &&(and) expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen &&(and) expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildOrExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false == InferBoolTypes(left, &casted_left, status)) {
        return false;
    }
    if (false == InferBoolTypes(right, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy(1)) {
        *output = builder.CreateOr(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen ||(or) expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen ||(or) expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildNotExpr(::llvm::Value* left,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    ::llvm::Value* casted_left = NULL;

    if (false == InferBoolTypes(left, &casted_left, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy(1)) {
        *output = builder.CreateNot(casted_left);
    } else {
        status.msg = "fail to codegen !(not) expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen !(not) expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildEqExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpEQ(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOEQ(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen == expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen == expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildNeqExpr(::llvm::Value* left, ::llvm::Value* right,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpNE(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpUNE(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen neq expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen == expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildGtExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSGT(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOGT(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen > expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen > expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildGeExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSGE(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOGE(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen >= expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen >= expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildLtExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSLT(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOLT(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen < expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen < expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildLeExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        InferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateICmpSLE(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
               casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOLE(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen <= expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen <= expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::IsAcceptType(::llvm::Type* type) {
    if (nullptr == type) {
        return false;
    }

    ::fesql::type::Type fesql_type;
    if (false == GetBaseType(type, &fesql_type)) {
        return false;
    }
    switch (fesql_type) {
        case ::fesql::type::kVoid:
        case ::fesql::type::kList:
        case ::fesql::type::kTimestamp:
        case ::fesql::type::kDate:
        case ::fesql::type::kVarchar:
            return false;
        default: {
            return true;
        }
    }
}

bool PredicateIRBuilder::InferBoolTypes(::llvm::Value* value,
                                        ::llvm::Value** casted_value,
                                        ::fesql::base::Status& status) {
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

    ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(block_->getContext());
    if (type != bool_ty) {
        if (!cast_expr_ir_builder_.BoolCast(value, casted_value, status)) {
            status.msg = "fail to codegen add expr: " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}
bool PredicateIRBuilder::InferBaseTypes(::llvm::Value* left,
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
        } else if (cast_expr_ir_builder_.IsStringCast(right_type)) {
            if (!cast_expr_ir_builder_.StringCast(left, casted_left, status)) {
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
}  // namespace codegen
}  // namespace fesql
