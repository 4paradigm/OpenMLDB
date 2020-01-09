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

PredicateIRBuilder::PredicateIRBuilder(::llvm::BasicBlock* block,
                                       ScopeVar* scope_var)
    : block_(block), sv_(scope_var), _cast_expr_ir_builder(block, scope_var) {}
PredicateIRBuilder::~PredicateIRBuilder() {}
bool PredicateIRBuilder::BuildAndExpr(::llvm::Value* left, ::llvm::Value* right,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    return false;
}
bool PredicateIRBuilder::BuildOrExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    return false;
}
bool PredicateIRBuilder::BuildNotExpr(::llvm::Value* left,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    return false;
}
bool PredicateIRBuilder::BuildEqExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    ::llvm::Value* casted_left = NULL;
    ::llvm::Value* casted_right = NULL;

    if (false ==
        inferBaseTypes(left, right, &casted_left, &casted_right, status)) {
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (casted_left->getType()->isIntegerTy()) {
        *output = builder.CreateFCmpOEQ(casted_left, casted_right);
    } else if (casted_left->getType()->isFloatTy() ||
        casted_left->getType()->isDoubleTy()) {
        *output = builder.CreateFCmpOEQ(casted_left, casted_right);
    } else {
        status.msg = "fail to codegen add expr: value types are invalid";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (nullptr == *output) {
        status.msg = "fail to codegen add expr";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    return true;
}
bool PredicateIRBuilder::BuildGtExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    return false;
}
bool PredicateIRBuilder::BuildGeExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    return false;
}
bool PredicateIRBuilder::BuildLtExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    return false;
}
bool PredicateIRBuilder::BuildLeExpr(::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** output,
                                     base::Status& status) {
    return false;
}
bool PredicateIRBuilder::IsAcceptType(::llvm::Type* type) {
    if (nullptr == type) {
        return false;
    }

    ::fesql::type::Type fesql_type;
    if (false == GetTableType(type, &fesql_type)) {
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
bool PredicateIRBuilder::inferBaseTypes(::llvm::Value* left,
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
        } else if (_cast_expr_ir_builder.isStringCast(right_type)) {
            if (!_cast_expr_ir_builder.StringCast(left, casted_left, status)) {
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
}