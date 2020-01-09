/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cast_expr_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"
namespace fesql {
namespace codegen {
CastExprIRBuilder::CastExprIRBuilder(::llvm::BasicBlock* block,
                                     ScopeVar* scope_var)
    : block_(block), sv_(scope_var) {}
CastExprIRBuilder::~CastExprIRBuilder() {}

bool CastExprIRBuilder::isIFCast(::llvm::Type* src, ::llvm::Type* dist) {
    return src->isIntegerTy() && (dist->isFloatTy() || dist->isDoubleTy());
}

bool CastExprIRBuilder::isSafeCast(::llvm::Type* src, ::llvm::Type* dist) {
    if (NULL == src || NULL == dist) {
        LOG(WARNING) << "cast type is null";
        return false;
    }

    if (src == dist) {
        return true;
    }
    ::fesql::type::Type src_type;
    ::fesql::type::Type dist_type;
    ::fesql::codegen::GetTableType(src, &src_type);
    ::fesql::codegen::GetTableType(dist, &dist_type);

    switch (src_type) {
        case ::fesql::type::kInt16: {
            return true;
        }
        case ::fesql::type::kInt32: {
            if (::fesql::type::kInt16 == dist_type) {
                return false;
            }
            return true;
        }
        case ::fesql::type::kInt64: {
            return false;
        }
        case ::fesql::type::kFloat: {
            if (::fesql::type::kDouble == dist_type) {
                return true;
            }
            return false;
        }
        case ::fesql::type::kDouble: {
            return false;
        }
        default: {
            return false;
        }
    }
}
/**
 *
 * @param value
 * @param type
 * @param output
 * @param status
 * @return
 */
bool CastExprIRBuilder::SafeCast(::llvm::Value* value, ::llvm::Type* type,
                                 ::llvm::Value** output, base::Status& status) {
    ::llvm::IRBuilder<> builder(block_);
    // Block entry (label_entry)
    if (false == ::llvm::CastInst::isCastable(value->getType(), type)) {
        status.msg = "can not safe cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (false == isSafeCast(value->getType(), type)) {
        status.msg = "unsafe cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Instruction::CastOps cast_op =
        ::llvm::CastInst::getCastOpcode(value, true, type, true);

    ::llvm::Value* cast_value = builder.CreateCast(cast_op, value, type);
    if (NULL == cast_value) {
        status.msg = "fail to cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    *output = cast_value;
    return true;
}
bool CastExprIRBuilder::UnSafeCast(::llvm::Value* value, ::llvm::Type* type,
                                   ::llvm::Value** output,
                                   base::Status& status) {
    ::llvm::IRBuilder<> builder(block_);
    // Block entry (label_entry)
    if (false == ::llvm::CastInst::isCastable(value->getType(), type)) {
        status.msg = "can not safe cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Instruction::CastOps cast_op =
        ::llvm::CastInst::getCastOpcode(value, true, type, true);
    ::llvm::Value* cast_value = builder.CreateCast(cast_op, value, type);
    if (NULL == cast_value) {
        status.msg = "fail to cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    *output = cast_value;
    return true;
}

bool CastExprIRBuilder::isStringCast(llvm::Type* type) {
    if (nullptr == type) {
        return false;
    }

    ::fesql::type::Type fesql_type;
    if (false == GetTableType(type, &fesql_type)) {
        return false;
    }

    return ::fesql::type::kVarchar == fesql_type;
}
/**
 * TODO(chenjing): string cast implement
 * cast fesql type to string
 * @param value
 * @param casted_value
 * @param status
 * @return
 */
bool CastExprIRBuilder::StringCast(llvm::Value* value,
                                   llvm::Value** casted_value,
                                   base::Status& status) {
}

}  // namespace codegen
}  // namespace fesql