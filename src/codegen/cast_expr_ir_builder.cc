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
CastExprIRBuilder::CastExprIRBuilder(::llvm::BasicBlock* block)
    : block_(block) {}
CastExprIRBuilder::~CastExprIRBuilder() {}

bool CastExprIRBuilder::IsIntFloat2PointerCast(::llvm::Type* src,
                                               ::llvm::Type* dist) {
    return src->isIntegerTy() && (dist->isFloatTy() || dist->isDoubleTy());
}

bool CastExprIRBuilder::IsSafeCast(::llvm::Type* src, ::llvm::Type* dist) {
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
        case ::fesql::type::kBool: {
            return true;
        }
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
    if (false == IsSafeCast(value->getType(), type)) {
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

bool CastExprIRBuilder::IsStringCast(llvm::Type* type) {
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
                                   base::Status& status) {}
/**
 * bool cast implement
 * cast fesql type to bool: compare value with 0
 * @param value
 * @param casted_value
 * @param status
 * @return
 */
bool CastExprIRBuilder::BoolCast(llvm::Value* value, llvm::Value** casted_value,
                                 base::Status& status) {
    ::llvm::IRBuilder<> builder(block_);
    llvm::Type* type = value->getType();
    if (type->isIntegerTy()) {
        *casted_value =
            builder.CreateICmpNE(value, ::llvm::ConstantInt::get(type, 0));
    } else if (type->isFloatingPointTy()) {
        *casted_value = builder.CreateFCmpOEQ(
            value, ::llvm::ConstantFP::get(type, ::llvm::APFloat(0.0)));
    } else {
        status.msg =
            "fail to codegen cast bool expr: value type isn't compatible";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
