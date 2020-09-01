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
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
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

    ::fesql::node::DataType src_type;
    ::fesql::node::DataType dist_type;
    ::fesql::codegen::GetBaseType(src, &src_type);
    ::fesql::codegen::GetBaseType(dist, &dist_type);
    if (::fesql::node::kVarchar == dist_type) {
        return true;
    }
    switch (src_type) {
        case ::fesql::node::kBool: {
            return true;
        }
        case ::fesql::node::kInt16: {
            if (::fesql::node::kBool == dist_type) {
                return false;
            } else {
                return true;
            }
        }
        case ::fesql::node::kInt32: {
            if (::fesql::node::kBool == dist_type ||
                ::fesql::node::kInt16 == dist_type) {
                return false;
            }
            return true;
        }
        case ::fesql::node::kInt64: {
            return ::fesql::node::kTimestamp == dist_type;
        }
        case ::fesql::node::kTimestamp: {
            return false;
        }
        case ::fesql::node::kFloat: {
            if (::fesql::node::kDouble == dist_type) {
                return true;
            }
            return false;
        }
        case ::fesql::node::kDouble: {
            return false;
        }
        default: {
            return false;
        }
    }
}
Status CastExprIRBuilder::SafeCast(const NativeValue& value, ::llvm::Type* type,
                                   NativeValue* output) {
    ::llvm::IRBuilder<> builder(block_);
    CHECK_TRUE(IsSafeCast(value.GetType(), type),
               "Safe cast fail: unsafe cast");
    if (value.IsConstNull()) {
        *output = NativeValue::CreateNull(type);
    } else {
        Status status;
        ::llvm::Value* output_value = nullptr;
        CHECK_TRUE(
            SafeCast(value.GetValue(&builder), type, &output_value, status))
        if (value.IsNullable()) {
            *output = NativeValue::CreateWithFlag(output_value,
                                                  value.GetIsNull(&builder));
        } else {
            *output = NativeValue::Create(output_value);
        }
    }
    return Status::OK();
}
bool CastExprIRBuilder::SafeCast(::llvm::Value* value, ::llvm::Type* type,
                                 ::llvm::Value** output, base::Status& status) {
    ::llvm::IRBuilder<> builder(block_);
    if (false == IsSafeCast(value->getType(), type)) {
        status.msg = "unsafe cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    // Block entry (label_entry)
    if (false == ::llvm::CastInst::isCastable(value->getType(), type)) {
        status.msg = "can not safe cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (TypeIRBuilder::IsTimestampPtr(type)) {
        return TimestampCast(value, output, status);
    } else if (codegen::IsStringType(type)) {
        return StringCast(value, output, status);
    } else if (value->getType()->isIntegerTy() && type->isIntegerTy()) {
        *output = builder.CreateZExt(value, type);
    } else if (value->getType()->isIntegerTy() && type->isFloatingPointTy()) {
        *output = builder.CreateUIToFP(value, type);
    } else {
        ::llvm::Instruction::CastOps cast_op =
            ::llvm::CastInst::getCastOpcode(value, true, type, true);
        *output = builder.CreateCast(cast_op, value, type);
    }
    if (NULL == *output) {
        status.msg = "fail to cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
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

    ::fesql::node::DataType fesql_type;
    if (false == GetBaseType(type, &fesql_type)) {
        return false;
    }

    return ::fesql::node::kVarchar == fesql_type;
}

// TODO(chenjing): string cast implement
// try to cast other type of value to string type
bool CastExprIRBuilder::StringCast(llvm::Value* value,
                                   llvm::Value** casted_value,
                                   base::Status& status) {
    StringIRBuilder string_ir_builder(block_->getModule());
    status = string_ir_builder.CastFrom(block_, value, casted_value);
    return status.isOK();
}

bool CastExprIRBuilder::TimestampCast(llvm::Value* value,
                                      llvm::Value** casted_value,
                                      base::Status& status) {
    ::llvm::Value* ts;
    if (!SafeCast(value, ::llvm::Type::getInt64Ty(block_->getContext()), &ts,
                  status)) {
        return false;
    }
    TimestampIRBuilder builder(block_->getModule());
    status = builder.CastFrom(block_, value, casted_value);
    return status.isOK();
}

// cast fesql type to bool: compare value with 0
bool CastExprIRBuilder::BoolCast(llvm::Value* value, llvm::Value** casted_value,
                                 base::Status& status) {
    ::llvm::IRBuilder<> builder(block_);
    llvm::Type* type = value->getType();
    if (type->isIntegerTy()) {
        *casted_value =
            builder.CreateICmpNE(value, ::llvm::ConstantInt::get(type, 0));
    } else if (type->isFloatTy()) {
        ::llvm::Value* float0 =
            ::llvm::ConstantFP::get(type, ::llvm::APFloat(0.0f));
        *casted_value = builder.CreateFCmpUNE(value, float0);
    } else if (type->isDoubleTy()) {
        ::llvm::Value* double0 =
            ::llvm::ConstantFP::get(type, ::llvm::APFloat(0.0));
        *casted_value = builder.CreateFCmpUNE(value, double0);
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
