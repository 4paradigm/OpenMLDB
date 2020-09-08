/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cast_expr_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "glog/logging.h"

using fesql::common::kCodegenError;

namespace fesql {
namespace codegen {
CastExprIRBuilder::CastExprIRBuilder(::llvm::BasicBlock* block)
    : block_(block) {}
CastExprIRBuilder::~CastExprIRBuilder() {}

bool CastExprIRBuilder::IsIntFloat2PointerCast(::llvm::Type* src,
                                               ::llvm::Type* dist) {
    return src->isIntegerTy() && (dist->isFloatTy() || dist->isDoubleTy());
}

Status CastExprIRBuilder::IsCastAccept(::llvm::Type* src, ::llvm::Type* dist,
                                       ::llvm::Type** output) {
    if (src == dist || IsSafeCast(src, dist)) {
        *output = dist;
        return Status::OK();
    }

    if (TypeIRBuilder::IsDatePtr(src) && TypeIRBuilder::IsNumber(dist) &&
        !TypeIRBuilder::IsBool(dist)) {
        return Status(common::kCodegenError,
                      "incastable from " + TypeIRBuilder::TypeName(src) +
                          " to " + TypeIRBuilder::TypeName(dist));
    }

    if (TypeIRBuilder::IsNumber(src) && TypeIRBuilder::IsDatePtr(dist)) {
        return Status(common::kCodegenError,
                      "incastable from " + TypeIRBuilder::TypeName(src) +
                          " to " + TypeIRBuilder::TypeName(dist));
    }
    *output = dist;
    return Status::OK();
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
            return false;
        }
        case ::fesql::node::kTimestamp: {
            return ::fesql::node::kInt64 == dist_type;
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
    return false;
}
Status CastExprIRBuilder::Cast(const NativeValue& value,
                               ::llvm::Type* cast_type, NativeValue* output) {
    ::llvm::Type* dist_type;
    CHECK_STATUS(IsCastAccept(value.GetType(), cast_type, &dist_type))
    if (IsSafeCast(value.GetType(), cast_type)) {
        CHECK_STATUS(SafeCast(value, cast_type, output));
    } else {
        CHECK_STATUS(UnSafeCast(value, cast_type, output));
    }
    return Status::OK();
}
Status CastExprIRBuilder::SafeCast(const NativeValue& value, ::llvm::Type* type,
                                   NativeValue* output) {
    ::llvm::IRBuilder<> builder(block_);
    CHECK_TRUE(IsSafeCast(value.GetType(), type), kCodegenError,
               "Safe cast fail: unsafe cast");
    Status status;
    if (value.IsConstNull()) {
        *output = NativeValue::CreateNull(type);
    } else if (TypeIRBuilder::IsTimestampPtr(type)) {
        TimestampIRBuilder timestamp_ir_builder(block_->getModule());
        CHECK_STATUS(timestamp_ir_builder.CastFrom(block_, value, output));
        return Status::OK();
    } else if (TypeIRBuilder::IsDatePtr(type)) {
        DateIRBuilder date_ir_builder(block_->getModule());
        CHECK_STATUS(date_ir_builder.CastFrom(block_, value, output));
        return Status::OK();
    } else if (TypeIRBuilder::IsStringPtr(type)) {
        StringIRBuilder string_ir_builder(block_->getModule());
        CHECK_STATUS(string_ir_builder.CastFrom(block_, value, output));
        return Status::OK();
    } else if (TypeIRBuilder::IsNumber(type)) {
        Status status;
        ::llvm::Value* output_value = nullptr;
        CHECK_TRUE(SafeCastNumber(value.GetValue(&builder), type, &output_value,
                                  status),
                   kCodegenError);
        if (value.IsNullable()) {
            *output = NativeValue::CreateWithFlag(output_value,
                                                  value.GetIsNull(&builder));
        } else {
            *output = NativeValue::Create(output_value);
        }
    } else {
        return Status(common::kCodegenError,
                      "Can't cast from " +
                          TypeIRBuilder::TypeName(value.GetType()) + " to " +
                          TypeIRBuilder::TypeName(type));
    }
    return Status::OK();
}
Status CastExprIRBuilder::UnSafeCast(const NativeValue& value,
                                     ::llvm::Type* type, NativeValue* output) {
    ::llvm::IRBuilder<> builder(block_);
    if (value.IsConstNull()) {
        *output = NativeValue::CreateNull(type);
    } else if (TypeIRBuilder::IsTimestampPtr(type)) {
        TimestampIRBuilder timestamp_ir_builder(block_->getModule());
        CHECK_STATUS(timestamp_ir_builder.CastFrom(block_, value, output));
        return Status::OK();
    } else if (TypeIRBuilder::IsDatePtr(type)) {
        DateIRBuilder date_ir_builder(block_->getModule());
        CHECK_STATUS(date_ir_builder.CastFrom(block_, value, output));
        return Status::OK();
    } else if (TypeIRBuilder::IsStringPtr(type)) {
        StringIRBuilder string_ir_builder(block_->getModule());
        CHECK_STATUS(string_ir_builder.CastFrom(block_, value, output));
        return Status::OK();
    } else {
        Status status;
        ::llvm::Value* output_value = nullptr;
        CHECK_TRUE(UnSafeCastNumber(value.GetValue(&builder), type,
                                    &output_value, status),
                   kCodegenError, status.msg);
        if (value.IsNullable()) {
            *output = NativeValue::CreateWithFlag(output_value,
                                                  value.GetIsNull(&builder));
        } else {
            *output = NativeValue::Create(output_value);
        }
    }
    return Status::OK();
}
bool CastExprIRBuilder::SafeCastNumber(::llvm::Value* value, ::llvm::Type* type,
                                       ::llvm::Value** output,
                                       base::Status& status) {
    if (value->getType() == type) {
        *output = value;
        return true;
    }
    ::llvm::IRBuilder<> builder(block_);
    if (value->getType()->isIntegerTy() && type->isIntegerTy()) {
        if (value->getType()->isIntegerTy(1)) {
            *output = builder.CreateZExt(value, type);
        } else {
            *output = builder.CreateSExt(value, type);
        }
    } else if (value->getType()->isFloatingPointTy() &&
               type->isFloatingPointTy()) {
        *output = builder.CreateFPExt(value, type);
    } else if (value->getType()->isIntegerTy(1) && type->isFloatingPointTy()) {
        *output = builder.CreateUIToFP(value, type);
    } else if (value->getType()->isIntegerTy() && type->isFloatingPointTy()) {
        *output = builder.CreateSIToFP(value, type);
    } else if (TypeIRBuilder::IsTimestampPtr(value->getType())) {
        ::llvm::Value* ts = nullptr;
        TimestampIRBuilder timestamp_ir_builder(block_->getModule());
        if (!timestamp_ir_builder.GetTs(block_, value, &ts)) {
            status.msg = "fail to codegen cast expr: extract timestamp error";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
        return SafeCastNumber(ts, type, output, status);
    } else {
        status.msg =
            "fail to codegen cast expr: value type isn't compatible: from " +
            TypeIRBuilder::TypeName(value->getType()) + " to " +
            TypeIRBuilder::TypeName(type);
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    if (NULL == *output) {
        status.msg = "fail to cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    return true;
}

bool CastExprIRBuilder::UnSafeCastNumber(::llvm::Value* value,
                                         ::llvm::Type* type,
                                         ::llvm::Value** output,
                                         base::Status& status) {
    if (IsSafeCast(value->getType(), type)) {
        return SafeCastNumber(value, type, output, status);
    }
    ::llvm::IRBuilder<> builder(block_);
    if (TypeIRBuilder::IsBool(type)) {
        return BoolCast(value, output, status);
    } else if (TypeIRBuilder::IsTimestampPtr(value->getType()) &&
               TypeIRBuilder::IsNumber(type)) {
        ::llvm::Value* ts = nullptr;
        TimestampIRBuilder timestamp_ir_builder(block_->getModule());
        if (!timestamp_ir_builder.GetTs(block_, value, &ts)) {
            status.msg = "fail to codegen cast expr: extract timestamp error";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
        return UnSafeCastNumber(ts, type, output, status);
    } else if (value->getType()->isIntegerTy() && type->isIntegerTy()) {
        *output = builder.CreateTrunc(value, type);
    } else if (value->getType()->isFloatingPointTy() &&
               type->isFloatingPointTy()) {
        *output = builder.CreateFPTrunc(value, type);
    } else if (value->getType()->isIntegerTy(1) && type->isFloatingPointTy()) {
        // bool -> float/double
        *output = builder.CreateUIToFP(value, type);
    } else if (value->getType()->isIntegerTy() && type->isFloatTy()) {
        if (value->getType()->getIntegerBitWidth() > 4) {
            value = builder.CreateTrunc(value, builder.getInt32Ty());
        }
        *output = builder.CreateSIToFP(value, builder.getFloatTy());
    } else if (value->getType()->isIntegerTy() && type->isDoubleTy()) {
        *output = builder.CreateSIToFP(value, builder.getDoubleTy());
    } else if (value->getType()->isFloatingPointTy() && type->isIntegerTy()) {
        *output = builder.CreateFPToSI(value, type);
    } else {
        status.msg = "Can't cast from " +
                     TypeIRBuilder::TypeName(value->getType()) + " to " +
                     TypeIRBuilder::TypeName(type);
        status.code = common::kCodegenError;
        return false;
    }
    if (NULL == *output) {
        status.msg = "fail to cast";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
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
    } else if (TypeIRBuilder::IsTimestampPtr(type)) {
        TimestampIRBuilder timestamp_ir_builder(block_->getModule());
        ::llvm::Value* ts = nullptr;
        if (!timestamp_ir_builder.GetTs(block_, value, &ts)) {
            status.msg = "fail to codegen cast bool expr: get ts error";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
        return BoolCast(ts, casted_value, status);
    } else {
        status.msg =
            "fail to codegen cast bool expr: value type isn't compatible";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
