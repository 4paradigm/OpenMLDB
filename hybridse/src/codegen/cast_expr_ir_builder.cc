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

#include "codegen/cast_expr_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "glog/logging.h"
#include "node/node_manager.h"

using hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {
CastExprIRBuilder::CastExprIRBuilder(::llvm::BasicBlock* block)
    : block_(block) {}
CastExprIRBuilder::~CastExprIRBuilder() {}

bool CastExprIRBuilder::IsIntFloat2PointerCast(::llvm::Type* src,
                                               ::llvm::Type* dist) {
    return src->isIntegerTy() && (dist->isFloatTy() || dist->isDoubleTy());
}
Status CastExprIRBuilder::InferNumberCastTypes(::llvm::Type* lhs,
                                               ::llvm::Type* rhs) {
    const node::TypeNode* left_type = nullptr;
    const node::TypeNode* right_type = nullptr;
    node::NodeManager tmp_node_manager;
    CHECK_TRUE(GetFullType(&tmp_node_manager, lhs, &left_type), kCodegenError,
               "invalid op type")
    CHECK_TRUE(GetFullType(&tmp_node_manager, rhs, &right_type), kCodegenError,
               "invalid op type")
    const node::TypeNode* output_type = nullptr;

    CHECK_STATUS(node::ExprNode::InferNumberCastTypes(
        &tmp_node_manager, left_type, right_type, &output_type))
    return Status::OK();
}
bool CastExprIRBuilder::IsSafeCast(::llvm::Type* lhs, ::llvm::Type* rhs) {
    const node::TypeNode* left_type = nullptr;
    const node::TypeNode* right_type = nullptr;
    node::NodeManager tmp_node_manager;
    if (!GetFullType(&tmp_node_manager, lhs, &left_type)) {
        return false;
    }
    if (!GetFullType(&tmp_node_manager, rhs, &right_type)) {
        return false;
    }
    return node::ExprNode::IsSafeCast(left_type, right_type);
}
Status CastExprIRBuilder::Cast(const NativeValue& value,
                               ::llvm::Type* cast_type, NativeValue* output) {
    CHECK_STATUS(TypeIRBuilder::BinaryOpTypeInfer(node::ExprNode::IsCastAccept,
                                                  value.GetType(), cast_type));
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
        if (TypeIRBuilder::IsStringPtr(type)) {
            StringIRBuilder string_ir_builder(block_->getModule());
            CHECK_STATUS(string_ir_builder.CreateNull(block_, output));
            return base::Status::OK();
        } else {
            *output = NativeValue::CreateNull(type);
        }
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
        if (TypeIRBuilder::IsStringPtr(type)) {
            StringIRBuilder string_ir_builder(block_->getModule());
            CHECK_STATUS(string_ir_builder.CreateNull(block_, output));
            return base::Status::OK();

        } else if (TypeIRBuilder::IsDatePtr(type)) {
            DateIRBuilder date_ir(block_->getModule());
            CHECK_STATUS(date_ir.CreateNull(block_, output));
            return base::Status::OK();
        } else {
            *output = NativeValue::CreateNull(type);
        }
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
    } else if (TypeIRBuilder::IsNumber(type) &&
               TypeIRBuilder::IsStringPtr(value.GetType())) {
        StringIRBuilder string_ir_builder(block_->getModule());
        CHECK_STATUS(
            string_ir_builder.CastToNumber(block_, value, type, output));
        return Status::OK();
    } else if (TypeIRBuilder::IsNumber(type) &&
               TypeIRBuilder::IsDatePtr(value.GetType())) {
        *output = NativeValue::CreateNull(type);
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
    } else if (value->getType()->isIntegerTy(8)) {
        value = builder.CreateTrunc(value, builder.getInt32Ty());
        *output = builder.CreateSIToFP(value, builder.getFloatTy());
    } else if (value->getType()->isIntegerTy() && type->isFloatTy()) {
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

// cast number type to bool: compare value with 0
// cast string type to bool: compare string size with 0
// cast timestamp type to bool: compare ts with 0
// cast date type to bool: compare date code with 0
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
    } else if (TypeIRBuilder::IsDatePtr(type)) {
        DateIRBuilder date_ir_builder(block_->getModule());
        ::llvm::Value* date = nullptr;
        if (!date_ir_builder.GetDate(block_, value, &date)) {
            status.msg = "fail to codegen cast bool expr: get date error";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
        return BoolCast(date, casted_value, status);
    } else if (TypeIRBuilder::IsStringPtr(type)) {
        StringIRBuilder string_ir_builder(block_->getModule());
        ::llvm::Value* size = nullptr;
        if (!string_ir_builder.GetSize(block_, value, &size)) {
            status.msg =
                "fail to codegen cast bool expr: get string size error";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
        return BoolCast(size, casted_value, status);
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
}  // namespace hybridse
