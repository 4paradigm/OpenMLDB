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

#include "codegen/timestamp_ir_builder.h"
#include <string>
#include <vector>
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/null_ir_builder.h"
#include "codegen/predicate_expr_ir_builder.h"
#include "glog/logging.h"
#include "node/sql_node.h"

using hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {
int32_t TimestampIRBuilder::TIME_ZONE = 8;
TimestampIRBuilder::TimestampIRBuilder(::llvm::Module* m)
    : StructTypeIRBuilder(m) {
    InitStructType();
}
TimestampIRBuilder::~TimestampIRBuilder() {}
void TimestampIRBuilder::InitStructType() {
    std::string name = "fe.timestamp";
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);
    ::llvm::Type* ts_ty = (::llvm::Type::getInt64Ty(m_->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(ts_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
    return;
}

base::Status TimestampIRBuilder::CastFrom(::llvm::BasicBlock* block,
                                          const NativeValue& src,
                                          NativeValue* output) {
    base::Status status;
    if (TypeIRBuilder::IsTimestampPtr(src.GetType())) {
        *output = src;
        return Status::OK();
    }

    if (src.IsConstNull()) {
        *output = NativeValue::CreateNull(GetType());
        return Status::OK();
    }
    ::llvm::IRBuilder<> builder(block);
    NativeValue ts;
    CastExprIRBuilder cast_builder(block);
    CondSelectIRBuilder cond_ir_builder;
    PredicateIRBuilder predicate_ir_builder(block);
    NullIRBuilder null_ir_builder;
    if (IsNumber(src.GetType())) {
        CHECK_STATUS(cast_builder.Cast(src, builder.getInt64Ty(), &ts));
        NativeValue cond;
        CHECK_STATUS(predicate_ir_builder.BuildGeExpr(
            ts, NativeValue::Create(builder.getInt64(0)), &cond));
        ::llvm::Value* timestamp;
        CHECK_TRUE(NewTimestamp(block, ts.GetValue(&builder), &timestamp),
                   kCodegenError,
                   "Fail to cast timestamp: new timestamp(ts) fail");
        CHECK_STATUS(
            cond_ir_builder.Select(block, cond, NativeValue::Create(timestamp),
                                   NativeValue::CreateNull(GetType()), output));

    } else if (IsStringPtr(src.GetType()) || IsDatePtr(src.GetType())) {
        ::llvm::IRBuilder<> builder(block);
        ::llvm::Value* dist = nullptr;
        ::llvm::Value* is_null_ptr = CreateAllocaAtHead(
            &builder, builder.getInt1Ty(), "timestamp_is_null_alloca");
        if (!CreateDefault(block, &dist)) {
            status.code = common::kCodegenError;
            status.msg = "Fail to cast date: create default date fail";
            return status;
        }
        ::std::string fn_name = "timestamp." + TypeName(src.GetType());

        auto cast_func = m_->getOrInsertFunction(
            fn_name,
            ::llvm::FunctionType::get(builder.getVoidTy(),
                                      {src.GetType(), dist->getType(),
                                       builder.getInt1Ty()->getPointerTo()},
                                      false));
        builder.CreateCall(cast_func,
                           {src.GetValue(&builder), dist, is_null_ptr});
        ::llvm::Value* should_return_null = builder.CreateLoad(is_null_ptr);
        null_ir_builder.CheckAnyNull(block, src, &should_return_null);
        *output = NativeValue::CreateWithFlag(dist, should_return_null);
    } else {
        status.msg =
            "fail to codegen cast bool expr: value type isn't compatible";
        status.code = common::kCodegenError;
        return status;
    }
    return base::Status::OK();
}
bool TimestampIRBuilder::CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                                  ::llvm::Value* dist) {
    if (nullptr == src || nullptr == dist) {
        LOG(WARNING) << "Fail to copy string: src or dist is null";
        return false;
    }
    if (!IsTimestampPtr(src->getType())) {
        LOG(WARNING) << "Fail to copy string: src isn't Timestamp Ptr";
        return false;
    }
    if (!IsTimestampPtr(dist->getType())) {
        LOG(WARNING) << "Fail to copy string: dist isn't Timestamp Ptr";
        return false;
    }
    ::llvm::Value* ts;
    if (!GetTs(block, src, &ts)) {
        return false;
    }
    if (!SetTs(block, dist, ts)) {
        return false;
    }
    return true;
}
bool TimestampIRBuilder::GetTs(::llvm::BasicBlock* block,
                               ::llvm::Value* timestamp,
                               ::llvm::Value** output) {
    return Load(block, timestamp, 0, output);
}
bool TimestampIRBuilder::SetTs(::llvm::BasicBlock* block,
                               ::llvm::Value* timestamp, ::llvm::Value* ts) {
    return Set(block, timestamp, 0, ts);
}
bool TimestampIRBuilder::Minute(::llvm::BasicBlock* block, ::llvm::Value* value,
                                ::llvm::Value** output, base::Status& status) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* ts;
    if (IsTimestampPtr(value->getType())) {
        if (!GetTs(block, value, &ts)) {
            return false;
        }
    } else {
        ts = value;
    }
    if (!IsInterger(ts->getType())) {
        LOG(WARNING)
            << "fail Get Minute, input value should be timestamp or int";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    ArithmeticIRBuilder arithmetic_builder(block);
    if (!arithmetic_builder.BuildModExpr(
            block, ts, builder.getInt64(1000 * 60 * 60), &ts, status)) {
        LOG(WARNING) << "Fail Get Minute " << status.msg;
        return false;
    }
    if (!arithmetic_builder.BuildSDivExpr(
            block, ts, builder.getInt64(1000 * 60), output, status)) {
        LOG(WARNING) << "Fail Get Minute " << status.msg;
        return false;
    }
    CastExprIRBuilder cast_builder(block);
    return cast_builder.UnSafeCastNumber(*output, builder.getInt32Ty(), output,
                                         status);
}
bool TimestampIRBuilder::Second(::llvm::BasicBlock* block, ::llvm::Value* value,
                                ::llvm::Value** output, base::Status& status) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* ts;
    if (IsTimestampPtr(value->getType())) {
        if (!GetTs(block, value, &ts)) {
            return false;
        }
    } else {
        ts = value;
    }
    if (!IsInterger(ts->getType())) {
        LOG(WARNING)
            << "fail Get Second, input value should be timestamp or int";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    ArithmeticIRBuilder arithmetic_builder(block);
    if (!arithmetic_builder.BuildModExpr(block, ts, builder.getInt64(1000 * 60),
                                         &ts, status)) {
        LOG(WARNING) << "Fail Get Second " << status.msg;
        return false;
    }
    if (!arithmetic_builder.BuildSDivExpr(block, ts, builder.getInt64(1000),
                                          output, status)) {
        LOG(WARNING) << "Fail Get Second " << status.msg;
        return false;
    }
    CastExprIRBuilder cast_builder(block);
    return cast_builder.UnSafeCastNumber(*output, builder.getInt32Ty(), output,
                                         status);
}
bool TimestampIRBuilder::Hour(::llvm::BasicBlock* block, ::llvm::Value* value,
                              ::llvm::Value** output, base::Status& status) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* ts;
    if (IsTimestampPtr(value->getType())) {
        if (!GetTs(block, value, &ts)) {
            return false;
        }
    } else {
        ts = value;
    }
    if (!IsInterger(ts->getType())) {
        LOG(WARNING)
            << "fail Get Hour, input value should be timestamp or interger";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* day_ms = nullptr;
    ArithmeticIRBuilder arithmetic_builder(block);
    if (TIME_ZONE > 0 &&
        !arithmetic_builder.BuildAddExpr(
            block, ts, builder.getInt64(1000 * 60 * 60 * TIME_ZONE), &day_ms,
            status)) {
        LOG(WARNING) << "Fail Get Hour " << status.msg;
        return false;
    }
    if (!arithmetic_builder.BuildModExpr(block, day_ms,
                                         builder.getInt64(1000 * 60 * 60 * 24),
                                         &day_ms, status)) {
        LOG(WARNING) << "Fail Get Hour " << status.msg;
        return false;
    }
    if (!arithmetic_builder.BuildSDivExpr(
            block, day_ms, builder.getInt64(1000 * 60 * 60), output, status)) {
        LOG(WARNING) << "Fail Get Hour " << status.msg;
        return false;
    }
    CastExprIRBuilder cast_builder(block);
    return cast_builder.UnSafeCastNumber(*output, builder.getInt32Ty(), output,
                                         status);
}
bool TimestampIRBuilder::CreateDefault(::llvm::BasicBlock* block,
                                       ::llvm::Value** output) {
    return NewTimestamp(block, output);
}
bool TimestampIRBuilder::NewTimestamp(::llvm::BasicBlock* block,
                                      ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* timestamp;
    if (!Create(block, &timestamp)) {
        return false;
    }
    if (!SetTs(block, timestamp,
               ::llvm::ConstantInt::get(
                   ::llvm::Type::getInt64Ty(m_->getContext()), 0, false))) {
        return false;
    }
    *output = timestamp;
    return true;
}
bool TimestampIRBuilder::NewTimestamp(::llvm::BasicBlock* block,
                                      ::llvm::Value* ts,
                                      ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* timestamp;
    if (!Create(block, &timestamp)) {
        return false;
    }
    if (!SetTs(block, timestamp, ts)) {
        return false;
    }
    *output = timestamp;
    return true;
}
base::Status TimestampIRBuilder::FDiv(::llvm::BasicBlock* block,
                                      ::llvm::Value* timestamp,
                                      ::llvm::Value* right,
                                      ::llvm::Value** output) {
    CHECK_TRUE(nullptr != timestamp && nullptr != right, kCodegenError,
               "Fail Timestamp FDiv: lhs or rhs is null")
    CHECK_TRUE(TypeIRBuilder::IsTimestampPtr(timestamp->getType()),
               kCodegenError, "Fail Timestamp FDiv: lhs type is ",
               TypeIRBuilder::TypeName(timestamp->getType()))
    CHECK_TRUE(TypeIRBuilder::IsNumber(right->getType()), kCodegenError,
               "Fail Timestamp FDiv: lhs type is ",
               TypeIRBuilder::TypeName(right->getType()))

    ::llvm::IRBuilder<> builder(block);
    CastExprIRBuilder cast_ir_builder(block);
    ::llvm::Value* casted_right = nullptr;
    Status status;
    CHECK_TRUE(cast_ir_builder.UnSafeCastNumber(right, builder.getDoubleTy(),
                                                &casted_right, status),
               kCodegenError, status.msg);
    ::llvm::Value* ts = nullptr;
    CHECK_TRUE(GetTs(block, timestamp, &ts), kCodegenError,
               "Fail Timestamp FDiv: fail to get ts");

    ArithmeticIRBuilder arithmetic_ir_builder(block);
    CHECK_TRUE(arithmetic_ir_builder.BuildFDivExpr(block, ts, casted_right,
                                                   output, status),
               kCodegenError, status.msg)
    return Status::OK();
}
// Adds the integer expression interval to the timestamp expression, The unit
// for interval is millisecond
base::Status TimestampIRBuilder::TimestampAdd(::llvm::BasicBlock* block,
                                              ::llvm::Value* timestamp,
                                              ::llvm::Value* duration,
                                              ::llvm::Value** output) {
    CHECK_TRUE(nullptr != timestamp && nullptr != duration, kCodegenError,
               "Fail Timestamp Add: lhs or rhs is null")
    CHECK_TRUE(TypeIRBuilder::IsTimestampPtr(timestamp->getType()),
               kCodegenError, "Fail Timestamp Add: lhs type is ",
               TypeIRBuilder::TypeName(timestamp->getType()))
    CHECK_TRUE(TypeIRBuilder::IsInterger(duration->getType()), kCodegenError,
               "Fail Timestamp Add: lhs type is ",
               TypeIRBuilder::TypeName(duration->getType()))

    ::llvm::IRBuilder<> builder(block);
    CastExprIRBuilder cast_ir_builder(block);
    ::llvm::Value* casted_right = nullptr;
    Status status;
    CHECK_TRUE(cast_ir_builder.UnSafeCastNumber(duration, builder.getInt64Ty(),
                                                &casted_right, status),
               kCodegenError, status.msg);
    ::llvm::Value* ts = nullptr;
    CHECK_TRUE(GetTs(block, timestamp, &ts), kCodegenError,
               "Fail Timestamp Add: fail to get ts");

    ArithmeticIRBuilder arithmetic_ir_builder(block);
    ::llvm::Value* add_ts = nullptr;
    CHECK_TRUE(arithmetic_ir_builder.BuildAddExpr(block, ts, casted_right,
                                                  &add_ts, status),
               kCodegenError, status.msg)
    CHECK_TRUE(NewTimestamp(block, add_ts, output), kCodegenError,
               "Fail Timestamp Add: new timestamp with ts error");
    return Status::OK();
}
}  // namespace codegen
}  // namespace hybridse
