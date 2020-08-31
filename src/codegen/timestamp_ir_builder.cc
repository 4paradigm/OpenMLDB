/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * timestamp_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/5/22
 *--------------------------------------------------------------------------
 **/
#include "codegen/timestamp_ir_builder.h"
#include <string>
#include <vector>
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"
#include "node/sql_node.h"
namespace fesql {
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
                                          ::llvm::Value* src,
                                          ::llvm::Value** output) {
    base::Status status;
    if (nullptr == src || nullptr == output) {
        status.code = common::kCodegenError;
        status.msg = "Fail to cast timestamp: src or dist is null";
        return status;
    }
    if (IsTimestampPtr(src->getType())) {
        *output = src;
        return status;
    }
    ::llvm::Value* ts = NULL;
    CastExprIRBuilder cast_builder(block);
    ::llvm::IRBuilder<> builder(block);
    if (!cast_builder.IsSafeCast(src->getType(), builder.getInt64Ty())) {
        status.code = common::kCodegenError;
        status.msg = "Fail to cast timestamp: src type " +
                     TypeIRBuilder::TypeName(src->getType());
        return status;
    }
    if (!cast_builder.SafeCast(src, builder.getInt64Ty(), &ts, status)) {
        return status;
    }
    if (!NewTimestamp(block, ts, output)) {
        status.code = common::kCodegenError;
        status.msg = "Fail to cast timestamp: new timestamp fail";
        return status;
    }
    return status;
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
    return Get(block, timestamp, 0, output);
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
    return cast_builder.UnSafeCast(*output, builder.getInt32Ty(), output,
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
    return cast_builder.UnSafeCast(*output, builder.getInt32Ty(), output,
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
    return cast_builder.UnSafeCast(*output, builder.getInt32Ty(), output,
                                   status);
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

}  // namespace codegen
}  // namespace fesql
