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
bool TimestampIRBuilder::CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
              ::llvm::Value* dist) {
    if (nullptr == src || nullptr == dist) {
        LOG(WARNING) << "Fail to copy string: src or dist is null";
        return false;
    }
    if (!IsTimestampPtr(src->getType()) || !IsTimestampPtr(dist->getType())) {
        LOG(WARNING) << "Fail to copy string: src or dist isn't Timestamp Ptr";
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
