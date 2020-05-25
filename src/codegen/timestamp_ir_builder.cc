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
TimestampIRBuilder::TimestampIRBuilder() : TypeIRBuilder() {}
TimestampIRBuilder::~TimestampIRBuilder() {}

::llvm::Type* TimestampIRBuilder::GetType(::llvm::Module* m) {
    std::string name = "fe.timestamp";
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m->getTypeByName(sr);
    if (stype != NULL) {
        return stype;
    }
    stype = ::llvm::StructType::create(m->getContext(), name);
    ::llvm::Type* ts_ty = (::llvm::Type::getInt64Ty(m->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(ts_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    return stype;
}

bool TimestampIRBuilder::GetTs(::llvm::BasicBlock* block,
                               ::llvm::Value* timestamp,
                               ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }

    ::llvm::Type* timestamp_type = NULL;
    bool ok = GetLLVMType(block, ::fesql::node::kTimestamp, &timestamp_type);
    if (!ok) return false;
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* ts_ptr =
        builder.CreateStructGEP(timestamp_type, timestamp, 0);
    *output = builder.CreateLoad(ts_ptr);
    return true;
}
bool TimestampIRBuilder::SetTs(::llvm::BasicBlock* block,
                               ::llvm::Value* timestamp, ::llvm::Value* ts) {
    if (block == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }

    ::llvm::Type* timestamp_type = NULL;
    bool ok = GetLLVMType(block, ::fesql::node::kTimestamp, &timestamp_type);
    if (!ok) return false;
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* ts_ptr =
        builder.CreateStructGEP(timestamp_type, timestamp, 0);
    builder.CreateStore(ts, ts_ptr);
    return true;
}
bool TimestampIRBuilder::NewTimestamp(::llvm::BasicBlock* block,
                                      ::llvm::Value* ts,
                                      ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Type* timestamp_type = NULL;
    bool ok = GetLLVMType(block, ::fesql::node::kTimestamp, &timestamp_type);
    if (!ok) return false;
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* timestamp = builder.CreateAlloca(timestamp_type);
    ::llvm::Value* ts_ptr =
        builder.CreateStructGEP(timestamp_type, timestamp, 0);
    builder.CreateStore(ts, ts_ptr, false);
    *output = timestamp;
    return true;
}

}  // namespace codegen
}  // namespace fesql
