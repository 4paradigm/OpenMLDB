/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * memory_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/7/22
 *--------------------------------------------------------------------------
 **/
#include "codegen/memery_ir_builder.h"
namespace fesql {
namespace codegen {
MemoryIRBuilder::MemoryIRBuilder(::llvm::BasicBlock* block)
    : block_(block) {}
MemoryIRBuilder::~MemoryIRBuilder() {}

base::Status MemoryIRBuilder::Alloc(::llvm::Value* request_size,
                                    ::llvm::Value** output) {
    CHECK_TRUE(nullptr != request_size, "fail to alloc memory, request size value is null");

    ::llvm::IRBuilder<> builder(block_);
    auto ptr_ty = builder.getInt8PtrTy();
    auto int32_ty = builder.getInt32Ty();
    auto alloc_func = block_->getModule()->getOrInsertFunction(
        "fesql_memery_pool_alloc",
        ::llvm::FunctionType::get(ptr_ty, {int32_ty}, false));
    ::llvm::Value* addr = builder.CreateCall(alloc_func, {request_size});
    CHECK_TRUE(
        nullptr != addr,
        "fail to alloc memory, invoke fesql_memery_pool_alloc function fail");
    *output = addr;
    return base::Status();
}
}  // namespace codegen
}  // namespace fesql