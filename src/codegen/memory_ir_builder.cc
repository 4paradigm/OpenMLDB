/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * memory_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/7/22
 *--------------------------------------------------------------------------
 **/
#include "codegen/memery_ir_builder.h"
#include "codegen/variable_ir_builder.h"
namespace fesql {
namespace codegen {
MemoryIRBuilder::MemoryIRBuilder(::llvm::BasicBlock* block, ScopeVar* sv)
    : block_(block), sv_(sv) {}
MemoryIRBuilder::~MemoryIRBuilder() {}

base::Status MemoryIRBuilder::Alloc(::llvm::Value* request_size,
                                    ::llvm::Value** output) {
    CHECK_TRUE(nullptr != sv_, "fail to alloc memory, scope var is null");

    VariableIRBuilder variable_ir_builder(block_, sv_);

    NativeValue mem_pool;
    CHECK_STATUS(variable_ir_builder.LoadMemoryPool(&mem_pool))
    CHECK_TRUE(nullptr != mem_pool.GetRaw(),
               "fail to alloc memory, mem_pool is null");

    ::llvm::IRBuilder<> builder(block_);
    auto ptr_ty = builder.getInt8PtrTy();
    auto int32_ty = builder.getInt32Ty();
    auto alloc_func = block_->getModule()->getOrInsertFunction(
        "fesql_memery_pool_alloc",
        ::llvm::FunctionType::get(ptr_ty, {ptr_ty, int32_ty}, false));
    ::llvm::Value* addr =
        builder.CreateCall(alloc_func, {mem_pool.GetRaw(), request_size});
    CHECK_TRUE(nullptr != addr, "fail to alloc memory, invoke fesql_memery_pool_alloc function fail");
    *output = addr;
    return base::Status();
}
}  // namespace codegen
}  // namespace fesql