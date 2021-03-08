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
#include "codegen/memery_ir_builder.h"

namespace fesql {
namespace codegen {

using common::kCodegenError;

MemoryIRBuilder::MemoryIRBuilder(::llvm::Module* m) : m_(m) {}
MemoryIRBuilder::~MemoryIRBuilder() {}

base::Status MemoryIRBuilder::Alloc(::llvm::BasicBlock* block,
                                    const NativeValue& request_size,
                                    NativeValue* output) {
    CHECK_TRUE(nullptr != request_size.GetRaw(), kCodegenError,
               "fail to alloc memory, request size value is null");

    ::llvm::IRBuilder<> builder(block);
    auto ptr_ty = builder.getInt8PtrTy();
    auto int32_ty = builder.getInt32Ty();
    auto alloc_func = m_->getOrInsertFunction(
        "fesql_memery_pool_alloc",
        ::llvm::FunctionType::get(ptr_ty, {int32_ty}, false));
    ::llvm::Value* addr =
        builder.CreateCall(alloc_func, {request_size.GetRaw()});
    CHECK_TRUE(
        nullptr != addr, kCodegenError,
        "fail to alloc memory, invoke fesql_memery_pool_alloc function fail");
    *output = NativeValue::Create(addr);
    return base::Status();
}

base::Status MemoryIRBuilder::MemoryCopy(::llvm::BasicBlock* block,
                                         const NativeValue& dist,
                                         const NativeValue& src,
                                         const NativeValue& size) {
    CHECK_TRUE(nullptr != dist.GetRaw(), kCodegenError,
               "fail to copy memory, dist llvm value is null");
    CHECK_TRUE(nullptr != src.GetRaw(), kCodegenError,
               "fail to copy memory, src llvm value is null");
    CHECK_TRUE(nullptr != size.GetRaw(), kCodegenError,
               "fail to copy memory, size llvm value is null");

    ::llvm::IRBuilder<> builder(block);

    codegen::CastExprIRBuilder cast_ir_builder(block);
    CHECK_TRUE(cast_ir_builder.IsSafeCast(size.GetType(), builder.getInt64Ty()),
               kCodegenError, "fail to add memory addr: size type invalid");
    ::llvm::Value* size_int64 = size.GetRaw();
    base::Status status;
    CHECK_TRUE(cast_ir_builder.SafeCastNumber(
                   size.GetRaw(), builder.getInt64Ty(), &size_int64, status),
               kCodegenError, "fail to add memory addr: size cast int64 fail");

    ::llvm::Value* ret =
        builder.CreateMemCpy(dist.GetRaw(), 1, src.GetRaw(), 1, size_int64);
    CHECK_TRUE(nullptr != ret, kCodegenError,
               "fail to copy memory, CreateMemCpy fail");
    return base::Status();
}

base::Status MemoryIRBuilder::MemoryAddrAdd(::llvm::BasicBlock* block,
                                            const NativeValue& addr,
                                            const NativeValue& size,
                                            NativeValue* new_addr) {
    CHECK_TRUE(nullptr != addr.GetRaw(), kCodegenError,
               "fail to add memory addr, addr llvm value is null");
    CHECK_TRUE(nullptr != size.GetRaw(), kCodegenError,
               "fail to add memory addr, size llvm value is null");

    codegen::CastExprIRBuilder cast_ir_builder(block);
    ::llvm::IRBuilder<> builder(block);
    CHECK_TRUE(cast_ir_builder.IsSafeCast(size.GetType(), builder.getInt64Ty()),
               kCodegenError, "fail to add memory addr: size type invalid");
    ::llvm::Value* size_int64 = size.GetRaw();
    base::Status status;
    CHECK_TRUE(cast_ir_builder.SafeCastNumber(
                   size.GetRaw(), builder.getInt64Ty(), &size_int64, status),
               kCodegenError, "fail to add memory addr: size cast int64 fail");

    ::llvm::Value* ret = builder.CreateInBoundsGEP(builder.getInt8Ty(),
                                                   addr.GetRaw(), size_int64);
    CHECK_TRUE(nullptr != ret, kCodegenError,
               "fail to add memory addr, CreateMemCpy fail");
    *new_addr = NativeValue::Create(ret);
    return base::Status();
}
}  // namespace codegen
}  // namespace fesql
