/*
 * Copyright (c) 2021 4Paradigm
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

#include "codegen/list_ir_builder.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/udf_ir_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {
ListIRBuilder::ListIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), sv_(scope_var) {}
ListIRBuilder::~ListIRBuilder() {}

Status ListIRBuilder::BuildIterator(::llvm::Value* list,
                                    const node::TypeNode* elem_type,
                                    ::llvm::Value** output) {
    CHECK_TRUE(list != nullptr, kCodegenError,
               "fail to codegen list[pos]: list is null");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        kCodegenError, "fail to get iterator ref type");
    ::llvm::Type* list_ref_type = nullptr;
    CHECK_TRUE(GetLLVMListType(block_->getModule(), elem_type, &list_ref_type),
               kCodegenError, "fail to get list ref type");

    ::std::string fn_name = "iterator.list_" + elem_type->GetName() +
                            ".iterator_" + elem_type->GetName();

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(builder.getContext());

    auto iter_function_ty = ::llvm::FunctionType::get(
        bool_ty, {list_ref_type->getPointerTo(), iter_ref_type->getPointerTo()},
        false);
    ::llvm::FunctionCallee callee =
        block_->getModule()->getOrInsertFunction(fn_name, iter_function_ty);

    // alloca memory on stack
    ::llvm::Value* iter_ref_ptr =
        CreateAllocaAtHead(&builder, iter_ref_type, "iter_ref_alloca");
    builder.CreateCall(callee, {list, iter_ref_ptr});

    // TODO(chenjing): check call res true
    *output = iter_ref_ptr;
    return Status::OK();
}

Status ListIRBuilder::BuildIteratorHasNext(::llvm::Value* iterator,
                                           const node::TypeNode* elem_type,
                                           ::llvm::Value** output) {
    CHECK_TRUE(nullptr != iterator, kCodegenError,
               "fail to codegen iter.has_next(): iterator is null");

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        kCodegenError, "fail to get iterator ref type");

    ::std::string fn_name = "has_next.iterator_" + elem_type->GetName();

    ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(builder.getContext());
    auto iter_has_next_fn_ty = ::llvm::FunctionType::get(
        bool_ty, {iter_ref_type->getPointerTo()}, false);

    ::llvm::FunctionCallee callee =
        block_->getModule()->getOrInsertFunction(fn_name, iter_has_next_fn_ty);

    *output = builder.CreateCall(callee, {iterator});
    return Status::OK();
}

Status ListIRBuilder::BuildStructTypeIteratorNext(
    ::llvm::Value* iterator, const node::TypeNode* elem_type,
    NativeValue* output) {
    CHECK_TRUE(nullptr != iterator, kCodegenError,
               "fail to codegen iter.has_next(): iterator is null");

    ::llvm::Type* struct_type = nullptr;
    CHECK_TRUE(
        GetLLVMType(block_, elem_type, &struct_type), kCodegenError,
        "fail to codegen iterator.next(): invalid value type of iterator");

    CHECK_TRUE(TypeIRBuilder::IsStructPtr(struct_type), kCodegenError,
               "fail to codegen struct iterator.next(), invalid struct type");

    struct_type = struct_type->getPointerElementType();
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* next_value_ptr =
        CreateAllocaAtHead(&builder, struct_type, "iter_next_struct_alloca");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        kCodegenError, "fail to get iterator ref type");
    ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(builder.getContext());
    auto iter_next_fn_ty = ::llvm::FunctionType::get(
        bool_ty, {iter_ref_type->getPointerTo(), struct_type->getPointerTo()},
        false);

    ::std::string fn_name =
        "next.iterator_" + elem_type->GetName() + "." + elem_type->GetName();
    ::llvm::FunctionCallee callee =
        block_->getModule()->getOrInsertFunction(fn_name, iter_next_fn_ty);

    builder.CreateCall(callee, {iterator, next_value_ptr});
    *output = NativeValue::Create(next_value_ptr);
    return Status::OK();
}

Status ListIRBuilder::BuildIteratorNext(::llvm::Value* iterator,
                                        const node::TypeNode* elem_type,
                                        bool elem_nullable,
                                        NativeValue* output) {
    CHECK_TRUE(nullptr != iterator, kCodegenError,
               "fail to codegen iter.has_next(): iterator is null");

    ::llvm::Type* v1_type = nullptr;
    CHECK_TRUE(
        GetLLVMType(block_, elem_type, &v1_type), kCodegenError,
        "fail to codegen iterator.next(): invalid value type of iterator");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        kCodegenError, "fail to get iterator ref type");

    if (elem_nullable) {
        if (TypeIRBuilder::IsStructPtr(v1_type)) {
            v1_type = reinterpret_cast<::llvm::PointerType*>(v1_type)
                          ->getElementType();
        }
        ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(block_->getContext());
        ::std::string fn_name =
            "next_nullable.iterator_" + elem_type->GetName();
        auto iter_next_fn_ty = ::llvm::FunctionType::get(
            ::llvm::Type::getVoidTy(block_->getContext()),
            {iter_ref_type->getPointerTo(), v1_type->getPointerTo(),
             bool_ty->getPointerTo()},
            false);

        ::llvm::FunctionCallee callee =
            block_->getModule()->getOrInsertFunction(fn_name, iter_next_fn_ty);

        ::llvm::IRBuilder<> builder(block_);
        ::llvm::Value* next_addr =
            CreateAllocaAtHead(&builder, v1_type, "iter_next_value_alloca");
        ::llvm::Value* is_null_addr =
            CreateAllocaAtHead(&builder, bool_ty, "iter_next_null_alloca");
        builder.CreateCall(callee, {iterator, next_addr, is_null_addr});

        ::llvm::Value* next_raw = next_addr;
        if (!TypeIRBuilder::IsStructPtr(next_addr->getType())) {
            next_raw = builder.CreateLoad(next_addr);
        }
        *output = NativeValue::CreateWithFlag(next_raw,
                                              builder.CreateLoad(is_null_addr));
    } else {
        if (TypeIRBuilder::IsStructPtr(v1_type)) {
            return BuildStructTypeIteratorNext(iterator, elem_type, output);
        }
        ::std::string fn_name = "next.iterator_" + elem_type->GetName();
        auto iter_next_fn_ty = ::llvm::FunctionType::get(
            v1_type, {iter_ref_type->getPointerTo()}, false);

        ::llvm::FunctionCallee callee =
            block_->getModule()->getOrInsertFunction(fn_name, iter_next_fn_ty);

        ::llvm::IRBuilder<> builder(block_);
        ::llvm::Value* next_value = builder.CreateCall(callee, {iterator});
        *output = NativeValue::Create(next_value);
    }
    return Status::OK();
}

Status ListIRBuilder::BuildIteratorDelete(::llvm::Value* iterator,
                                          const node::TypeNode* elem_type,
                                          ::llvm::Value** output) {
    CHECK_TRUE(nullptr != iterator, kCodegenError,
               "fail to codegen iter.delete(): iterator is null");

    ::llvm::Type* v1_type = nullptr;
    CHECK_TRUE(
        GetLLVMType(block_, elem_type, &v1_type), kCodegenError,
        "fail to codegen iterator.delete(): invalid value type of iterator");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        kCodegenError, "fail to get iterator ref type");

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* bool_ty = ::llvm::Type::getInt1Ty(builder.getContext());
    ::std::string fn_name = "delete_iterator.iterator_" + elem_type->GetName();
    auto iter_delete_fn_ty = ::llvm::FunctionType::get(
        bool_ty, {iter_ref_type->getPointerTo()}, false);
    ::llvm::FunctionCallee callee =
        block_->getModule()->getOrInsertFunction(fn_name, iter_delete_fn_ty);

    ::llvm::Value* ret = builder.CreateCall(callee, {iterator});
    *output = ret;
    return Status::OK();
}
}  // namespace codegen
}  // namespace fesql
