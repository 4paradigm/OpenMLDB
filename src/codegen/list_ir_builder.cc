/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * list_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/2/14
 *--------------------------------------------------------------------------
 **/
#include "codegen/list_ir_builder.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "glog/logging.h"
namespace fesql {
namespace codegen {
ListIRBuilder::ListIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), sv_(scope_var) {}
ListIRBuilder::~ListIRBuilder() {}

bool ListIRBuilder::BuilStructTypedAt(::llvm::Value* list, ::llvm::Value* pos,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    if (nullptr == list) {
        status.msg = "fail to codegen list[pos]: list is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    CastExprIRBuilder castExprIrBuilder(block_);
    if (!pos->getType()->isIntegerTy()) {
        status.msg = "fail to codegen list[pos]: invalid pos type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Value* casted_pos = nullptr;
    if (false == castExprIrBuilder.UnSafeCast(
                     pos, ::llvm::Type::getInt32Ty(block_->getContext()),
                     &casted_pos, status)) {
        status.msg = "fail to codegen list[pos]: invalid pos type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::node::TypeNode type_node;
    if (false == GetFullType(list->getType(), &type_node) ||
        fesql::node::kList != type_node.base_) {
        status.msg = "fail to codegen list[pos]: invalid list type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Type* struct_type = nullptr;
    if (false == GetLLVMType(block_, type_node.generics_[0], &struct_type)) {
        status.msg =
            "fail to codegen iterator.next(): invalid value type of iterator";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (!TypeIRBuilder::IsStructPtr(struct_type)) {
        status.msg =
            "fail to codegen struct iterator.next(), invalid struct type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    struct_type = struct_type->getPointerElementType();
    ::std::string fn_name = "at." + type_node.GetName() + "." +
                            node::TypeNode(node::kInt32).GetName();
    ::llvm::FunctionType* fn_type = ::llvm::FunctionType::get(
        builder.getVoidTy(),
        {list->getType(), builder.getInt32Ty(), struct_type->getPointerTo()},
        false);
    ::llvm::FunctionCallee fn =
        block_->getModule()->getOrInsertFunction(fn_name, fn_type);
    ::llvm::Value* at_value_ptr = builder.CreateAlloca(struct_type);

    builder.CreateCall(fn, {list, casted_pos, at_value_ptr});
    *output = at_value_ptr;
    return true;
}
bool ListIRBuilder::BuildAt(::llvm::Value* list, ::llvm::Value* pos,
                            ::llvm::Value** output, base::Status& status) {
    if (nullptr == list) {
        status.msg = "fail to codegen list[pos]: list is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    CastExprIRBuilder castExprIrBuilder(block_);
    if (!pos->getType()->isIntegerTy()) {
        status.msg = "fail to codegen list[pos]: invalid pos type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Value* casted_pos = nullptr;
    if (false == castExprIrBuilder.UnSafeCast(
                     pos, ::llvm::Type::getInt32Ty(block_->getContext()),
                     &casted_pos, status)) {
        status.msg = "fail to codegen list[pos]: invalid pos type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::node::TypeNode type_node;
    if (false == GetFullType(list->getType(), &type_node) ||
        fesql::node::kList != type_node.base_) {
        status.msg = "fail to codegen list[pos]: invalid list type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Type* v1_type = nullptr;
    if (false == GetLLVMType(block_, type_node.generics_[0], &v1_type)) {
        status.msg =
            "fail to codegen iterator.next(): invalid value type of iterator";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (TypeIRBuilder::IsStructPtr(v1_type)) {
        return BuilStructTypedAt(list, pos, output, status);
    }
    ::llvm::IRBuilder<> builder(block_);
    ::std::string fn_name = "at." + type_node.GetName() + "." +
                            node::TypeNode(node::kInt32).GetName();
    ::llvm::FunctionType* fn_type = ::llvm::FunctionType::get(
        v1_type, {list->getType(), builder.getInt32Ty()}, false);
    ::llvm::FunctionCallee fn =
        block_->getModule()->getOrInsertFunction(fn_name, fn_type);
    *output = builder.CreateCall(fn, {list, casted_pos});
    return true;
}

Status ListIRBuilder::BuildIterator(::llvm::Value* list,
                                    const node::TypeNode* elem_type,
                                    ::llvm::Value** output) {
    CHECK_TRUE(list != nullptr, "fail to codegen list[pos]: list is null");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        "fail to get iterator ref type");
    ::llvm::Type* list_ref_type = nullptr;
    CHECK_TRUE(GetLLVMListType(block_->getModule(), elem_type, &list_ref_type),
               "fail to get list ref type");

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
    ::llvm::Value* iter_ref_ptr = builder.CreateAlloca(iter_ref_type);
    builder.CreateCall(callee, {list, iter_ref_ptr});

    // TODO(chenjing): check call res true
    *output = iter_ref_ptr;
    return Status::OK();
}

Status ListIRBuilder::BuildIteratorHasNext(::llvm::Value* iterator,
                                           const node::TypeNode* elem_type,
                                           ::llvm::Value** output) {
    CHECK_TRUE(nullptr != iterator,
               "fail to codegen iter.has_next(): iterator is null");

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        "fail to get iterator ref type");

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
    CHECK_TRUE(nullptr != iterator,
               "fail to codegen iter.has_next(): iterator is null");

    ::llvm::Type* struct_type = nullptr;
    CHECK_TRUE(
        GetLLVMType(block_, elem_type, &struct_type),
        "fail to codegen iterator.next(): invalid value type of iterator");

    CHECK_TRUE(TypeIRBuilder::IsStructPtr(struct_type),
               "fail to codegen struct iterator.next(), invalid struct type");

    struct_type = struct_type->getPointerElementType();
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* next_value_ptr = builder.CreateAlloca(struct_type);

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        "fail to get iterator ref type");
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
    CHECK_TRUE(nullptr != iterator,
               "fail to codegen iter.has_next(): iterator is null");

    ::llvm::Type* v1_type = nullptr;
    CHECK_TRUE(
        GetLLVMType(block_, elem_type, &v1_type),
        "fail to codegen iterator.next(): invalid value type of iterator");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        "fail to get iterator ref type");

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
        ::llvm::Value* next_addr = builder.CreateAlloca(v1_type);
        ::llvm::Value* is_null_addr = builder.CreateAlloca(bool_ty);
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
    CHECK_TRUE(nullptr != iterator,
               "fail to codegen iter.delete(): iterator is null");

    ::llvm::Type* v1_type = nullptr;
    CHECK_TRUE(
        GetLLVMType(block_, elem_type, &v1_type),
        "fail to codegen iterator.delete(): invalid value type of iterator");

    ::llvm::Type* iter_ref_type = NULL;
    CHECK_TRUE(
        GetLLVMIteratorType(block_->getModule(), elem_type, &iter_ref_type),
        "fail to get iterator ref type");

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
