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
#include "glog/logging.h"
namespace fesql {
namespace codegen {
ListIRBuilder::ListIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), sv_(scope_var) {}
ListIRBuilder::~ListIRBuilder() {}

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
    ::llvm::Value* casted_pos;
    if (false == castExprIrBuilder.UnSafeCast(
                     pos, ::llvm::Type::getInt32Ty(block_->getContext()),
                     &casted_pos, status)) {
        status.msg = "fail to codegen list[pos]: invalid pos type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::type::Type base;
    fesql::type::Type v1;
    fesql::type::Type v2;
    if (false == GetFullType(list->getType(), &base, &v1, &v2) ||
        fesql::type::kList != base) {
        status.msg = "fail to codegen list[pos]: invalid list type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::std::string type_name;
    if (false == GetFesqlTypeName(v1, type_name)) {
        status.msg = "fail to codegen list[pos]: invliad value type of list";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::std::string fn_name = "list_at_" + type_name;
    ::llvm::Function* fn =
        block_->getModule()->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.msg =
            "faili to codegen list[pos]: can't find function " + fn_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* list_i8_ptr = builder.CreatePointerCast(list, i8_ptr_ty);
    *output = builder.CreateCall(
        fn->getFunctionType(), fn,
        ::llvm::ArrayRef<::llvm::Value*>{list_i8_ptr, casted_pos});
    if (nullptr == *output) {
        status.msg = "fail to codegen list[pos]: call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}
bool ListIRBuilder::BuildIterator(::llvm::Value* list, ::llvm::Value** output,
                                  base::Status& status) {
    if (nullptr == list) {
        status.msg = "fail to codegen list[pos]: list is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    fesql::type::Type base;
    fesql::type::Type v1;
    fesql::type::Type v2;
    if (false == GetFullType(list->getType(), &base, &v1, &v2) ||
        fesql::type::kList != base) {
        status.msg = "fail to codegen list[pos]: invalid list type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::std::string type_name;
    if (false == GetFesqlTypeName(v1, type_name)) {
        status.msg = "fail to codegen list[pos]: invliad value type of list";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::std::string fn_name = "list_iterator_" + type_name;
    ::llvm::Function* fn =
        block_->getModule()->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.msg =
            "faili to codegen list[pos]: can't find function " + fn_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Value* list_i8_ptr = builder.CreatePointerCast(list, i8_ptr_ty);

    ::llvm::Type* iter_ref_type = NULL;
    if (!GetLLVMIteratorType(block_->getModule(), v1, &iter_ref_type)) {
        LOG(WARNING) << "fail to get iterator ref type";
        return false;
    }

    uint32_t col_iterator_size;
    if (!GetLLVMIteratorSize(v1, &col_iterator_size)) {
        LOG(WARNING) << "fail to get col list size";
    }

    // alloca memory on stack for col list
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, col_iterator_size);
    ::llvm::Value* col_iter = builder.CreateAlloca(array_type);

    // alloca memory on stack
    ::llvm::Value* iter_ref = builder.CreateAlloca(iter_ref_type);
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(iter_ref_type, iter_ref, 0);
    data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, col_iter->getType()->getPointerTo());
    builder.CreateStore(col_iter, data_ptr_ptr, false);
    ::llvm::Value* iter_i8_ptr = builder.CreatePointerCast(iter_ref, i8_ptr_ty);

    ::llvm::Value* call_res = builder.CreateCall(
        fn->getFunctionType(), fn,
        ::llvm::ArrayRef<::llvm::Value*>{list_i8_ptr, iter_i8_ptr});
    if (nullptr == call_res) {
        status.msg = "fail to codegen list.iterator(): call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    // TODO(chenjing): check call res true
    *output = iter_ref;
    return true;
}
bool ListIRBuilder::BuildIteratorHasNext(::llvm::Value* iterator,
                                         ::llvm::Value** output,
                                         base::Status& status) {
    if (nullptr == iterator) {
        status.msg = "fail to codegen iter.has_next(): iterator is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::type::Type base;
    fesql::type::Type v1;
    fesql::type::Type v2;
    if (false == GetFullType(iterator->getType(), &base, &v1, &v2) ||
        fesql::type::kListIterator != base) {
        status.msg =
            "fail to codegen iterator.has_next(): invalid iterator type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::std::string type_name;
    if (false == GetFesqlTypeName(v1, type_name)) {
        status.msg =
            "fail to codegen iterator.has_next(): invliad value type of "
            "iterator";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::std::string fn_name = "list_iterator_has_next_" + type_name;
    ::llvm::Function* fn =
        block_->getModule()->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.msg =
            "faili to codegen iterator.has_next(): can't find function " +
            fn_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* list_i8_ptr = builder.CreatePointerCast(iterator, i8_ptr_ty);
    *output = builder.CreateCall(fn->getFunctionType(), fn,
                                 ::llvm::ArrayRef<::llvm::Value*>{list_i8_ptr});
    if (nullptr == *output) {
        status.msg = "fail to codegen list[pos]: call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}
bool ListIRBuilder::BuildIteratorNext(::llvm::Value* iterator,
                                      ::llvm::Value** output,
                                      base::Status& status) {
    if (nullptr == iterator) {
        status.msg = "fail to codegen iter.has_next(): iterator is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::type::Type base;
    fesql::type::Type v1;
    fesql::type::Type v2;
    if (false == GetFullType(iterator->getType(), &base, &v1, &v2) ||
        fesql::type::kListIterator != base) {
        status.msg = "fail to codegen iterator.next(): invalid iterator type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::std::string type_name;
    if (false == GetFesqlTypeName(v1, type_name)) {
        status.msg =
            "fail to codegen iterator.next(): invalid value type of iterator";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Type* v1_type = nullptr;
    if (false == GetLLVMType(block_, v1, &v1_type)) {
        status.msg =
            "fail to codegen iterator.next(): invalid value type of iterator";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::std::string fn_name = "list_iterator_next_" + type_name;
    ::llvm::Function* fn =
        block_->getModule()->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.msg =
            "faili to codegen iterator.next(): can't find function " + fn_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* list_i8_ptr = builder.CreatePointerCast(iterator, i8_ptr_ty);
    ::llvm::Value* next_addr = builder.CreateAlloca(v1_type);
    ::llvm::Value* next_addr_i8_ptr =
        builder.CreatePointerCast(next_addr, i8_ptr_ty);
    ::llvm::Value* call_res = builder.CreateCall(
        fn->getFunctionType(), fn,
        ::llvm::ArrayRef<::llvm::Value*>{list_i8_ptr, next_addr_i8_ptr});
    if (nullptr == call_res) {
        status.msg = "fail to codegen iterator.next(): call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    *output = builder.CreateLoad(v1_type, next_addr, "next_v");
    return true;
}
}  // namespace codegen
}  // namespace fesql
