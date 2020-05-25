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

    ::std::string fn_name = "at_" + type_node.GetName();
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
    ::llvm::Value* at_value_ptr = builder.CreateAlloca(struct_type);

    ::llvm::Value* ret = builder.CreateCall(
        fn->getFunctionType(), fn,
        ::llvm::ArrayRef<::llvm::Value*>{list, casted_pos, at_value_ptr});
    if (nullptr == ret) {
        status.msg = "fail to codegen list[pos]: call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
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

    if (v1_type->isStructTy()) {
        return BuilStructTypedAt(list, pos, output, status);
    }

    ::std::string fn_name = "at_" + type_node.GetName();
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
    *output =
        builder.CreateCall(fn->getFunctionType(), fn,
                           ::llvm::ArrayRef<::llvm::Value*>{list, casted_pos});
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
    fesql::node::TypeNode type_node;
    if (false == GetFullType(list->getType(), &type_node) ||
        fesql::node::kList != type_node.base_) {
        status.msg = "fail to codegen list[pos]: invalid list type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::std::string fn_name = "iterator_" + type_node.GetName();
    ::llvm::Function* fn =
        block_->getModule()->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.msg =
            "faili to codegen iterator: can't find function " + fn_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* iter_ref_type = NULL;
    if (!GetLLVMIteratorType(block_->getModule(), type_node.generics_[0],
                             &iter_ref_type)) {
        LOG(WARNING) << "fail to get iterator ref type";
        return false;
    }
    // alloca memory on stack
    ::llvm::Value* iter_ref = builder.CreateAlloca(iter_ref_type);
    ::llvm::Value* iter_i8_ptr = builder.CreatePointerCast(iter_ref, i8_ptr_ty);
    ::llvm::Value* call_res =
        builder.CreateCall(fn->getFunctionType(), fn,
                           ::llvm::ArrayRef<::llvm::Value*>{list, iter_i8_ptr});
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

    fesql::node::TypeNode type_node;
    if (false == GetFullType(iterator->getType(), &type_node) ||
        fesql::node::kIterator != type_node.base_) {
        status.msg =
            "fail to codegen iterator.has_next(): invalid iterator type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::std::string fn_name = "has_next_" + type_node.GetName();
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
    *output = builder.CreateCall(fn->getFunctionType(), fn,
                                 ::llvm::ArrayRef<::llvm::Value*>{iterator});
    if (nullptr == *output) {
        status.msg = "fail to codegen list[pos]: call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}
bool ListIRBuilder::BuildStructTypeIteratorNext(::llvm::Value* iterator,
                                                ::llvm::Value** output,
                                                base::Status& status) {
    if (nullptr == iterator) {
        status.msg = "fail to codegen iter.has_next(): iterator is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::node::TypeNode type_node;
    if (false == GetFullType(iterator->getType(), &type_node) ||
        fesql::node::kIterator != type_node.base_) {
        status.msg = "fail to codegen iterator.next(): invalid iterator type";
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
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* next_value_ptr = builder.CreateAlloca(struct_type);

    ::std::string fn_name = "next_" + type_node.GetName();
    ::llvm::Function* fn =
        block_->getModule()->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.msg =
            "fail to codegen iterator.next(): can't find function " + fn_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Value* ret = builder.CreateCall(
        fn->getFunctionType(), fn,
        ::llvm::ArrayRef<::llvm::Value*>{iterator, next_value_ptr});
    if (nullptr == ret) {
        status.msg = "fail to codegen iterator.next(): call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    *output = next_value_ptr;
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

    fesql::node::TypeNode type_node;
    if (false == GetFullType(iterator->getType(), &type_node) ||
        fesql::node::kIterator != type_node.base_) {
        status.msg = "fail to codegen iterator.next(): invalid iterator type";
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

    if (v1_type->isStructTy()) {
        return BuildStructTypeIteratorNext(iterator, output, status);
    }
    ::std::string fn_name = "next_" + type_node.GetName();
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
    ::llvm::Value* next_value = builder.CreateCall(
        fn->getFunctionType(), fn, ::llvm::ArrayRef<::llvm::Value*>{iterator});
    if (nullptr == next_value) {
        status.msg = "fail to codegen iterator.next(): call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    *output = next_value;
    return true;
}
bool ListIRBuilder::BuildIteratorDelete(::llvm::Value* iterator,
                                        ::llvm::Value** output,
                                        base::Status& status) {
    if (nullptr == iterator) {
        status.msg = "fail to codegen iter.delete(): iterator is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    fesql::node::TypeNode type_node;
    if (false == GetFullType(iterator->getType(), &type_node) ||
        fesql::node::kIterator != type_node.base_) {
        status.msg = "fail to codegen iterator.delete(): invalid iterator type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Type* v1_type = nullptr;
    if (false == GetLLVMType(block_, type_node.generics_[0], &v1_type)) {
        status.msg =
            "fail to codegen iterator.delete(): invalid value type of iterator";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ::std::string fn_name = "delete_" + type_node.GetName();
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
    ::llvm::Value* ret = builder.CreateCall(
        fn->getFunctionType(), fn, ::llvm::ArrayRef<::llvm::Value*>{iterator});
    if (nullptr == ret) {
        status.msg = "fail to codegen iterator.delete(): call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    *output = ret;
    return true;
}
}  // namespace codegen
}  // namespace fesql
