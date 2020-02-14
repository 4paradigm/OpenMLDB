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
#include "ir_base_builder.h"
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
    ::llvm::Value *list_i8_ptr = builder.CreatePointerCast(list, i8_ptr_ty);
    *output = builder.CreateCall(fn->getFunctionType(), fn, ::llvm::ArrayRef<::llvm::Value*>{list_i8_ptr, casted_pos});
    if (nullptr == *output) {
        status.msg = "fail to codegen list[pos]: call function error";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}
}  // namespace codegen
}  // namespace fesql