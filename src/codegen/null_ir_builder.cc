/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * null_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/8/18
 *--------------------------------------------------------------------------
 **/
#include "codegen/null_ir_builder.h"
namespace fesql {
namespace codegen {
NullIRBuilder::NullIRBuilder() {}
NullIRBuilder::~NullIRBuilder() {}
base::Status NullIRBuilder::CheckAnyNull(::llvm::BasicBlock *block,
                             const NativeValue &value,
                             ::llvm::Value **should_ret_null) {
    CHECK_TRUE(nullptr != should_ret_null,
              "fail to check any null: should ret null llvm value is null");
    ::llvm::IRBuilder<> builder(block);
    if (value.IsNullable()) {
        if (*should_ret_null == nullptr) {
            *should_ret_null = value.GetIsNull(&builder);
        } else {
            *should_ret_null =
                builder.CreateOr(*should_ret_null, value.GetIsNull(&builder));
        }
    }
    return base::Status::OK();
}
base::Status NullIRBuilder::CheckAllNull(::llvm::BasicBlock *block,
                                         const NativeValue &value,
                                         ::llvm::Value **should_ret_null) {
    CHECK_TRUE(nullptr != should_ret_null,
               "fail to check all null: should ret null llvm value is null");
    ::llvm::IRBuilder<> builder(block);
    if (value.IsNullable()) {
        if (*should_ret_null == nullptr) {
            *should_ret_null = value.GetIsNull(&builder);
        } else {
            *should_ret_null =
                builder.CreateAnd(*should_ret_null, value.GetIsNull(&builder));
        }
    }
    return base::Status::OK();
}
}  // namespace codegen
}  // namespace fesql
