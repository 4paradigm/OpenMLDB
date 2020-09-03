/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * NullIRBuilder.h
 *
 * Author: chenjing
 * Date: 2020/8/18
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_NULL_IR_BUILDER_H_

#define SRC_CODEGEN_NULL_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/native_value.h"
namespace fesql {
namespace codegen {
class NullIRBuilder {
 public:
    NullIRBuilder();
    ~NullIRBuilder();
    base::Status CheckAnyNull(::llvm::BasicBlock* block,
                              const NativeValue& value,
                              ::llvm::Value** should_ret_null);
    base::Status CheckAllNull(::llvm::BasicBlock* block,
                              const NativeValue& value,
                              ::llvm::Value** should_ret_null);
    static base::Status SafeNullBinaryExpr(
        ::llvm::BasicBlock* block, const NativeValue& left,
        const NativeValue& right,
        const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*,
                                 ::llvm::Value*, ::llvm::Value**,
                                 base::Status&)>,
        NativeValue* output);
    static base::Status SafeNullUnaryExpr(
        ::llvm::BasicBlock* block, const NativeValue& left,
        const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*,
                                 ::llvm::Value**, base::Status&)>,
        NativeValue* output);
    static base::Status SafeNullCastExpr(
        ::llvm::BasicBlock* block, const NativeValue& left, ::llvm::Type* type,
        const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*,
                                 ::llvm::Type* type, ::llvm::Value**,
                                 base::Status&)>,
        NativeValue* output);
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_NULL_IR_BUILDER_H_
