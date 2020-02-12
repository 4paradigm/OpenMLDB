/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mutable_value_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/2/11
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_VARIABLE_IR_BUILDER_H_
#define SRC_CODEGEN_VARIABLE_IR_BUILDER_H_

#include <string>
#include "base/status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/type.pb.h"
namespace fesql {
namespace codegen {
class VariableIRBuilder {
 public:
    VariableIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~VariableIRBuilder();

    bool LoadValue(std::string name, ::llvm::Value** output,
                   base::Status& status);  // NOLINT (runtime/references)
    bool StoreValue(const std::string& name, ::llvm::Value* value,
                    base::Status& status);  // NOLINT (runtime/references)
    bool StoreValue(const std::string& name, ::llvm::Value* value,
                    bool is_register,
                    base::Status& status);  // NOLINT (runtime/references)



 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_VARIABLE_IR_BUILDER_H_
