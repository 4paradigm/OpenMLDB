/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * list_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/2/14
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_LIST_IR_BUILDER_H_
#define SRC_CODEGEN_LIST_IR_BUILDER_H_

#include <string>
#include "base/fe_status.h"
#include "codegen/native_value.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "node/sql_node.h"
#include "proto/fe_type.pb.h"
namespace fesql {
namespace codegen {

using base::Status;

class ListIRBuilder {
 public:
    ListIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~ListIRBuilder();

    bool BuildAt(::llvm::Value* list, ::llvm::Value* pos,
                 ::llvm::Value** output, base::Status& status);  // NOLINT
    Status BuildIterator(::llvm::Value* list, const node::TypeNode* elem_type,
                         ::llvm::Value** output);
    Status BuildIteratorHasNext(::llvm::Value* iterator,
                                const node::TypeNode* elem_type,
                                ::llvm::Value** output);
    Status BuildIteratorNext(::llvm::Value* iterator,
                             const node::TypeNode* elem_type,
                             bool elem_nullable, NativeValue* output);
    Status BuildIteratorDelete(::llvm::Value* iterator,
                               const node::TypeNode* elem_type,
                               ::llvm::Value** output);

 private:
    Status BuildStructTypeIteratorNext(::llvm::Value* iterator,
                                       const node::TypeNode* elem_type,
                                       NativeValue* output);
    bool BuilStructTypedAt(::llvm::Value* list, ::llvm::Value* pos,
                           ::llvm::Value** output,
                           base::Status& status);  // NOLINT
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_LIST_IR_BUILDER_H_
