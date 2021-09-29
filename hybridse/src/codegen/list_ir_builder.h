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

#ifndef HYBRIDSE_SRC_CODEGEN_LIST_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_LIST_IR_BUILDER_H_

#include <string>
#include "base/fe_status.h"
#include "codegen/native_value.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "node/sql_node.h"
#include "proto/fe_type.pb.h"
namespace hybridse {
namespace codegen {

using base::Status;

class ListIRBuilder {
 public:
    ListIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~ListIRBuilder();

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
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_LIST_IR_BUILDER_H_
