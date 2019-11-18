/*
 * fn_ir_builder.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CODEGEN_FN_IR_BUILDER_H_
#define CODEGEN_FN_IR_BUILDER_H_

#include <vector>
#include "codegen/scope_var.h"
#include "llvm/IR/Module.h"
#include "node/sql_node.h"

namespace fesql {
namespace codegen {

// FnIRBuilder
class FnIRBuilder {
 public:
    // TODO provide a module manager
    FnIRBuilder(::llvm::Module* module);
    ~FnIRBuilder();
    bool Build(const ::fesql::node::FnNodeList* node);

    bool BuildFnHead(const ::fesql::node::FnNodeFnDef* fn_def,
                     ::llvm::Function** fn);

    bool BuildStmt(int32_t pindent, const ::fesql::node::FnNode* node,
                   ::llvm::BasicBlock* block);

    bool BuildAssignStmt(const ::fesql::node::FnAssignNode* node,
                         ::llvm::BasicBlock* block);

    bool BuildReturnStmt(const ::fesql::node::FnReturnStmt* node,
                         ::llvm::BasicBlock* block);

 private:

    bool MapLLVMType(const ::fesql::node::DataType& fn_type,
                     ::llvm::Type** type);

    bool BuildParas(const ::fesql::node::FnNodeList* node,
                    std::vector<::llvm::Type*>& paras);

    bool FillArgs(const ::fesql::node::FnNodeList* node, ::llvm::Function* fn);

 private:
    ::llvm::Module* module_;
    ScopeVar sv_;
};

}  // namespace codegen
}  // namespace fesql
#endif /* !CODEGEN_FN_IR_BUILDER_H_ */
