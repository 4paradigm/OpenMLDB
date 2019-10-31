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
#include "llvm/IR/Module.h"
#include "codegen/scope_var.h"
#include "ast/fn_ast.h"

namespace fesql {
namespace codegen {

// FnIRBuilder 
class FnIRBuilder {

public:
    // TODO provide a module manager
    FnIRBuilder(::llvm::Module* module);
    ~FnIRBuilder();
    bool Build(const ::fesql::ast::FnNode* node);

    bool BuildFnHead(const ::fesql::ast::FnNodeFnDef* fn_def,
                     ::llvm::Function** fn);

    bool BuildStmt(int32_t pindent, 
            const ::fesql::ast::FnNode* node, 
            ::llvm::BasicBlock* block);

    bool BuildAssignStmt(const ::fesql::ast::FnAssignNode* node, 
            ::llvm::BasicBlock* block);
    bool BuildReturnStmt(const ::fesql::ast::FnNode* node,
            ::llvm::BasicBlock* block);
private:

    bool MapLLVMType(const ::fesql::ast::FnNodeType& fn_type,
            ::llvm::Type** type);

    bool BuildParas(const ::fesql::ast::FnNode* node,
            std::vector<::llvm::Type*>& paras);

    bool FillArgs(const ::fesql::ast::FnNode* node,
            ::llvm::Function* fn);
private:
    ::llvm::Module* module_;
    ScopeVar sv_;
};

} // namespace of codegen
} // namespace of fesql
#endif /* !CODEGEN_FN_IR_BUILDER_H_ */
