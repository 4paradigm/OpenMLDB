/*
 * expr_ir_builder.h
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

#ifndef CODEGEN_EXPR_IR_BUILDER_H_
#define CODEGEN_EXPR_IR_BUILDER_H_

#include "llvm/IR/IRBuilder.h"
#include "codegen/scope_var.h"
#include "ast/fn_ast.h"

namespace fesql {
namespace codegen {

class ExprIRBuilder {

public:

    ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~ExprIRBuilder();

    bool Build(::fesql::node::FnNode* node, ::llvm::Value** output);

    bool BuildBinaryExpr(::fesql::node::FnBinaryExpr* node, ::llvm::Value** output);
    bool BuildUnaryExpr(::fesql::node::FnNode* node, ::llvm::Value** output);

private:
    ::llvm::BasicBlock* block_;
    ScopeVar* scope_var_;
};

} // namespace of codegen
} // namespace of fesql
#endif /* !CODEGEN_EXPR_IR_BUILDER_H_ */
