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

#include "codegen/scope_var.h"
#include "codegen/buf_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "node/sql_node.h"

namespace fesql {
namespace codegen {

class SQLExprIRBuilder {

 public:

    SQLExprIRBuilder(::llvm::BasicBlock* block, 
            ScopeVar* scope_var,
            BufIRBuilder* buf_ir_builder,
            const std::string& row_ptr_name,
            const std::string& row_size_name,
            const std::string& output_ptr_name,
            ::llvm::Module* module);

    ~SQLExprIRBuilder();

    bool Build(const ::fesql::node::ExprNode* node,
            ::llvm::Value** output,
            std::string& col_name);

 private:

    bool BuildColumnRef(const ::fesql::node::ColumnRefNode* node,
            ::llvm::Value** output);

    bool BuildCallFn(const ::fesql::node::FuncNode* fn,
            ::llvm::Value** output);

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
    std::string row_ptr_name_;
    std::string output_ptr_name_;
    BufIRBuilder* buf_ir_builder_;
    ::llvm::Module* module_;
    std::string row_size_name_;
};

class ExprIRBuilder {
 public:
    ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~ExprIRBuilder();

    bool Build(const ::fesql::node::ExprNode* node, ::llvm::Value** output);

    bool BuildBinaryExpr(const ::fesql::node::BinaryExpr* node,
                         ::llvm::Value** output);

    bool BuildUnaryExpr(const ::fesql::node::UnaryExpr* node,
                        ::llvm::Value** output);

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* scope_var_;
};


}  // namespace codegen
}  // namespace fesql
#endif  // CODEGEN_EXPR_IR_BUILDER_H_
