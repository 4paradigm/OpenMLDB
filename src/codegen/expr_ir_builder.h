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

#ifndef SRC_CODEGEN_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_EXPR_IR_BUILDER_H_

#include <memory>
#include <string>
#include <vector>
#include "base/status.h"
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/row_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/variable_ir_builder.h"
#include "codegen/window_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "node/sql_node.h"

namespace fesql {
namespace codegen {

class ExprIRBuilder {
 public:
    ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);

    ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var,
                  const vm::Schema& schema, const bool row_mode,
                  const std::string& row_ptr_name,
                  const std::string& window_ptr_name,
                  const std::string& row_size_name, ::llvm::Module* module);

    ~ExprIRBuilder();

    bool Build(const ::fesql::node::ExprNode* node, ::llvm::Value** output,
               ::fesql::base::Status& status);  // NOLINT

 private:
    bool BuildColumnIterator(const std::string& col, ::llvm::Value** output,
                             ::fesql::base::Status& status);  // NOLINT
    bool BuildColumnItem(const std::string& col, ::llvm::Value** output,
                         ::fesql::base::Status& status);  // NOLINT
    bool BuildColumnRef(const ::fesql::node::ColumnRefNode* node,
                        ::llvm::Value** output,
                        ::fesql::base::Status& status);  // NOLINT

    bool BuildCallFn(const ::fesql::node::CallExprNode* fn,
                     ::llvm::Value** output,
                     ::fesql::base::Status& status);  // NOLINT

    bool BuildBinaryExpr(const ::fesql::node::BinaryExpr* node,
                         ::llvm::Value** output,
                         ::fesql::base::Status& status);  // NOLINT

    bool BuildUnaryExpr(const ::fesql::node::UnaryExpr* node,
                        ::llvm::Value** output,
                        ::fesql::base::Status& status);  // NOLINT

    bool BuildStructExpr(const ::fesql::node::StructExpr* node,
                         ::llvm::Value** output,
                         ::fesql::base::Status& status);  // NOLINT
    ::llvm::Function* GetFuncion(
        const std::string& col,
        const std::vector<node::TypeNode>& generic_types,
        base::Status& status);  // NOLINT

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
    vm::Schema schema_;
    bool row_mode_;
    std::string row_ptr_name_;
    std::string window_ptr_name_;
    std::string row_size_name_;
    VariableIRBuilder variable_ir_builder_;
    // TODO(chenjing): remove following ir builder member
    ArithmeticIRBuilder arithmetic_ir_builder_;
    PredicateIRBuilder predicate_ir_builder_;
    ::llvm::Module* module_;
    std::unique_ptr<RowDecodeIRBuilder> row_ir_builder_;
    std::unique_ptr<WindowDecodeIRBuilder> window_ir_builder_;
    bool IsUADF(std::string function_name);
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_EXPR_IR_BUILDER_H_
