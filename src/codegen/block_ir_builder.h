/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * control_flow_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/2/12
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_BLOCK_IR_BUILDER_H_
#define SRC_CODEGEN_BLOCK_IR_BUILDER_H_

#include <vector>
#include "base/status.h"
#include "codegen/scope_var.h"
#include "codegen/variable_ir_builder.h"
#include "llvm/IR/Module.h"
#include "node/sql_node.h"

namespace fesql {
namespace codegen {

// FnIRBuilder
//
class BlockIRBuilder {
 public:
    // TODO(wangtaize) provide a module manager
    explicit BlockIRBuilder(ScopeVar* scope_var);
    ~BlockIRBuilder();

    bool BuildBlock(const node::FnNodeList* statements, llvm::BasicBlock* block,
                    llvm::BasicBlock* end_block,
                    base::Status& status);  // NOLINT

 private:
    bool BuildAssignStmt(const ::fesql::node::FnAssignNode* node,
                         ::llvm::BasicBlock* block,
                         base::Status& status);  // NOLINT

    bool BuildReturnStmt(const ::fesql::node::FnReturnStmt* node,
                         ::llvm::BasicBlock* block,
                         base::Status& status);  // NOLINT

    bool BuildIfElseBlock(const ::fesql::node::FnIfElseBlock* node,
                          llvm::BasicBlock* block, llvm::BasicBlock* end_block,
                          base::Status& status);  // NOLINT
    bool BuildForInBlock(const ::fesql::node::FnForInBlock* node,
                         llvm::BasicBlock* start_block,
                         llvm::BasicBlock* end_block,
                         base::Status& status);  // NOLINT
    ScopeVar* sv_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_BLOCK_IR_BUILDER_H_
