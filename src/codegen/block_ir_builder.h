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
#include "base/fe_status.h"
#include "codegen/context.h"
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
    explicit BlockIRBuilder(CodeGenContext* ctx);
    ~BlockIRBuilder();

    bool BuildBlock(const node::FnNodeList* statements,
                    base::Status& status);  // NOLINT

 private:
    bool BuildAssignStmt(const ::fesql::node::FnAssignNode* node,
                         base::Status& status);  // NOLINT

    bool BuildReturnStmt(const ::fesql::node::FnReturnStmt* node,
                         base::Status& status);  // NOLINT

    bool BuildIfElseBlock(const ::fesql::node::FnIfElseBlock* node,
                          base::Status& status);  // NOLINT
    bool BuildForInBlock(const ::fesql::node::FnForInBlock* node,
                         base::Status& status);  // NOLINT

    bool DoBuildBranchBlock(const ::fesql::node::FnIfElseBlock* if_else_block,
                            size_t branch_idx, CodeGenContext* ctx,
                            Status& status);  // NOLINT

    CodeGenContext* ctx_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_BLOCK_IR_BUILDER_H_
