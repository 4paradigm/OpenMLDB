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

#ifndef HYBRIDSE_SRC_CODEGEN_BLOCK_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_BLOCK_IR_BUILDER_H_

#include <vector>
#include "base/fe_status.h"
#include "codegen/context.h"
#include "codegen/scope_var.h"
#include "codegen/variable_ir_builder.h"
#include "llvm/IR/Module.h"
#include "node/sql_node.h"

namespace hybridse {
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
    bool BuildAssignStmt(const ::hybridse::node::FnAssignNode* node,
                         base::Status& status);  // NOLINT

    bool BuildReturnStmt(const ::hybridse::node::FnReturnStmt* node,
                         base::Status& status);  // NOLINT

    bool BuildIfElseBlock(const ::hybridse::node::FnIfElseBlock* node,
                          base::Status& status);  // NOLINT
    bool BuildForInBlock(const ::hybridse::node::FnForInBlock* node,
                         base::Status& status);  // NOLINT

    bool DoBuildBranchBlock(
        const ::hybridse::node::FnIfElseBlock* if_else_block, size_t branch_idx,
        CodeGenContext* ctx,
        Status& status);  // NOLINT

    CodeGenContext* ctx_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_BLOCK_IR_BUILDER_H_
