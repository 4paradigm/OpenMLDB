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

#include "codegen/block_ir_builder.h"

#include "codegen/context.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/list_ir_builder.h"
#include "codegen/struct_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "llvm/IR/IRBuilder.h"

using ::hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {

BlockIRBuilder::BlockIRBuilder(CodeGenContext *ctx) : ctx_(ctx) {}
BlockIRBuilder::~BlockIRBuilder() {}

bool hybridse::codegen::BlockIRBuilder::BuildBlock(
    const hybridse::node::FnNodeList *statements,
    hybridse::base::Status &status) {
    if (statements == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node or block is null";
        LOG(WARNING) << status;
        return false;
    }

    if (statements->children.empty()) {
        return true;
    }

    for (const node::FnNode *node : statements->children) {
        switch (node->GetType()) {
            case node::kFnAssignStmt: {
                if (!BuildAssignStmt(
                        dynamic_cast<const ::hybridse::node::FnAssignNode *>(
                            node),
                        status)) {
                    return false;
                }
                break;
            }
            case node::kFnReturnStmt: {
                if (!BuildReturnStmt(
                        dynamic_cast<const node::FnReturnStmt *>(node),
                        status)) {
                    return false;
                }
                break;
            }
            case node::kFnIfElseBlock: {
                if (!BuildIfElseBlock(
                        dynamic_cast<const ::hybridse::node::FnIfElseBlock *>(
                            node),
                        status)) {
                    return false;
                }
                break;
            }
            case node::kFnForInBlock: {
                if (!BuildForInBlock(
                        dynamic_cast<const ::hybridse::node::FnForInBlock *>(
                            node),
                        status)) {
                    return false;
                }
                break;
            }
            default: {
                status.code = common::kCodegenError;
                status.msg = "fail to codegen for unrecognized fn type " +
                             node::NameOfSqlNodeType(node->GetType());
                LOG(WARNING) << status;
                return false;
            }
        }
    }
    return true;
}

bool BlockIRBuilder::DoBuildBranchBlock(
    const ::hybridse::node::FnIfElseBlock *if_else_block, size_t branch_idx,
    CodeGenContext *ctx, Status &status) {
    if (branch_idx == 0) {
        // if () {}
        return BuildBlock(if_else_block->if_block_->block_, status);

    } else if (branch_idx <= if_else_block->elif_blocks_.size()) {
        // else if () {}
        auto node = if_else_block->elif_blocks_[branch_idx - 1];
        auto elif_block = dynamic_cast<hybridse::node::FnElifBlock *>(node);

        NativeValue elif_condition;
        ExprIRBuilder expr_builder(ctx_);
        status = expr_builder.Build(elif_block->elif_node_->expression_,
                                    &elif_condition);
        if (!status.isOK()) {
            LOG(WARNING) << "fail to codegen else if condition: " << status;
            return false;
        }

        status = ctx_->CreateBranch(
            elif_condition,
            [&]() {
                bool ok = BuildBlock(elif_block->block_, status);
                CHECK_TRUE(ok, kCodegenError,
                           "fail to codegen block: ", status.str());
                return Status::OK();
            },
            [&]() {
                bool ok = DoBuildBranchBlock(if_else_block, branch_idx + 1, ctx,
                                             status);
                CHECK_TRUE(ok, kCodegenError,
                           "fail to codegen block: ", status.str());
                return Status::OK();
            });

    } else {
        // else {}
        if (nullptr != if_else_block->else_block_) {
            bool else_ok =
                BuildBlock(if_else_block->else_block_->block_, status);
            if (!else_ok) {
                LOG(WARNING) << "fail to codegen else block: " << status;
                return false;
            }
        }
    }
    return true;
}

bool BlockIRBuilder::BuildIfElseBlock(
    const ::hybridse::node::FnIfElseBlock *if_else_block,
    base::Status &status) {  // NOLINE
    if (if_else_block == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen if else block: node is null";
        LOG(WARNING) << status;
        return false;
    }

    // first condition
    ExprIRBuilder expr_builder(ctx_);
    NativeValue condition;
    status = expr_builder.Build(if_else_block->if_block_->if_node->expression_,
                                &condition);
    if (!status.isOK()) {
        LOG(WARNING) << "fail to codegen condition expression: " << status;
        return false;
    }

    status = ctx_->CreateBranch(
        condition,
        [&]() {
            CHECK_TRUE(DoBuildBranchBlock(if_else_block, 0, ctx_, status),
                       kCodegenError, "fail to codegen block:", status.str());
            return Status::OK();
        },
        [&]() {
            CHECK_TRUE(DoBuildBranchBlock(if_else_block, 1, ctx_, status),
                       kCodegenError, "fail to codegen block:", status.str());
            return Status::OK();
        });

    if (!status.isOK()) {
        LOG(WARNING) << "fail to codegen if else block: " << status;
        return false;
    }
    return true;
}

bool BlockIRBuilder::BuildForInBlock(const ::hybridse::node::FnForInBlock *node,
                                     base::Status &status) {
    if (node == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen for block: node is null";
        LOG(WARNING) << status;
        return false;
    }

    ListIRBuilder list_ir_builder(ctx_->GetCurrentBlock(),
                                  ctx_->GetCurrentScope()->sv());
    ExprIRBuilder expr_builder(ctx_);

    // loop start
    NativeValue container_value_wrapper;
    status = expr_builder.Build(node->for_in_node_->in_expression_,
                                &container_value_wrapper);
    if (!status.isOK()) {
        LOG(WARNING) << "fail to build for condition expression: " << status;
        return false;
    }
    llvm::Value *container_value = container_value_wrapper.GetValue(ctx_);

    const hybridse::node::TypeNode *container_type_node = nullptr;
    if (false == GetFullType(ctx_->node_manager(), container_value->getType(),
                             &container_type_node) ||
        hybridse::node::kList != container_type_node->base()) {
        status.msg = "fail to codegen list[pos]: invalid list type";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    const hybridse::node::TypeNode *elem_type_node =
        container_type_node->generics_[0];
    const bool elem_nullable = false;

    llvm::Value *iterator = nullptr;
    status = list_ir_builder.BuildIterator(container_value, elem_type_node,
                                           &iterator);
    if (!status.isOK()) {
        LOG(WARNING) << "fail to build iterator expression: " << status;
        return false;
    }

    status = ctx_->CreateWhile(
        [&](::llvm::Value **condition) {
            // build has next
            ListIRBuilder list_ir_builder(ctx_->GetCurrentBlock(),
                                          ctx_->GetCurrentScope()->sv());
            return list_ir_builder.BuildIteratorHasNext(
                iterator, elem_type_node, condition);
        },
        [&]() {
            // build body
            ListIRBuilder list_ir_builder(ctx_->GetCurrentBlock(),
                                          ctx_->GetCurrentScope()->sv());
            VariableIRBuilder var_ir_builder(ctx_->GetCurrentBlock(),
                                             ctx_->GetCurrentScope()->sv());
            NativeValue next;
            CHECK_STATUS(list_ir_builder.BuildIteratorNext(
                iterator, elem_type_node, elem_nullable, &next));
            auto var_key = node->for_in_node_->var_->GetExprString();
            if (!var_ir_builder.StoreValue(var_key, next, false, status)) {
                return status;
            }
            if (!BuildBlock(node->block_, status)) {
                LOG(WARNING) << "fail to codegen block: " << status;
                return status;
            }
            return Status::OK();
        });

    // delete iterator
    ListIRBuilder list_ir_builder_delete(ctx_->GetCurrentBlock(),
                                         ctx_->GetCurrentScope()->sv());
    llvm::Value *ret_delete = nullptr;
    status = list_ir_builder_delete.BuildIteratorDelete(
        iterator, elem_type_node, &ret_delete);
    return status.isOK();
}

bool BlockIRBuilder::BuildReturnStmt(const ::hybridse::node::FnReturnStmt *node,
                                     base::Status &status) {  // NOLINE
    if (node == nullptr || node->return_expr_ == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "node or return expr is null";
        LOG(WARNING) << status;
        return false;
    }
    ScopeVar *sv = ctx_->GetCurrentScope()->sv();
    ::llvm::BasicBlock *block = ctx_->GetCurrentBlock();
    ::llvm::IRBuilder<> builder(block);
    ExprIRBuilder expr_builder(ctx_);
    VariableIRBuilder var_ir_builder(block, sv);
    NativeValue value_wrapper;
    status = expr_builder.Build(node->return_expr_, &value_wrapper);
    if (!status.isOK()) {
        LOG(WARNING) << "fail to codegen return expression: " << status;
        return false;
    }
    ::llvm::Value *value = value_wrapper.GetValue(&builder);
    if (TypeIRBuilder::IsStructPtr(value->getType())) {
        auto struct_builder = StructTypeIRBuilder::CreateStructTypeIRBuilder(block->getModule(), value->getType());
        if (!struct_builder.ok()) {
            status.code = kCodegenError;
            status.msg = struct_builder.status().ToString();
            return false;
        }
        NativeValue ret_value;
        if (!var_ir_builder.LoadRetStruct(&ret_value, status)) {
            LOG(WARNING) << "fail to load ret struct address";
            return false;
        }
        if (!struct_builder.value()->CopyFrom(block, value, ret_value.GetValue(&builder))) {
            return false;
        }
        value = builder.getInt1(true);
    }
    builder.CreateRet(value);
    return true;
}

bool BlockIRBuilder::BuildAssignStmt(const ::hybridse::node::FnAssignNode *node,
                                     base::Status &status) {  // NOLINE
    if (node == NULL || node->expression_ == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "node or block is null";
        LOG(WARNING) << status;
        return false;
    }
    ExprIRBuilder builder(ctx_);
    VariableIRBuilder variable_ir_builder(ctx_->GetCurrentBlock(),
                                          ctx_->GetCurrentScope()->sv());
    NativeValue value;
    status = builder.Build(node->expression_, &value);
    if (!status.isOK()) {
        LOG(WARNING) << "fail to codegen expr" << status;
        return false;
    }
    auto var_key = node->var_->GetExprString();
    return variable_ir_builder.StoreValue(var_key, value, false, status);
}

}  // namespace codegen
}  // namespace hybridse
