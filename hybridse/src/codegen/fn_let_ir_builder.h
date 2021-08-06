/*
 * Copyright 2021 4Paradigm
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

#ifndef SRC_CODEGEN_FN_LET_IR_BUILDER_H_
#define SRC_CODEGEN_FN_LET_IR_BUILDER_H_
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "codegen/expr_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "proto/fe_type.pb.h"
#include "vm/catalog.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace codegen {

using hybridse::base::Status;

class RowFnLetIRBuilder {
 public:
    explicit RowFnLetIRBuilder(CodeGenContext* ctx);

    ~RowFnLetIRBuilder();

    Status Build(const std::string& name, const node::LambdaNode* project_func,
                 const node::FrameNode* primary_frame,
                 const std::vector<const node::FrameNode*>& project_frames,
                 const vm::Schema& output_schema);

 private:
    bool BuildFnHeader(const std::string& name,
                       const std::vector<::llvm::Type*>& args_type,
                       ::llvm::Type* ret_type, ::llvm::Function** fn);

    bool FillArgs(const std::vector<std::string>& args, ::llvm::Function* fn,
                  ScopeVar* sv);

    bool EncodeBuf(
        const std::map<uint32_t, NativeValue>* values, const vm::Schema& schema,
        VariableIRBuilder& variable_ir_builder,  // NOLINT (runtime/references)
        ::llvm::BasicBlock* block, const std::string& output_ptr_name);

    Status BuildProject(ExprIRBuilder* expr_ir_builder, const uint32_t index,
                        const node::ExprNode* expr,
                        std::map<uint32_t, NativeValue>* output);

    Status BindProjectFrame(ExprIRBuilder* expr_ir_builder,
                            const node::FrameNode* frame,
                            const node::LambdaNode* compile_func,
                            ::llvm::BasicBlock* block, ScopeVar* sv);

 private:
    CodeGenContext* ctx_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // SRC_CODEGEN_FN_LET_IR_BUILDER_H_
