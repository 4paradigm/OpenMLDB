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

#ifndef HYBRIDSE_SRC_CODEGEN_UDF_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_UDF_IR_BUILDER_H_

#include <map>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/Module.h"

#include "node/sql_node.h"
namespace hybridse {
namespace codegen {

using base::Status;

class UdfIRBuilder {
 public:
    UdfIRBuilder(CodeGenContext* ctx, node::ExprNode* frame_arg,
                 const node::FrameNode* frame);

    ~UdfIRBuilder() {}

    Status BuildCall(const node::FnDefNode* fn,
                     const std::vector<const node::TypeNode*>& arg_types,
                     const std::vector<NativeValue>& args, NativeValue* output);

    Status BuildUdfCall(const node::UdfDefNode* fn,
                        const std::vector<NativeValue>& args,
                        NativeValue* output);

    Status BuildLambdaCall(const node::LambdaNode* fn,
                           const std::vector<NativeValue>& args,
                           NativeValue* output);

    Status BuildExternCall(const node::ExternalFnDefNode* fn,
                           const std::vector<NativeValue>& args,
                           NativeValue* output);

    Status BuildDynamicUdfCall(const node::DynamicUdfFnDefNode* fn,
                           const std::vector<NativeValue>& args,
                           NativeValue* output);

    Status BuildCodeGenUdfCall(
        const node::UdfByCodeGenDefNode* fn,
        const std::vector<NativeValue>& args, NativeValue* output);

    Status BuildUdafCall(const node::UdafDefNode* fn,
                         const std::vector<NativeValue>& args,
                         NativeValue* output);

    Status GetUdfCallee(const node::UdfDefNode* fn,
                        ::llvm::FunctionCallee* callee, bool* return_by_arg);

 private:
    Status ExpandLlvmCallArgs(const node::TypeNode* dtype, bool nullable,
                              const NativeValue& value,
                              ::llvm::IRBuilder<>* builder,
                              std::vector<::llvm::Value*>* arg_vec,
                              ::llvm::Value** should_ret_null);

    Status ExpandLlvmCallVariadicArgs(const NativeValue& value,
                                      ::llvm::IRBuilder<>* builder,
                                      std::vector<::llvm::Value*>* arg_vec,
                                      ::llvm::Value** should_ret_null);

    Status ExpandLlvmCallReturnArgs(const node::TypeNode* dtype, bool nullable,
                                    ::llvm::IRBuilder<>* builder,
                                    std::vector<::llvm::Value*>* arg_vec);

    Status ExtractLlvmReturnValue(const node::TypeNode* dtype, bool nullable,
                                  const std::vector<::llvm::Value*>& llvm_args,
                                  ::llvm::IRBuilder<>* builder, size_t* pos_idx,
                                  NativeValue* output);

    Status BuildLlvmCall(const node::FnDefNode* fn,
                         ::llvm::FunctionCallee callee,
                         const std::vector<const node::TypeNode*>& arg_types,
                         const std::vector<int>& arg_nullable,
                         const std::vector<NativeValue>& args,
                         bool return_by_arg, NativeValue* output);

    CodeGenContext* ctx_;
    node::ExprNode* frame_arg_;
    const node::FrameNode* frame_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_UDF_IR_BUILDER_H_
