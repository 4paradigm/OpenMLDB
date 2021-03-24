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

#ifndef SRC_CODEGEN_UDF_IR_BUILDER_H_
#define SRC_CODEGEN_UDF_IR_BUILDER_H_

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

class UDFIRBuilder {
 public:
    UDFIRBuilder(CodeGenContext* ctx, node::ExprNode* frame_arg,
                 const node::FrameNode* frame);

    ~UDFIRBuilder() {}

    Status BuildCall(const node::FnDefNode* fn,
                     const std::vector<const node::TypeNode*>& arg_types,
                     const std::vector<NativeValue>& args, NativeValue* output);

    Status BuildUDFCall(const node::UDFDefNode* fn,
                        const std::vector<const node::TypeNode*>& arg_types,
                        const std::vector<NativeValue>& args,
                        NativeValue* output);

    Status BuildLambdaCall(const node::LambdaNode* fn,
                           const std::vector<const node::TypeNode*>& arg_types,
                           const std::vector<NativeValue>& args,
                           NativeValue* output);

    Status BuildExternCall(const node::ExternalFnDefNode* fn,
                           const std::vector<const node::TypeNode*>& arg_types,
                           const std::vector<NativeValue>& args,
                           NativeValue* output);

    Status BuildCodeGenUDFCall(
        const node::UDFByCodeGenDefNode* fn,
        const std::vector<const node::TypeNode*>& arg_types,
        const std::vector<NativeValue>& args, NativeValue* output);

    Status BuildUDAFCall(const node::UDAFDefNode* fn,
                         const std::vector<const node::TypeNode*>& arg_types,
                         const std::vector<NativeValue>& args,
                         NativeValue* output);

    Status GetUDFCallee(const node::UDFDefNode* fn,
                        const std::vector<const node::TypeNode*>& arg_types,
                        ::llvm::FunctionCallee* callee, bool* return_by_arg);

 private:
    Status ExpandLLVMCallArgs(const node::TypeNode* dtype, bool nullable,
                              const NativeValue& value,
                              ::llvm::IRBuilder<>* builder,
                              std::vector<::llvm::Value*>* arg_vec,
                              ::llvm::Value** should_ret_null);

    Status ExpandLLVMCallVariadicArgs(const NativeValue& value,
                                      ::llvm::IRBuilder<>* builder,
                                      std::vector<::llvm::Value*>* arg_vec,
                                      ::llvm::Value** should_ret_null);

    Status ExpandLLVMCallReturnArgs(const node::TypeNode* dtype, bool nullable,
                                    ::llvm::IRBuilder<>* builder,
                                    std::vector<::llvm::Value*>* arg_vec);

    Status ExtractLLVMReturnValue(const node::TypeNode* dtype, bool nullable,
                                  const std::vector<::llvm::Value*>& llvm_args,
                                  ::llvm::IRBuilder<>* builder, size_t* pos_idx,
                                  NativeValue* output);

    Status BuildLLVMCall(const node::FnDefNode* fn,
                         ::llvm::FunctionCallee callee,
                         const std::vector<NativeValue>& args,
                         bool return_by_arg, NativeValue* output);

    Status GetLLVMFunctionType(const node::FnDefNode* fn,
                               ::llvm::FunctionType** func_ty);

    CodeGenContext* ctx_;
    node::ExprNode* frame_arg_;
    const node::FrameNode* frame_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // SRC_CODEGEN_UDF_IR_BUILDER_H_
