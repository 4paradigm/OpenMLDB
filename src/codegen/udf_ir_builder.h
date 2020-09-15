/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/

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
namespace fesql {
namespace codegen {

using base::Status;

class UDFIRBuilder {
 public:
    UDFIRBuilder(CodeGenContext* ctx, node::ExprNode* frame_arg,
                 node::FrameNode* frame);

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
    node::FrameNode* frame_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_IR_BUILDER_H_
