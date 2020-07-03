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
    UDFIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var,
                 const vm::SchemasContext* schemas_context,
                 ::llvm::Module* module);

    ~UDFIRBuilder() {}

    Status BuildCall(const node::FnDefNode* fn,
                     const std::vector<const node::TypeNode*>& arg_types,
                     const std::vector<NativeValue>& args, NativeValue* output);

    Status BuildUDFCall(const node::UDFDefNode* fn,
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

    Status GetExternCallee(const node::ExternalFnDefNode* fn,
                           const std::vector<const node::TypeNode*>& arg_types,
                           ::llvm::FunctionCallee* callee, bool* return_by_arg);

    Status BuildCallWithLLVMCallee(::llvm::FunctionCallee callee,
                                   const std::vector<llvm::Value*>& args,
                                   bool return_by_arg, ::llvm::Value** output);

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
    ::llvm::Module* module_;
    const vm::SchemasContext* schemas_context_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_IR_BUILDER_H_
