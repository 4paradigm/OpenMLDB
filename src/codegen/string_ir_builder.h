/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * string_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/5/26
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_STRING_IR_BUILDER_H_
#define SRC_CODEGEN_STRING_IR_BUILDER_H_
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/struct_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codegen {

class StringIRBuilder : public StructTypeIRBuilder {
 public:
    explicit StringIRBuilder(::llvm::Module* m);
    ~StringIRBuilder();
    void InitStructType();
    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewString(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewString(::llvm::BasicBlock* block, const std::string& str,
                   ::llvm::Value** output);
    bool NewString(::llvm::BasicBlock* block, ::llvm::Value* size,
                   ::llvm::Value* data, ::llvm::Value** output);
    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                  ::llvm::Value* dist);
    bool GetSize(::llvm::BasicBlock* block, ::llvm::Value* str,
                 ::llvm::Value** output);
    bool SetSize(::llvm::BasicBlock* block, ::llvm::Value* str,
                 ::llvm::Value* size);
    bool GetData(::llvm::BasicBlock* block, ::llvm::Value* str,
                 ::llvm::Value** output);
    bool SetData(::llvm::BasicBlock* block, ::llvm::Value* str,
                 ::llvm::Value* data);
    base::Status CastFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                          ::llvm::Value** output);
    base::Status Compare(::llvm::BasicBlock* block, const NativeValue& s1,
                         const NativeValue& s2, NativeValue* output);
    base::Status Concat(::llvm::BasicBlock* block,
                        const std::vector<NativeValue>& strs,
                        NativeValue* output);
    base::Status ConcatWS(::llvm::BasicBlock* block, const NativeValue& on,
                          const std::vector<NativeValue>& strs,
                          NativeValue* output);
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_STRING_IR_BUILDER_H_
