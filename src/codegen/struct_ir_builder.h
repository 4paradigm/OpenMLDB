/*
 * struct_ir_builder.h
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


#ifndef SRC_CODEGEN_STRUCT_IR_BUILDER_H_
#define SRC_CODEGEN_STRUCT_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/type_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codegen {

class StructTypeIRBuilder : public TypeIRBuilder {
 public:
    explicit StructTypeIRBuilder(::llvm::Module*);
    ~StructTypeIRBuilder();
    static StructTypeIRBuilder* CreateStructTypeIRBuilder(::llvm::Module*,
                                                          ::llvm::Type*);
    static bool StructCopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                               ::llvm::Value* dist);
    virtual void InitStructType() = 0;
    ::llvm::Type* GetType();
    bool Create(::llvm::BasicBlock* block, ::llvm::Value** output);
    virtual bool CreateDefault(::llvm::BasicBlock* block,
                               ::llvm::Value** output) = 0;
    bool Get(::llvm::BasicBlock* block, ::llvm::Value* struct_value,
             unsigned int idx, ::llvm::Value** output);
    bool Set(::llvm::BasicBlock* block, ::llvm::Value* struct_value,
             unsigned int idx, ::llvm::Value* value);

    virtual bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                          ::llvm::Value* dist) = 0;
    virtual base::Status CastFrom(::llvm::BasicBlock* block,
                                  const NativeValue& src,
                                  NativeValue* output) = 0;

 protected:
    ::llvm::Module* m_;
    ::llvm::Type* struct_type_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_STRUCT_IR_BUILDER_H_
