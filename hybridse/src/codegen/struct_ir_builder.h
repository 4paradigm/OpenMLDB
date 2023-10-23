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

#ifndef HYBRIDSE_SRC_CODEGEN_STRUCT_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_STRUCT_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/native_value.h"
#include "codegen/type_ir_builder.h"

namespace hybridse {
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
    bool Create(::llvm::BasicBlock* block, ::llvm::Value** output) const;
    virtual bool CreateDefault(::llvm::BasicBlock* block,
                               ::llvm::Value** output) = 0;

    // Load the 'idx' th field into ''*output'
    // NOTE: not all types are loaded correctly, e.g for array type
    bool Load(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx, ::llvm::Value** output) const;
    // store 'value' into 'idx' field
    bool Set(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx, ::llvm::Value* value) const;
    // Get the address of 'idx' th field
    bool Get(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx, ::llvm::Value** output) const;

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
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_STRUCT_IR_BUILDER_H_
