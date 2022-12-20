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

#ifndef HYBRIDSE_SRC_CODEGEN_STRING_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_STRING_IR_BUILDER_H_
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/struct_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace codegen {

class StringIRBuilder : public StructTypeIRBuilder {
 public:
    explicit StringIRBuilder(::llvm::Module* m);
    ~StringIRBuilder();
    void InitStructType() override;
    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output);
    base::Status CreateNull(::llvm::BasicBlock* block, NativeValue* output);
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
    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src,
                          NativeValue* output);

    base::Status Compare(::llvm::BasicBlock* block, const NativeValue& s1,
                         const NativeValue& s2, NativeValue* output);
    base::Status Concat(::llvm::BasicBlock* block,
                        const std::vector<NativeValue>& strs,
                        NativeValue* output);
    base::Status ConcatWS(::llvm::BasicBlock* block, const NativeValue& on,
                          const std::vector<NativeValue>& strs,
                          NativeValue* output);

    base::Status CastFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                          ::llvm::Value** output);
    base::Status CastToNumber(::llvm::BasicBlock* block, const NativeValue& src,
                              ::llvm::Type* type, NativeValue* output);
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_STRING_IR_BUILDER_H_
