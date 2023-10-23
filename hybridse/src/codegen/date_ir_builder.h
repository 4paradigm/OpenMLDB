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

#ifndef HYBRIDSE_SRC_CODEGEN_DATE_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_DATE_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/struct_ir_builder.h"

namespace hybridse {
namespace codegen {

class DateIRBuilder : public StructTypeIRBuilder {
 public:
    explicit DateIRBuilder(::llvm::Module* m);
    ~DateIRBuilder();
    void InitStructType() override;

    base::Status CreateNull(::llvm::BasicBlock* block, NativeValue* output);
    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) override;

    bool NewDate(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                 ::llvm::Value** output);
    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist);
    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src, NativeValue* output);

    bool GetDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                 ::llvm::Value** output);
    bool SetDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                 ::llvm::Value* code);
    bool Day(::llvm::BasicBlock* block, ::llvm::Value* date,
             ::llvm::Value** output, base::Status& status);  // NOLINT
    bool Month(::llvm::BasicBlock* block, ::llvm::Value* date,
               ::llvm::Value** output, base::Status& status);  // NOLINT
    bool Year(::llvm::BasicBlock* block, ::llvm::Value* date,
              ::llvm::Value** output, base::Status& status);  // NOLINT
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_DATE_IR_BUILDER_H_
