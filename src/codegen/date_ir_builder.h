/*
 * date_ir_builder.h
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


#ifndef SRC_CODEGEN_DATE_IR_BUILDER_H_
#define SRC_CODEGEN_DATE_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/null_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/struct_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codegen {

class DateIRBuilder : public StructTypeIRBuilder {
 public:
    explicit DateIRBuilder(::llvm::Module* m);
    ~DateIRBuilder();
    void InitStructType();
    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewDate(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                 ::llvm::Value** output);
    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                  ::llvm::Value* dist);
    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src,
                          NativeValue* output);
    base::Status CastFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                          ::llvm::Value** output);
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
}  // namespace fesql
#endif  // SRC_CODEGEN_DATE_IR_BUILDER_H_
