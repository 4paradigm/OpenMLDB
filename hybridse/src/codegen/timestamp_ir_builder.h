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

#ifndef HYBRIDSE_SRC_CODEGEN_TIMESTAMP_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_TIMESTAMP_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/struct_ir_builder.h"

namespace hybridse {
namespace codegen {

class TimestampIRBuilder : public StructTypeIRBuilder {
 public:
    explicit TimestampIRBuilder(::llvm::Module* m);
    ~TimestampIRBuilder();
    void InitStructType();
    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewTimestamp(::llvm::BasicBlock* block, ::llvm::Value** output);
    bool NewTimestamp(::llvm::BasicBlock* block, ::llvm::Value* ts,
                      ::llvm::Value** output);
    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                  ::llvm::Value* dist);
    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src,
                          NativeValue* output);
    bool GetTs(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
               ::llvm::Value** output);
    bool SetTs(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
               ::llvm::Value* ts);
    bool Minute(::llvm::BasicBlock* block, ::llvm::Value* ts,
                ::llvm::Value** output, base::Status& status);  // NOLINT
    bool Hour(::llvm::BasicBlock* block, ::llvm::Value* ts,
              ::llvm::Value** output, base::Status& status);  // NOLINT

    bool Second(::llvm::BasicBlock* block, ::llvm::Value* ts,
                ::llvm::Value** output, base::Status& status);  // NOLINT
    base::Status FDiv(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
                      ::llvm::Value* right, ::llvm::Value** output);
    base::Status TimestampAdd(::llvm::BasicBlock* block,
                              ::llvm::Value* timestamp, ::llvm::Value* right,
                              ::llvm::Value** output);
    static int32_t TIME_ZONE;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_TIMESTAMP_IR_BUILDER_H_
