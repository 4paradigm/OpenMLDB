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


#ifndef SRC_CODEGEN_COND_SELECT_IR_BUILDER_H_
#define SRC_CODEGEN_COND_SELECT_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/native_value.h"
namespace fesql {
namespace codegen {
class CondSelectIRBuilder {
 public:
    CondSelectIRBuilder();
    ~CondSelectIRBuilder();
    base::Status Select(::llvm::BasicBlock* block, const NativeValue& cond,
                        const NativeValue& left, const NativeValue& right,
                        NativeValue* output);
};
}  // namespace codegen
}  // namespace fesql

#endif  // SRC_CODEGEN_COND_SELECT_IR_BUILDER_H_
