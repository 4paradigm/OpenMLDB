/*
 * src/codegen/memery_ir_builder.h
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * memery_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/7/22
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_MEMERY_IR_BUILDER_H_
#define SRC_CODEGEN_MEMERY_IR_BUILDER_H_
#include <string>
#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
namespace fesql {
namespace codegen {

class MemoryIRBuilder {
 public:
    explicit MemoryIRBuilder(::llvm::Module* m);
    ~MemoryIRBuilder();

    base::Status Alloc(::llvm::BasicBlock* block,
                       const NativeValue& request_size,
                       NativeValue* output);  // NOLINT
    base::Status MemoryCopy(::llvm::BasicBlock* block, const NativeValue& dist,
                            const NativeValue& src, const NativeValue& size);

    base::Status MemoryAddrAdd(::llvm::BasicBlock* block,
                               const NativeValue& addr, const NativeValue& size,
                               NativeValue* new_addr);

 private:
    ::llvm::Module* m_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_MEMERY_IR_BUILDER_H_
