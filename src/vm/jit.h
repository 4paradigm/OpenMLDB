/*
 * jit.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_VM_JIT_H_
#define SRC_VM_JIT_H_

#include "llvm/ExecutionEngine/Orc/LLJIT.h"

namespace fesql {
namespace vm {

struct JITString {
    int32_t size;
    int8_t* data;
};

class FeSQLJIT : public ::llvm::orc::LLJIT {
    template <typename,
             typename,
             typename> friend class ::llvm::orc::LLJITBuilderSetters;

 public:

    ::llvm::Error AddIRModule(::llvm::orc::JITDylib& jd, // NOLINT
            ::llvm::orc::ThreadSafeModule tsm,
            ::llvm::orc::VModuleKey key);

    ::llvm::orc::VModuleKey CreateVModule();

    void ReleaseVModule(::llvm::orc::VModuleKey key);

    ~FeSQLJIT();
 protected:
    FeSQLJIT(::llvm::orc::LLJITBuilderState& s, ::llvm::Error& e);
};

class FeSQLJITBuilder
    : public ::llvm::orc::LLJITBuilderState,
      public ::llvm::orc::LLJITBuilderSetters<FeSQLJIT, 
      FeSQLJITBuilder, ::llvm::orc::LLJITBuilderState> {};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_JIT_H_
