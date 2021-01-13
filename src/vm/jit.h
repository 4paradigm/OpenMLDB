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

#include <map>
#include <memory>
#include <string>
#include "llvm/ExecutionEngine/GenericValue.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "vm/jit_wrapper.h"

#ifdef LLVM_EXT_ENABLE
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/MCJIT.h"
#endif

namespace fesql {
namespace vm {

struct JITString {
    int32_t size;
    int8_t* data;
};

class FeSQLJIT : public ::llvm::orc::LLJIT {
    template <typename, typename, typename>
    friend class ::llvm::orc::LLJITBuilderSetters;

 public:
    void Init();

    ::llvm::Error AddIRModule(::llvm::orc::JITDylib& jd,  // NOLINT
                              ::llvm::orc::ThreadSafeModule tsm,
                              ::llvm::orc::VModuleKey key);

    bool OptModule(::llvm::Module* m);

    ::llvm::orc::VModuleKey CreateVModule();

    void ReleaseVModule(::llvm::orc::VModuleKey key);

    // add to main module
    bool AddSymbol(const std::string& name, void* fn_ptr);

    // add to main module
    bool AddSymbol(::llvm::orc::JITDylib& jd,  // NOLINT
                   const std::string& name, void* fn_ptr);

    static bool AddSymbol(::llvm::orc::JITDylib& jd,           // NOLINT
                          ::llvm::orc::MangleAndInterner& mi,  // NOLINT
                          const std::string& fn_name, void* fn_ptr);
    ~FeSQLJIT();

 protected:
    FeSQLJIT(::llvm::orc::LLJITBuilderState& s, ::llvm::Error& e);  // NOLINT
};

class FeSQLJITBuilder
    : public ::llvm::orc::LLJITBuilderState,
      public ::llvm::orc::LLJITBuilderSetters<FeSQLJIT, FeSQLJITBuilder,
                                              ::llvm::orc::LLJITBuilderState> {
};

template <typename T>
std::string LLVMToString(const T& value) {
    std::string str;
    ::llvm::raw_string_ostream ss(str);
    ss << value;
    ss.flush();
    return str;
}

class FeSQLLLJITWrapper : public FeSQLJITWrapper {
 public:
    FeSQLLLJITWrapper() {}
    ~FeSQLLLJITWrapper() {}

    bool Init() override;

    bool OptModule(::llvm::Module* module) override;

    bool AddModule(std::unique_ptr<llvm::Module> module,
                   std::unique_ptr<llvm::LLVMContext> llvm_ctx) override;

    bool AddExternalFunction(const std::string& name, void* addr) override;

    fesql::vm::RawPtrHandle FindFunction(const std::string& funcname) override;

 private:
    std::unique_ptr<FeSQLJIT> jit_;
    std::unique_ptr<::llvm::orc::MangleAndInterner> mi_;
};

#ifdef LLVM_EXT_ENABLE
class FeSQLMCJITWrapper : public FeSQLJITWrapper {
 public:
    explicit FeSQLMCJITWrapper(const JITOptions& jit_options)
        : jit_options_(jit_options) {}
    ~FeSQLMCJITWrapper() {}

    bool Init() override;

    bool OptModule(::llvm::Module* module) override;

    bool AddModule(std::unique_ptr<llvm::Module> module,
                   std::unique_ptr<llvm::LLVMContext> llvm_ctx) override;

    bool AddExternalFunction(const std::string& name, void* addr) override;

    fesql::vm::RawPtrHandle FindFunction(const std::string& funcname) override;

 private:
    bool CheckInitialized() const;
    bool CheckError();

    const JITOptions jit_options_;
    std::string err_str_ = "";
    std::map<std::string, void*> extern_functions_;
    llvm::ExecutionEngine* execution_engine_ = nullptr;
};
#endif

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_JIT_H_
