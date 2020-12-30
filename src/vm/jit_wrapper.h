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

#ifndef SRC_VM_JIT_WRAPPER_H_
#define SRC_VM_JIT_WRAPPER_H_

#include <memory>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "base/raw_buffer.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/IR/Module.h"
#include "vm/core_api.h"

namespace fesql {
namespace vm {

class JITOptions;

class FeSQLJITWrapper {
 public:
    FeSQLJITWrapper() {}
    virtual ~FeSQLJITWrapper() {}
    FeSQLJITWrapper(const FeSQLJITWrapper&) = delete;

    virtual bool Init() = 0;

    virtual bool OptModule(::llvm::Module* module) = 0;

    virtual bool AddModule(std::unique_ptr<llvm::Module> module,
                           std::unique_ptr<llvm::LLVMContext> llvm_ctx) = 0;

    virtual bool AddExternalFunction(const std::string& name, void* addr) = 0;

    bool AddModuleFromBuffer(const base::RawBuffer&);

    virtual fesql::vm::RawPtrHandle FindFunction(
        const std::string& funcname) = 0;

    static FeSQLJITWrapper* Create(const JITOptions& jit_options);
    static FeSQLJITWrapper* Create();

    static bool InitJITSymbols(FeSQLJITWrapper* jit);
};

void InitBuiltinJITSymbols(FeSQLJITWrapper* jit_ptr);

class JITOptions {
 public:
    bool is_enable_mcjit() const { return enable_mcjit_; }
    void set_enable_mcjit(bool flag) { enable_mcjit_ = flag; }

    bool is_enable_vtune() const { return enable_vtune_; }
    void set_enable_vtune(bool flag) { enable_vtune_ = flag; }

    bool is_enable_gdb() const { return enable_gdb_; }
    void set_enable_gdb(bool flag) { enable_gdb_ = flag; }

    bool is_enable_perf() const { return enable_perf_; }
    void set_enable_perf(bool flag) { enable_perf_ = flag; }

 private:
    bool enable_mcjit_ = false;
    bool enable_vtune_ = false;
    bool enable_gdb_ = false;
    bool enable_perf_ = false;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_JIT_WRAPPER_H_
