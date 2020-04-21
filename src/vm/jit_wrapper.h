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

#include "llvm/IR/Module.h"

#include "base/raw_buffer.h"
#include "base/status.h"
#include "vm/jit.h"
#include "vm/runner.h"

namespace fesql {
namespace vm {

typedef const int8_t* RawFunctionPtr;


// TODO(bxq): remove dummy runner
class DummyRunner : public Runner {
 public:
    DummyRunner(): Runner(0) {}
    ~DummyRunner() {}
    std::shared_ptr<DataHandler> Run(
        fesql::vm::RunnerContext& ctx) override {  // NOLINT
        return nullptr;
    }

    fesql::codec::Row RunRowProject(RawFunctionPtr fn,
                                    const fesql::codec::Row row) {
        return this->RowProject(fn, row);
    }
};


class FeSQLJITWrapper {
 public:
    FeSQLJITWrapper() {}
    ~FeSQLJITWrapper() {}
    FeSQLJITWrapper(const FeSQLJITWrapper&) = delete;

    bool Init();

    bool AddModule(std::unique_ptr<llvm::Module> module,
                   std::unique_ptr<llvm::LLVMContext> llvm_ctx);

    bool AddModuleFromBuffer(const base::RawBuffer&);

    RawFunctionPtr FindFunction(const std::string& funcname);

 private:
    std::unique_ptr<FeSQLJIT> jit_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_JIT_WRAPPER_H_
