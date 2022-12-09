/*
 * Copyright 2021 4Paradigm
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
#ifndef HYBRIDSE_SRC_VM_JIT_RUNTIME_H_
#define HYBRIDSE_SRC_VM_JIT_RUNTIME_H_

#include <list>

#include "base/fe_object.h"
#include "base/mem_pool.h"

namespace hybridse {
namespace vm {

class JitRuntime {
 public:
    JitRuntime() {}

    /**
     * Get TLS JIT runtime instance.
     */
    static JitRuntime* get();

    /**
     * Allocate raw memory with specified bytes.
     * Return nullptr on failure. All allocated memory
     * will be released by `ReleaseRunStep()`.
     */
    int8_t* AllocManaged(size_t bytes);

    openmldb::base::ByteMemoryPool* GetMemPool() { return &mem_pool_; }

    /**
     * Register object to be managed by runtime.
     * All managed objects will be released by `ReleaseRunStep()`.
     */
    void AddManagedObject(base::FeBaseObject* obj);

    /**
     * Initialize before each single run step
     */
    void InitRunStep();

    /**
     * Release resources allocated in run step
     */
    void ReleaseRunStep();

 private:
    openmldb::base::ByteMemoryPool mem_pool_;
    std::list<base::FeBaseObject*> allocated_obj_pool_;

    static thread_local JitRuntime tls_runtime_inst_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_JIT_RUNTIME_H_
