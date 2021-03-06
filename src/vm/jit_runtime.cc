/*
 * Copyright (C) 4Paradigm
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
#include "vm/jit_runtime.h"

namespace fesql {
namespace vm {

thread_local JITRuntime JITRuntime::tls_runtime_inst_;

JITRuntime* JITRuntime::get() { return &tls_runtime_inst_; }

int8_t* JITRuntime::AllocManaged(size_t bytes) {
    return reinterpret_cast<int8_t*>(mem_pool_.Alloc(bytes));
}

void JITRuntime::AddManagedObject(base::FeBaseObject* obj) {
    if (obj != nullptr) {
        allocated_obj_pool_.push_back(obj);
    }
}

void JITRuntime::InitRunStep() {}

void JITRuntime::ReleaseRunStep() {
    mem_pool_.Reset();
    for (base::FeBaseObject* obj : allocated_obj_pool_) {
        if (obj != nullptr) {
            delete obj;
        }
    }
    allocated_obj_pool_.clear();
}

}  // namespace vm
}  // namespace fesql
