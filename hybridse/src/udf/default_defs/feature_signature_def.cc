/**
 * Copyright (c) 2022 4Paradigm Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "absl/strings/str_split.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

namespace v1 {

struct VariadicConcat {
    static void Init(StringRef* value, std::string* addr) {
        new (addr) std::string(value->data_);
    }

    template<typename V>
    static std::string* Update(std::string* state, V value) {
        state->append(" ").append(std::to_string(value));
        return state;
    }

    static void Output(std::string* state, StringRef* output) {
        char* buffer = AllocManagedStringBuf(state->size() + 1);
        memcpy(buffer, state->c_str(), state->size());
        buffer[state->size()] = 0;
        output->data_ = buffer;
        output->size_ = state->size();
        state->~basic_string();
    }
};

} // namespace v1

void DefaultUdfLibrary::InitFeatureSignature() {
    RegisterVariadicUdf<StringRef>("variadic_concat").init(v1::VariadicConcat::Init)
        .update<Opaque<std::string>, int32_t>(v1::VariadicConcat::Update<int32_t>)
        .update<Opaque<std::string>, float>(v1::VariadicConcat::Update<float>)
        .update<Opaque<std::string>, double>(v1::VariadicConcat::Update<double>)
        .output<Opaque<std::string>>(v1::VariadicConcat::Output);
}

} // namespace udf
} // namespace hybridse
