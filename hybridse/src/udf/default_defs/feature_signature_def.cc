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

enum FeatureSignatureType {
    kFeatureSignatureNumeric=100,
    kFeatureSignatureCategory=101,
    kFeatureSignatureBinaryLabel=200,
    kFeatureSignatureMulticlassLabel=201,
    kFeatureSignatureRegressionLabel=202,
};

template <typename T>
struct Numeric {
    using Ret = Tuple<int32_t, T>;
    using Args = std::tuple<T>;
    void operator()(T v, int32_t* feature_signature, T* ret) {
        *feature_signature = kFeatureSignatureNumeric;
        *ret = v;
    }
};


void TestDoubleIt(int32_t a, int32_t* ret1, bool* nullflag, int32_t* ret2) {
    *ret1 = a;
    *ret2 = a;
    *nullflag = false;
}

struct VariadicConcat {
    static void Init(StringRef* value, std::string* addr) {
        new (addr) std::string(value->data_);
    }

    template<typename V>
    static std::string* Update(std::string* state, V value) {
        state->append(" ").append(std::to_string(value));
        return state;
    }

    static std::string* Update2(std::string* state, int32_t value1, int32_t value2) {
        state->append(" ").append("(" + std::to_string(value1) + ", " + std::to_string(value2) + ")");
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
    RegisterExternal("test_double_it")
        .return_and_args<Tuple<Nullable<int32_t>, int32_t>, int32_t>(v1::TestDoubleIt);
    RegisterVariadicUdf<Opaque<std::string>, StringRef>("variadic_concat")
        .init(v1::VariadicConcat::Init)
        .update<int32_t>(v1::VariadicConcat::Update<int32_t>)
        .update<float>(v1::VariadicConcat::Update<float>)
        .update<double>(v1::VariadicConcat::Update<double>)
        .update<Tuple<int32_t, int32_t>>(v1::VariadicConcat::Update2)
        .output(v1::VariadicConcat::Output);
}

} // namespace udf
} // namespace hybridse
