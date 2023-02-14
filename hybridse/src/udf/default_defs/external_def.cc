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

#include <cstdint>
#include <map>
#include <cmath>

#include "udf/udf_registry.h"
#include "absl/strings/str_cat.h"

namespace hybridse {
namespace udf {

template <typename T>
struct ShannonEntropy {
    using CType = typename DataTypeTrait<T>::CCallArgType;

    // intermedate state: ([key -> count], total count)
    using ContainerT = std::pair<std::map<T, int64_t>, int64_t>;

    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        std::string prefix = absl::StrCat(helper.name(), "_", DataTypeTrait<T>::to_string());
        helper.templates<Nullable<double>, Opaque<ContainerT>, Nullable<T>>()
            .init(prefix + "_init", Init)
            .update(prefix + "_update" , Update)
            .output(prefix + "_output" , reinterpret_cast<void*>(Output), true);
    }

    static void Init(ContainerT* addr) { new (addr) ContainerT({}, 0); }

    static ContainerT* Update(ContainerT* ctr, CType value, bool is_null) {
        if (is_null) {
            return ctr;
        }
        if (ctr->first.count(value) == 0) {
            ctr->first.emplace(value, 1);
        } else {
            ctr->first.at(value)++;
        }
        ctr->second++;
    }

    static void Output(ContainerT* ctr, double* ret, bool* is_null) {
        if (ctr->second == 0) {
            *is_null = true;
            return;
        }
        double agg = 0.0;
        auto cnt = ctr->second;
        for (auto& kv : ctr->first) {
            double p_x = kv.second / cnt;
            agg -= p_x * log2(p_x);
        }
        *ret = agg;
        *is_null = false;
    }
};

}  // namespace udf
}  // namespace hybridse
