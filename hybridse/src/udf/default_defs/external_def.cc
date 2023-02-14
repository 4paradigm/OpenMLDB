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

#include <cmath>
#include <cstdint>
#include <map>

#include "absl/strings/str_cat.h"
#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

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
        auto val = container::ContainerStorageTypeTrait<T>::to_stored_value(value);
        if (ctr->first.count(val) == 0) {
            ctr->first.emplace(val, 1);
        } else {
            ctr->first.at(val)++;
        }
        ctr->second++;

        return ctr;
    }

    static void Output(ContainerT* ctr, double* ret, bool* is_null) {
        if (ctr->second == 0) {
            *is_null = true;
        } else {
            double agg = 0.0;
            double cnt = ctr->second;
            for (auto& kv : ctr->first) {
                double p_x = kv.second / cnt;
                agg -= p_x * log2(p_x);
            }
            *ret = agg;
            *is_null = false;
        }

        ctr->~ContainerT();
    }
};

void DefaultUdfLibrary::InitStatisticsUdafs() {
    RegisterUdafTemplate<ShannonEntropy>("entropy")
        .doc(R"(
            @brief Calculate Shannon entropy of a column of values. Null values are skipped.

            @param value Specify value column to aggregate on.

            Example:

            | t1 |
            |  1 |
            |  1 |
            |  2 |
            |  3 |
            @code{.sql}
             select entropy(col1) from t1
             -- output 1.5
            @endcode

            @since 0.7.2
             )")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, StringRef, openmldb::base::Timestamp,
                 openmldb::base::Date>();
}

}  // namespace udf
}  // namespace hybridse
