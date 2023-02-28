/**
 * Copyright (c) 2023 4Paradigm Authors
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

struct StdSampleOut {
    static void Output(double total, size_t cnt, double* ret, bool* is_null) {
        if (cnt <= 1) {
            *is_null = true;
            return;
        }

        *ret = std::sqrt(total / (cnt - 1));
        *is_null = false;
    }
};

struct StdPopOut {
    static void Output(double total, size_t cnt, double* ret, bool* is_null) {
        *ret = std::sqrt(total / cnt);
        *is_null = false;
    }
};

struct VarPopOut {
    static void Output(double total, size_t cnt, double* ret, bool* is_null) {
        *ret = total / cnt;
        *is_null = false;
    }
};

struct VarSampleOut {
    static void Output(double total, size_t cnt, double* ret, bool* is_null) {
        if (cnt <= 1) {
            *is_null = true;
            return;
        }
        *ret = total / (cnt - 1);
        *is_null = false;
    }
};

// stdev_samp, stddev_pop, var_samp, var_pop
template <typename StatisticTrait>
struct StdTemplate {
    template <typename T>
    struct Impl {
        using ContainerT = std::pair<std::vector<T>, double>;

        void operator()(UdafRegistryHelper& helper) {  // NOLINT
            std::string suffix = absl::StrCat(".opaque_std_pair_std_vector_double_", DataTypeTrait<T>::to_string());
            std::string prefix = helper.name();
            helper.templates<Nullable<double>, Opaque<ContainerT>, Nullable<T>>()
                .init(prefix + "_init" + suffix, Init)
                .update(prefix + "_update" + suffix, Update)
                .output(prefix + "_output" + suffix, reinterpret_cast<void*>(Output), true);
        }

        static void Init(ContainerT* ptr) { new (ptr) ContainerT(std::vector<T>(), 0.0); }

        static ContainerT* Update(ContainerT* ptr, T t, bool is_null) {
            if (!is_null) {
                ptr->first.emplace_back(t);
                ptr->second += t;
            }
            return ptr;
        }

        static void Output(ContainerT* ptr, double* ret, bool* is_null) {
            size_t cnt = ptr->first.size();
            if (cnt == 0) {
                *is_null = true;
            } else {
                double avg = ptr->second / cnt;
                double total = 0;
                for (size_t i = 0; i < cnt; i++) {
                    total += std::pow(ptr->first[i] - avg, 2);
                }
                StatisticTrait::Output(total, cnt, ret, is_null);
            }
            ptr->~ContainerT();
        }
    };
};


// Shannon entropy
// Given the discrete random variable X that is of *N* symbols total consisting of *n* different values:
// H2(X) = - SUM ( (count[i] / N) * log2(count[i] / N) , for i in 1 -> n)
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
        auto [it, inserted] = ctr->first.try_emplace(val, 0);
        it->second++;
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
    RegisterUdafTemplate<StdTemplate<StdPopOut>::Impl>("stddev_pop")
        .doc(R"(
            @brief Compute population standard deviation of values, i.e., `sqrt( sum((x_i - avg)^2) / n )`

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT stddev_pop(value) OVER w;
                -- output 1.118034
            @endcode
            @since 0.7.2
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<StdTemplate<StdSampleOut>::Impl>("stddev")
        .doc(R"(
            @brief Compute sample standard deviation of values, i.e., `sqrt( sum((x_i - avg)^2) / (n-1) )`

            Alias function: `std`, `stddev_samp`

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT stddev(value) OVER w;
                -- output 1.290994
            @endcode
            @since 0.7.2
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<StdTemplate<VarPopOut>::Impl>("var_pop")
        .doc(R"(
            @brief Compute population variance of values, i.e., `sum((x_i - avg)^2) / n`

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |3|
            |6|
            @code{.sql}
                SELECT var_pop(value) OVER w;
                -- output 6.0
            @endcode
            @since 0.8.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<StdTemplate<VarSampleOut>::Impl>("var_samp")
        .doc(R"(
            @brief Compute population variance of values, i.e., `sum((x_i - avg)^2) / (n-1)`

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |3|
            |6|
            @code{.sql}
                SELECT var_samp(value) OVER w;
                -- output 9.0
            @endcode
            @since 0.8.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterAlias("std", "stddev");
    RegisterAlias("stddev_samp", "stddev");
    RegisterAlias("variance", "var_samp");

    RegisterUdafTemplate<ShannonEntropy>("entropy")
        .doc(R"(
            @brief Calculate Shannon entropy of a column of values. Null values are skipped.

            @param value Specify value column to aggregate on.

            Example:

            | col1 |
            |  1 |
            |  1 |
            |  2 |
            |  3 |

            @code{.sql}
             select entropy(col1) from t1
             -- output 1.5
            @endcode

            @since 0.8.0
             )")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, StringRef, openmldb::base::Timestamp,
                 openmldb::base::Date>();
}

}  // namespace udf
}  // namespace hybridse
