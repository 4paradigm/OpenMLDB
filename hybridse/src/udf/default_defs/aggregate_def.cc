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
#include "udf/default_defs/containers.h"
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
            .update(prefix + "_update", Update)
            .output(prefix + "_output", reinterpret_cast<void*>(Output), true);
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

template <typename Key, typename Value>
struct RatioCmp {};

template <typename Key>
struct RatioCmp<Key, std::pair<int64_t, int64_t>> {
    using T = std::pair<Key, std::pair<int64_t, int64_t>>;
    bool operator()(const T& lhs, const T& rhs) const {
        double lratio = static_cast<double>(lhs.second.first) / lhs.second.second;
        double rratio = static_cast<double>(rhs.second.first) / rhs.second.second;
        if (lratio == rratio) {
            return lhs.first < rhs.first;
        }
        return lratio < rratio;
    }
};

template <typename K>
struct RatioCateTrait {
    template <typename V>
    struct Impl {
        // StorageV is (cond_cnt, total_cnt)
        using ContainerT = udf::container::BoundedGroupByDict<K, V, std::pair<int64_t, int64_t>, RatioCmp>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;
        using StorageValue = typename ContainerT::StorageValue;

        // FormatValueF
        static uint32_t FormatValueFn(const StorageValue& val, char* buf, size_t size) {
            double ratio = static_cast<double>(val.first) / val.second;
            return v1::format_string(ratio, buf, size);
        }
    };
};

template <typename K>
struct TopNValueRatioCateOp {
    template <typename V>
    struct Impl {
        using TypeTrait = typename RatioCateTrait<K>::template Impl<V>;
        using ContainerT = typename TypeTrait::ContainerT;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;
        using Container = std::pair<ContainerT, int64_t>;

        static inline Container* Update(Container* ptr, InputV value, bool is_value_null, bool cond, bool is_cond_null,
                                        InputK key, bool is_key_null, int64_t bound) {
            if (ptr->second == 0) {
                ptr->second = bound;
            }
            if (is_key_null || is_value_null) {
                return ptr;
            }

            auto& map = ptr->first.map();
            auto stored_key = ContainerT::to_stored_key(key);
            auto [iter, exists] = map.try_emplace(stored_key, 0, 0);
            iter->second.second++;
            if (cond && !is_cond_null) {
                iter->second.first++;
            }
            return ptr;
        }

        static inline void Output(Container* ptr, codec::StringRef* output) {
            ptr->first.OutputTopNByValue(ptr->second, TypeTrait::FormatValueFn, output);
        }
    };
};

template <typename K>
struct TopNKeyRatioCateOp {
    template <typename V>
    struct Impl {
        using TypeTrait = typename RatioCateTrait<K>::template Impl<V>;
        using ContainerT = typename TypeTrait::ContainerT;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;
        using Container = std::pair<ContainerT, int64_t>;

        static inline Container* Update(Container* ptr, InputV value, bool is_value_null, bool cond, bool is_cond_null,
                                        InputK key, bool is_key_null, int64_t bound) {
            if (ptr->second == 0) {
                ptr->second = bound;
            }
            if (is_key_null || is_value_null) {
                return ptr;
            }

            auto& map = ptr->first.map();
            auto stored_key = ContainerT::to_stored_key(key);
            auto [iter, exists] = map.try_emplace(stored_key, 0, 0);
            iter->second.second++;
            if (cond && !is_cond_null) {
                iter->second.first++;
            }
            if (map.size() > static_cast<uint64_t>(bound)) {
                map.erase(map.begin());
            }
            return ptr;
        }

        static inline void Output(Container* ptr, codec::StringRef* output) {
            ContainerT::OutputString(&ptr->first, true, output, TypeTrait::FormatValueFn);
        }
    };
};

template <typename K>
struct TopNKeyRatioCate {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<container::TopNCateWhereImpl<RatioCateTrait<K>::template Impl,
                                                                TopNKeyRatioCateOp<K>::template Impl>::template Impl>(
                helper.name())
            .doc(helper.GetDoc())
            // value category
            .template args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp, StringRef, Date>();
    }
};

template <typename K>
struct TopNValueRatioCate {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<container::TopNCateWhereImpl<RatioCateTrait<K>::template Impl,
                                                                TopNValueRatioCateOp<K>::template Impl>::template Impl>(
                helper.name())
            .doc(helper.GetDoc())
            // value category
            .template args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp, StringRef, Date>();
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

    RegisterUdafTemplate<TopNKeyRatioCate>("top_n_key_ratio_cate")
        .doc(R"(
            @brief Ratios (cond match cnt / total cnt) for groups.

            For each group, ratio value is `value` expr count matches condtion divide total rows count. NULL groups or NULL values are never take into count.
            Output string for top N category keys in descend order. Each group is represented as 'K:V' and separated by comma(,).
            Empty string returned if no rows selected.

            @param value  Specify value column to aggregate on.
            @param condition  Ratio filter condition .
            @param catagory  Specify catagory column to group by.
            @param n  Top N.

            Example:

            value|condition|catagory
            --|--|--
            0|true|x
            2|true|x
            4|true|x
            1|true|y
            3|false|y
            5|true|z
            6|true|z

            @code{.sql}
                SELECT top_n_key_ratio_cate_where(value, condition, catagory, 2) from t;
                -- output "z:1.000000,y:0.500000"
            @endcode

            @since 0.8.1
            )")
        // type of categories
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUdafTemplate<TopNValueRatioCate>("top_n_value_ratio_cate")
        .doc(R"(
            @brief Ratios (cond match cnt / total cnt) for groups.

            For each group, ratio value is `value` expr count matches condtion divide total rows count. NULL groups or NULL values are never take into count.
            Output string for top N aggregate values in descend order. Each group is represented as 'K:V' and separated by comma(,).
            Empty string returned if no rows selected.

            @param value  Specify value column to aggregate on.
            @param condition  Ratio filter condition .
            @param catagory  Specify catagory column to group by.
            @param n  Top N.

            Example:

            value|condition|catagory
            --|--|--
            0|true|x
            2|true|x
            4|true|x
            1|true|y
            3|false|y
            5|true|z
            6|true|z

            @code{.sql}
                SELECT top_n_value_ratio_cate_where(value, condition, catagory, 2) from t;
                -- output "z:1.000000,x:1.000000"
            @endcode

            @since 0.8.1
            )")
        // type of categories
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();
}

}  // namespace udf
}  // namespace hybridse
