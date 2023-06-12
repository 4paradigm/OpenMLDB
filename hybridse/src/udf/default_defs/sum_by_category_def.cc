/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"
#include "udf/default_defs/containers.h"

using openmldb::base::Date;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;

namespace hybridse {
namespace udf {

template <typename K>
struct SumCateDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<Impl>("sum_cate")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, V>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        // FormatValueF
        static uint32_t FormatValueFn(const typename ContainerT::StorageValue& val, char* buf, size_t size) {
            return v1::format_string(val, buf, size);
        }

        void operator()(UdafRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" +
                                 DataTypeTrait<K>::to_string() + "_" +
                                 DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<K>>()
                .init("sum_cate_init" + suffix, ContainerT::Init)
                .update("sum_cate_update" + suffix, Update)
                .output("sum_cate_output" + suffix, Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputV value,
                                  bool is_value_null, InputK key,
                                  bool is_key_null) {
            if (is_key_null || is_value_null) {
                return ptr;
            }
            auto& map = ptr->map();
            auto stored_key = ContainerT::to_stored_key(key);
            auto iter = map.find(stored_key);
            if (iter == map.end()) {
                map.insert(iter,
                           {stored_key, ContainerT::to_stored_value(value)});
            } else {
                auto& single = iter->second;
                single += ContainerT::to_stored_value(value);
            }
            return ptr;
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ContainerT::OutputString(
                ptr, false, output, FormatValueFn);
            ContainerT::Destroy(ptr);
        }
    };
};

template <typename K>
struct SumCateWhereDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<Impl>("sum_cate_where")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, V>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using SumCateImpl = typename SumCateDef<K>::template Impl<V>;

        void operator()(UdafRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" +
                                 DataTypeTrait<K>::to_string() + "_" +
                                 DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>>()
                .init("sum_cate_where_init" + suffix, ContainerT::Init)
                .update("sum_cate_where_update" + suffix, Update)
                .output("sum_cate_where_output" + suffix, SumCateImpl::Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputV value,
                                  bool is_value_null, bool cond,
                                  bool is_cond_null, InputK key,
                                  bool is_key_null) {
            if (cond && !is_cond_null) {
                SumCateImpl::Update(ptr, value, is_value_null, key,
                                    is_key_null);
            }
            return ptr;
        }
    };
};

template <typename K>
struct TopKSumCateWhereDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<Impl>("top_n_key_sum_cate_where")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, V>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using CateImpl = typename SumCateDef<K>::template Impl<V>;

        void operator()(UdafRegistryHelper& helper) {  // NOLINT
            std::string suffix;

            suffix = ".i32_bound_opaque_dict_" + DataTypeTrait<K>::to_string() +
                     "_" + DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>, int32_t>()
                .init("top_n_key_sum_cate_where_init" + suffix,
                      ContainerT::Init)
                .update("top_n_key_sum_cate_where_update" + suffix,
                        UpdateI32Bound)
                .output("top_n_key_sum_cate_where_output" + suffix, Output);

            suffix = ".i64_bound_opaque_dict_" + DataTypeTrait<K>::to_string() +
                     "_" + DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>, int64_t>()
                .init("top_n_key_sum_cate_where_init" + suffix,
                      ContainerT::Init)
                .update("top_n_key_sum_cate_where_update" + suffix, Update)
                .output("top_n_key_sum_cate_where_output" + suffix, Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputV value,
                                  bool is_value_null, bool cond,
                                  bool is_cond_null, InputK key,
                                  bool is_key_null, int64_t bound) {
            if (cond && !is_cond_null) {
                CateImpl::Update(ptr, value, is_value_null, key,
                                    is_key_null);
                auto& map = ptr->map();
                if (bound >= 0 && map.size() > static_cast<size_t>(bound)) {
                    map.erase(map.begin());
                }
            }
            return ptr;
        }

        static ContainerT* UpdateI32Bound(ContainerT* ptr, InputV value,
                                          bool is_value_null, bool cond,
                                          bool is_cond_null, InputK key,
                                          bool is_key_null, int32_t bound) {
            return Update(ptr, value, is_value_null, cond, is_cond_null, key,
                          is_key_null, bound);
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ContainerT::OutputString(
                ptr, true, output, [](const V& sum, char* buf, size_t size) {
                    return v1::format_string(sum, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

template <typename K>
struct TopNValueSumCateWhereDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<container::TopNValueImpl<SumCateDef<K>::template Impl>::template Impl>(helper.name())
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }
};

void DefaultUdfLibrary::InitSumByCateUdafs() {
    RegisterUdafTemplate<SumCateDef>("sum_cate")
        .doc(R"(
            @brief Compute sum of values grouped by category key and output string.
            Each group is represented as 'K:V' and separated by comma in outputs
            and are sorted by key in ascend order.

            @param value  Specify value column to aggregate on.
            @param catagory  Specify catagory column to group by.

            Example:

            value|catagory
            --|--
            0|x
            1|y
            2|x
            3|y
            4|x
            @code{.sql}
                SELECT sum_cate(value, catagory) OVER w;
                -- output "x:6,y:4"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUdafTemplate<SumCateWhereDef>("sum_cate_where")
        .doc(R"(
            @brief Compute sum of values matching specified condition grouped by
    category key and output string. Each group is represented as 'K:V' and
    separated by comma in outputs and are sorted by key in ascend order.

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.
            @param catagory  Specify catagory column to group by.

            Example:

            value|condition|catagory
            --|--|--
            0|true|x
            1|false|y
            2|false|x
            3|true|y
            4|true|x

            @code{.sql}
                SELECT sum_cate_where(value, condition, category) OVER w;
                -- output "x:4,y:3"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUdafTemplate<TopKSumCateWhereDef>("top_n_key_sum_cate_where")
        .doc(R"(
            @brief Compute sum of values matching specified condition grouped by
    category key. Output string for top N category keys in descend order. Each group is
    represented as 'K:V' and separated by comma(,). Empty string returned if no rows selected.

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.
            @param catagory  Specify catagory column to group by.
            @param n  Fetch top n keys.

            Example:

            value|condition|catagory
            --|--|--
            0|true|x
            1|true|y
            2|false|x
            3|true|y
            4|false|x
            5|true|z
            6|true|z
            @code{.sql}
                SELECT top_n_key_sum_cate_where(value, condition, catagory, 2)
    OVER w;
                -- output "z:11,y:4"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUdafTemplate<TopNValueSumCateWhereDef>("top_n_value_sum_cate_where")
        .doc(R"(
            @brief Compute sum of values matching specified condition grouped by
    category key. Output string for top N aggregate values in descend order. Each group is
    represented as 'K:V' and separated by comma(,). Empty string returned if no rows selected.

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.
            @param catagory  Specify catagory column to group by.
            @param n  Top N.

            Example:

            value|condition|catagory
            --|--|--
            0|true|x
            1|true|y
            2|false|x
            3|false|y
            4|true|x
            5|true|z
            6|true|z
            @code{.sql}
                SELECT top_n_value_sum_cate_where(value, condition, catagory, 2)
    OVER w;
                -- output "z:11,x:4"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();
}

}  // namespace udf
}  // namespace hybridse
