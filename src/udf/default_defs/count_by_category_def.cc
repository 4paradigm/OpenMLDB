/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * agg_by_category_def.cc
 *--------------------------------------------------------------------------
 **/
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

using fesql::codec::Date;
using fesql::codec::ListRef;
using fesql::codec::StringRef;
using fesql::codec::Timestamp;
using fesql::codegen::CodeGenContext;
using fesql::codegen::NativeValue;
using fesql::common::kCodegenError;
using fesql::node::TypeNode;

namespace fesql {
namespace udf {
template <typename K>
struct CountCateDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUDAFTemplate<Impl>("count_cate")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, int64_t>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" +
                                 DataTypeTrait<K>::to_string() + "_" +
                                 DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<K>>()
                .init("count_cate_init" + suffix, ContainerT::Init)
                .update("count_cate_update" + suffix, Update)
                .output("count_cate_output" + suffix, Output);
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
                map.insert(iter, {stored_key, 1});
            } else {
                auto& single = iter->second;
                single += 1;
            }
            return ptr;
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ContainerT::OutputString(
                ptr, false, output,
                [](const int64_t& count, char* buf, size_t size) {
                    return v1::format_string(count, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

template <typename K>
struct CountCateWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUDAFTemplate<Impl>("count_cate_where")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, int64_t>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using CountCateImpl = typename CountCateDef<K>::template Impl<V>;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" +
                                 DataTypeTrait<K>::to_string() + "_" +
                                 DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>>()
                .init("count_cate_where_init" + suffix, ContainerT::Init)
                .update("count_cate_where_update" + suffix, Update)
                .output("count_cate_where_output" + suffix,
                        CountCateImpl::Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputV value,
                                  bool is_value_null, bool cond,
                                  bool is_cond_null, InputK key,
                                  bool is_key_null) {
            if (cond && !is_cond_null) {
                CountCateImpl::Update(ptr, value, is_value_null, key,
                                      is_key_null);
            }
            return ptr;
        }
    };
};

template <typename K>
struct TopKCountCateWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUDAFTemplate<Impl>("top_n_key_count_cate_where")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, int64_t>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using AvgCateImpl = typename CountCateDef<K>::template Impl<V>;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix;

            suffix = ".i32_bound_opaque_dict_" + DataTypeTrait<K>::to_string() +
                     "_" + DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>, int32_t>()
                .init("top_n_key_count_cate_where_init" + suffix,
                      ContainerT::Init)
                .update("top_n_key_count_cate_where_update" + suffix,
                        UpdateI32Bound)
                .output("top_n_key_count_cate_where_output" + suffix, Output);

            suffix = ".i64_bound_opaque_dict_" + DataTypeTrait<K>::to_string() +
                     "_" + DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>, int64_t>()
                .init("top_n_key_count_cate_where_init" + suffix,
                      ContainerT::Init)
                .update("top_n_key_count_cate_where_update" + suffix, Update)
                .output("top_n_key_count_cate_where_output" + suffix, Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputV value,
                                  bool is_value_null, bool cond,
                                  bool is_cond_null, InputK key,
                                  bool is_key_null, int64_t bound) {
            if (cond && !is_cond_null) {
                AvgCateImpl::Update(ptr, value, is_value_null, key,
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
                ptr, true, output,
                [](const int64_t& count, char* buf, size_t size) {
                    return v1::format_string(count, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

void DefaultUDFLibrary::InitCountByCateUDAFs() {
    RegisterUDAFTemplate<CountCateDef>("count_cate")
        .doc(R"(
            Compute count of values grouped by category key and output string.
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
                SELECT count_cate(value, catagory) OVER w;
                -- output "x:3,y:2"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<CountCateWhereDef>("count_cate_where")
        .doc(R"(
            Compute count of values matching specified condition grouped by
    category key and output string. Each group is represented as 'K:V' and
    separated by comma in outputs and are sorted by key in ascend order.

            @param catagory  Specify catagory column to group by.
            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.

            Example:
            value|condition|catagory
            --|--|--
            0|true|x
            1|false|y
            2|false|x
            3|true|y
            4|true|x

            @code{.sql}
                SELECT count_cate_where(catagory, value, condition) OVER w;
                -- output "x:2,y:1"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<TopKCountCateWhereDef>("top_n_key_count_cate_where")
        .doc(R"(
            Compute count of values matching specified condition grouped by
    category key. Output string for top N keys in descend order. Each group is
    represented as 'K:V' and separated by comma.

            @param catagory  Specify catagory column to group by.
            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.
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
                SELECT top_n_key_count_cate_where(value, condition, catagory, 2)
    OVER w;
                -- output "z:2,y:2"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();
}

}  // namespace udf
}  // namespace fesql
