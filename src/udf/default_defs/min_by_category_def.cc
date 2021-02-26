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
struct MinCateDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUDAFTemplate<Impl>("min_cate")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, V>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" +
                                 DataTypeTrait<K>::to_string() + "_" +
                                 DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<K>>()
                .init("min_cate_init" + suffix, ContainerT::Init)
                .update("min_cate_update" + suffix, Update)
                .output("min_cate_output" + suffix, Output);
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
                if (single > ContainerT::to_stored_value(value)) {
                    single = ContainerT::to_stored_value(value);
                }
            }
            return ptr;
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ContainerT::OutputString(
                ptr, false, output, [](const V& min, char* buf, size_t size) {
                    return v1::format_string(min, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

template <typename K>
struct MinCateWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUDAFTemplate<Impl>("min_cate_where")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, V>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using CountCateImpl = typename MinCateDef<K>::template Impl<V>;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" +
                                 DataTypeTrait<K>::to_string() + "_" +
                                 DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>>()
                .init("min_cate_where_init" + suffix, ContainerT::Init)
                .update("min_cate_where_update" + suffix, Update)
                .output("min_cate_where_output" + suffix,
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
struct TopKMinCateWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUDAFTemplate<Impl>("top_n_key_min_cate_where")
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, V>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using AvgCateImpl = typename MinCateDef<K>::template Impl<V>;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix;

            suffix = ".i32_bound_opaque_dict_" + DataTypeTrait<K>::to_string() +
                     "_" + DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>, int32_t>()
                .init("top_n_key_min_cate_where_init" + suffix,
                      ContainerT::Init)
                .update("top_n_key_min_cate_where_update" + suffix,
                        UpdateI32Bound)
                .output("top_n_key_min_cate_where_output" + suffix, Output);

            suffix = ".i64_bound_opaque_dict_" + DataTypeTrait<K>::to_string() +
                     "_" + DataTypeTrait<V>::to_string();
            helper
                .templates<StringRef, Opaque<ContainerT>, Nullable<V>,
                           Nullable<bool>, Nullable<K>, int64_t>()
                .init("top_n_key_min_cate_where_init" + suffix,
                      ContainerT::Init)
                .update("top_n_key_min_cate_where_update" + suffix, Update)
                .output("top_n_key_min_cate_where_output" + suffix, Output);
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
                ptr, true, output, [](const V& min, char* buf, size_t size) {
                    return v1::format_string(min, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

void DefaultUDFLibrary::InitMinByCateUDAFs() {
    RegisterUDAFTemplate<MinCateDef>("min_cate")
        .doc(R"(
            Compute minimum of values grouped by category key and output string.
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
                SELECT min_cate(value, catagory) OVER w;
                -- output "x:0,y:1"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<MinCateWhereDef>("min_cate_where")
        .doc(R"(
            Compute minimum of values matching specified condition grouped by
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
            1|true|y
            4|true|x
            3|true|y

            @code{.sql}
                SELECT min_cate_where(catagory, value, condition) OVER w;
                -- output "x:0,y:1"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<TopKMinCateWhereDef>("top_n_key_min_cate_where")
        .doc(R"(
            Compute minimum of values matching specified condition grouped by
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
                SELECT top_n_key_min_cate_where(value, condition, catagory, 2)
    OVER w;
                -- output "z:5,y:1"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, Date, Timestamp, StringRef>();
}

}  // namespace udf
}  // namespace fesql
