/*
 * Copyright 2022 4Paradigm authors
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

#ifndef HYBRIDSE_SRC_UDF_DEFAULT_DEFS_CONTAINERS_H_
#define HYBRIDSE_SRC_UDF_DEFAULT_DEFS_CONTAINERS_H_

#include <string>
#include <utility>

#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {
namespace container {

// default update action: update ContainerT only cond is true
template <template <typename> typename CateImpl>
struct DefaultUpdateAction {
    template <typename V>
    struct Impl {
        using GroupContainerT = typename CateImpl<V>::ContainerT;
        using InputK = typename GroupContainerT::InputK;
        using InputV = typename GroupContainerT::InputV;
        using ContainerT = std::pair<GroupContainerT, int64_t>;

        static inline ContainerT* Update(ContainerT* ptr, InputV value, bool is_value_null, bool cond,
                                         bool is_cond_null, InputK key, bool is_key_null, int64_t bound) {
            if (ptr->second == 0) {
                ptr->second = bound;
            }
            if (cond && !is_cond_null) {
                CateImpl<V>::Update(&ptr->first, value, is_value_null, key, is_key_null);
            }
            return ptr;
        }
    };
};

// Top N Value wrapper over *CateWhere
// Base template class for 'top_n_value_*_cate_where' udafs
//
// CateImpl requirements
// - ContainerT
// - ContainerT* Update(ContainerT* ptr, InputV value, bool is_value_null, InputK key, bool is_key_null)
// - uint32_t FormatValueFn(const V& val, char* buf, size_t size) {
//
template <template <typename> typename CateImpl,
          template <typename > typename UpdateAction = DefaultUpdateAction<CateImpl>::template Impl>
struct TopNValueImpl {
    template <typename V>
    struct Impl {
        using GroupContainerT = typename CateImpl<V>::ContainerT;
        using K = typename GroupContainerT::Key;

        using InputK = typename GroupContainerT::InputK;
        using InputV = typename GroupContainerT::InputV;
        // (key-value group, bound)
        using ContainerT = std::pair<GroupContainerT, int64_t>;

        void operator()(UdafRegistryHelper& helper) {  // NOLINT
            std::string suffix;
            absl::string_view prefix = helper.name();

            suffix = absl::StrCat(".i32_bound_opaque_dict_", DataTypeTrait<K>::to_string(), "_",
                                  DataTypeTrait<V>::to_string());
            helper.templates<StringRef, Opaque<ContainerT>, Nullable<V>, Nullable<bool>, Nullable<K>, int32_t>()
                .init(absl::StrCat(prefix, "_init", suffix), Init)
                .update(absl::StrCat(prefix, "_update", suffix), UpdateI32Bound)
                .output(absl::StrCat(prefix, "_output", suffix), Output);

            suffix = absl::StrCat(".i64_bound_opaque_dict_", DataTypeTrait<K>::to_string(), "_",
                                  DataTypeTrait<V>::to_string());
            helper.templates<StringRef, Opaque<ContainerT>, Nullable<V>, Nullable<bool>, Nullable<K>, int64_t>()
                .init(absl::StrCat(prefix, "_init", suffix), Init)
                .update(absl::StrCat(prefix, "_update", suffix), Update)
                .output(absl::StrCat(prefix, "_output", suffix), Output);
        }

        static void Init(ContainerT* addr) {
            new (addr) ContainerT();
            addr->second = 0;
        }

        static ContainerT* Update(ContainerT* ptr, InputV value, bool is_value_null, bool cond, bool is_cond_null,
                                  InputK key, bool is_key_null, int64_t bound) {
            return UpdateAction<V>::Update(ptr, value, is_value_null, cond, is_cond_null, key, is_key_null, bound);
        }

        static ContainerT* UpdateI32Bound(ContainerT* ptr, InputV value, bool is_value_null, bool cond,
                                          bool is_cond_null, InputK key, bool is_key_null, int32_t bound) {
            return Update(ptr, value, is_value_null, cond, is_cond_null, key, is_key_null, bound);
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ptr->first.OutputTopNByValue(
                ptr->second,
                CateImpl<V>::FormatValueFn,
                output);
            ptr->~ContainerT();
        }
    };
};

}  // namespace container
}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_DEFAULT_DEFS_CONTAINERS_H_
