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

// =========================================================== //
//      External Array Function Defines
// =========================================================== //

template <typename T>
struct ArrayContains {
    // udf registry types
    using Args = std::tuple<ArrayRef<T>, Nullable<T>>;

    using ParamType = typename DataTypeTrait<T>::CCallArgType;

    // type binding, udf registry type -> function param type
    // - bool/intxx/float/double -> bool/intxx/float/double
    // - Timestamp/Date/StringRef -> Timestamp*/Date*/StringRef*
    bool operator()(ArrayRef<T>* arr, ParamType v, bool is_null) {
        for (uint64_t i = 0; i < arr->size; ++i) {
            if constexpr (std::is_pointer_v<ParamType>) {
                // null or same value returns true
                if ((is_null && arr->nullables[i]) || (!arr->nullables[i] && *arr->raw[i] == *v)) {
                    return true;
                }
            } else {
                if ((is_null && arr->nullables[i]) || (!arr->nullables[i] && arr->raw[i] == v)) {
                    return true;
                }
            }
        }
        return false;
    }
};

template <typename T>
struct IsIn {
    // udf registry types
    using Args = std::tuple<Nullable<T>, ArrayRef<T>>;

    using ParamType = typename DataTypeTrait<T>::CCallArgType;

    bool operator()(ParamType v, bool is_null, ArrayRef<T>* arr) {
        for (uint64_t i = 0; i < arr->size; ++i) {
            if constexpr (std::is_pointer_v<ParamType>) {
                // null or same value returns true
                if ((is_null && arr->nullables[i]) || (!arr->nullables[i] && *arr->raw[i] == *v)) {
                    return true;
                }
            } else {
                if ((is_null && arr->nullables[i]) || (!arr->nullables[i] && arr->raw[i] == v)) {
                    return true;
                }
            }
        }
        return false;
    }
};

void SplitString(StringRef* str, StringRef* delimeter, ArrayRef<StringRef>* array) {
    std::vector<absl::string_view> stats = absl::StrSplit(absl::string_view(str->data_, str->size_),
                                                          absl::string_view(delimeter->data_, delimeter->size_));

    array->size = stats.size();
    if (!stats.empty()) {
        v1::AllocManagedArray(array, stats.size());

        for (size_t i = 0; i < stats.size(); ++i) {
            auto s = stats[i];
            if (s.empty()) {
                array->raw[i]->size_ = 0;
                array->nullables[i] = true;
            } else {
                char* buf = v1::AllocManagedStringBuf(s.size());
                memcpy(buf, s.data(), s.size());
                array->raw[i]->data_ = buf;
                array->raw[i]->size_ = s.size();
                array->nullables[i] = false;
            }
        }
    }
}

// =========================================================== //
//      UDF Register Entry
// =========================================================== //
void DefaultUdfLibrary::InitArrayUdfs() {
    RegisterExternalTemplate<ArrayContains>("array_contains")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp, Date, StringRef>()
        .doc(R"(
             @brief array_contains(array, value) - Returns true if the array contains the value.

             Example:

             @code{.sql}
                 select array_contains([2,2], 2) as c0;
                 -- output true
             @endcode

             @since 0.7.0
             )");

    RegisterExternalTemplate<IsIn>("isin")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp, Date, StringRef>()
        .doc(R"(
             @brief isin(value, array) - Returns true if the array contains the value.

             Example:

             @code{.sql}
                 select isin(2, [2,2]) as c0;
                 -- output true
             @endcode

             @since 0.9.1
             )");

    RegisterExternal("split_array")
        .returns<ArrayRef<StringRef>>()
        .return_by_arg(true)
        .args<StringRef, StringRef>(reinterpret_cast<void*>(SplitString))
        .doc(R"(
            @brief Split string to array of string by delimeter.

             @code{.sql}
                 select array_contains(split_array("2,1", ","), "1") as c0;
                 -- output true
             @endcode

            @since 0.7.0)");
}
}  // namespace udf
}  // namespace hybridse
