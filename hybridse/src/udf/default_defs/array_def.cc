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
#include "codegen/struct_ir_builder.h"
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
        // NOTE: array_contains([null], null) returns null
        // this might not expected
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

void array_join(ArrayRef<StringRef>* arr, StringRef* del, bool del_null, StringRef* out) {
    int sz = 0;
    for (int i = 0; i < arr->size; ++i) {
        if (!arr->nullables[i]) {
            if (!del_null && i > 0) {
                sz += del->size_;
            }
            sz += arr->raw[i]->size_;
        }
    }

    auto buf = udf::v1::AllocManagedStringBuf(sz);
    memset(buf, 0, sz);

    int32_t idx = 0;
    for (int i = 0; i < arr->size; ++i) {
        if (!arr->nullables[i]) {
            if (!del_null && i > 0) {
                memcpy(buf + idx, del->data_, del->size_);
                idx += del->size_;
            }
            memcpy(buf + idx, arr->raw[i]->data_, arr->raw[i]->size_);
            idx += arr->raw[i]->size_;
        }
    }

    out->data_ = buf;
    out->size_ = sz;
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
    RegisterExternal("array_join")
        .args<ArrayRef<StringRef>, Nullable<StringRef>>(array_join)
        .doc(R"(
             @brief array_join(array, delimiter) - Concatenates the elements of the given array using the delimiter. Any null value is filtered.

             Example:

             @code{.sql}
                 select array_join(["1", "2"], "-");
                 -- output "1-2"
             @endcode
             @since 0.9.2)");

    RegisterCodeGenUdf("array_combine")
        .variadic_args<Nullable<StringRef>>(
            [](UdfResolveContext* ctx, const ExprAttrNode& delimit, const std::vector<ExprAttrNode>& arg_attrs,
               ExprAttrNode* out) -> base::Status {
                CHECK_TRUE(!arg_attrs.empty(), common::kCodegenError, "at least one array required by array_combine");
                for (auto & val : arg_attrs) {
                    CHECK_TRUE(val.type()->IsArray(), common::kCodegenError, "argument to array_combine must be array");
                }
                auto nm = ctx->node_manager();
                out->SetType(nm->MakeNode<node::TypeNode>(node::kArray, nm->MakeNode<node::TypeNode>(node::kVarchar)));
                out->SetNullable(false);
                return {};
            },
            [](codegen::CodeGenContext* ctx, codegen::NativeValue del, const std::vector<codegen::NativeValue>& args,
               const node::ExprAttrNode& return_info, codegen::NativeValue* out) -> base::Status {
                auto os = codegen::Combine(ctx, del, args);
                CHECK_TRUE(os.ok(), common::kCodegenError, os.status().ToString());
                *out = os.value();
                return {};
            })
        .doc(R"(
                @brief array_combine(delimiter, array1, array2, ...)

                return array of strings for input array1, array2, ... doing cartesian product. Each product is joined with
                {delimiter} as a string. Empty string used if {delimiter} is null.

                Example:

                @code{.sql}
                    select array_combine("-", ["1", "2"], ["3", "4"]);
                    -- output ["1-3", "1-4", "2-3", "2-4"]
                @endcode
                @since 0.9.2
             )");
}
}  // namespace udf
}  // namespace hybridse
