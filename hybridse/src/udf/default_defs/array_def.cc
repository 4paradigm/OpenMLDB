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

#include "udf/default_defs/containers.h"
#include "udf/default_udf_library.h"

namespace hybridse {
namespace udf {
void DefaultUdfLibrary::InitArrayUdfs() {
    RegisterExternalTemplate<container::ArrayContains>("array_contains")
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

    RegisterExternal("splita")
        .returns<ArrayRef<StringRef>>()
        .return_by_arg(true)
        .args<Nullable<StringRef>, StringRef>(
            reinterpret_cast<void*>(&FZStringOpsDef::SingleSplit))
        .doc(R"(
            @brief Split string to array of string by delimeter.

            @since 0.7.0)");
}
}  // namespace udf
}  // namespace hybridse
