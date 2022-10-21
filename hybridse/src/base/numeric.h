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

#ifndef HYBRIDSE_SRC_BASE_NUMERIC_H_
#define HYBRIDSE_SRC_BASE_NUMERIC_H_

#include <cstdint>

namespace hybridse {
namespace base {

// get the inverse value of int64 includes INT64_MIN
//
// NOTE: this is just a wrapper that handles INT64_MIN, as the ad-hoc fix to window frame overflow
// It is not recommended to use it elsewhere. Instead, get rid of INT64_MIN
inline int64_t safe_inverse(int64_t v) {
    if (v == INT64_MIN) {
        return INT64_MAX;
    }

    return -1 * v;
}
}  // namespace base
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_BASE_NUMERIC_H_
