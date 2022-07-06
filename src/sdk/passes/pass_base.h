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

#include "base/fe_object.h"
#include "base/fe_status.h"

#ifndef HYBRIDSE_INCLUDE_PASSES_PASS_BASE_H_
#define HYBRIDSE_INCLUDE_PASSES_PASS_BASE_H_

namespace hybridse {
namespace passes {

template <typename T, typename CTX>
class PassBase : public base::FeBaseObject {
 public:
    PassBase() = default;
    virtual ~PassBase() {}

    virtual base::Status Apply(CTX* ctx, T* input, T** out) = 0;
};

}  // namespace passes
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_PASSES_PASS_BASE_H_
