/**
 * Copyright (c) 2024 OpenMLDB Authors
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

#ifndef SRC_SDK_INTERNAL_SYSTEM_VARIABLE_H_
#define SRC_SDK_INTERNAL_SYSTEM_VARIABLE_H_

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"

namespace openmldb {
namespace sdk {
namespace internal {

using SystemVariables = absl::flat_hash_map<absl::string_view, absl::flat_hash_set<absl::string_view>>;

const SystemVariables& GetSystemVariablePresets();

// check if the stmt 'set {key} = {val}' has a valid semantic
// key and value for system variable is case insensetive
absl::Status CheckSystemVariableSet(absl::string_view key, absl::string_view val);

}  // namespace internal
}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_INTERNAL_SYSTEM_VARIABLE_H_
