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

#include "sdk/internal/system_variable.h"

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"

namespace openmldb {
namespace sdk {
namespace internal {

static SystemVariables CreateSystemVariablePresets() {
    SystemVariables map = {
        {"execute_mode", {"online", "request", "offline"}},
        // TODO(someone): add all
    };
    return map;
}

const SystemVariables& GetSystemVariablePresets() {
    static const SystemVariables& map = *new auto(CreateSystemVariablePresets());
    return map;
}
absl::Status CheckSystemVariableSet(absl::string_view key, absl::string_view val) {
    auto& presets = GetSystemVariablePresets();
    auto it = presets.find(absl::AsciiStrToLower(key));
    if (it == presets.end()) {
        return absl::InvalidArgumentError(absl::Substitute("key '$0' not found as a system variable", key));
    }

    if (it->second.find(absl::AsciiStrToLower(val)) == it->second.end()) {
        return absl::InvalidArgumentError(
            absl::Substitute("invalid value for system variable '$0', expect one of [$1], but got $2", key,
                             absl::StrJoin(it->second, ", "), val));
    }

    return absl::OkStatus();
}
}  // namespace internal
}  // namespace sdk
}  // namespace openmldb
