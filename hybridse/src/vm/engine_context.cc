/**
 * Copyright (c) 2024 OpenMLDB authors
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

#include "vm/engine_context.h"

#include "absl/container/flat_hash_map.h"

namespace hybridse {
namespace vm {

static absl::flat_hash_map<absl::string_view, EngineMode> createModeMap() {
    absl::flat_hash_map<absl::string_view, EngineMode> map = {
        {"online", kBatchMode},  // 'online' default to batch
        {"batch", kBatchMode},
        {"online_preview", kBatchMode},
        {"online_batch", kBatchMode},
        {"request", kRequestMode},
        {"online_request", kRequestMode},
        {"batch_request", kBatchRequestMode},
        {"batchrequest", kBatchRequestMode},
        {"online_batchrequest", kBatchRequestMode},
        {"offline", kOffline},
    };
    return map;
}

static const auto& getModeMap() {
    static const absl::flat_hash_map<absl::string_view, EngineMode> mode_map = *new auto(createModeMap());
    return mode_map;
}

std::string EngineModeName(EngineMode mode) {
    switch (mode) {
        case kBatchMode:
            return "kBatchMode";
        case kRequestMode:
            return "kRequestMode";
        case kBatchRequestMode:
            return "kBatchRequestMode";
        case kMockRequestMode:
            return "kMockRequestMode";
        case kOffline:
            return "kOffline";
        default:
            return "unknown";
    }
}

absl::StatusOr<EngineMode> UnparseEngineMode(absl::string_view str) {
    auto& m = getModeMap();
    auto it = m.find(str);
    if (it != m.end()) {
        return it->second;
    }

    return absl::NotFoundError(absl::StrCat("no matching mode for string '", str, "'"));
}

}  // namespace vm
}  // namespace hybridse
