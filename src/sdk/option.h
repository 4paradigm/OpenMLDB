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

#ifndef SRC_SDK_OPTION_H_
#define SRC_SDK_OPTION_H_

#include <string>

namespace openmldb {
namespace sdk {

struct DeleteOption {
    std::optional<uint32_t> idx = std::nullopt;
    std::string key;
    std::string ts_name;
    std::optional<uint64_t> start_ts = std::nullopt;
    std::optional<uint64_t> end_ts = std::nullopt;
    bool enable_decode_value = true;
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_OPTION_H_
