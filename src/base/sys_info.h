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

#ifndef SRC_BASE_SYS_INFO_H_
#define SRC_BASE_SYS_INFO_H_

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "base/status.h"

namespace openmldb::base {

constexpr const char* MEM_TOTAL = "MemTotal";
constexpr const char* MEM_AVAILABLE = "MemAvailable";

struct SysInfo {
    uint64_t mem_total = 0;       // unit is kB
    uint64_t mem_used = 0;        // unit is kB
    uint64_t mem_available = 0;   // unit is kB
};

base::Status GetSysMem(SysInfo* info) {
#if defined(__linux__)
    FILE *fd = fopen("/proc/meminfo", "r");
    if (fd == nullptr) {
        return {ReturnCode::kError, "fail to open meminfo file"};
    }
    char line[256];
    auto parse = [](absl::string_view str, absl::string_view key, uint64_t* val) -> base::Status {
        str.remove_prefix(key.size() + 1);
        str.remove_suffix(2);
        str = absl::StripAsciiWhitespace(str);
        if (!absl::SimpleAtoi(str, val)) {
            return {ReturnCode::kError, absl::StrCat("fail to parse ", key)};
        }
        return {};
    };
    int parse_cnt = 0;
    while (fgets(line, sizeof(line), fd)) {
        absl::string_view str_view(line);
        str_view = absl::StripAsciiWhitespace(str_view);
        if (absl::StartsWith(str_view, MEM_TOTAL)) {
            if (auto status = parse(str_view, MEM_TOTAL, &info->mem_total); !status.OK()) {
                return status;
            }
            parse_cnt++;
        } else if (absl::StartsWith(str_view, MEM_AVAILABLE)) {
            if (auto status = parse(str_view, MEM_AVAILABLE, &info->mem_available); !status.OK()) {
                return status;
            }
            parse_cnt++;
        }
        if (parse_cnt >= 2) {
            break;
        }
    }
    if (parse_cnt != 2) {
        return {ReturnCode::kError, "fail to parse meminfo"};
    }
    info->mem_used = info->mem_total - info->mem_available;
    fclose(fd);
#endif
    return {};
}

}  // namespace openmldb::base

#endif  // SRC_BASE_SYS_INFO_H_
