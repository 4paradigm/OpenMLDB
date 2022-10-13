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

#ifndef SRC_BASE_TIME_H_
#define SRC_BASE_TIME_H_

#include <ctime>
#include <string>

namespace openmldb {
namespace base {

constexpr int32_t TZ = 8;
constexpr time_t TZ_OFFSET = TZ * 3600000;

std::string Convert2FormatTime(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &t);
    return std::string(buf);
}

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_TIME_H_
