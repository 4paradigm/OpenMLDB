/*
 * Copyright 2022 4Paradigm
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

#ifndef SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_UTIL_H_
#define SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_UTIL_H_

#include <vector>
#include <string>
#include <sstream>

#include "absl/random/bit_gen_ref.h"
#include "absl/random/random.h"
#include "absl/time/time.h"

#include "statistics/query_response_time/query_response_time.h"

namespace openmldb {
namespace statistics {

inline std::string Format(const std::vector<std::vector<absl::Duration>>& time_dis) {
    std::stringstream ss;
    for (auto& row : time_dis) {
        ss << "[";
        for (auto t : row) {
            ss << t << ", ";
        }
        ss << "]" << std::endl;
    }

    return ss.str();
}

// generate a list of random durations that in (start, end]
std::vector<absl::Duration> GenTimes(absl::BitGenRef gen, uint32_t cnt, uint64_t start_exclusive,
                                     uint64_t end_inclusive) {
    std::vector<absl::Duration> times;
    times.reserve(cnt);
    for (size_t i = 0; i < cnt; ++i) {
        times.push_back(
            absl::Microseconds(absl::Uniform(absl::IntervalOpenClosed, gen, start_exclusive, end_inclusive)));
    }
    return times;
}

std::vector<absl::Duration> GenTimes(absl::BitGenRef gen, uint32_t cnt, absl::Duration start_exclusive,
                                     absl::Duration end_inclusive) {
    uint64_t start = absl::ToInt64Microseconds(start_exclusive);
    uint64_t end = absl::ToInt64Microseconds(end_inclusive);
    return GenTimes(gen, cnt, start, end);
}

const std::vector<std::vector<absl::Duration>>& InitialTimeDistribution() {
    static const std::vector<std::vector<absl::Duration>> time_dis(TIME_DISTRIBUTION_BUCKET_COUNT);
    return time_dis;
}

// generate random time distribution the same as TimeCollector do inside
// each row represent a time interval as TimeCollector
// each row's size is random from 0 to max_ele_each_row
// for simpleness the first and last row is always empty
std::vector<std::vector<absl::Duration>> GenTimeDistribution(absl::BitGenRef gen, size_t max_ele_each_row = 10) {
    std::vector<std::vector<absl::Duration>> time_dis(TIME_DISTRIBUTION_BUCKET_COUNT);
    absl::Duration start_us = absl::Microseconds(1);
    for (size_t i = 1; i < TIME_DISTRIBUTION_BUCKET_COUNT - 1; ++i) {
        time_dis[i] = GenTimes(gen, absl::Uniform(gen, 0u, max_ele_each_row), start_us, start_us * 10);
        start_us *= TIME_DISTRIBUTION_BASE;
    }
    return time_dis;
}
}  // namespace statistics
}  // namespace openmldb

#endif  // SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_UTIL_H_
