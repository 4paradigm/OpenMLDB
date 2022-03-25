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

#ifndef SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_H_
#define SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_H_

#include <atomic>
#include <vector>

#include "absl/time/time.h"

namespace openmldb {
namespace statistics {

// A time distribution divide time interval from 0 to MAX into several parts.
// start exclusive and end inclusive, use second as unit
//
// the first part:
//     ( 0, TIME_DISTRIBUTION_BASE ^ (-1 * TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT) ]
//
// in middle:
//     ( TIME_DISTRIBUTION_BASE ^ n, TIME_DISTRIBUTION_BASE ^ (n+1) ]
// where n is integer start from (-1 * TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT) to
// (TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT - 2)
//
// and the last one:
//     (TIME_DISTRIBUTION_BASE ^ (TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT - 1), MAX ]

#define TIME_DISTRIBUTION_BASE 10

// total number of negative power point in time distribution
// e.g. 6 means 10 ^ -6 - 10 ^-1
#define TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT 6
// total number of non-negative power point in time distribution
// e.g. 7 means 10 ^ 0 - 10 ^ 6
#define TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT 7

// total number of time intervals by current configuration
#define TIME_DISTRIBUTION_BUCKET_COUNT \
    (TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT + TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT + 1)

#define MAX_STRING "MAX"

struct ResponseTimeRow {
    ResponseTimeRow(absl::Duration upper_bound, uint32_t cnt, absl::Duration total)
        : upper_bound_(upper_bound), count_(cnt), total_(total) {}

    absl::Duration upper_bound_;
    uint32_t count_;
    absl::Duration total_;
};

class TimeDistributionHelper {
 public:
    TimeDistributionHelper(uint32_t base, uint32_t negative_cnt, uint32_t non_negative_cnt)
        : base_(base),
          negative_count_(negative_cnt),
          non_negative_count_(non_negative_cnt),
          bucket_count_(negative_count_ + non_negative_count_ + 1) {
        upper_bounds_.reserve(bucket_count_);
        Setup();
    }

    ~TimeDistributionHelper() {}

    /// \brief return upper bound for `idx` th time interval, idx start from 0
    absl::Duration UpperBound(size_t idx) const;

    /// \brief return number of time intervals in the whole distribution
    uint32_t BucketCount() const;

 private:
    void Setup();

    const uint32_t base_;
    const uint32_t negative_count_;
    const uint32_t non_negative_count_;
    const uint32_t bucket_count_;
    // a list of upper bounds for time intervals, the last element is infinite
    std::vector<absl::Duration> upper_bounds_;
};

class TimeCollector {
 public:
    // construct from fresh data
    TimeCollector();

    // construct from existing data
    // TimeCollector(std::initializer_list<std::initializer_list<ResponseTimeRow>> data);

    ~TimeCollector() {}

    /// \brief collect time and save to states
    void Collect(absl::Duration time);

    /// \brief reset collector states and start a fresh one
    void Flush();

    /// \brief helper function to get the bucket index for the given time duration
    size_t GetBucketIdx(absl::Duration time);

    ResponseTimeRow GetResponseRow(size_t idx) const {
        return {GetUpperBound(idx), GetCount(idx), GetTotalUnited(idx)};
    }

    absl::Duration GetUpperBound(size_t idx) const { return helper_.UpperBound(idx); }

    uint32_t GetCount(size_t idx) const { return count_[idx]; }

    uint64_t GetTotal(size_t idx) const { return total_[idx]; }

    absl::Duration GetTotalUnited(size_t idx) const { return absl::Microseconds(GetTotal(idx)); }

 private:
    void Setup();

    TimeDistributionHelper helper_;
    std::atomic<uint32_t> count_[TIME_DISTRIBUTION_BUCKET_COUNT];
    std::atomic<uint64_t> total_[TIME_DISTRIBUTION_BUCKET_COUNT];
};

}  // namespace statistics
}  // namespace openmldb

#endif  // SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_H_
