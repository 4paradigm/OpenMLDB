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
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "glog/logging.h"

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

#define MAX_STRING "inf"

inline std::string GetDurationAsString(absl::Duration d) {
    if (d == absl::InfiniteDuration()) {
        return MAX_STRING;
    }
    return std::to_string(absl::ToInt64Microseconds(d));
}

inline absl::Duration ParseDurationFromRawInt(absl::string_view raw) {
    auto dur = absl::InfiniteDuration();
    if (raw != MAX_STRING) {
        try {
            dur = absl::Microseconds(std::stoll(raw.data()));
        } catch (std::exception const &e) {
            LOG(ERROR) << "[ERROR] parse number from string: " << raw << "\n" << e.what();
        }
    }
    return dur;
}

struct ResponseTimeRow {
    ResponseTimeRow(absl::Duration time, uint32_t cnt, absl::Duration total)
        : time_(time), count_(cnt), total_(total) {}
    ResponseTimeRow(const ResponseTimeRow& row)
        : time_(row.time_), count_(row.count_), total_(row.total_) {}
    virtual ~ResponseTimeRow() {}

    // get time_ as int64 micro seconds
    uint64_t GetTimeAsUs() const { return absl::ToInt64Microseconds(time_); }

    // get total_ as int64 micro seconds
    uint64_t GetTotalAsUs() const { return absl::ToInt64Microseconds(total_); }

    std::string GetTimeAsStr() const { return GetDurationAsString(time_); }

    std::string GetTotalAsStr() const { return GetDurationAsString(total_); }

    absl::Duration time_;
    uint32_t count_;
    absl::Duration total_;
};

inline bool operator==(const ResponseTimeRow& lhs, const ResponseTimeRow& rhs) {
    return lhs.time_ == rhs.time_ && lhs.total_ == rhs.total_ && lhs.count_ == rhs.count_;
}

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
    absl::StatusOr<absl::Duration> UpperBound(size_t idx) const;

    bool IndexOutOfBound(size_t idx) const { return idx >= BucketCount(); }

    /// \brief return number of time intervals in the whole distribution
    uint32_t BucketCount() const { return bucket_count_; }

 private:
    void Setup();

    const uint32_t base_;
    const uint32_t negative_count_;
    const uint32_t non_negative_count_;
    const uint32_t bucket_count_;
    // a list of upper bounds for time intervals, the last element is infinite
    std::vector<absl::Duration> upper_bounds_;
};

/// Thread safe wrapper for QUERY TIME DISTRIBUTION counters
/// all methods provided meant atomic
class TimeCollector {
 public:
    // construct from fresh data
    TimeCollector();

    // collector is not copyable
    TimeCollector(const TimeCollector& c) = delete;

    ~TimeCollector() {}

    /// \brief collect time and save to states
    void Collect(absl::Duration time);

    /// \brief reset collector states and start a fresh one
    /// \return old data
    std::vector<ResponseTimeRow> Flush();

    /// \brief helper function to get the bucket index for the given time duration
    size_t GetBucketIdx(absl::Duration time);

    absl::StatusOr<absl::Duration> GetUpperBound(size_t idx) const;

    uint32_t BucketCount() const;

    absl::StatusOr<ResponseTimeRow> GetRow(size_t idx) const;

 private:
    // unsafe methods

    uint32_t GetCount(size_t idx) const;

    uint64_t GetTotal(size_t idx) const;

    absl::Duration GetTotalUnited(size_t idx) const { return absl::Microseconds(GetTotal(idx)); }

    void Setup();

 private:
    TimeDistributionHelper helper_;
    std::atomic<uint32_t> count_[TIME_DISTRIBUTION_BUCKET_COUNT];
    std::atomic<uint64_t> total_[TIME_DISTRIBUTION_BUCKET_COUNT];
};

}  // namespace statistics
}  // namespace openmldb

#endif  // SRC_STATISTICS_QUERY_RESPONSE_TIME_QUERY_RESPONSE_TIME_H_
