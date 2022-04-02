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

#include "statistics/query_response_time/query_response_time.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "glog/logging.h"

namespace openmldb {
namespace statistics {

TimeCollector::TimeCollector()
    : helper_(TIME_DISTRIBUTION_BASE, TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT,
              TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT) {
    Setup();
}

// TimeCollector(std::initializer_list<std::initializer_list<ResponseTimeRow>> data) {}

void TimeCollector::Collect(absl::Duration time) {
    for (size_t idx = 0; idx < helper_.BucketCount(); ++idx) {
        if (time <= helper_.UpperBound(idx).value()) {
            count_[idx].fetch_add(1, std::memory_order_relaxed);
            total_[idx].fetch_add(absl::ToInt64Microseconds(time), std::memory_order_relaxed);
            break;
        }
    }
}

std::vector<ResponseTimeRow> TimeCollector::Flush() {
    std::vector<ResponseTimeRow> rows;
    rows.reserve(BucketCount());
    for (size_t idx = 0; idx < helper_.BucketCount(); ++idx) {
        auto cnt = count_[idx].exchange(0, std::memory_order_relaxed);
        auto total = total_[idx].exchange(0, std::memory_order_relaxed);
        rows.emplace_back(helper_.UpperBound(idx).value(), cnt, absl::Microseconds(total));
    }
    return rows;
}

size_t TimeCollector::GetBucketIdx(absl::Duration time) {
    size_t idx = 0;
    while (idx < helper_.BucketCount()) {
        // do not check the status from UpperBound since we assume it is always ok
        if (time <= helper_.UpperBound(idx).value()) {
            return idx;
        }
        idx++;
    }
    // compiler don't known but this line should never reach
    return helper_.BucketCount() - 1;
}

absl::StatusOr<absl::Duration> TimeCollector::GetUpperBound(size_t idx) const { return helper_.UpperBound(idx); }

uint32_t TimeCollector::BucketCount() const { return helper_.BucketCount(); }

void TimeCollector::Setup() {
    for (size_t idx = 0; idx < helper_.BucketCount(); ++idx) {
        count_[idx] = 0;
        total_[idx] = 0;
    }
}

absl::StatusOr<ResponseTimeRow> TimeCollector::GetRow(size_t idx) const {
    auto bound = GetUpperBound(idx);
    if (!bound.ok()) {
        return bound.status();
    }
    return ResponseTimeRow{bound.value(), GetCount(idx), GetTotalUnited(idx)};
}

uint32_t TimeCollector::GetCount(size_t idx) const { return count_[idx].load(std::memory_order_relaxed); }

uint64_t TimeCollector::GetTotal(size_t idx) const { return total_[idx].load(std::memory_order_relaxed); }

absl::StatusOr<absl::Duration> TimeDistributionHelper::UpperBound(size_t idx) const {
    if (IndexOutOfBound(idx)) {
        return absl::OutOfRangeError(absl::StrCat("idx ", idx, " out of bucket count ", bucket_count_));
    }

    return upper_bounds_[idx];
}

void TimeDistributionHelper::Setup() {
    absl::Duration bound = absl::Microseconds(1);
    size_t idx = 0;
    while (idx++ < bucket_count_ - 1) {
        upper_bounds_.push_back(bound);
        bound *= base_;
    }
    upper_bounds_.push_back(absl::InfiniteDuration());
}

}  // namespace statistics
}  // namespace openmldb
