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
        if (time <= helper_.UpperBound(idx)) {
            count_[idx] += 1;
            total_[idx] += absl::ToInt64Microseconds(time);
            break;
        }
    }
}

void TimeCollector::Flush() { Setup(); }

size_t TimeCollector::GetBucketIdx(absl::Duration time) {
    size_t idx = 0;
    while (idx < helper_.BucketCount()) {
        if (time <= helper_.UpperBound(idx)) {
            return idx;
        }
        idx++;
    }
    return helper_.BucketCount();
}

void TimeCollector::Setup() {
    for (size_t idx = 0; idx < helper_.BucketCount(); ++idx) {
        count_[idx] = 0;
        total_[idx] = 0;
    }
}

absl::Duration TimeDistributionHelper::UpperBound(size_t idx) const {
    if (idx > bucket_count_) {
        DCHECK(false) << "index " << idx << " out of bound";
        return absl::Seconds(0);
    }

    return upper_bounds_[idx];
}

uint32_t TimeDistributionHelper::BucketCount() const { return bucket_count_; }

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
