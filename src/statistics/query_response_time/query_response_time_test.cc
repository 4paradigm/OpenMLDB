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

#include <numeric>
#include <thread>
#include <vector>

#include "absl/time/time.h"
#include "absl/random/random.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace statistics {

class TimeCollectorTest : public ::testing::Test {
 public:
    ~TimeCollectorTest() override {}

 protected:
    void SetUp() override { collector_ = new TimeCollector(); }

    void TearDown() override { delete collector_; }

    TimeCollector* collector_;
};

TEST_F(TimeCollectorTest, StateAtomicityInOneBucketTest) {
    collector_->Flush();

    // times located in the same bucket in TimeCollector: (1, 10]
    const size_t bucket_idx = collector_->GetBucketIdx(absl::Seconds(2));
    std::vector<absl::Duration const> times = {absl::Seconds(2), absl::Seconds(10), absl::Seconds(5), absl::Seconds(7),
                                               absl::Seconds(4)};

    auto collect = [this, &times](size_t idx) { collector_->Collect(times[idx]); };

    std::vector<std::thread> threads;
    threads.reserve(times.size());
    for (size_t idx = 0; idx < times.size(); ++idx) {
        threads.emplace_back(collect, idx);
    }

    for (auto& t : threads) {
        t.join();
    }

    uint32_t count = times.size();
    uint64_t total = absl::ToInt64Microseconds(std::accumulate(times.begin(), times.end(), absl::Seconds(0)));

    EXPECT_EQ(TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT + 1, bucket_idx);
    EXPECT_EQ(count, collector_->GetCount(bucket_idx));
    EXPECT_EQ(total, collector_->GetTotal(bucket_idx));
}

TEST_F(TimeCollectorTest, StateAtomicityBetweenBucketsTest) {
    collector_->Flush();

    absl::BitGen bitgen;

    auto random_num = [&bitgen](size_t start_exclusive, size_t end_inclusive) {
        return absl::Uniform(absl::IntervalOpenClosed, bitgen, start_exclusive, end_inclusive);
    };

    std::vector<std::vector<absl::Duration>> times = {
        // (0, 10 ^ -6]
        {absl::Microseconds(1)},
        // (10 ^ -6,, 10 ^ -5]
        {absl::Microseconds(random_num(1, 10)), absl::Microseconds(random_num(1, 10)),
         absl::Microseconds(random_num(1, 10)), absl::Microseconds(10)},
        // (10 ^ -5,, 10 ^ -4]
        {absl::Microseconds(random_num(10, 100)), absl::Microseconds(random_num(10, 100))},
        // (10 ^ -4,, 10 ^ -3]
        {absl::Microseconds(random_num(100, 1000)), absl::Microseconds(random_num(100, 1000)), absl::Milliseconds(1)},
        // (10 ^ -3,, 10 ^ -2]
        {},
        // (10 ^ -2,, 10 ^ -1]
        {absl::Milliseconds(random_num(10, 100)), absl::Milliseconds(100)},
        // (10 ^ -1,, 10 ^ 0]
        {absl::Seconds(1), absl::Milliseconds(789)},
        // (10 ^ 0,, 10 ^ 1]
        {absl::Seconds(random_num(1, 10)), absl::Seconds(random_num(1, 10)), absl::Seconds(10)},
        // (10 ^ 1,, 10 ^ 2]
        {absl::Seconds(random_num(10, 100)), absl::Seconds(random_num(10, 100)), absl::Seconds(random_num(10, 100))},
        // (10 ^ 2,, 10 ^ 3]
        {absl::Seconds(random_num(100, 1000)), absl::Seconds(799), absl::Seconds(1000)},
        // (10 ^ 3,, 10 ^ 4]
        {absl::Hours(1)},
        // (10 ^ 4,, 10 ^ 5]
        {},
        // (10 ^ 5,, 10 ^ 6]
        {},
        // (10 ^ 6,, max]
        {},
    };


    // pretty print the input
    std::stringstream ss;
    for (auto& vec : times) {
        ss << "[";
        for (auto dur : vec) {
            ss << dur << ", ";
        }
        ss << "]" << std::endl;
    }
    LOG(INFO) << "Input data: \n" << ss.str();

    auto collect = [this](std::vector<absl::Duration> durs) {
        for (auto dur : durs) {
            collector_->Collect(dur);
        }
    };

    std::vector<absl::Duration> vecs[4];
    size_t cnt = 0;

    for (auto& vec : times) {
        for (auto dur : vec) {
            vecs[cnt % 4].push_back(dur);
            cnt++;
        }
    }
    std::vector<std::thread> threads;
    threads.reserve(4);
    for (auto i = 0; i < 4; ++i) {
        threads.emplace_back(collect, vecs[i]);
    }

    for (auto& t : threads) {
        t.join();
    }

    // assert the results
    std::stringstream col_ss;
    for (auto idx = 0; idx < times.size(); ++idx) {
        uint32_t count = times[idx].size();
        uint64_t total =
            absl::ToInt64Microseconds(std::accumulate(times[idx].begin(), times[idx].end(), absl::Seconds(0)));
        col_ss << "[" << collector_->GetUpperBound(idx) << ", " << collector_->GetCount(idx) << ", "
               << absl::Microseconds(collector_->GetTotal(idx)) << "]" << std::endl;
        EXPECT_EQ(count, collector_->GetCount(idx));
        EXPECT_EQ(total, collector_->GetTotal(idx));
    }
    LOG(INFO) << "Time distribution in collector:\n" << col_ss.str();
}

}  // namespace statistics
}  // namespace openmldb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
