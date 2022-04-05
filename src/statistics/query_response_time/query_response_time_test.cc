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
#include <string>
#include <thread>
#include <vector>

#include "absl/random/random.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "statistics/query_response_time/query_response_time_util.h"

namespace openmldb {
namespace statistics {

class TimeCollectorTest : public ::testing::Test {
 public:
    ~TimeCollectorTest() override {}

 protected:
    TimeCollector collector_;
    absl::BitGen bitgen_;
    TimeDistributionHelper helper_ = {TIME_DISTRIBUTION_BASE, TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT,
                                      TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT};
};

inline std::string Format(const std::vector<ResponseTimeRow>& rows) {
    std::stringstream ss;
    for (auto& row : rows) {
        ss << row.time_ << ", " << row.count_ << ", " << row.total_ << std::endl;
    }
    return ss.str();
}

void ExpectRowsEq(const TimeDistributionHelper& helper, const std::vector<std::vector<absl::Duration>>& original,
                  const std::vector<ResponseTimeRow>& rs) {
    LOG(INFO) << "input time distribution:\n" << Format(original);
    LOG(INFO) << "collected time distribution:\n" << Format(rs);
    ASSERT_EQ(original.size(), rs.size());
    for (size_t i = 0; i < original.size(); ++i) {
        absl::Duration time = helper.UpperBound(i).value_or(absl::InfiniteDuration());
        auto cnt = original[i].size();
        auto total = std::accumulate(original[i].begin(), original[i].end(), absl::Seconds(0));

        EXPECT_EQ(ResponseTimeRow(time, cnt, total), rs.at(i));
    }
}

TEST_F(TimeCollectorTest, StateAtomicityInOneBucketTest) {
    collector_.Flush();

    // times located in the same bucket in TimeCollector: (1, 10]
    const size_t bucket_idx = collector_.GetBucketIdx(absl::Seconds(2));
    std::vector<absl::Duration> times = {absl::Seconds(2), absl::Seconds(10), absl::Seconds(5), absl::Seconds(7),
                                         absl::Seconds(4)};

    auto collect = [this, &times](size_t idx) { collector_.Collect(times[idx]); };

    std::vector<std::thread> threads;
    threads.reserve(times.size());
    for (size_t idx = 0; idx < times.size(); ++idx) {
        threads.emplace_back(collect, idx);
    }

    for (auto& t : threads) {
        t.join();
    }

    uint32_t count = times.size();
    absl::Duration total = std::accumulate(times.begin(), times.end(), absl::Seconds(0));

    EXPECT_EQ(TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT + 1, bucket_idx);
    auto row = collector_.GetRow(bucket_idx);
    ASSERT_TRUE(row.ok());
    EXPECT_EQ(count, row->count_);
    EXPECT_EQ(total, row->total_);
}

TEST_F(TimeCollectorTest, StateAtomicityBetweenBucketsTest) {
    collector_.Flush();

    std::vector<std::vector<absl::Duration>> times = GenTimeDistribution(bitgen_);

    LOG(INFO) << "Input data: \n" << Format(times);

    auto collect = [this](std::vector<absl::Duration> durs) {
        for (auto dur : durs) {
            collector_.Collect(dur);
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
    for (size_t i = 0; i < 4; ++i) {
        threads.emplace_back(collect, vecs[i]);
    }

    for (auto& t : threads) {
        t.join();
    }

    // assert the results
    std::stringstream col_ss;
    for (size_t idx = 0; idx < times.size(); ++idx) {
        uint32_t count = times[idx].size();
        absl::Duration total = std::accumulate(times[idx].begin(), times[idx].end(), absl::Seconds(0));
        auto row = collector_.GetRow(idx);
        ASSERT_TRUE(row.ok());
        col_ss << "[" << row->time_ << ", " << row.value().count_ << ", " << row.value().total_ << "]"
               << std::endl;
        EXPECT_EQ(count, row.value().count_);
        EXPECT_EQ(total, row.value().total_);
    }
    LOG(INFO) << "Time distribution in collector:\n" << col_ss.str();
}

// not losing counters when collect and flush in the same time
TEST_F(TimeCollectorTest, FlushTest) {
    collector_.Flush();

    auto ts = GenTimeDistribution(bitgen_, 100);

    auto collect = [this, &ts]() {
        for (auto& row : ts) {
            for (auto t : row) {
                collector_.Collect(t);
                absl::SleepFor(absl::Milliseconds(1));
            }
        }
    };

    std::thread t1(collect);

    auto rows = collector_.Flush();
    for (size_t i = 0; i < 100; ++i) {
        auto rs = collector_.Flush();
        for (size_t i = 0; i < rs.size(); ++i) {
            rows[i].count_ += rs[i].count_;
            rows[i].total_ += rs[i].total_;
            absl::SleepFor(absl::Milliseconds(1));
        }
    }

    t1.join();

    auto rs = collector_.Flush();
    for (size_t i = 0; i < rs.size(); ++i) {
        rows[i].count_ += rs[i].count_;
        rows[i].total_ += rs[i].total_;
    }

    ExpectRowsEq(helper_, ts, rows);
}

}  // namespace statistics
}  // namespace openmldb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
