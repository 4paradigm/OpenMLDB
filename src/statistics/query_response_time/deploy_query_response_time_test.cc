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

#include "statistics/query_response_time/deploy_query_response_time.h"

#include <thread>

#include "absl/random/bit_gen_ref.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace statistics {

class DeployTimeCollectorTest : public ::testing::Test {
 public:
    ~DeployTimeCollectorTest() override {}

 protected:
    absl::BitGen gen_;
    TimeDistributionHelper helper_ = {TIME_DISTRIBUTION_BASE, TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT,
                                      TIME_DISTRIBUTION_NON_NEGATIVE_POWER_COUNT};
};

inline std::string Format(const DeployResponseTimeRow& row) {
    return absl::StrCat(row.deploy_name_, ", ", absl::FormatDuration(row.upper_bound_), ", ", row.count_, ", ",
                        absl::FormatDuration(row.total_));
}

inline std::string Format(const std::vector<DeployResponseTimeRow>& rows) {
    std::stringstream ss;
    for (auto& row : rows) {
        ss << Format(row) << std::endl;
    }
    return ss.str();
}

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
    for (auto i = 0; i < cnt; ++i) {
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

// generate random time distribution the same as TimeCollector do inside
// each row represent a time interval as TimeCollector
// each row's size is random from 0 to 10
// the first and last row is always empty
std::vector<std::vector<absl::Duration>> GenTimeDistribution(absl::BitGenRef gen) {
    std::vector<std::vector<absl::Duration>> time_dis(TIME_DISTRIBUTION_BUCKET_COUNT);
    absl::Duration start_us = absl::Microseconds(1);
    for (auto i = 1; i < TIME_DISTRIBUTION_BUCKET_COUNT - 1; ++i) {
        time_dis[i] = GenTimes(gen, absl::Uniform(gen, 0, 10), start_us, start_us * 10);
        start_us *= TIME_DISTRIBUTION_BASE;
    }
    return time_dis;
}

TEST_F(DeployTimeCollectorTest, DeployCollectorSingleDeployAndSingleInterval) {
    DeployQueryTimeCollector col;
    ASSERT_TRUE(col.GetRows().empty());

    std::string deploy_name = "deploy1";

    col.AddDeploy(deploy_name);
    auto rows = col.GetRows();

    ASSERT_EQ(1 * TIME_DISTRIBUTION_BUCKET_COUNT, rows.size());
    size_t cnt = 0;
    absl::Duration dur = absl::Microseconds(1);
    while (cnt < rows.size() - 1) {
        EXPECT_EQ(DeployResponseTimeRow(deploy_name, dur, 0u, absl::Seconds(0)), rows[cnt]);
        cnt++;
        dur *= TIME_DISTRIBUTION_BASE;
    }
    EXPECT_EQ(DeployResponseTimeRow(deploy_name, absl::InfiniteDuration(), 0u, absl::Seconds(0)), rows[cnt]);

    // create a random vector whose value is in (1, 10]
    std::vector<absl::Duration> times = GenTimes(gen_, 100, absl::Seconds(1), absl::Seconds(10));

    for (auto t : times) {
        col.Collect(deploy_name, t);
    }

    absl::Duration total = std::accumulate(times.begin(), times.end(), absl::Seconds(0));
    auto row = col.GetRow(deploy_name, TIME_DISTRIBUTION_NEGATIVE_POWER_COUNT + 1);
    EXPECT_TRUE(row.ok());
    EXPECT_EQ(DeployResponseTimeRow(deploy_name, absl::Seconds(10), times.size(), total), row.value());
}

TEST_F(DeployTimeCollectorTest, DeployCollectorSingleDeployThreadSafe) {
    DeployQueryTimeCollector col;

    std::string deploy_name = "deploy1";

    col.AddDeploy(deploy_name);
    auto time_dis = GenTimeDistribution(gen_);
    LOG(INFO) << "input time distribution:\n" << Format(time_dis);

    std::vector<absl::Duration> series[4];
    auto times_cnt = 0;
    for (auto& times : time_dis) {
        for (auto t : times) {
            series[times_cnt % 4].push_back(t);
            times_cnt++;
        }
    }

    auto collect = [&col, deploy_name](std::vector<absl::Duration> times) {
        for (auto dur : times) {
            col.Collect(deploy_name, dur);
        }
    };

    std::vector<std::thread> threads;
    for (auto& times : series) {
        threads.emplace_back(collect, times);
    }

    for (auto& t : threads) {
        t.join();
    }

    auto rows = col.GetRows();
    LOG(INFO) << "collected time distribution:\n" << Format(rows);
    for (auto i = 0; i < time_dis.size(); ++i) {
        absl::Duration time = helper_.UpperBound(i).value_or(absl::InfiniteDuration());
        auto cnt = time_dis[i].size();
        auto total = std::accumulate(time_dis[i].begin(), time_dis[i].end(), absl::Seconds(0));

        EXPECT_EQ(DeployResponseTimeRow(deploy_name, time, cnt, total), rows.at(i));
    }
}

}  // namespace statistics
}  // namespace openmldb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
