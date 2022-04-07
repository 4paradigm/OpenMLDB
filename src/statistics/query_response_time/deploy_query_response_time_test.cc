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

#include <functional>
#include <thread>

#include "absl/random/bit_gen_ref.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "statistics/query_response_time/query_response_time_util.h"

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
    return absl::StrCat(row.deploy_name_, ", ", absl::FormatDuration(row.time_), ", ", row.count_, ", ",
                        absl::FormatDuration(row.total_));
}

inline std::string Format(const std::vector<DeployResponseTimeRow>& rows) {
    std::stringstream ss;
    for (auto& row : rows) {
        ss << Format(row) << std::endl;
    }
    return ss.str();
}

// HACK: helper better not appear here
void ExpectRowsEq(const TimeDistributionHelper& helper, const std::string& name,
                  const std::vector<std::vector<absl::Duration>>& original,
                  const std::vector<DeployResponseTimeRow>& rs) {
    LOG(INFO) << "input time distribution:\n" << Format(original);
    LOG(INFO) << "collected time distribution:\n" << Format(rs);
    ASSERT_EQ(original.size(), rs.size());
    for (auto i = 0u; i < original.size(); ++i) {
        absl::Duration time = helper.UpperBound(i).value_or(absl::InfiniteDuration());
        auto cnt = original[i].size();
        auto total = std::accumulate(original[i].begin(), original[i].end(), absl::Seconds(0));

        EXPECT_EQ(DeployResponseTimeRow(name, time, cnt, total), rs.at(i));
    }
}

TEST_F(DeployTimeCollectorTest, SingleDeployThreadSafe) {
    DeployQueryTimeCollector col;

    std::string deploy_name = "deploy1";

    col.AddDeploy(deploy_name).IgnoreError();
    auto time_dis = GenTimeDistribution(gen_);

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
            col.Collect(deploy_name, dur).IgnoreError();
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
    ExpectRowsEq(helper_, deploy_name, time_dis, rows);
}

TEST_F(DeployTimeCollectorTest, MultiDeployThreadSafe) {
    DeployQueryTimeCollector col;

    std::string deploy1 = "dp1";
    std::string deploy2 = "dp2";
    std::string deploy3 = "dp3";

    col.AddDeploy(deploy1).IgnoreError();
    col.AddDeploy(deploy2).IgnoreError();
    col.AddDeploy(deploy3).IgnoreError();

    auto ts1 = GenTimeDistribution(gen_);
    auto ts2 = GenTimeDistribution(gen_);
    auto ts3 = GenTimeDistribution(gen_);

    std::vector<absl::Duration> series[6];
    int prefix = 0;
    for (auto& ts : {ts1, ts2, ts3}) {
        int cnt = 0;
        for (auto& row : ts) {
            for (auto d : row) {
                series[prefix * 2 + cnt % 2].push_back(d);
                cnt++;
            }
        }
        prefix++;
    }

    auto collect = [&col](std::string deploy_name, std::vector<absl::Duration> times) {
        for (auto time : times) {
            col.Collect(deploy_name, time).IgnoreError();
        }
    };

    // spawn 6 threads, every two collecting one time distribution
    std::vector<std::thread> threads;
    threads.emplace_back(collect, deploy1, series[0]);
    threads.emplace_back(collect, deploy1, series[1]);
    threads.emplace_back(collect, deploy2, series[2]);
    threads.emplace_back(collect, deploy2, series[3]);
    threads.emplace_back(collect, deploy3, series[4]);
    threads.emplace_back(collect, deploy3, series[5]);

    for (auto& t : threads) {
        t.join();
    }

    // check results
    auto rs1 = col.GetRows(deploy1);
    ASSERT_TRUE(rs1.ok());
    ExpectRowsEq(helper_, deploy1, ts1, rs1.value());

    auto rs2 = col.GetRows(deploy2);
    ASSERT_TRUE(rs2.ok());
    ExpectRowsEq(helper_, deploy2, ts2, rs2.value());

    auto rs3 = col.GetRows(deploy3);
    ASSERT_TRUE(rs3.ok());
    ExpectRowsEq(helper_, deploy3, ts3, rs3.value());
}

// test wirte & read concurrently won't miss up counters
TEST_F(DeployTimeCollectorTest, ReadWriteSafe) {
    DeployQueryTimeCollector col;
    std::string default_dp = "default_dp";
    col.AddDeploy(default_dp).IgnoreError();

    auto ts = GenTimeDistribution(gen_);

    auto add_deploy = [&col]() {
        // continuously add deploy so rehash happens on collectors_
        for (int i = 0; i < 100; i ++) {
            col.AddDeploy(absl::StrCat("dp", i)).IgnoreError();
            absl::SleepFor(absl::Milliseconds(1));
        }
    };

    auto collect = [&col, &ts, &default_dp]() {
        for (auto& row : ts) {
            for (auto t : row) {
                col.Collect(default_dp, t).IgnoreError();
                absl::SleepFor(absl::Milliseconds(1));
            }
        }
    };

    std::thread t1(collect);
    std::thread t2(add_deploy);

    t1.join();
    t2.join();

    auto rs1 = col.GetRows(default_dp);
    ASSERT_TRUE(rs1.ok());
    ExpectRowsEq(helper_, default_dp, ts, rs1.value());

    auto rs2 = col.Flush();

    auto rs3 = col.GetRows(default_dp);
    ASSERT_TRUE(rs3.ok());
    ExpectRowsEq(helper_, default_dp, InitialTimeDistribution(), rs3.value());
}

TEST_F(DeployTimeCollectorTest, FailConditions) {
    DeployQueryTimeCollector col;
    std::string dp1 = "dp1";
    std::string dp2 = "dp2";

    ASSERT_TRUE(col.AddDeploy(dp1).ok());

    ASSERT_TRUE(absl::IsAlreadyExists(col.AddDeploy(dp1)));

    ASSERT_TRUE(absl::IsNotFound(col.Collect(dp2, absl::Seconds(1))));
    ASSERT_TRUE(col.Collect(dp1, absl::Seconds(10)).ok());

    ASSERT_TRUE(col.DeleteDeploy(dp1).ok());
    ASSERT_TRUE(absl::IsNotFound(col.DeleteDeploy(dp2)));

    ASSERT_TRUE(absl::IsNotFound(col.GetRows(dp1).status()));
}

TEST_F(DeployTimeCollectorTest, FlushTest) {
    DeployQueryTimeCollector col;
    std::string dp1 = "dp1";
    std::string dp2 = "dp2";

    col.AddDeploy(dp1).IgnoreError();
    col.AddDeploy(dp2).IgnoreError();

    auto ts1 = GenTimeDistribution(gen_, 50);
    auto ts2 = GenTimeDistribution(gen_, 50);

    auto collect = [&col](const std::string& dp, const std::vector<std::vector<absl::Duration>>& ts) {
        for (auto& row : ts) {
            for (auto t : row) {
                col.Collect(dp, t).IgnoreError();
                absl::SleepFor(absl::Milliseconds(1));
            }
        }
    };

    auto rows = col.Flush();

    std::thread t1(std::bind(collect, dp1, ts1));
    std::thread t2(std::bind(collect, dp2, ts2));

    for (auto i = 0u; i < 100; i++) {
        auto rs = col.Flush();
        for (auto idx = 0u; idx < rs.size(); ++idx) {
            // assume the returned rs are always in same order
            rows[idx].count_ += rs[idx].count_;
            rows[idx].total_ += rs[idx].total_;
        }
        absl::SleepFor(absl::Milliseconds(1));
    }

    t1.join();
    t2.join();

    auto rs = col.Flush();
    for (auto idx = 0u; idx < rs.size(); ++idx) {
        // assume the returned rs are always in same order
        rows[idx].count_ += rs[idx].count_;
        rows[idx].total_ += rs[idx].total_;
    }

    std::vector<DeployResponseTimeRow> v1;
    v1.reserve(helper_.BucketCount());
    std::vector<DeployResponseTimeRow> v2;
    v2.reserve(helper_.BucketCount());
    for (auto& row : rows) {
        if (row.deploy_name_ == dp1) {
            v1.push_back(row);
        } else if (row.deploy_name_ == dp2) {
            v2.push_back(row);
        }
    }

    ExpectRowsEq(helper_, dp1, ts1, v1);
    ExpectRowsEq(helper_, dp2, ts2, v2);
}

}  // namespace statistics
}  // namespace openmldb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
