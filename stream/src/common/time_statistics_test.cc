/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <vector>
#include <memory>

#include "common/time_statistics.h"

namespace streaming {
namespace interval_join {

class TimeStatisticsTest : public ::testing::Test {};

TEST_F(TimeStatisticsTest, MergeStatisticsTest) {
    int stat_num = 5;
    std::vector<TimeStatistics> stats;
    int64_t average = 0;
    for (int i = 0; i < stat_num; i++) {
        stats.emplace_back(TimeStatistics(i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7, i + 8));
        average += i;
    }

    std::unique_ptr<TimeStatistics> merged_stat = TimeStatistics::MergeStatistics(stats);
    average /= stat_num;

    ASSERT_EQ(merged_stat->prepare_time, average);
    ASSERT_EQ(merged_stat->lookup_time, average + 1);
    ASSERT_EQ(merged_stat->match_time, average + 2);
    ASSERT_EQ(merged_stat->output_time, average + 3);
    ASSERT_EQ(merged_stat->insert_time, average + 4);
    ASSERT_EQ(merged_stat->clean_insert_time, average + 5);
    ASSERT_EQ(merged_stat->cleaning_time, average + 6);
    ASSERT_EQ(merged_stat->base_output_time, average + 7);
    ASSERT_EQ(merged_stat->probe_output_time, average + 8);
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
