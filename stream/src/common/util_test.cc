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

#include <unistd.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <limits>

#include <string>
#include <fstream>

#include "common/util.h"
#include "stream/simple_data_stream.h"

namespace streaming {
namespace interval_join {

class UtilTest : public ::testing::Test {};

TEST_F(UtilTest, GetLatencyTest) {
    int ele_num = FLAGS_latency_unit * 3;
    std::string path = "latency.csv";
    SimpleDataStream ds(ele_num);
    auto& eles = ds.elements();
    double res = 0;
    for (int i = 0 ; i < ele_num; i++) {
        eles[i]->set_read_time(0);
        eles[i]->set_output_time(i);
        res += i;
    }
    // check return value
    ASSERT_EQ(res / ele_num, GetLatency(&ds, path));

    // check writing to file
    std::ifstream file(path);
    ASSERT_TRUE(file.is_open());
    std::string row;
    ASSERT_TRUE(std::getline(file, row));
    EXPECT_EQ(row, "latency,count");

    ASSERT_TRUE(std::getline(file, row));
    EXPECT_EQ(row, "0,0");
    for (int i = 1; i <= ele_num / FLAGS_latency_unit; i++) {
        ASSERT_TRUE(std::getline(file, row));
        EXPECT_EQ(row, absl::StrCat(FLAGS_latency_unit * i, ",", FLAGS_latency_unit * i)) << "i = " << i;
    }
    ASSERT_TRUE(std::getline(file, row));
    EXPECT_EQ(row, absl::StrCat(std::numeric_limits<int64_t>::max(), ",", ele_num));
    unlink(path.c_str());
}

TEST_F(UtilTest, GetJoinCountsTest) {
    int ele_num = 100;
    double res = 0;
    SimpleDataStream ds(ele_num);
    auto& eles = ds.elements();
    for (int i = 0; i < ele_num; i++) {
        if (i % 2 == 0) {
            eles[i]->set_type(ElementType::kProbe);
            for (int j = 0; j < i; j++) {
                eles[i]->increment_join_count();
            }
            res += i;
        } else {
            eles[i]->set_type(ElementType::kBase);
        }
    }
    // only care about counts for probe elements
    ASSERT_EQ(GetJoinCounts(&ds), res / (ele_num / 2));
}

TEST_F(UtilTest, GetEffectivenessTest) {
    int ele_num = 100;
    SimpleDataStream ds(ele_num);
    auto& eles = ds.elements();
    double base_res = 0;
    double probe_res = 0;
    for (int i = 0; i < ele_num; i++) {
        eles[i]->set_effectiveness(i * 1.0 / ele_num);
        if (i % 2 == 0) {
            eles[i]->set_type(ElementType::kBase);
            base_res += eles[i]->effectiveness();
        } else {
            eles[i]->set_type(ElementType::kProbe);
            probe_res += eles[i]->effectiveness();
        }
    }
    base_res /= ele_num / 2;
    probe_res /= ele_num / 2 + ele_num % 2;
    ASSERT_EQ(GetBaseEffectiveness(&ds), base_res);
    ASSERT_EQ(GetProbeEffectiveness(&ds), probe_res);
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

