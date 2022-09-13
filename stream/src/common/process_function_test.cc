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

#include "common/count_process_function.h"
#include "common/sum_process_function.h"
#include "common/distinct_count_process_function.h"

namespace streaming {
namespace interval_join {

class ProcessFunctionTest : public ::testing::Test {};

TEST_F(ProcessFunctionTest, CountProcessFunctionSmokeTest) {
    {
        int ele_num = 10;
        Element base_ele("key", "value", 0);
        Element eles[ele_num];  // NOLINT

        CountProcessFunction function(&base_ele);
        EXPECT_EQ("0", function.GetResult());
        EXPECT_EQ(base_ele.ts(), function.GetTs());

        for (int i = 0; i < ele_num; i++) {
            function.AddElement(&eles[i]);
            EXPECT_EQ(std::to_string(i + 1), function.GetResult());
        }
    }
}

TEST_F(ProcessFunctionTest, SumProcessFunctionSmokeTest) {
    {
        int ele_num = 10;
        Element base_ele("key", "value", 0);
        Element eles[ele_num];  // NOLINT

        SumProcessFunction function(&base_ele);
        EXPECT_EQ(0, std::stod(function.GetResult()));
        EXPECT_EQ(base_ele.ts(), function.GetTs());

        for (int i = 0; i < ele_num; i++) {
            eles[i] = Element("key", std::to_string(i), i);
            function.AddElement(&eles[i]);
            EXPECT_EQ((i * (i + 1)) / 2, std::stod(function.GetResult()));
        }

        // the value input may not be integers --> still acceptable
        Element double_ele("key", "0.5", 0);
        function.AddElement(&double_ele);
        EXPECT_EQ(((ele_num - 1) * ele_num) / 2 + 0.5, std::stod(function.GetResult()));

        // the value input cannot be converted to number --> output does not change
        function.AddElement(&base_ele);
        EXPECT_EQ(((ele_num - 1) * ele_num) / 2 + 0.5, std::stod(function.GetResult()));
    }
}

TEST_F(ProcessFunctionTest, SumProcessFunctionAtomicSmokeTest) {
    {
        int ele_num = 10;
        Element base_ele("key", "value", 0, ElementType::kProbe);
        Element eles[ele_num];  // NOLINT

        SumProcessFunction function(&base_ele);
        EXPECT_EQ(0, std::stod(function.GetResultAtomic()));
        EXPECT_EQ(base_ele.ts(), function.GetTs());

        for (int i = 0; i < ele_num; i++) {
            eles[i] = Element("key", std::to_string(i), i, ElementType::kProbe);
            function.AddElementAtomic(&eles[i]);
            EXPECT_EQ((i * (i + 1)) / 2, std::stod(function.GetResultAtomic()));
        }

        // the value input may not be integers --> still acceptable
        Element double_ele("key", "0.5", 0, ElementType::kProbe);
        function.AddElementAtomic(&double_ele);
        EXPECT_EQ(((ele_num - 1) * ele_num) / 2 + 0.5, std::stod(function.GetResultAtomic()));

        // the value input cannot be converted to number --> output does not change
        function.AddElementAtomic(&base_ele);
        EXPECT_EQ(((ele_num - 1) * ele_num) / 2 + 0.5, std::stod(function.GetResultAtomic()));
    }
}

TEST_F(ProcessFunctionTest, DistinctCountCountProcessFunctionSmokeTest) {
    {
        int ele_num = 10;
        Element base_ele("key", "value", 0);
        Element eles[ele_num];  // NOLINT

        DistinctCountProcessFunction function(&base_ele);
        EXPECT_EQ("0", function.GetResult());
        EXPECT_EQ(base_ele.ts(), function.GetTs());

        // every two elements have the same values
        for (int i = 0; i < ele_num; i++) {
            eles[i] = Element("key", std::to_string(i / 2), i);
            function.AddElement(&eles[i]);
            EXPECT_EQ(std::to_string(i / 2 + 1), function.GetResult());
        }
    }
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
