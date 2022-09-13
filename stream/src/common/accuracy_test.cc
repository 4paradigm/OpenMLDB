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

#include "common/accuracy.h"

namespace streaming {
namespace interval_join {

class AccuracyTest : public ::testing::Test {};

TEST_F(AccuracyTest, CalculateAccuracyTest) {
    int ele_num = 100;
    // when key_num == 1: single key; when key_num > 1: multiple keys
    int key_nums[] = {1, 10};

    for (int key_num : key_nums) {
        std::vector<std::shared_ptr<Element>> correct_output;
        correct_output.resize(ele_num);
        for (int i = 0; i < ele_num; i++) {
            correct_output[i] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i), i);
        }


        // same results
        std::vector<std::shared_ptr<Element>> same_output;
        same_output.resize(ele_num);
        for (int i = 0; i < ele_num; i++) {
            same_output[i] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i), i);
        }
        ASSERT_EQ(CalculateAccuracy(correct_output, same_output), 1);


        // out-of-order results
        std::vector<std::shared_ptr<Element>> reverse_output;
        reverse_output.resize(ele_num);
        for (int i = 0; i < ele_num; i++) {
            int idx = ele_num - i - 1;
            reverse_output[i] = std::make_shared<Element>
                    (std::to_string(idx % key_num), std::to_string(idx), idx);
        }
        ASSERT_EQ(CalculateAccuracy(correct_output, reverse_output), 1);


        // duplicate results (later elements have correct values)
        std::vector<std::shared_ptr<Element>> correct_duplicate_output;
        correct_duplicate_output.resize(ele_num * 2);
        for (int i = 0; i < ele_num; i++) {
            // wrong values
            correct_duplicate_output[i * 2] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i + 1), i);
            // correct values
            correct_duplicate_output[i * 2 + 1] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i), i);
        }
        ASSERT_EQ(CalculateAccuracy(correct_output, correct_duplicate_output), 1);


        // duplicate results (previous elements have correct values)
        std::vector<std::shared_ptr<Element>> false_duplicate_output;
        false_duplicate_output.resize(ele_num * 2);
        for (int i = 0; i < ele_num; i++) {
            // correct values
            false_duplicate_output[i * 2] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i), i);
            // wrong values
            false_duplicate_output[i * 2 + 1] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i + 1), i);
        }
        ASSERT_EQ(CalculateAccuracy(correct_output, false_duplicate_output), 1);

        // missing results
        std::vector<std::shared_ptr<Element>> missing_output;
        missing_output.resize(ele_num / 2);
        for (int i = 0; i < ele_num / 2; i++) {
            missing_output[i] = std::make_shared<Element>
                    (std::to_string(i % key_num), std::to_string(i), i);
        }
        ASSERT_EQ(CalculateAccuracy(correct_output, missing_output), 0.5);


        // false results
        std::vector<std::shared_ptr<Element>> false_output;
        false_output.resize(ele_num);
        for (int i = 0; i < ele_num; i++) {
            if (i % 3 == 0) {
                // false key
                false_output[i] = std::make_shared<Element>
                        (std::to_string(i % key_num + 1), std::to_string(i), i);
            } else if (i % 3 == 1) {
                // false timestamp
                false_output[i] = std::make_shared<Element>
                        (std::to_string(i % key_num), std::to_string(i), i * (-1));
            } else {
                // false value
                false_output[i] = std::make_shared<Element>
                        (std::to_string(i % key_num), std::to_string(i + 1), i);
            }
        }
        ASSERT_EQ(CalculateAccuracy(correct_output, false_output), 0);
    }
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
