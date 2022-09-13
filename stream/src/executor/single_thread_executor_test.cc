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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include <string>

#include "executor/single_thread_executor.h"
#include "stream/simple_data_stream.h"
#include "stream/memory_data_stream.h"

namespace streaming {
namespace interval_join {

class SingleThreadExecutorTest : public ::testing::Test {};

TEST_F(SingleThreadExecutorTest, SmokeTest) {
    int base_count = 100;
    int probe_count = 90;
    int window = 10;
    Element eles[base_count + probe_count];  // NOLINT
    MemoryDataStream input(StreamType::kMix);

    // for first probe_count of base and probe elements: probe is placed before base with same ts
    for (int i = 0; i < probe_count * 2; i++) {
        eles[i] = Element("key", "value", i / 2);
        if (i % 2 == 0) {
            eles[i].set_type(ElementType::kProbe);
        } else {
            eles[i].set_type(ElementType::kBase);
        }
        input.Put(&eles[i]);
    }
    // remaining base_count - probe_count of base elements
    for (int i = probe_count * 2; i < base_count + probe_count; i++) {
        eles[i] = Element("key", "value", i - probe_count);
        eles[i].set_type(ElementType::kBase);
        input.Put(&eles[i]);
    }

    MemoryDataStream output;
    JoinConfiguration configuration(window, 100, 100, OpType::Count);
    SingleThreadExecutor executor(&configuration, &input, &output);

    executor.Run();
    executor.Join();
    ASSERT_EQ(output.size(), base_count);
    // for the first 10 base elements: elements in time window (inclusive) == ts + 1
    for (int i = 0; i < window; i++) {
        ASSERT_EQ(output.Get()->value(), std::to_string(i + 1));
    }
    // for base elements in the middle: elements in time window (inclusive) == window + 1
    for (int i = window; i < probe_count; i++) {
        ASSERT_EQ(output.Get()->value(), "11");
    }
    // for the last 10 base elements:
    // elements in time window (inclusive) == probe_count - (ts - window)
    for (int i = probe_count; i < base_count; i++) {
        ASSERT_EQ(output.Get()->value(), std::to_string(probe_count + window - i));
    }
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
