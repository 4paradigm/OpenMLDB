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

#include "base/concurrentlist.h"

#include <vector>
#include <thread>

#include "base/skiplist.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace base {

using ::openmldb::base::DefaultComparator;
DefaultComparator cmp;

TEST(ConcurrentListTest, ShouldPushNewValue) {
    ConcurrentList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    std::vector<std::thread> threads_vec(4);
    for (int i = 0; i < 4; ++i) {
        int start = 10000 * i, end = 10000 * (i + 1);
        threads_vec[i] = std::thread([&list = list, start, end]() {
            for (int i = start; i < end; ++i) list.Insert(i, 5);
        });
    }
    for (auto &t : threads_vec) {
        t.join();
    }
    std::vector<int> values;
    for (auto it = list.begin(); it != list.end(); ++it) {
        values.push_back(it.GetValue());
    }
    std::cout << values.size() << std::endl;
    ASSERT_EQ(values.size(), 40001);
}

TEST(ConcurrentListTest, GC) {
    ConcurrentList<int, uint64_t, DefaultComparator> list(cmp);
    for (int i = 0; i < 100000; i++) {
        list.Insert(i, i+5);
    }
    list.GC();
    for (auto it = list.begin(); it != list.end(); it++) {
        std::cout << it.GetKey() << " : " << it.GetValue() << std::endl;
    }
    ASSERT_EQ(list.GetSize(), 1001);
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
