/*
 * Copyright 2021 4Paradigm
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
#include "base/lru_cache.h"

#include "gtest/gtest.h"

namespace openmldb::base {
class LRUCacheTest : public ::testing::Test {};

TEST_F(LRUCacheTest, normalEvict) {
    lru_cache<int, int> cache(1);
    cache.upsert(0, 0);
    // evict 0,0
    cache.upsert(1, 1);
    ASSERT_FALSE(cache.contains(0));
}

TEST_F(LRUCacheTest, upsert) {
    lru_cache<int, int> cache(2);
    cache.upsert(0, 0);
    cache.upsert(1, 1);
    // update key 0
    cache.upsert(0, -1);
    // insert 2, key 1 will be evicted
    cache.upsert(2, 2);
    ASSERT_FALSE(cache.contains(1));
    ASSERT_EQ(cache.get(0), -1);
}

}  // namespace openmldb::base
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
