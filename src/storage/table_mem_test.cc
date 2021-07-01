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

#include "common/timer.h"
#include "gtest/gtest.h"
#include "storage/table.h"
#ifdef TCMALLOC_ENABLE
#include "gperftools/heap-checker.h"
#endif

namespace openmldb {
namespace storage {

class TableMemTest : public ::testing::Test {
 public:
    TableMemTest() {}
    ~TableMemTest() {}
};

TEST_F(TableMemTest, Memory) {
#ifdef TCMALLOC_ENABLE
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    HeapLeakChecker checker("test_mem");
    Table* table = new Table("tx_log", 1, 1, 8, 1);
    table->Init();
    for (uint32_t i = 0; i < 100; i++) {
        table->Put("test", now - i * 60 * 1000, "test", 4);
    }
    uint64_t cnt = table->SchedGc();
    ASSERT_EQ(98, cnt);
    Iterator* it = table->NewIterator("test");
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(now, it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
    ASSERT_EQ(true, checker.NoLeaks());
#endif
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
