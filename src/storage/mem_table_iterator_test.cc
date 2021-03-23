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


#include "gtest/gtest.h"
#include "storage/mem_table.h"
#include "timer.h"  //NOLINT

namespace fedb {
namespace storage {

class MemTableIteratorTest : public ::testing::Test {};

TEST_F(MemTableIteratorTest, smoketest) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::api::TTLType::kAbsoluteTime);
    std::string key = "test";
    std::string value = "test";
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Init();
    table->Put(key, now, value.c_str(), value.size());
    ::hybridse::vm::WindowIterator* it = table->NewWindowIterator(0);
    it->SeekToFirst();
    ASSERT_TRUE(it != NULL);
    ASSERT_TRUE(it->Valid());
    ::hybridse::codec::Row row = it->GetKey();
    ASSERT_EQ(row.ToString(), key);
    std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
    wit->SeekToFirst();
    ASSERT_TRUE(wit->Valid());
    ::hybridse::codec::Row value2 = wit->GetValue();
    ASSERT_EQ(value2.ToString(), value);
    ASSERT_EQ(now, wit->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

}  // namespace storage
}  // namespace fedb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
