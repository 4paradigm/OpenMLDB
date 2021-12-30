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

#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "storage/mem_table.h"

namespace openmldb {
namespace storage {

class MemTableIteratorTest : public ::testing::Test {};

TEST_F(MemTableIteratorTest, smoketest) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime);
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

TEST_F(MemTableIteratorTest, latest) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kLatestTime, 0, 3);
    MemTable table(table_meta);
    table.Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(i);
        for (int j = 0; j < 10; j++) {
            std::vector<std::string> row = {key , "mcc", std::to_string(now - j * (60 * 1000))};
            ::openmldb::api::PutRequest request;
            ::openmldb::api::Dimension* dim = request.add_dimensions();
            dim->set_idx(0);
            dim->set_key(key);
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            table.Put(0, value, request.dimensions());
        }
    }
    ::hybridse::vm::WindowIterator* it = table.NewWindowIterator(0);
    it->Seek("card2");
    ASSERT_TRUE(it != NULL);
    ASSERT_TRUE(it->Valid());
    ::hybridse::codec::Row row = it->GetKey();
    ASSERT_EQ(row.ToString(), "card2");
    std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
    wit->SeekToFirst();
    int cnt = 0;
    while (wit->Valid()) {
        cnt++;
        wit->Next();
    }
    ASSERT_EQ(3, cnt);
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
