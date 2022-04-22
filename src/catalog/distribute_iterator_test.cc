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

#include "catalog/distribute_iterator.h"

#include <string>
#include <vector>
#include <utility>

#include "client/tablet_client.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "storage/mem_table.h"
#include "storage/table.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"
#include "rpc/rpc_client.h"

DECLARE_string(db_root_path);

namespace openmldb {
namespace catalog {

using ::openmldb::codec::SchemaCodec;

class DistributeIteratorTest : public ::testing::Test {};

::openmldb::api::TableMeta CreateTableMeta(uint32_t tid, uint32_t pid) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_db("db1");
    table_meta.set_name("table1");
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kAbsoluteTime, 0, 0);
    return table_meta;
}

std::shared_ptr<openmldb::storage::Table> CreateTable(uint32_t tid, uint32_t pid) {
    auto table = std::make_shared<openmldb::storage::MemTable>(CreateTableMeta(tid, pid));
    table->Init();
    return table;
}

void PutData(std::shared_ptr<openmldb::storage::Table> table) {
    codec::SDKCodec codec(*(table->GetTableMeta()));
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(table->GetId()) + std::to_string(i);
        for (int j = 0; j < 10; j++) {
            std::vector<std::string> row = {key , "mcc", std::to_string(now - j * (60 * 1000))};
            ::openmldb::api::PutRequest request;
            ::openmldb::api::Dimension* dim = request.add_dimensions();
            dim->set_idx(0);
            dim->set_key(key);
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            table->Put(0, value, request.dimensions());
        }
    }
}

void PutData(const ::openmldb::api::TableMeta& table_meta,
        std::shared_ptr<openmldb::client::TabletClient> client) {
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(table_meta.tid()) + std::to_string(i);
        for (int j = 0; j < 10; j++) {
            std::vector<std::string> row = {key , "mcc", std::to_string(now - j * (60 * 1000))};
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            std::vector<std::pair<std::string, uint32_t>> dimensions = {{key, 0}};
            client->Put(table_meta.tid(), table_meta.pid(), 0, value, dimensions);
        }
    }
}

TEST_F(DistributeIteratorTest, AllInMemory) {
    uint32_t tid = 1;
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(1, 1);
    auto table2 = CreateTable(1, 3);
    tables->emplace(1, table1);
    tables->emplace(3, table2);
    FullTableIterator it(tid, tables, {});
    it.SeekToFirst();
    ASSERT_FALSE(it.Valid());
    PutData((*tables)[1]);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 50);
    PutData((*tables)[3]);
    it.SeekToFirst();
    count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 100);
}

TEST_F(DistributeIteratorTest, Empty) {
    uint32_t tid = 2;
    FullTableIterator it(tid, {}, {});
    it.SeekToFirst();
    ASSERT_FALSE(it.Valid());
}

TEST_F(DistributeIteratorTest, AllInRemote) {
    uint32_t tid = 3;
    FLAGS_db_root_path = "/tmp/" + ::openmldb::test::GenRand();
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 4)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {4, client2}};
    FullTableIterator it(tid, {}, tablet_clients);
    it.SeekToFirst();
    ASSERT_FALSE(it.Valid());
    PutData(metas[0], client1);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 50);
    PutData(metas[1], client2);
    it.SeekToFirst();
    count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 100);
}

TEST_F(DistributeIteratorTest, Hybrid) {
    uint32_t tid = 3;
    FLAGS_db_root_path = "/tmp/" + ::openmldb::test::GenRand();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 3);
    auto table2 = CreateTable(tid, 7);
    tables->emplace(3, table1);
    tables->emplace(7, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 4)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {4, client2}};
    FullTableIterator it(tid, tables, tablet_clients);
    it.SeekToFirst();
    ASSERT_FALSE(it.Valid());
    PutData(metas[0], client1);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 50);
    PutData(metas[1], client2);
    PutData((*tables)[3]);
    PutData((*tables)[7]);
    it.SeekToFirst();
    count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 200);
}

}  // namespace catalog
}  // namespace openmldb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
