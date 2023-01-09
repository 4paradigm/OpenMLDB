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
DECLARE_uint32(traverse_cnt_limit);
DECLARE_uint32(max_traverse_cnt);
DECLARE_uint32(max_traverse_pk_cnt);

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
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kAbsoluteTime, 0, 0);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts",
                                 ::openmldb::type::kAbsoluteTime, 0, 0);
    return table_meta;
}

std::shared_ptr<openmldb::storage::Table> CreateTable(uint32_t tid, uint32_t pid) {
    auto table = std::make_shared<openmldb::storage::MemTable>(CreateTableMeta(tid, pid));
    table->Init();
    return table;
}

void PutKey(const std::string& key, std::shared_ptr<openmldb::storage::Table> table,
        int cnt = 10, int same_ts_cnt = 1) {
    codec::SDKCodec codec(*(table->GetTableMeta()));
    for (int j = 0; j < cnt; j++) {
        uint64_t ts = 1 + j;
        for (int k = 0; k < same_ts_cnt; k++) {
            std::vector<std::string> row = {key , "mcc", std::to_string(ts)};
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

void PutKey(const std::string& key, const ::openmldb::api::TableMeta& table_meta,
        std::shared_ptr<openmldb::client::TabletClient> client, int cnt = 10, int same_ts_cnt = 1) {
    codec::SDKCodec codec(table_meta);
    for (int j = 0; j < cnt; j++) {
        uint64_t ts = 1 + j;
        for (int k = 0; k < same_ts_cnt; k++) {
            std::vector<std::string> row = {key , "mcc", std::to_string(ts)};
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            std::vector<std::pair<std::string, uint32_t>> dimensions = {{key, 0}};
            client->Put(table_meta.tid(), table_meta.pid(), 0, value, dimensions);
        }
    }
}

void PutData(std::shared_ptr<openmldb::storage::Table> table) {
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(table->GetPid()) + "|" + std::to_string(i);
        PutKey(key, table);
    }
}

void PutData(const ::openmldb::api::TableMeta& table_meta,
        std::shared_ptr<openmldb::client::TabletClient> client) {
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(table_meta.pid()) + "|" + std::to_string(i);
        PutKey(key, table_meta, client);
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
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
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
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
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
        // multiple calls to GetKey/GetValue will get the same and valid result
        auto buf = it.GetValue().buf();
        auto size = it.GetValue().size();
        codec::RowView row_view(*table1->GetSchema(), buf, size);
        std::string col1, col2;
        int64_t col3;
        row_view.GetStrValue(0, &col1);
        row_view.GetStrValue(1, &col2);
        row_view.GetInt64(2, &col3);
        DLOG(INFO) << "row " << count << ": " << col1 << "," << col2 << ", " << col3;
        ASSERT_EQ(col1.substr(0, 4), "card");
        ASSERT_EQ(col2, "mcc");
        ASSERT_TRUE(col3 > 0 && col3 <= 10);
        it.Next();
    }
    ASSERT_EQ(count, 200);
}

TEST_F(DistributeIteratorTest, FullTableTraverseLimit) {
    uint32_t old_limit = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 100;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
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
        // multiple calls to GetKey/GetValue will get the same and valid result
        auto buf = it.GetValue().buf();
        auto size = it.GetValue().size();
        codec::RowView row_view(*table1->GetSchema(), buf, size);
        std::string col1, col2;
        int64_t col3;
        row_view.GetStrValue(0, &col1);
        row_view.GetStrValue(1, &col2);
        row_view.GetInt64(2, &col3);
        DLOG(INFO) << "row " << count << ": " << col1 << "," << col2 << ", " << col3;
        ASSERT_EQ(col1.substr(0, 4), "card");
        ASSERT_EQ(col2, "mcc");
        ASSERT_TRUE(col3 > 0 && col3 <= 10);
        it.Next();
    }
    ASSERT_EQ(count, 100);
    FLAGS_max_traverse_cnt = old_limit;
}

TEST_F(DistributeIteratorTest, TraverseLimitSingle) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 3;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    brpc::Server tablet1;
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 0)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{0, client1}};
    for (int i = 0; i < 10; i++) {
        std::string key = "card" + std::to_string(i);
        PutKey(key, metas[0], tablet_clients[0]);
    }
    FullTableIterator it(tid, {}, tablet_clients);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 100);
    FLAGS_traverse_cnt_limit = old_limit;
}

TEST_F(DistributeIteratorTest, TraverseLimit) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 100;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 0);
    auto table2 = CreateTable(tid, 2);
    tables->emplace(0, table1);
    tables->emplace(2, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 3)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {3, client2}};
    std::map<uint32_t, uint32_t> cout_map = {{0, 0}, {1, 0}, {2, 0}, {3, 0}};
    for (int i = 0; i < 100; i++) {
        std::string key = "card" + std::to_string(i);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key)) % 4;
        cout_map[pid]++;
        if (pid % 2 == 0) {
            PutKey(key, (*tables)[pid]);
        } else {
            PutKey(key, metas[pid == 1 ? 0 : 1], tablet_clients[pid]);
        }
    }
    FullTableIterator it(tid, tables, tablet_clients);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 1000);
    FLAGS_traverse_cnt_limit = old_limit;
}

TEST_F(DistributeIteratorTest, WindowIterator) {
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 0);
    auto table2 = CreateTable(tid, 2);
    tables->emplace(0, table1);
    tables->emplace(2, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 3)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {3, client2}};
    for (int i = 0; i < 20; i++) {
        std::string key = "card" + std::to_string(i);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key)) % 4;
        if (pid % 2 == 0) {
            PutKey(key, (*tables)[pid]);
        } else {
            PutKey(key, metas[pid == 1 ? 0 : 1], tablet_clients[pid]);
        }
    }
    DistributeWindowIterator w_it(tid, 4, tables, 0, "card", tablet_clients);
    for (int i = 0; i < 20; i++) {
        std::string key = "card" + std::to_string(i);
        w_it.Seek(key);
        ASSERT_TRUE(w_it.Valid());
        ASSERT_EQ(w_it.GetKey().ToString(), key);
        auto it = w_it.GetValue();
        it->SeekToFirst();
        int count = 0;
        while (it->Valid()) {
            count++;
            it->Next();
        }
        ASSERT_EQ(count, 10);
    }
    int count = 0;
    w_it.SeekToFirst();
    while (w_it.Valid()) {
        count++;
        w_it.Next();
    }
    ASSERT_EQ(count, 20);
    w_it.Seek("card11");
    count = 0;
    while (w_it.Valid()) {
        count++;
        w_it.Next();
    }
    ASSERT_EQ(count, 17);
    w_it.Seek("card15");
    count = 0;
    while (w_it.Valid()) {
        count++;
        w_it.Next();
    }
    ASSERT_EQ(count, 10);
}

TEST_F(DistributeIteratorTest, RemoteIterator) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 7;
    uint32_t tid = 3;
    auto tables = std::make_shared<Tables>();
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    std::vector<std::string> endpoints = {"127.0.0.1:9230"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 0)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{0, client1}};
    codec::SDKCodec codec(metas[0]);
    int64_t now = 1999;
    std::string key = "card0";
    for (int j = 0; j < 1000; j++) {
        std::vector<std::string> row = {key , "mcc", std::to_string(now)};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        std::vector<std::pair<std::string, uint32_t>> dimensions = {{key, 0}};
        client1->Put(tid, 0, 0, value, dimensions);
    }
    for (int j = 1000; j < 2000; j++) {
        std::vector<std::string> row = {key , "mcc", std::to_string(now - j)};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        std::vector<std::pair<std::string, uint32_t>> dimensions = {{key, 0}};
        client1->Put(tid, 0, 0, value, dimensions);
    }
    DistributeWindowIterator w_it(tid, 1, tables, 0, "card", tablet_clients);
    w_it.Seek(key);
    ASSERT_TRUE(w_it.Valid());
    ASSERT_EQ(w_it.GetKey().ToString(), key);
    auto it = w_it.GetValue();
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;

        // multiple calls to GetKey/GetValue will get the same and valid result
        auto buf = it->GetValue().buf();
        auto size = it->GetValue().size();
        codec::RowView row_view(metas[0].column_desc(), buf, size);
        std::string col1, col2;
        int64_t col3;
        row_view.GetStrValue(0, &col1);
        row_view.GetStrValue(1, &col2);
        row_view.GetInt64(2, &col3);
        DLOG(INFO) << "row " << count << ": " << col1 << "," << col2 << ", " << col3;
        ASSERT_EQ(col1.substr(0, 4), "card");
        ASSERT_EQ(col2, "mcc");
        ASSERT_TRUE(col3 > now - 2000 && col3 <= now);

        it->Next();
    }
    ASSERT_EQ(count, 2000);

    DistributeWindowIterator w_it2(tid, 1, tables, 0, "card", tablet_clients);
    w_it2.Seek(key);
    ASSERT_TRUE(w_it2.Valid());
    ASSERT_EQ(w_it2.GetKey().ToString(), key);
    it = w_it2.GetValue();
    it->Seek(now - 1500);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(it->GetKey(), now - 1500);
    count = 0;
    while (it->Valid()) {
        ASSERT_EQ(now - 1500 - count, it->GetKey());
        count++;
        it->Next();
    }
    ASSERT_EQ(count, 500);
    FLAGS_traverse_cnt_limit = old_limit;
}

TEST_F(DistributeIteratorTest, RemoteIteratorSecondIndex) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 7;
    uint32_t tid = 3;
    auto tables = std::make_shared<Tables>();
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    std::vector<std::string> endpoints = {"127.0.0.1:9230"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 0)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{0, client1}};
    codec::SDKCodec codec(metas[0]);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    std::string key = "mcc0";
    for (int j = 0; j < 1000; j++) {
        std::vector<std::string> row = {"card0" , key, std::to_string(now - j)};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        std::vector<std::pair<std::string, uint32_t>> dimensions = {{"card0", 0}, {key, 1}};
        client1->Put(tid, 0, 0, value, dimensions);
    }
    key = "mcc1";
    for (int j = 1000; j < 2000; j++) {
        std::vector<std::string> row = {"card0" , key, std::to_string(now - j)};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        std::vector<std::pair<std::string, uint32_t>> dimensions = {{"card0", 0}, {key, 1}};
        client1->Put(tid, 0, 0, value, dimensions);
    }
    DistributeWindowIterator w_it(tid, 1, tables, 0, "mcc", tablet_clients);
    w_it.Seek(key);
    ASSERT_TRUE(w_it.Valid());
    ASSERT_EQ(w_it.GetKey().ToString(), key);
    auto it = w_it.GetValue();
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        // multiple calls to GetKey/GetValue will get the same and valid result
        auto ts = it->GetKey();
        ASSERT_EQ(now - 1000 - count, ts);
        auto buf = it->GetValue().buf();
        auto size = it->GetValue().size();
        codec::RowView row_view(metas[0].column_desc(), buf, size);
        std::string col1, col2;
        int64_t col3;
        row_view.GetStrValue(0, &col1);
        row_view.GetStrValue(1, &col2);
        row_view.GetInt64(2, &col3);
        DLOG(INFO) << "row " << count << ": " << col1 << "," << col2 << ", " << col3;
        ASSERT_EQ(col1, "card0");
        ASSERT_EQ(col2, key);
        ASSERT_EQ(col3, ts);

        count++;
        it->Next();
    }
    ASSERT_EQ(count, 1000);

    DistributeWindowIterator w_it2(tid, 1, tables, 0, "mcc", tablet_clients);
    key = "mcc0";
    w_it2.Seek(key);
    ASSERT_TRUE(w_it2.Valid());
    ASSERT_EQ(w_it2.GetKey().ToString(), key);
    it = w_it2.GetValue();
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(it->GetKey(), now);
    count = 0;
    while (it->Valid()) {
        // multiple calls to GetKey/GetValue will get the same and valid result
        auto ts = it->GetKey();
        ASSERT_EQ(now - count, ts);
        auto buf = it->GetValue().buf();
        auto size = it->GetValue().size();
        codec::RowView row_view(metas[0].column_desc(), buf, size);
        std::string col1, col2;
        int64_t col3;
        row_view.GetStrValue(0, &col1);
        row_view.GetStrValue(1, &col2);
        row_view.GetInt64(2, &col3);
        DLOG(INFO) << "row " << count << ": " << col1 << "," << col2 << ", " << col3;
        ASSERT_EQ(col1, "card0");
        ASSERT_EQ(col2, key);
        ASSERT_EQ(col3, it->GetKey());

        count++;
        it->Next();
    }
    ASSERT_EQ(count, 1000);

    DistributeWindowIterator w_it3(tid, 1, tables, 0, "mcc", tablet_clients);
    key = "mcc0";
    w_it3.SeekToFirst();
    ASSERT_TRUE(w_it3.Valid());
    ASSERT_EQ(w_it3.GetKey().ToString(), key);

    count = 0;
    while (w_it3.Valid()) {
        it = w_it3.GetValue();
        it->SeekToFirst();
        ASSERT_TRUE(it->Valid());
        while (it->Valid()) {
            auto ts = it->GetKey();
            ASSERT_TRUE(now - 2000 < ts && ts <= now);
            auto buf = it->GetValue().buf();
            auto size = it->GetValue().size();
            codec::RowView row_view(metas[0].column_desc(), buf, size);
            std::string col1, col2;
            int64_t col3;
            row_view.GetStrValue(0, &col1);
            row_view.GetStrValue(1, &col2);
            row_view.GetInt64(2, &col3);
            DLOG(INFO) << "row " << count << ": " << col1 << "," << col2 << ", " << col3;
            ASSERT_EQ(col1, "card0");
            ASSERT_EQ(col2.substr(0, 3), "mcc");
            ASSERT_EQ(col3, ts);

            count++;
            it->Next();
        }
        w_it3.Next();
    }
    ASSERT_EQ(count, 2000);
    FLAGS_traverse_cnt_limit = old_limit;
}

TEST_F(DistributeIteratorTest, MoreTsCnt) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 7;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 0);
    auto table2 = CreateTable(tid, 2);
    tables->emplace(0, table1);
    tables->emplace(2, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 3)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {3, client2}};
    std::map<uint32_t, uint32_t> cout_map = {{0, 0}, {1, 0}, {2, 0}, {3, 0}};
    for (int i = 0; i < 50; i++) {
        std::string key = "card" + std::to_string(i);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key)) % 4;
        cout_map[pid]++;
        if (pid % 2 == 0) {
            PutKey(key, (*tables)[pid], 100);
        } else {
            PutKey(key, metas[pid == 1 ? 0 : 1], tablet_clients[pid], 100);
        }
    }
    DistributeWindowIterator it(tid, 4, tables, 0, "card", tablet_clients);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        auto row_it = it.GetValue();
        int row_cnt = 0;
        while (row_it->Valid()) {
            row_cnt++;
            row_it->Next();
        }
        ASSERT_EQ(row_cnt, 100);
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 50);
    it.Seek("card26");
    count = 0;
    while (it.Valid()) {
        auto row_it = it.GetValue();
        int row_cnt = 0;
        while (row_it->Valid()) {
            row_cnt++;
            row_it->Next();
        }
        ASSERT_EQ(row_cnt, 100);
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 44);
    it.Seek("card44");
    count = 0;
    while (it.Valid()) {
        auto row_it = it.GetValue();
        int row_cnt = 0;
        while (row_it->Valid()) {
            row_cnt++;
            row_it->Next();
        }
        ASSERT_EQ(row_cnt, 100);
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 13);
    FLAGS_traverse_cnt_limit = old_limit;
}

TEST_F(DistributeIteratorTest, TraverseSameTs) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 7;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 0);
    auto table2 = CreateTable(tid, 2);
    tables->emplace(0, table1);
    tables->emplace(2, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 3)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {3, client2}};
    std::map<uint32_t, uint32_t> cout_map = {{0, 0}, {1, 0}, {2, 0}, {3, 0}};
    for (int i = 0; i < 20; i++) {
        std::string key = "card" + std::to_string(i);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key)) % 4;
        cout_map[pid]++;
        if (pid % 2 == 0) {
            PutKey(key, (*tables)[pid], 2, 50);
        } else {
            PutKey(key, metas[pid == 1 ? 0 : 1], tablet_clients[pid], 2, 50);
        }
    }
    FullTableIterator it(tid, tables, tablet_clients);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, 2000);
    FLAGS_traverse_cnt_limit = old_limit;
}

TEST_F(DistributeIteratorTest, WindowIteratorLimit) {
    uint32_t old_max_pk_cnt = FLAGS_max_traverse_pk_cnt;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 0);
    auto table2 = CreateTable(tid, 2);
    tables->emplace(0, table1);
    tables->emplace(2, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 3)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {3, client2}};
    for (int i = 0; i < 20; i++) {
        std::string key = "card" + std::to_string(i);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key)) % 4;
        if (pid % 2 == 0) {
            PutKey(key, (*tables)[pid]);
        } else {
            PutKey(key, metas[pid == 1 ? 0 : 1], tablet_clients[pid]);
        }
    }

    FLAGS_max_traverse_pk_cnt = 10;
    {
        DistributeWindowIterator w_it(tid, 4, tables, 0, "card", tablet_clients);
        for (int i = 0; i < 20; i++) {
            std::string key = "card" + std::to_string(i);
            w_it.Seek(key);
            ASSERT_TRUE(w_it.Valid());
            ASSERT_EQ(w_it.GetKey().ToString(), key);
            auto it = w_it.GetValue();
            it->SeekToFirst();
            int count = 0;
            while (it->Valid()) {
                count++;
                it->Next();
            }
            ASSERT_EQ(count, 10);
        }
        int count = 0;
        w_it.SeekToFirst();
        while (w_it.Valid()) {
            count++;
            w_it.Next();
        }
        ASSERT_EQ(count, FLAGS_max_traverse_pk_cnt);
        w_it.Seek("card11");
        count = 0;
        while (w_it.Valid()) {
            count++;
            w_it.Next();
        }
        ASSERT_EQ(count, FLAGS_max_traverse_pk_cnt);
        w_it.Seek("card15");
        count = 0;
        while (w_it.Valid()) {
            count++;
            w_it.Next();
        }
        ASSERT_EQ(count, FLAGS_max_traverse_pk_cnt);
    }

    FLAGS_max_traverse_pk_cnt = 20;
    {
        DistributeWindowIterator w_it(tid, 4, tables, 0, "card", tablet_clients);
        for (int i = 0; i < 20; i++) {
            std::string key = "card" + std::to_string(i);
            w_it.Seek(key);
            ASSERT_TRUE(w_it.Valid());
            ASSERT_EQ(w_it.GetKey().ToString(), key);
            auto it = w_it.GetValue();
            it->SeekToFirst();
            int count = 0;
            while (it->Valid()) {
                count++;
                it->Next();
            }
            ASSERT_EQ(count, 10);
        }
        int count = 0;
        w_it.SeekToFirst();
        while (w_it.Valid()) {
            count++;
            w_it.Next();
        }
        ASSERT_EQ(count, FLAGS_max_traverse_pk_cnt);
        w_it.Seek("card11");
        count = 0;
        while (w_it.Valid()) {
            count++;
            w_it.Next();
        }
        ASSERT_EQ(count, 17);
        w_it.Seek("card15");
        count = 0;
        while (w_it.Valid()) {
            count++;
            w_it.Next();
        }
        ASSERT_EQ(count, 10);
    }

    FLAGS_max_traverse_pk_cnt = old_max_pk_cnt;
}

TEST_F(DistributeIteratorTest, IteratorZero) {
    uint32_t old_limit = FLAGS_traverse_cnt_limit;
    FLAGS_traverse_cnt_limit = 7;
    uint32_t tid = 3;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    auto tables = std::make_shared<Tables>();
    auto table1 = CreateTable(tid, 0);
    auto table2 = CreateTable(tid, 2);
    tables->emplace(0, table1);
    tables->emplace(2, table2);
    std::vector<std::string> endpoints = {"127.0.0.1:9230", "127.0.0.1:9231"};
    brpc::Server tablet1;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[0], &tablet1));
    brpc::Server tablet2;
    ASSERT_TRUE(::openmldb::test::StartTablet(endpoints[1], &tablet2));
    auto client1 = std::make_shared<openmldb::client::TabletClient>(endpoints[0], endpoints[0]);
    ASSERT_EQ(client1->Init(), 0);
    auto client2 = std::make_shared<openmldb::client::TabletClient>(endpoints[1], endpoints[1]);
    ASSERT_EQ(client2->Init(), 0);
    std::vector<::openmldb::api::TableMeta> metas = {CreateTableMeta(tid, 1), CreateTableMeta(tid, 3)};
    ASSERT_TRUE(client1->CreateTable(metas[0]));
    ASSERT_TRUE(client2->CreateTable(metas[1]));
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients = {{1, client1}, {3, client2}};
    std::map<uint32_t, uint32_t> cout_map = {{0, 0}, {1, 0}, {2, 0}, {3, 0}};
    int expect = 0;
    int key_cnt = 50;
    for (int i = 0; i < key_cnt; i++) {
        std::string key = "card" + std::to_string(i);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key)) % 4;
        DLOG(INFO) << "key " << key << " pid " << pid;
        cout_map[pid]++;
        uint64_t now = ::baidu::common::timer::get_micros() / 1000;
        if (pid % 2 == 0) {
            codec::SDKCodec codec(*((*tables)[pid]->GetTableMeta()));
            for (int j = 0; j < 20; j++) {
                uint64_t ts = now - j * (60 * 1000);
                if (j % 2 == 0) {
                    ts = 0;
                }
                std::vector<std::string> row = {key , "mcc", std::to_string(ts)};
                ::openmldb::api::PutRequest request;
                ::openmldb::api::Dimension* dim = request.add_dimensions();
                dim->set_idx(0);
                dim->set_key(key);
                std::string value;
                ASSERT_EQ(0, codec.EncodeRow(row, &value));
                (*tables)[pid]->Put(0, value, request.dimensions());
                expect++;
            }
        } else {
            auto& table_meta = metas[pid == 1 ? 0 : 1];
            codec::SDKCodec codec(table_meta);
            for (int j = 0; j < 20; j++) {
                uint64_t ts = now - j * (60 * 1000);
                if (j % 2 == 0) {
                    ts = 0;
                }
                std::vector<std::string> row = {key , "mcc", std::to_string(ts)};
                std::string value;
                ASSERT_EQ(0, codec.EncodeRow(row, &value));
                std::vector<std::pair<std::string, uint32_t>> dimensions = {{key, 0}};
                tablet_clients[pid]->Put(table_meta.tid(), table_meta.pid(), 0, value, dimensions);
                expect++;
            }
        }
    }
    FullTableIterator it(tid, tables, tablet_clients);
    it.SeekToFirst();
    int count = 0;
    while (it.Valid()) {
        count++;
        it.Next();
    }
    ASSERT_EQ(count, expect);

    DistributeWindowIterator wit(tid, 4, tables, 0, "card", tablet_clients);
    wit.SeekToFirst();
    count = 0;
    while (wit.Valid()) {
        auto row_it = wit.GetValue();
        int row_cnt = 0;
        while (row_it->Valid()) {
            row_cnt++;
            row_it->Next();
        }
        ASSERT_EQ(row_cnt, 20);
        count++;
        wit.Next();
    }
    ASSERT_EQ(count, key_cnt);
    for (int i = 0; i < key_cnt; i++) {
        std::string key = "card" + std::to_string(i);
        wit.Seek(key);
        ASSERT_TRUE(wit.Valid());
        auto row_it = wit.GetValue();
        int row_cnt = 0;
        while (row_it->Valid()) {
            row_cnt++;
            row_it->Next();
        }
        ASSERT_EQ(row_cnt, 20);
    }
    FLAGS_traverse_cnt_limit = old_limit;
}

}  // namespace catalog
}  // namespace openmldb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
