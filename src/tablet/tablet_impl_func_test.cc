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

#include <fcntl.h>
#include <gflags/gflags.h>
#include <sys/stat.h>

#include "base/file_util.h"
#include "base/strings.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "storage/mem_table.h"
#include "storage/ticket.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(gc_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_int32(binlog_delete_interval);

namespace openmldb {
namespace tablet {

using ::openmldb::api::TableStatus;
using ::openmldb::codec::SchemaCodec;

::openmldb::api::TableMeta GetTableMeta() {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table");
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::openmldb::type::kString);
    return table_meta;
}

void CreateBaseTable(::openmldb::storage::Table*& table,  // NOLINT
                     const ::openmldb::type::TTLType& ttl_type, uint64_t ttl, uint64_t start_ts,
                     openmldb::common::StorageMode storage_mode) {
    auto table_meta = GetTableMeta();
    table_meta.set_storage_mode(storage_mode);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ttl_type, ttl, ttl);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ttl_type, ttl, ttl);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "card", "ts1", ttl_type, ttl, ttl);
    table = new ::openmldb::storage::MemTable(table_meta);
    table->Init();
    codec::SDKCodec codec(table_meta);
    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i % 100), "mcc" + std::to_string(i),
            "13", std::to_string(start_ts + i), std::to_string(start_ts + i), "value" + std::to_string(i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_TRUE(table->Put(0, value, request.dimensions()));
    }
    return;
}

class DiskTestEnvironment : public ::testing::Environment{
    virtual void SetUp() {
        ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
        ::openmldb::base::RemoveDirRecursive(FLAGS_ssd_root_path);
    }
    virtual void TearDown() {
        ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
        ::openmldb::base::RemoveDirRecursive(FLAGS_ssd_root_path);
    }
};

class TabletFuncTest : public ::testing::TestWithParam<::openmldb::common::StorageMode> {
 public:
    TabletFuncTest() {}
    ~TabletFuncTest() {}
};

void RunGetTimeIndexAssert(std::vector<QueryIt>* q_its, uint64_t base_ts, uint64_t expired_ts) {
    ::openmldb::tablet::TabletImpl tablet_impl;
    std::string value;
    uint64_t ts;
    int32_t code = 0;
    ::openmldb::api::TableMeta meta = GetTableMeta();
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema = q_its->begin()->table->GetAllVersionSchema();
    ::openmldb::storage::TTLSt ttl(expired_ts, 0, ::openmldb::storage::kAbsoluteTime);
    ttl.abs_ttl = expired_ts;
    // get the st kSubKeyGt
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(100 + base_ts);
        request.set_type(::openmldb::api::GetType::kSubKeyGt);
        request.set_et(100 + base_ts);
        request.set_et_type(::openmldb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 900 + base_ts);
        ASSERT_EQ(row[5], "value900");
    }

    // get the st kSubKeyLe
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(100 + base_ts);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        request.set_et(100 + base_ts);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 100 + base_ts);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value100");
    }

    // get the st 900kSubKeyLe
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(900 + base_ts);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        request.set_et(100 + base_ts);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 900 + base_ts);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value900");
    }

    // get the st 899kSubKeyLe
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(899 + base_ts);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        request.set_et(100 + base_ts);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 800 + base_ts);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value800");
    }

    // get the st 800 kSubKeyLe
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(899 + base_ts);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        request.set_et(800 + base_ts);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 800 + base_ts);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value800");
    }

    // get the st 800 kSubKeyLe
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(899 + base_ts);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        request.set_et(800 + base_ts);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGt);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(1, code);
    }
}

void RunGetLatestIndexAssert(std::vector<QueryIt>* q_its) {
    ::openmldb::tablet::TabletImpl tablet_impl;
    std::string value;
    uint64_t ts;
    int32_t code = 0;
    ::openmldb::api::TableMeta meta = GetTableMeta();
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema = q_its->begin()->table->GetAllVersionSchema();
    ::openmldb::storage::TTLSt ttl(0, 10, ::openmldb::storage::kLatestTime);
    // get the st kSubKeyGt
    {
        // for the legacy
        ::openmldb::api::GetRequest request;
        request.set_ts(1100);
        request.set_type(::openmldb::api::GetType::kSubKeyGt);
        request.set_et(1100);
        request.set_et_type(::openmldb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ((int64_t)ts, 1900);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value900");
    }

    // get the st == et
    {
        ::openmldb::api::GetRequest request;
        request.set_ts(1100);
        request.set_type(::openmldb::api::GetType::kSubKeyEq);
        request.set_et(1100);
        request.set_et_type(::openmldb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ((int64_t)ts, 1100);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value100");
    }

    // get the st < et
    {
        ::openmldb::api::GetRequest request;
        request.set_ts(1100);
        request.set_type(::openmldb::api::GetType::kSubKeyEq);
        request.set_et(1101);
        request.set_et_type(::openmldb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(-1, code);
    }

    // get the st > et
    {
        ::openmldb::api::GetRequest request;
        request.set_ts(1101);
        request.set_type(::openmldb::api::GetType::kSubKeyEq);
        request.set_et(1100);
        request.set_et_type(::openmldb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(-1, code);
    }

    // get the st > et
    {
        ::openmldb::api::GetRequest request;
        request.set_ts(1201);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        request.set_et(1200);
        request.set_et_type(::openmldb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ((signed)ts, 1200);
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value, &row);
        ASSERT_EQ(row[5], "value200");
    }
}

TEST_P(TabletFuncTest, GetLatestIndex_default_iterator) {
    openmldb::common::StorageMode storage_mode = GetParam();
    ::openmldb::storage::Table* table;
    CreateBaseTable(table, ::openmldb::type::TTLType::kLatestTime, 10, 1000, storage_mode);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::openmldb::storage::Ticket>();
    ::openmldb::storage::TableIterator* it = table->NewIterator("card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetLatestIndexAssert(&query_its);
}

TEST_P(TabletFuncTest, GetLatestIndex_ts0_iterator) {
    openmldb::common::StorageMode storage_mode = GetParam();
    ::openmldb::storage::Table* table = NULL;
    CreateBaseTable(table, ::openmldb::type::TTLType::kLatestTime, 10, 1000, storage_mode);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::openmldb::storage::Ticket>();
    ::openmldb::storage::TableIterator* it = table->NewIterator(0, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetLatestIndexAssert(&query_its);
}

TEST_P(TabletFuncTest, GetLatestIndex_ts1_iterator) {
    openmldb::common::StorageMode storage_mode = GetParam();
    ::openmldb::storage::Table* table = NULL;
    CreateBaseTable(table, ::openmldb::type::TTLType::kLatestTime, 10, 1000, storage_mode);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::openmldb::storage::Ticket>();
    ::openmldb::storage::TableIterator* it = table->NewIterator(1, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetLatestIndexAssert(&query_its);
}

TEST_P(TabletFuncTest, GetTimeIndex_default_iterator) {
    openmldb::common::StorageMode storage_mode = GetParam();
    uint64_t base_ts = ::baidu::common::timer::get_micros();
    ::openmldb::storage::Table* table = NULL;
    CreateBaseTable(table, ::openmldb::type::TTLType::kAbsoluteTime, 1000, base_ts, storage_mode);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::openmldb::storage::Ticket>();
    ::openmldb::storage::TableIterator* it = table->NewIterator("card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetTimeIndexAssert(&query_its, base_ts, base_ts - 100);
}

TEST_P(TabletFuncTest, GetTimeIndex_ts0_iterator) {
    openmldb::common::StorageMode storage_mode = GetParam();
    uint64_t base_ts = ::baidu::common::timer::get_micros();
    ::openmldb::storage::Table* table = NULL;
    CreateBaseTable(table, ::openmldb::type::TTLType::kAbsoluteTime, 1000, base_ts, storage_mode);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::openmldb::storage::Ticket>();
    ::openmldb::storage::TableIterator* it = table->NewIterator(0, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetTimeIndexAssert(&query_its, base_ts, base_ts - 100);
}

TEST_P(TabletFuncTest, GetTimeIndex_ts1_iterator) {
    openmldb::common::StorageMode storage_mode = GetParam();
    uint64_t base_ts = ::baidu::common::timer::get_micros();
    ::openmldb::storage::Table* table = NULL;
    CreateBaseTable(table, ::openmldb::type::TTLType::kAbsoluteTime, 1000, base_ts, storage_mode);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::openmldb::storage::Ticket>();
    ::openmldb::storage::TableIterator* it = table->NewIterator(1, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetTimeIndexAssert(&query_its, base_ts, base_ts - 100);
}

INSTANTIATE_TEST_CASE_P(TabletMemAndHDD, TabletFuncTest,
                        ::testing::Values(::openmldb::common::kMemory, ::openmldb::common::kHDD,
                                          ::openmldb::common::kSSD));

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    FLAGS_ssd_root_path = tmp_path.GetTempPath("ssd");
    FLAGS_hdd_root_path = tmp_path.GetTempPath("hdd");
    return RUN_ALL_TESTS();
}
