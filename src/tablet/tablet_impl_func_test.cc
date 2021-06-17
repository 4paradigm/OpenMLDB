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
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <sys/stat.h>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "base/kv_iterator.h"
#include "base/strings.h"
#include "codec/flat_array.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "storage/mem_table.h"
#include "storage/ticket.h"
#include "tablet/tablet_impl.h"

DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(gc_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_int32(binlog_delete_interval);

namespace fedb {
namespace tablet {

using ::fedb::api::TableStatus;
using ::fedb::codec::SchemaCodec;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

void CreateBaseTable(::fedb::storage::Table*& table,  // NOLINT
                     const ::fedb::type::TTLType& ttl_type, uint64_t ttl,
                     uint64_t start_ts) {
    ::fedb::api::TableMeta table_meta;
    table_meta.set_name("table");
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::fedb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);

    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::fedb::type::kBigInt);

    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ttl_type, ttl, ttl);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ttl_type, ttl, ttl);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "card", "ts1", ttl_type, ttl, ttl);

    table = new ::fedb::storage::MemTable(table_meta);
    table->Init();
    for (int i = 0; i < 1000; i++) {
        ::fedb::api::PutRequest request;
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card" + std::to_string(i % 100));
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc" + std::to_string(i));
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(start_ts + i);
        ts = request.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(start_ts + i);
        std::string value = "value" + std::to_string(i);
        ASSERT_TRUE(
            table->Put(request.dimensions(), request.ts_dimensions(), value));
    }
    return;
}

class TabletFuncTest : public ::testing::Test {
 public:
    TabletFuncTest() {}
    ~TabletFuncTest() {}
};

void RunGetTimeIndexAssert(std::vector<QueryIt>* q_its, uint64_t base_ts,
                           uint64_t expired_ts) {
    ::fedb::tablet::TabletImpl tablet_impl;
    std::string value;
    uint64_t ts;
    int32_t code = 0;
    ::fedb::api::TableMeta meta;
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema = q_its->begin()->table->GetAllVersionSchema();
    ::fedb::storage::TTLSt ttl(expired_ts, 0, ::fedb::storage::kAbsoluteTime);
    ttl.abs_ttl = expired_ts;
    // get the st kSubKeyGt
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(100 + base_ts);
        request.set_type(::fedb::api::GetType::kSubKeyGt);
        request.set_et(100 + base_ts);
        request.set_et_type(::fedb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 900 + base_ts);
        ASSERT_EQ(value, "value900");
    }

    // get the st kSubKeyLe
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(100 + base_ts);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        request.set_et(100 + base_ts);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 100 + base_ts);
        ASSERT_EQ(value, "value100");
    }

    // get the st 900kSubKeyLe
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(900 + base_ts);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        request.set_et(100 + base_ts);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 900 + base_ts);
        ASSERT_EQ(value, "value900");
    }

    // get the st 899kSubKeyLe
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(899 + base_ts);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        request.set_et(100 + base_ts);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 800 + base_ts);
        ASSERT_EQ(value, "value800");
    }

    // get the st 800 kSubKeyLe
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(899 + base_ts);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        request.set_et(800 + base_ts);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ(ts, 800 + base_ts);
        ASSERT_EQ(value, "value800");
    }

    // get the st 800 kSubKeyLe
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(899 + base_ts);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        request.set_et(800 + base_ts);
        request.set_et_type(::fedb::api::GetType::kSubKeyGt);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(1, code);
    }
}

void RunGetLatestIndexAssert(std::vector<QueryIt>* q_its) {
    ::fedb::tablet::TabletImpl tablet_impl;
    std::string value;
    uint64_t ts;
    int32_t code = 0;
    ::fedb::api::TableMeta meta;
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema = q_its->begin()->table->GetAllVersionSchema();
    ::fedb::storage::TTLSt ttl(0, 10, ::fedb::storage::kLatestTime);
    // get the st kSubKeyGt
    {
        // for the legacy
        ::fedb::api::GetRequest request;
        request.set_ts(1100);
        request.set_type(::fedb::api::GetType::kSubKeyGt);
        request.set_et(1100);
        request.set_et_type(::fedb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ((int64_t)ts, 1900);
        ASSERT_EQ(value, "value900");
    }

    // get the st == et
    {
        ::fedb::api::GetRequest request;
        request.set_ts(1100);
        request.set_type(::fedb::api::GetType::kSubKeyEq);
        request.set_et(1100);
        request.set_et_type(::fedb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ((int64_t)ts, 1100);
        ASSERT_EQ(value, "value100");
    }

    // get the st < et
    {
        ::fedb::api::GetRequest request;
        request.set_ts(1100);
        request.set_type(::fedb::api::GetType::kSubKeyEq);
        request.set_et(1101);
        request.set_et_type(::fedb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(-1, code);
    }

    // get the st > et
    {
        ::fedb::api::GetRequest request;
        request.set_ts(1101);
        request.set_type(::fedb::api::GetType::kSubKeyEq);
        request.set_et(1100);
        request.set_et_type(::fedb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(-1, code);
    }

    // get the st > et
    {
        ::fedb::api::GetRequest request;
        request.set_ts(1201);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        request.set_et(1200);
        request.set_et_type(::fedb::api::GetType::kSubKeyEq);
        CombineIterator combine_it(*q_its, request.ts(), request.type(), ttl);
        combine_it.SeekToFirst();
        code = tablet_impl.GetIndex(&request, meta, vers_schema, &combine_it, &value, &ts);
        ASSERT_EQ(0, code);
        ASSERT_EQ((signed)ts, 1200);
        ASSERT_EQ(value, "value200");
    }
}

TEST_F(TabletFuncTest, GetLatestIndex_default_iterator) {
    ::fedb::storage::Table* table;
    CreateBaseTable(table, ::fedb::type::TTLType::kLatestTime, 10, 1000);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::fedb::storage::Ticket>();
    ::fedb::storage::TableIterator* it =
        table->NewIterator("card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetLatestIndexAssert(&query_its);
}

TEST_F(TabletFuncTest, GetLatestIndex_ts0_iterator) {
    ::fedb::storage::Table* table = NULL;
    CreateBaseTable(table, ::fedb::type::TTLType::kLatestTime, 10, 1000);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::fedb::storage::Ticket>();
    ::fedb::storage::TableIterator* it =
        table->NewIterator(0, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetLatestIndexAssert(&query_its);
}

TEST_F(TabletFuncTest, GetLatestIndex_ts1_iterator) {
    ::fedb::storage::Table* table = NULL;
    CreateBaseTable(table, ::fedb::type::TTLType::kLatestTime, 10, 1000);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::fedb::storage::Ticket>();
    ::fedb::storage::TableIterator* it =
        table->NewIterator(1, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetLatestIndexAssert(&query_its);
}

TEST_F(TabletFuncTest, GetTimeIndex_default_iterator) {
    uint64_t base_ts = ::baidu::common::timer::get_micros();
    ::fedb::storage::Table* table = NULL;
    CreateBaseTable(table, ::fedb::type::TTLType::kAbsoluteTime, 1000, base_ts);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::fedb::storage::Ticket>();
    ::fedb::storage::TableIterator* it =
        table->NewIterator("card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetTimeIndexAssert(&query_its, base_ts, base_ts - 100);
}

TEST_F(TabletFuncTest, GetTimeIndex_ts0_iterator) {
    uint64_t base_ts = ::baidu::common::timer::get_micros();
    ::fedb::storage::Table* table = NULL;
    CreateBaseTable(table, ::fedb::type::TTLType::kAbsoluteTime, 1000, base_ts);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::fedb::storage::Ticket>();
    ::fedb::storage::TableIterator* it =
        table->NewIterator(0, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetTimeIndexAssert(&query_its, base_ts, base_ts - 100);
}

TEST_F(TabletFuncTest, GetTimeIndex_ts1_iterator) {
    uint64_t base_ts = ::baidu::common::timer::get_micros();
    ::fedb::storage::Table* table = NULL;
    CreateBaseTable(table, ::fedb::type::TTLType::kAbsoluteTime, 1000, base_ts);
    std::vector<QueryIt> query_its(1);
    query_its[0].ticket = std::make_shared<::fedb::storage::Ticket>();
    ::fedb::storage::TableIterator* it =
        table->NewIterator(1, "card0", *query_its[0].ticket);
    query_its[0].it.reset(it);
    query_its[0].table.reset(table);
    RunGetTimeIndexAssert(&query_its, base_ts, base_ts - 100);
}

}  // namespace tablet
}  // namespace fedb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    return RUN_ALL_TESTS();
}
