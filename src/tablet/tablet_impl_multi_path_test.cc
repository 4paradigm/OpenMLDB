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
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(recycle_bin_ssd_root_path);
DECLARE_string(recycle_bin_hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(gc_interval);
DECLARE_int32(gc_safe_offset);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_int32(binlog_delete_interval);

namespace openmldb {
namespace tablet {

using ::openmldb::codec::SchemaCodec;

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

using ::openmldb::api::TableStatus;

void CreateBaseTablet(::openmldb::tablet::TabletImpl& tablet,  // NOLINT
                      const ::openmldb::type::TTLType& ttl_type, uint64_t ttl, uint64_t start_ts, uint32_t tid,
                      uint32_t pid, openmldb::common::StorageMode storage_mode) {
    ::openmldb::api::CreateTableRequest crequest;
    ::openmldb::api::TableMeta* table_meta = crequest.mutable_table_meta();
    ::openmldb::common::TTLSt ttl_st;
    ttl_st.set_abs_ttl(ttl);
    ttl_st.set_lat_ttl(ttl);
    ttl_st.set_ttl_type(ttl_type);
    table_meta->set_name("table");
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_seg_cnt(8);
    table_meta->set_storage_mode(storage_mode);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta->set_key_entry_max_height(8);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "value", ::openmldb::type::kString);
    ::openmldb::common::ColumnKey* column_key = table_meta->add_column_key();
    column_key->set_index_name("card");
    column_key->set_ts_name("ts1");
    column_key->mutable_ttl()->CopyFrom(ttl_st);
    column_key = table_meta->add_column_key();
    column_key->set_index_name("mcc");
    column_key->set_ts_name("ts2");
    column_key->mutable_ttl()->CopyFrom(ttl_st);
    ::openmldb::api::CreateTableResponse cresponse;
    MockClosure closure;
    tablet.CreateTable(NULL, &crequest, &cresponse, &closure);
    ASSERT_EQ(0, cresponse.code());
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    for (int i = 0; i < 1000; i++) {
        ::openmldb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(pid);
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string k1 = "card" + std::to_string(i % 100);
        dim->set_key(k1);
        dim = request.add_dimensions();
        dim->set_idx(1);
        std::string k2 = "mcc" + std::to_string(i % 100);
        dim->set_key(k2);
        uint64_t time = start_ts + i;
        auto value = request.mutable_value();
        std::vector<std::string> row = {k1, k2, "11", std::to_string(time), std::to_string(time),
            "value" + std::to_string(i)};
        sdk_codec.EncodeRow(row, value);
        ::openmldb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        {
            ::openmldb::api::GetRequest request;
            request.set_tid(tid);
            request.set_pid(pid);
            request.set_key(k1);
            request.set_ts(time);
            request.set_idx_name("card");
            ::openmldb::api::GetResponse response;
            MockClosure closure;
            tablet.Get(NULL, &request, &response, &closure);
            ASSERT_EQ(0, response.code());
            std::vector<std::string> result;
            sdk_codec.DecodeRow(response.value(), &result);
            ASSERT_EQ(row.size(), result.size());
            for (size_t idx = 0; idx < row.size(); idx++) {
                ASSERT_EQ(row.at(idx), result.at(idx));
            }
        }

        {
            ::openmldb::api::GetRequest request;
            request.set_tid(tid);
            request.set_pid(pid);
            request.set_key(k2);
            request.set_ts(time);
            request.set_idx_name("mcc");
            ::openmldb::api::GetResponse response;
            MockClosure closure;
            tablet.Get(NULL, &request, &response, &closure);
            ASSERT_EQ(0, response.code());
            std::vector<std::string> result;
            sdk_codec.DecodeRow(response.value(), &result);
            ASSERT_EQ(row.size(), result.size());
            for (size_t idx = 0; idx < row.size(); idx++) {
                ASSERT_EQ(row.at(idx), result.at(idx));
            }
        }
    }
    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(tid);
    dr.set_pid(pid);
    ::openmldb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
}

void CreateTableWithoutDBRootPath(::openmldb::tablet::TabletImpl& tablet,  // NOLINT
                                  const ::openmldb::type::TTLType& ttl_type, uint64_t ttl, uint64_t start_ts,
                                  uint32_t tid, uint32_t pid, openmldb::common::StorageMode storage_mode) {
    ::openmldb::api::CreateTableRequest crequest;
    ::openmldb::api::TableMeta* table_meta = crequest.mutable_table_meta();
    table_meta->set_name("table");
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_storage_mode(storage_mode);
    table_meta->set_seg_cnt(8);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta->set_key_entry_max_height(8);
    auto column_desc = table_meta->add_column_desc();
    column_desc->set_name("idx0");
    column_desc->set_data_type(::openmldb::type::kString);
    auto column_desc1 = table_meta->add_column_desc();
    column_desc1->set_name("value");
    column_desc1->set_data_type(::openmldb::type::kString);
    auto column_key = table_meta->add_column_key();
    column_key->set_index_name("idx0");
    column_key->add_col_name("idx0");
    ::openmldb::common::TTLSt* ttl_st = column_key->mutable_ttl();
    ttl_st->set_abs_ttl(ttl);
    ttl_st->set_lat_ttl(ttl);
    ttl_st->set_ttl_type(ttl_type);
    ::openmldb::api::CreateTableResponse cresponse;
    MockClosure closure;
    tablet.CreateTable(NULL, &crequest, &cresponse, &closure);
    ASSERT_EQ(138, cresponse.code());
}

class TabletMultiPathTest : public ::testing::Test {
 public:
    TabletMultiPathTest() {}
    ~TabletMultiPathTest() {}
};

TEST_F(TabletMultiPathTest, MemoryCreateWithoutDBPath) {
    std::string old_db_path = FLAGS_db_root_path;
    FLAGS_db_root_path = "";
    ::openmldb::tablet::TabletImpl tablet_impl;
    tablet_impl.Init("");
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 100, 0,
                                 openmldb::common::kMemory);
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 101, 0,
                                 openmldb::common::kMemory);
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 102, 0,
                                 openmldb::common::kMemory);
    FLAGS_db_root_path = old_db_path;
}

TEST_F(TabletMultiPathTest, HDDCreateWithoutDBPath) {
    std::string old_hdd_path = FLAGS_hdd_root_path;
    FLAGS_hdd_root_path = "";
    ::openmldb::tablet::TabletImpl tablet_impl;
    tablet_impl.Init("");
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 100, 0,
                                 openmldb::common::kHDD);
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 101, 0,
                                 openmldb::common::kHDD);
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 102, 0,
                                 openmldb::common::kHDD);
    FLAGS_hdd_root_path = old_hdd_path;
}

TEST_F(TabletMultiPathTest, SSDCreateWithoutDBPath) {
    std::string old_ssd_path = FLAGS_ssd_root_path;
    FLAGS_ssd_root_path = "";
    ::openmldb::tablet::TabletImpl tablet_impl;
    tablet_impl.Init("");
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 100, 0,
                                 openmldb::common::kSSD);
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 101, 0,
                                 openmldb::common::kSSD);
    CreateTableWithoutDBRootPath(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, 102, 0,
                                 openmldb::common::kSSD);
    FLAGS_ssd_root_path = old_ssd_path;
}

TEST_F(TabletMultiPathTest, MemoryTestReadWriteAbsolute) {
    ::openmldb::tablet::TabletImpl tablet_impl;
    tablet_impl.Init("");
    for (uint32_t i = 0; i < 5; i++) {
        CreateBaseTablet(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, i + 1, i % 10,
                         openmldb::common::kMemory);
    }
}

TEST_F(TabletMultiPathTest, HDDTestReadWriteAbsolute) {
    ::openmldb::tablet::TabletImpl tablet_impl;
    tablet_impl.Init("");
    for (uint32_t i = 0; i < 5; i++) {
        CreateBaseTablet(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, i + 1, i % 10,
                         openmldb::common::kHDD);
    }
}

TEST_F(TabletMultiPathTest, SSDTestReadWriteAbsolute) {
    ::openmldb::tablet::TabletImpl tablet_impl;
    tablet_impl.Init("");
    for (uint32_t i = 0; i < 5; i++) {
        CreateBaseTablet(tablet_impl, ::openmldb::type::TTLType::kAbsoluteTime, 0, 1000, i + 1, i % 10,
                         openmldb::common::kSSD);
    }
}

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::test::InitRandomDiskFlags("tablet_impl_multi_path_test");
    return RUN_ALL_TESTS();
}
