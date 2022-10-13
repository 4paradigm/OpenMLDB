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

#include "tablet/tablet_impl.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <sys/stat.h>

#include <algorithm>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "base/kv_iterator.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "codec/codec.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "test/util.h"

DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(gc_interval);
DECLARE_int32(disk_gc_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_int32(binlog_delete_interval);
DECLARE_uint32(max_traverse_cnt);
DECLARE_bool(recycle_bin_enabled);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(recycle_bin_ssd_root_path);
DECLARE_string(recycle_bin_hdd_root_path);
DECLARE_string(endpoint);
DECLARE_uint32(recycle_ttl);

namespace openmldb {
namespace tablet {

using ::openmldb::api::TableStatus;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;
using ::openmldb::codec::SchemaCodec;
using ::openmldb::type::TTLType::kAbsoluteTime;
using ::openmldb::type::TTLType::kLatestTime;
using ::openmldb::type::TTLType::kAbsAndLat;
using ::openmldb::type::TTLType::kAbsOrLat;

uint32_t counter = 10;
static const ::openmldb::base::DefaultComparator scmp;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

void RemoveData(const std::string& path) {
    ::openmldb::base::RemoveDir(path + "/data");
    ::openmldb::base::RemoveDir(path);
    ::openmldb::base::RemoveDir(FLAGS_hdd_root_path);
    ::openmldb::base::RemoveDir(FLAGS_ssd_root_path);
}

class DiskTestEnvironment : public ::testing::Environment{
    virtual void SetUp() {
        ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
    }
    virtual void TearDown() {
        ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
    }
};

class TabletImplTest : public ::testing::TestWithParam<::openmldb::common::StorageMode> {
 public:
    TabletImplTest() {}
    ~TabletImplTest() {}
};

bool RollWLogFile(::openmldb::storage::WriteHandle** wh, ::openmldb::storage::LogParts* logs,
                  const std::string& log_path, uint32_t& binlog_index, uint64_t offset,  // NOLINT
                  bool append_end = true) {
    if (*wh != NULL) {
        if (append_end) {
            (*wh)->EndLog();
        }
        delete *wh;
        *wh = NULL;
    }
    std::string name = ::openmldb::base::FormatToString(binlog_index, 8) + ".log";
    ::openmldb::base::MkdirRecur(log_path);
    std::string full_path = log_path + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    logs->Insert(binlog_index, offset);
    *wh = new ::openmldb::storage::WriteHandle("off", name, fd);
    binlog_index++;
    return true;
}

void PrepareLatestTableData(TabletImpl& tablet, int32_t tid,  // NOLINT
                            int32_t pid) {
    for (int32_t i = 0; i < 100; i++) {
        ::openmldb::api::PutRequest prequest;
        ::openmldb::test::SetDimension(0, std::to_string(i % 10), prequest.add_dimensions());
        prequest.set_time(i + 1);
        prequest.set_value(::openmldb::test::EncodeKV(std::to_string(i % 10), std::to_string(i)));
        prequest.set_tid(tid);
        prequest.set_pid(pid);
        ::openmldb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    for (int32_t i = 0; i < 100; i++) {
        ::openmldb::api::PutRequest prequest;
        ::openmldb::test::SetDimension(0, "10", prequest.add_dimensions());
        prequest.set_time(i % 10 + 1);
        prequest.set_value(::openmldb::test::EncodeKV("10", std::to_string(i)));
        prequest.set_tid(tid);
        prequest.set_pid(pid);
        ::openmldb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
}

void AddDefaultSchema(uint64_t abs_ttl, uint64_t lat_ttl, ::openmldb::type::TTLType ttl_type,
                      ::openmldb::api::TableMeta* table_meta) {
    table_meta->set_format_version(1);
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
    ttl_st->set_abs_ttl(abs_ttl);
    ttl_st->set_lat_ttl(lat_ttl);
    ttl_st->set_ttl_type(ttl_type);
}

void AddDefaultAggregatorBaseSchema(::openmldb::api::TableMeta* table_meta) {
    table_meta->set_name("t0");
    table_meta->set_pid(1);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "id", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_col", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col3", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col4", openmldb::type::DataType::kDouble);

    SchemaCodec::SetIndex(table_meta->add_column_key(), "idx1", "id", "ts_col", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(),
                          "idx2", "col3", "ts_col", ::openmldb::type::kAbsoluteTime, 0, 0);
    return;
}

void AddDefaultAggregatorSchema(::openmldb::api::TableMeta* table_meta) {
    table_meta->set_name("pre_aggr_1");
    table_meta->set_pid(1);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "key", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_start", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_end", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "num_rows", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "agg_val", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "binlog_offset", openmldb::type::DataType::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "filter_key", openmldb::type::DataType::kString);

    SchemaCodec::SetIndex(table_meta->add_column_key(),
                          "key", "key|filter_key", "ts_start", ::openmldb::type::kAbsoluteTime, 0, 0);
}

std::string EncodeAggrRow(const std::string& key, int64_t ts, int32_t val) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_format_version(1);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "id", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts_col", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "col3", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "col4", openmldb::type::DataType::kDouble);
    ::openmldb::codec::SDKCodec sdk_codec(table_meta);
    std::string result;
    std::vector<std::string> row = {key, std::to_string(ts), std::to_string(val), std::to_string(0.0)};
    sdk_codec.EncodeRow(row, &result);
    return result;
}

void PackDefaultDimension(const std::string& key, ::openmldb::api::PutRequest* request) {
    auto dimension = request->add_dimensions();
    dimension->set_key(key);
    dimension->set_idx(0);
}

int PutKVData(uint32_t tid, uint32_t pid, const std::string& key, const std::string& value, uint64_t time,
        TabletImpl* tablet) {
    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension(key, &prequest);
    prequest.set_time(time);
    prequest.set_value(::openmldb::test::EncodeKV(key, value));
    prequest.set_tid(tid);
    prequest.set_pid(pid);
    ::openmldb::api::PutResponse presponse;
    MockClosure closure;
    tablet->Put(NULL, &prequest, &presponse, &closure);
    return presponse.code();
}

int GetTTL(TabletImpl& tablet, uint32_t tid, uint32_t pid, const std::string& index_name,  // NOLINT
           ::openmldb::common::TTLSt* ttl) {
    ::openmldb::api::GetTableSchemaRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::openmldb::api::GetTableSchemaResponse response;
    MockClosure closure;
    tablet.GetTableSchema(NULL, &request, &response, &closure);
    if (response.code() != 0) {
        return response.code();
    }
    for (const auto& index : response.table_meta().column_key()) {
        if (index_name.empty() || index.index_name() == index_name) {
            if (index.has_ttl()) {
                ttl->CopyFrom(index.ttl());
                return 0;
            }
            break;
        }
    }
    return -1;
}
int CreateDefaultTable(const std::string& db, const std::string& name, uint32_t tid, uint32_t pid, uint32_t abs_ttl,
                       uint32_t lat_ttl, ::openmldb::type::TTLType ttl_type, common::StorageMode storage_mode,
                       TabletImpl* tablet) {
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_db(db);
    table_meta->set_name(name);
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta->set_storage_mode(storage_mode);
    AddDefaultSchema(abs_ttl, lat_ttl, ttl_type, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet->CreateTable(NULL, &request, &response, &closure);
    return response.code();
}







TEST_P(TabletImplTest, LoadWithDeletedKey) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    ::openmldb::api::TableMeta table_meta_test;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_seg_cnt(8);
        table_meta->set_term(1024);
        table_meta->set_format_version(1);
        table_meta->set_storage_mode(storage_mode);
        ::openmldb::common::ColumnDesc* column_desc1 = table_meta->add_column_desc();
        column_desc1->set_name("card");
        column_desc1->set_data_type(::openmldb::type::kString);
        ::openmldb::common::ColumnDesc* column_desc2 = table_meta->add_column_desc();
        column_desc2->set_name("mcc");
        column_desc2->set_data_type(::openmldb::type::kString);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "", ::openmldb::type::kAbsoluteTime, 0, 0);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "mcc", "mcc", "", ::openmldb::type::kAbsoluteTime, 0, 0);

        table_meta->add_replicas("127.0.0.1:9527");
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
        for (int i = 0; i < 5; i++) {
            ::openmldb::api::PutRequest request;
            std::vector<std::string> row = {"card" + std::to_string(i), "mcc" + std::to_string(i)};
            auto value = request.mutable_value();
            sdk_codec.EncodeRow(row, value);
            request.set_time(1100);
            request.set_tid(id);
            request.set_pid(1);
            ::openmldb::api::Dimension* d1 = request.add_dimensions();
            d1->set_idx(0);
            d1->set_key("card" + std::to_string(i));
            ::openmldb::api::Dimension* d2 = request.add_dimensions();
            d2->set_idx(1);
            d2->set_key("mcc" + std::to_string(i));
            ::openmldb::api::PutResponse response;
            MockClosure closure;
            tablet.Put(NULL, &request, &response, &closure);
            ASSERT_EQ(0, response.code());
        }
        ::openmldb::api::DeleteIndexRequest deleteindex_request;
        ::openmldb::api::GeneralResponse deleteindex_response;
        // delete first index should fail
        deleteindex_request.set_idx_name("mcc");
        deleteindex_request.set_tid(id);
        deleteindex_request.set_pid(1);
        tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response, &closure);
        ASSERT_EQ(0, deleteindex_response.code());
    }
    // load
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::LoadTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        ::openmldb::api::GeneralResponse response;
        MockClosure closure;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("mcc0");
        sr.set_idx_name("mcc");
        sr.set_st(1200);
        sr.set_et(1000);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(108, srp.code());

        sr.set_pk("card0");
        sr.set_idx_name("card");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());

        ASSERT_EQ((signed)srp.count(), 1);
    }
}

















INSTANTIATE_TEST_CASE_P(TabletMemAndHDD, TabletImplTest,
                        ::testing::Values(::openmldb::common::kMemory,/*::openmldb::common::kSSD,*/
                                          ::openmldb::common::kHDD));

TEST_F(TabletImplTest, CreateAggregator) {
    TabletImpl tablet;
    tablet.Init("");
    ::openmldb::api::TableMeta base_table_meta;
    uint32_t aggr_table_id;
    uint32_t base_table_id;
    {
        // base table
        uint32_t id = counter++;
        base_table_id = id;
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_tid(id);
        AddDefaultAggregatorBaseSchema(table_meta);
        base_table_meta.CopyFrom(*table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        // pre aggr table
        uint32_t id = counter++;
        aggr_table_id = id;
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_tid(id);
        AddDefaultAggregatorSchema(table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // create aggregator
    {
        ::openmldb::api::CreateAggregatorRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_base_table_meta();
        table_meta->CopyFrom(base_table_meta);
        request.set_aggr_table_tid(aggr_table_id);
        request.set_aggr_table_pid(1);
        request.set_aggr_col("col3");
        request.set_aggr_func("sum");
        request.set_index_pos(0);
        request.set_order_by_col("ts_col");
        request.set_bucket_size("100");
        ::openmldb::api::CreateAggregatorResponse response;
        MockClosure closure;
        tablet.CreateAggregator(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        ::openmldb::api::CreateAggregatorRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_base_table_meta();
        table_meta->CopyFrom(base_table_meta);
        request.set_aggr_table_tid(aggr_table_id);
        request.set_aggr_table_pid(1);
        request.set_aggr_col("col4");
        request.set_aggr_func("sum");
        request.set_index_pos(0);
        request.set_order_by_col("ts_col");
        request.set_bucket_size("1d");
        ::openmldb::api::CreateAggregatorResponse response;
        MockClosure closure;
        tablet.CreateAggregator(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    auto aggrs = tablet.GetAggregators(base_table_meta.tid(), 1);
    ASSERT_EQ(aggrs->size(), 2);
    ASSERT_EQ(aggrs->at(0)->GetAggrType(), ::openmldb::storage::AggrType::kSum);
    ASSERT_EQ(aggrs->at(0)->GetWindowType(), ::openmldb::storage::WindowType::kRowsNum);
    ASSERT_EQ(aggrs->at(0)->GetWindowSize(), 100);
    ASSERT_EQ(aggrs->at(1)->GetAggrType(), ::openmldb::storage::AggrType::kSum);
    ASSERT_EQ(aggrs->at(1)->GetWindowType(), ::openmldb::storage::WindowType::kRowsRange);
    ASSERT_EQ(aggrs->at(1)->GetWindowSize(), 60 * 60 * 24 * 1000);
    {
        MockClosure closure;
        ::openmldb::api::DropTableRequest dr;
        dr.set_tid(base_table_id);
        dr.set_pid(1);
        ::openmldb::api::DropTableResponse drs;
        tablet.DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
        dr.set_tid(aggr_table_id);
        dr.set_pid(1);
        tablet.DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
    }
}

TEST_F(TabletImplTest, AggregatorRecovery) {
    uint32_t aggr_table_id;
    uint32_t base_table_id;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::TableMeta base_table_meta;
        // base table
        uint32_t id = counter++;
        base_table_id = id;
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_tid(id);
        AddDefaultAggregatorBaseSchema(table_meta);
        base_table_meta.CopyFrom(*table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        // pre aggr table
        id = counter++;
        aggr_table_id = id;
        table_meta = request.mutable_table_meta();
        table_meta->Clear();
        table_meta->set_tid(id);
        AddDefaultAggregatorSchema(table_meta);
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        // create aggr
        ::openmldb::api::CreateAggregatorRequest aggr_request;
        table_meta = aggr_request.mutable_base_table_meta();
        table_meta->CopyFrom(base_table_meta);
        aggr_request.set_aggr_table_tid(aggr_table_id);
        aggr_request.set_aggr_table_pid(1);
        aggr_request.set_aggr_col("col3");
        aggr_request.set_aggr_func("sum");
        aggr_request.set_index_pos(0);
        aggr_request.set_order_by_col("ts_col");
        aggr_request.set_bucket_size("2");
        ::openmldb::api::CreateAggregatorResponse aggr_response;
        tablet.CreateAggregator(NULL, &aggr_request, &aggr_response, &closure);
        ASSERT_EQ(0, response.code());
        // put
        {
            ::openmldb::api::PutRequest prequest;
            ::openmldb::test::SetDimension(0, "id1", prequest.add_dimensions());
            prequest.set_time(11);
            prequest.set_value(EncodeAggrRow("id1", 1, 1));
            prequest.set_tid(base_table_id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            MockClosure closure;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    {
        // aggr_table empty
        TabletImpl tablet;
        tablet.Init("");
        MockClosure closure;
        ::openmldb::api::LoadTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(base_table_id);
        table_meta->set_pid(1);
        ::openmldb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        table_meta = request.mutable_table_meta();
        table_meta->Clear();
        table_meta->set_name("pre_aggr_1");
        table_meta->set_tid(aggr_table_id);
        table_meta->set_pid(1);
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        sleep(3);

        ::openmldb::api::ScanRequest sr;
        sr.set_tid(aggr_table_id);
        sr.set_pid(1);
        sr.set_pk("id1");
        sr.set_st(100);
        sr.set_et(0);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        auto aggrs = tablet.GetAggregators(base_table_id, 1);
        ASSERT_EQ(aggrs->size(), 1);
        auto aggr = aggrs->at(0);
        ::openmldb::storage::AggrBuffer* aggr_buffer;
        aggr->GetAggrBuffer("id1", &aggr_buffer);
        ASSERT_EQ(aggr_buffer->aggr_cnt_, 1);
        ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 1);
        ASSERT_EQ(aggr_buffer->binlog_offset_, 1);

        // put data to base table
        for (int32_t i = 2; i <= 100; i++) {
            ::openmldb::api::PutRequest prequest;
            ::openmldb::test::SetDimension(0, "id1", prequest.add_dimensions());
            prequest.set_time(i);
            prequest.set_value(EncodeAggrRow("id1", i, i));
            prequest.set_tid(base_table_id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            MockClosure closure;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
        for (int32_t i = 1; i <= 100; i++) {
            ::openmldb::api::PutRequest prequest;
            ::openmldb::test::SetDimension(0, "id2", prequest.add_dimensions());
            prequest.set_time(i);
            prequest.set_value(EncodeAggrRow("id2", i, i));
            prequest.set_tid(base_table_id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            MockClosure closure;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
        // out of order put
        {
            ::openmldb::api::PutRequest prequest;
            ::openmldb::test::SetDimension(0, "id2", prequest.add_dimensions());
            prequest.set_time(50);
            prequest.set_value(EncodeAggrRow("id2", 50, 50));
            prequest.set_tid(base_table_id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            MockClosure closure;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    // recovery
    {
        TabletImpl tablet;
        tablet.Init("");
        MockClosure closure;
        ::openmldb::api::LoadTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(base_table_id);
        table_meta->set_pid(1);
        ::openmldb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        table_meta = request.mutable_table_meta();
        table_meta->Clear();
        table_meta->set_name("pre_aggr_1");
        table_meta->set_tid(aggr_table_id);
        table_meta->set_pid(1);
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        sleep(3);

        ::openmldb::api::ScanRequest sr;
        sr.set_tid(aggr_table_id);
        sr.set_pid(1);
        sr.set_pk("id1");
        sr.set_st(100);
        sr.set_et(0);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(49, (signed)srp.count());
        sr.set_tid(aggr_table_id);
        sr.set_pid(1);
        sr.set_pk("id2");
        sr.set_st(100);
        sr.set_et(0);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        // 50 = 49 (the number of aggr value) + 1 (the number of out-of-order put)
        ASSERT_EQ(50, (signed)srp.count());
        auto aggrs = tablet.GetAggregators(base_table_id, 1);
        ASSERT_EQ(aggrs->size(), 1);
        auto aggr = aggrs->at(0);
        ::openmldb::storage::AggrBuffer* aggr_buffer;
        aggr->GetAggrBuffer("id1", &aggr_buffer);
        ASSERT_EQ(aggr_buffer->aggr_cnt_, 2);
        ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 199);
        ASSERT_EQ(aggr_buffer->binlog_offset_, 100);
        aggr->GetAggrBuffer("id2", &aggr_buffer);
        ASSERT_EQ(aggr_buffer->aggr_cnt_, 2);

        ::openmldb::api::DropTableRequest dr;
        dr.set_tid(base_table_id);
        dr.set_pid(1);
        ::openmldb::api::DropTableResponse drs;
        tablet.DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
        dr.set_tid(aggr_table_id);
        dr.set_pid(1);
        tablet.DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
    }
}

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::AddGlobalTestEnvironment(new ::openmldb::tablet::DiskTestEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::openmldb::tablet::GenRand();
    FLAGS_ssd_root_path = "/tmp/ssd/" + ::openmldb::tablet::GenRand();
    FLAGS_hdd_root_path = "/tmp/hdd/" + ::openmldb::tablet::GenRand();
    FLAGS_recycle_bin_root_path = "/tmp/recycle/" + ::openmldb::tablet::GenRand();
    FLAGS_recycle_bin_ssd_root_path = "/tmp/ssd/recycle/" + ::openmldb::tablet::GenRand();
    FLAGS_recycle_bin_hdd_root_path = "/tmp/hdd/recycle/" + ::openmldb::tablet::GenRand();
    FLAGS_recycle_bin_enabled = true;
    return RUN_ALL_TESTS();
}
