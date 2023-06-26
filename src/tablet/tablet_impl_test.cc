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
#include <snappy.h>
#include <gflags/gflags.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <sys/stat.h>

#include <algorithm>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/kv_iterator.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "codec/codec.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
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

void PrepareLatestTableData(TabletImpl& tablet, int32_t tid, int32_t pid, bool compress = false) { // NOLINT
    for (int32_t i = 0; i < 100; i++) {
        ::openmldb::api::PutRequest prequest;
        ::openmldb::test::SetDimension(0, std::to_string(i % 10), prequest.add_dimensions());
        prequest.set_time(i + 1);
        std::string value = ::openmldb::test::EncodeKV(std::to_string(i % 10), std::to_string(i));
        if (compress) {
            std::string compressed;
            ::snappy::Compress(value.c_str(), value.length(), &compressed);
            value.swap(compressed);
        }
        prequest.set_value(value);
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
        std::string value = ::openmldb::test::EncodeKV("10", std::to_string(i));
        if (compress) {
            std::string compressed;
            ::snappy::Compress(value.c_str(), value.length(), &compressed);
            value.swap(compressed);
        }
        prequest.set_value(value);
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

std::pair<int, int> ScanFromTablet(uint32_t tid, uint32_t pid, const std::string& key, const std::string& idx_name,
        uint64_t st, uint64_t et, TabletImpl* tablet) {
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(tid);
    sr.set_pid(pid);
    sr.set_pk(key);
    if (!idx_name.empty()) {
        sr.set_idx_name(idx_name);
    }
    sr.set_st(st);
    sr.set_et(et);
    ::openmldb::api::ScanResponse srp;
    MockClosure closure;
    tablet->Scan(NULL, &sr, &srp, &closure);
    return std::make_pair(srp.code(), srp.count());
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

TEST_F(TabletImplTest, Init) {
    FLAGS_recycle_bin_enabled = true;
    auto create_and_drop_table = [](TabletImpl* tablet, uint32_t id, common::StorageMode storage_mode = common::kMemory,
                                    bool succeed = false) {
        ASSERT_EQ(0, CreateDefaultTable("db0", "t0", id, 0, 0, 0, kLatestTime, storage_mode, tablet));
        MockClosure closure;
        ::openmldb::api::DropTableRequest dr;
        dr.set_tid(id);
        dr.set_pid(0);
        auto task_info = dr.mutable_task_info();
        task_info->set_op_id(0);
        task_info->set_op_type(api::kDropTableRemoteOP);
        task_info->set_task_type(api::kDropTable);
        task_info->set_status(api::kInited);
        ::openmldb::api::DropTableResponse drs;
        tablet->DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
        sleep(1);

        api::TaskStatusRequest tr;
        api::TaskStatusResponse trs;
        tablet->GetTaskStatus(NULL, &tr, &trs, &closure);
        auto task = trs.task(0);
        if (succeed) {
            ASSERT_EQ(api::TaskStatus::kDone, task.status());
        } else {
            ASSERT_EQ(api::TaskStatus::kFailed, task.status());
        }
    };

    {
        TabletImpl tablet;
        FLAGS_recycle_bin_root_path = "";
        ASSERT_TRUE(tablet.Init(""));
        create_and_drop_table(&tablet, counter++);
    }
    ::openmldb::test::TempPath tmp_path;
    {
        TabletImpl tablet;
        FLAGS_recycle_bin_root_path = tmp_path.GetTempPath();
        ASSERT_TRUE(tablet.Init(""));
        create_and_drop_table(&tablet, counter++, common::kMemory, true);
    }

    {
        TabletImpl tablet;
        FLAGS_recycle_bin_hdd_root_path = "";
        ASSERT_TRUE(tablet.Init(""));
        create_and_drop_table(&tablet, counter++, common::kHDD);
    }

    {
        TabletImpl tablet;
        FLAGS_recycle_bin_hdd_root_path = tmp_path.GetTempPath();
        ASSERT_TRUE(tablet.Init(""));
        create_and_drop_table(&tablet, counter++, common::kHDD, true);
    }

    {
        TabletImpl tablet;
        FLAGS_recycle_bin_ssd_root_path = "";
        ASSERT_TRUE(tablet.Init(""));
        create_and_drop_table(&tablet, counter++, common::kSSD);
    }

    {
        TabletImpl tablet;
        FLAGS_recycle_bin_ssd_root_path = tmp_path.GetTempPath();
        ASSERT_TRUE(tablet.Init(""));
        create_and_drop_table(&tablet, counter++, common::kSSD, true);
    }
}

TEST_F(TabletImplTest, ChangeRole) {
    TabletImpl tablet;
    tablet.Init("");
    // create table
    MockClosure closure;
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("db0", "t0", id, 0, 0, 0, kLatestTime, common::kMemory, &tablet));
    PutKVData(id, 0, "key", "value1", 1, &tablet);
    {
        ::openmldb::api::QueryRequest request;
        request.set_db("db0");
        request.set_sql("select * from t0;");
        request.set_is_batch(true);
        request.set_parameter_row_size(0);
        request.set_parameter_row_slices(1);
        ::openmldb::api::QueryResponse response;
        brpc::Controller cntl;
        tablet.Query(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, response.count());
    }
    {
        ::openmldb::api::ChangeRoleRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_mode(::openmldb::api::TableMode::kTableFollower);
        ::openmldb::api::ChangeRoleResponse response;
        MockClosure closure;
        tablet.ChangeRole(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        ::openmldb::api::QueryRequest request;
        request.set_db("db0");
        request.set_sql("select * from t0;");
        request.set_is_batch(true);
        request.set_parameter_row_size(0);
        request.set_parameter_row_slices(1);
        ::openmldb::api::QueryResponse response;
        brpc::Controller cntl;
        tablet.Query(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(0, response.count());
    }
}

TEST_P(TabletImplTest, CountLatestTable) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 0, 0, kLatestTime, storage_mode, &tablet));
    PrepareLatestTableData(tablet, id, 0);
    MockClosure closure;
    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        // disktable and memtable behave inconsistently when putting duplicate data
        // See issue #1240 for more information
        if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_EQ(100u, response.count());
        } else {
            ASSERT_EQ(10u, response.count());
        }
    }

    {
        //
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        request.set_enable_remove_duplicated_record(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        // default
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        // default st et type
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1u, response.count());
    }

    {
        // st type=le et type=ge
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(2u, response.count());
    }

    {
        // st type=le et type=ge
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(90);
        request.set_et(81);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1u, response.count());
    }
}

TEST_P(TabletImplTest, CountTimeTable) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 0, 0, kAbsoluteTime, storage_mode, &tablet));
    PrepareLatestTableData(tablet, id, 0);

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (int32_t)response.count());
    }

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (int64_t)(response.count()));
    }

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        // disktable and memtable behave inconsistently when putting duplicate data
        // See issue #1240 for more information
        if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_EQ(100, (signed)response.count());
        } else {
            ASSERT_EQ(10, (signed)response.count());
        }
    }

    {
        //
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        request.set_enable_remove_duplicated_record(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (signed)response.count());
    }

    {
        // default
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (signed)response.count());
    }

    {
        // default st et type
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.count());
    }

    {
        // st type=le et type=ge
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(2, (signed)response.count());
    }

    {
        // st type=le et type=ge
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(90);
        request.set_et(81);
        request.set_et_type(::openmldb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.count());
    }
}

TEST_P(TabletImplTest, ScanLatestTable) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 0, 5, kLatestTime, storage_mode, &tablet));
    PrepareLatestTableData(tablet, id, 0);

    // scan with default type
    {
        std::string key = "1";
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk(key);
        sr.set_st(92);
        sr.set_et(90);
        auto srp = std::make_shared<::openmldb::api::ScanResponse>();
        tablet.Scan(NULL, &sr, srp.get(), &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(1, (signed)srp->count());
        ::openmldb::base::ScanKvIterator kv_it(key, srp);
        ASSERT_TRUE(kv_it.Valid());
        ASSERT_EQ(92l, (signed)kv_it.GetKey());
        ASSERT_STREQ("91", ::openmldb::test::DecodeV(kv_it.GetValue().ToString()).c_str());
        kv_it.Next();
        ASSERT_FALSE(kv_it.Valid());
    }

    // scan with default et ge
    {
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("1");
        sr.set_st(92);
        sr.set_et(0);
        auto srp = std::make_shared<::openmldb::api::ScanResponse>();
        tablet.Scan(NULL, &sr, srp.get(), &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(5, (signed)srp->count());
        ::openmldb::base::ScanKvIterator kv_it(sr.pk(), srp);
        ASSERT_TRUE(kv_it.Valid());
        ASSERT_EQ(92l, (signed)kv_it.GetKey());
        ASSERT_STREQ("91", ::openmldb::test::DecodeV(kv_it.GetValue().ToString()).c_str());
        kv_it.Next();
        ASSERT_TRUE(kv_it.Valid());
    }
}

TEST_P(TabletImplTest, Get) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    // table not found
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(1);
        request.set_pid(0);
        request.set_key("test");
        request.set_ts(0);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, storage_mode, &tablet));
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    // key not found
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
    }
    // put/get expired key
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test0", &prequest);
        prequest.set_time(now - 2 * 60 * 1000);
        prequest.set_value(::openmldb::test::EncodeKV("test0", "value0"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test0");
        request.set_idx_name("idx0");
        request.set_ts(0);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        printf("response key:%s \n", response.key().c_str());
        ASSERT_EQ(109, response.code());
    }
    // put some key
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_time(now);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test10"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_time(now - 2);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test9"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        prequest.set_time(now - 120 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 2);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(response.value()));
        request.set_ts(0);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test10", ::openmldb::test::DecodeV(response.value()));
    }
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 1);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
    }
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 2);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(response.value()));
    }
    {
        // get expired key
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 120 * 1000);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(307, response.code());
    }
    // create latest ttl table
    id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 5, kLatestTime, storage_mode, &tablet));
    int num = 10;
    while (num) {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_time(num);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test" + std::to_string(num)));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        num--;
    }
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(5);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_NE(0, response.code());
        request.set_ts(6);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test6", ::openmldb::test::DecodeV(response.value()));
        request.set_ts(0);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test10", ::openmldb::test::DecodeV(response.value()));
        request.set_ts(7);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test7", ::openmldb::test::DecodeV(response.value()));
    }
}

void CheckTTLFromMeta(uint32_t tid, uint32_t pid, ::openmldb::common::StorageMode storage_mode,
        const std::string& index_name, uint64_t abs_ttl, uint64_t lat_ttl) {
    std::string file_path;
    if (storage_mode == ::openmldb::common::kMemory) {
        file_path = FLAGS_db_root_path;
    } else if (storage_mode == ::openmldb::common::kSSD) {
        file_path = FLAGS_ssd_root_path;
    } else {
        file_path = FLAGS_hdd_root_path;
    }
    file_path += "/" + std::to_string(tid) + "_" + std::to_string(pid) + "/table_meta.txt";
    int fd = open(file_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    ::openmldb::api::TableMeta table_meta;
    google::protobuf::TextFormat::Parse(&fileInput, &table_meta);
    ASSERT_GT(table_meta.column_key_size(), 0);
    for (const auto& cur_column_key : table_meta.column_key()) {
        if (index_name.empty() || cur_column_key.index_name() == index_name) {
            ASSERT_EQ(cur_column_key.ttl().abs_ttl(), abs_ttl);
            ASSERT_EQ(cur_column_key.ttl().lat_ttl(), lat_ttl);
        }
    }
}

int UpdateTTL(uint32_t tid, uint32_t pid, ::openmldb::type::TTLType ttl_type,
        uint64_t abs_ttl, uint64_t lat_ttl, openmldb::tablet::TabletImpl* tablet) {
    ::openmldb::api::UpdateTTLRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    auto ttl = request.mutable_ttl();
    ttl->set_ttl_type(ttl_type);
    ttl->set_abs_ttl(abs_ttl);
    ttl->set_lat_ttl(lat_ttl);
    ::openmldb::api::UpdateTTLResponse response;
    MockClosure closure;
    tablet->UpdateTTL(NULL, &request, &response, &closure);
    return response.code();
}

TEST_P(TabletImplTest, UpdateTTLAbsoluteTime) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int32_t old_gc_interval = FLAGS_gc_interval;
    int32_t old_disk_gc_interval = FLAGS_disk_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    FLAGS_disk_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 100, 0, kAbsoluteTime, storage_mode, &tablet));
    // table not exist
    ASSERT_EQ(100, UpdateTTL(0, 0, ::openmldb::type::kAbsoluteTime, 0, 0, &tablet));
    // bigger than max ttl, tablet side won't check
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsoluteTime, 60 * 24 * 365 * 30 * 2, 0, &tablet));
    // ttl type mismatch is allowed
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kLatestTime, 0, 0, &tablet));

    // normal case
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test", &prequest);
    prequest.set_time(now - 60 * 60 * 1000);
    prequest.set_value(::openmldb::test::EncodeKV("test", "test9"));
    prequest.set_tid(id);
    prequest.set_pid(0);
    ::openmldb::api::PutResponse presponse;
    MockClosure closure;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
    ::openmldb::api::GetRequest grequest;
    grequest.set_tid(id);
    grequest.set_pid(0);
    grequest.set_key("test");
    grequest.set_ts(0);
    ::openmldb::api::GetResponse gresponse;
    tablet.Get(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
    ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));
    // ExecuteGcRequest
    ::openmldb::api::ExecuteGcRequest request_execute;
    ::openmldb::api::GeneralResponse response_execute;
    request_execute.set_tid(id);
    request_execute.set_pid(0);
    // etTableStatusRequest
    ::openmldb::api::GetTableStatusRequest gr;
    ::openmldb::api::GetTableStatusResponse gres;
    // ttl update to zero
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsoluteTime, 0, 0, &tablet));

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, cur_ttl.abs_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 0, 0);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
    }
    // ttl update from zero to no zero
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsoluteTime, 50, 0, &tablet));

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(50, cur_ttl.abs_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 50, 0);

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());
    }
    // update from 50 to 100
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsoluteTime, 100, 0, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, cur_ttl.abs_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 100, 0);
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
    }
    FLAGS_gc_interval = old_gc_interval;
    FLAGS_disk_gc_interval = old_disk_gc_interval;
}

TEST_P(TabletImplTest, UpdateTTLLatest) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int32_t old_gc_interval = FLAGS_gc_interval;
    int32_t old_disk_gc_interval = FLAGS_disk_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    FLAGS_disk_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    // create table
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 0, 1, kLatestTime, storage_mode, &tablet));
    // table not exist
    ASSERT_EQ(100, UpdateTTL(0, 0, ::openmldb::type::kLatestTime, 0, 0, &tablet));
    // reach the max ttl, tablet side won't check
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kLatestTime, 0, 20000, &tablet));
    // ttl type mismatch is allowed
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsoluteTime, 0, 0, &tablet));
    // normal case
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kLatestTime, 0, 2, &tablet));
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(2, cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 0, 2);
    }
    FLAGS_gc_interval = old_gc_interval;
    FLAGS_disk_gc_interval = old_disk_gc_interval;
}

TEST_P(TabletImplTest, CreateTableWithSchema) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    {
        uint32_t id = counter++;
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(131, response.code());
    }
    {
        uint32_t id = counter++;
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        auto column = table_meta->add_column_desc();
        column->set_name("card");
        column->set_data_type(::openmldb::type::kString);

        column = table_meta->add_column_desc();
        column->set_name("amt");
        column->set_data_type(::openmldb::type::kDouble);

        column = table_meta->add_column_desc();
        column->set_name("apprv_cde");
        column->set_data_type(::openmldb::type::kInt);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "", ::openmldb::type::kAbsoluteTime, 0, 0);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        ::openmldb::api::GetTableSchemaRequest request0;
        request0.set_tid(id);
        request0.set_pid(1);
        ::openmldb::api::GetTableSchemaResponse response0;
        tablet.GetTableSchema(NULL, &request0, &response0, &closure);
        ASSERT_EQ(3, response0.table_meta().column_desc_size());
    }
}

TEST_P(TabletImplTest, MultiGet) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_storage_mode(storage_mode);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    auto column = table_meta->add_column_desc();
    column->set_name("card");
    column->set_data_type(::openmldb::type::kString);

    column = table_meta->add_column_desc();
    column->set_name("amt");
    column->set_data_type(::openmldb::type::kString);

    column = table_meta->add_column_desc();
    column->set_name("apprv_cde");
    column->set_data_type(::openmldb::type::kInt);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "amt", "amt", "", ::openmldb::type::kAbsoluteTime, 0, 0);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    // put
    for (int i = 0; i < 5; i++) {
        std::vector<std::string> input;
        input.push_back("test" + std::to_string(i));
        input.push_back("abcd" + std::to_string(i));
        input.push_back("1212" + std::to_string(i));
        std::string value;
        ::openmldb::codec::RowCodec::EncodeRow(input, table_meta->column_desc(), 1, value);
        ::openmldb::api::PutRequest request;
        request.set_time(1100);
        request.set_value(value);
        request.set_tid(id);
        request.set_pid(1);
        ::openmldb::api::Dimension* d = request.add_dimensions();
        d->set_key("test" + std::to_string(i));
        d->set_idx(0);
        d = request.add_dimensions();
        d->set_key("abcd" + std::to_string(i));
        d->set_idx(1);
        ::openmldb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get
    ::openmldb::api::GetRequest get_request;
    ::openmldb::api::GetResponse get_response;
    get_request.set_tid(id);
    get_request.set_pid(1);
    get_request.set_key("abcd2");
    get_request.set_ts(1100);
    get_request.set_idx_name("amt");

    tablet.Get(NULL, &get_request, &get_response, &closure);
    ASSERT_EQ(0, get_response.code());
    ASSERT_EQ(1100, (signed)get_response.ts());
    std::string value(get_response.value());
    std::vector<std::string> vec;
    ::openmldb::codec::RowCodec::DecodeRow(table_meta->column_desc(), ::openmldb::base::Slice(value), vec);
    ASSERT_EQ(3, (signed)vec.size());
    ASSERT_STREQ("test2", vec[0].c_str());
    ASSERT_STREQ("abcd2", vec[1].c_str());
    ASSERT_STREQ("12122", vec[2].c_str());

    // delete index
    ::openmldb::api::DeleteIndexRequest deleteindex_request;
    ::openmldb::api::GeneralResponse deleteindex_response;
    // delete first index should fail
    deleteindex_request.set_idx_name("pk");
    deleteindex_request.set_tid(id);
    deleteindex_request.set_pid(1);
    tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response, &closure);
    // Some functions in tablet_impl only support memtable now
    // refer to issue #1438
    if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(142, deleteindex_response.code());
    } else {
        ASSERT_EQ(701, deleteindex_response.code());
    }
    // delete other index
    deleteindex_request.set_idx_name("amt");
    deleteindex_request.set_tid(id);
    tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response, &closure);
    // Some functions in tablet_impl only support memtable now
    // refer to issue #1438
    if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(0, deleteindex_response.code());
    } else {
        ASSERT_EQ(701, deleteindex_response.code());
    }

    // get index not found
    get_request.set_tid(id);
    get_request.set_pid(1);
    get_request.set_key("abcd2");
    get_request.set_ts(1100);
    get_request.set_idx_name("amt");
    tablet.Get(NULL, &get_request, &get_response, &closure);
    // Some functions in tablet_impl only support memtable now
    // refer to issue #1438
    if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(108, get_response.code());
    } else {
        ASSERT_EQ(0, get_response.code());
    }

    // scan index not found
    ::openmldb::api::ScanRequest scan_request;
    ::openmldb::api::ScanResponse scan_response;
    scan_request.set_tid(id);
    scan_request.set_pid(1);
    scan_request.set_pk("abcd2");
    scan_request.set_st(1100);
    scan_request.set_idx_name("amt");
    tablet.Scan(NULL, &scan_request, &scan_response, &closure);
    // Some functions in tablet_impl only support memtable now
    // refer to issue #1438
    if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(108, scan_response.code());
    } else {
        ASSERT_EQ(0, scan_response.code());
    }
}


TEST_P(TabletImplTest, CreateTable) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    TabletImpl tablet;
    tablet.Init("");
    ::openmldb::common::TTLSt ttl_st;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        std::string file;
        if (storage_mode == ::openmldb::common::kMemory) {
            file = FLAGS_db_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        } else if (storage_mode == ::openmldb::common::kHDD) {
            file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        } else {
            file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        }
        int fd = open(file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::TableMeta table_meta_test;
        google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
        ASSERT_EQ(table_meta_test.tid(), (signed)id);
        ASSERT_STREQ(table_meta_test.name().c_str(), "t0");

        table_meta->set_name("");
        table_meta->set_storage_mode(storage_mode);
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(129, response.code());
    }
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_name("t0");
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(129, response.code());
    }
}


TEST_P(TabletImplTest, ScanWithDuplicateSkip) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kAbsoluteTime, storage_mode, &tablet));
    MockClosure closure;
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9527);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "testx"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9528);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "testx"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9528);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "testx"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9529);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "testx"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(9530);
    sr.set_et(0);
    sr.set_enable_remove_duplicated_record(true);
    ::openmldb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(3, (signed)srp.count());
}

TEST_P(TabletImplTest, ScanWithLatestN) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kLatestTime, storage_mode, &tablet));
    MockClosure closure;
    for (int ts = 9527; ts < 9540; ts++) {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(ts);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test" + std::to_string(ts)));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(0);
    sr.set_et(0);
    sr.set_limit(2);
    auto srp = std::make_shared<::openmldb::api::ScanResponse>();
    tablet.Scan(NULL, &sr, srp.get(), &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(2, (signed)srp->count());
    ::openmldb::base::ScanKvIterator kv_it(sr.pk(), srp);
    ASSERT_EQ(9539, (signed)kv_it.GetKey());
    ASSERT_STREQ("test9539", ::openmldb::test::DecodeV(kv_it.GetValue().ToString()).c_str());
    kv_it.Next();
    ASSERT_EQ(9538, (signed)kv_it.GetKey());
    ASSERT_STREQ("test9538", ::openmldb::test::DecodeV(kv_it.GetValue().ToString()).c_str());
    kv_it.Next();
    ASSERT_FALSE(kv_it.Valid());
}


TEST_P(TabletImplTest, Traverse) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("db0", "t0", id, 1, 0, 0, kAbsoluteTime, storage_mode, &tablet));
    MockClosure closure;
    for (int ts = 9527; ts < 9540; ts++) {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(ts);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test" + std::to_string(ts)));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::openmldb::api::TraverseRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_limit(100);
    auto srp = std::make_shared<::openmldb::api::TraverseResponse>();
    tablet.Traverse(NULL, &sr, srp.get(), &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(13, (signed)srp->count());
    ::openmldb::base::TraverseKvIterator kv_it(srp);
    for (int cnt = 0; cnt < 13; cnt++) {
        uint64_t cur_ts = 9539 - cnt;
        ASSERT_EQ(cur_ts, kv_it.GetKey());
        ASSERT_STREQ(std::string("test" + std::to_string(cur_ts)).c_str(),
                ::openmldb::test::DecodeV(kv_it.GetValue().ToString()).c_str());
        kv_it.Next();
    }
    ASSERT_FALSE(kv_it.Valid());
}

TEST_P(TabletImplTest, TraverseTTL) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    // disktable and memtable behave inconsistently with max_traverse_cnt
    // refer to issue #1249
    if (storage_mode != openmldb::common::kMemory) {
        GTEST_SKIP();
    }
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_storage_mode(storage_mode);
    table_meta->set_seg_cnt(1);
    AddDefaultSchema(5, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t key_base = 10000;
    for (int i = 0; i < 100; i++) {
        uint64_t ts = cur_time - 10 * 60 * 1000 + i;
        ::openmldb::api::PutRequest prequest;
        std::string key = "test" + std::to_string(key_base + i);
        PackDefaultDimension(key, &prequest);
        prequest.set_time(ts);
        prequest.set_value(::openmldb::test::EncodeKV(key, "test" + std::to_string(ts)));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    key_base = 21000;
    for (int i = 0; i < 60; i++) {
        uint64_t ts = cur_time + i;
        ::openmldb::api::PutRequest prequest;
        std::string key = "test" + std::to_string(key_base + i);
        PackDefaultDimension(key, &prequest);
        prequest.set_time(ts);
        prequest.set_value(::openmldb::test::EncodeKV(key, "test" + std::to_string(ts)));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::openmldb::api::TraverseRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_limit(100);
    ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(0, (signed)srp->count());
    ASSERT_EQ("test10050", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(0, (signed)srp->count());
    ASSERT_EQ("test10099", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(25, (signed)srp->count());
    ASSERT_EQ("test21024", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(25, (signed)srp->count());
    ASSERT_EQ("test21049", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(10, (signed)srp->count());
    ASSERT_EQ("test21059", srp->pk());
    ASSERT_TRUE(srp->is_finish());
    FLAGS_max_traverse_cnt = old_max_traverse;
}

TEST_P(TabletImplTest, TraverseTTLTS) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    // disktable and memtable behave inconsistently with max_traverse_cnt
    // refer to issue #1249
    if (storage_mode != openmldb::common::kMemory) {
        GTEST_SKIP();
    }
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_storage_mode(storage_mode);
    table_meta->set_seg_cnt(1);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "card", ::openmldb::type::kVarchar);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "mcc", ::openmldb::type::kVarchar);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 5, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 5, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "mcc", "mcc", "ts2", ::openmldb::type::kAbsoluteTime, 5, 0);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t key_base = 10000;
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    for (int i = 0; i < 30; i++) {
        for (int idx = 0; idx < 2; idx++) {
            uint64_t ts_value = cur_time - 10 * 60 * 1000 - idx;
            uint64_t ts1_value = cur_time - idx;
            std::vector<std::string> row = {"card" + std::to_string(key_base + i),
                    "mcc" + std::to_string(key_base + i), "12", std::to_string(ts_value), std::to_string(ts1_value)};
            ::openmldb::api::PutRequest prequest;
            ::openmldb::api::Dimension* dim = prequest.add_dimensions();
            dim->set_idx(0);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(1);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(2);
            dim->set_key("mcc" + std::to_string(key_base + i));
            auto value = prequest.mutable_value();
            sdk_codec.EncodeRow(row, value);
            prequest.set_tid(id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    key_base = 21000;
    for (int i = 0; i < 30; i++) {
        for (int idx = 0; idx < 2; idx++) {
            uint64_t ts1_value = cur_time - 10 * 60 * 1000 - idx;
            uint64_t ts_value = cur_time - idx;
            std::vector<std::string> row = {"card" + std::to_string(key_base + i),
                    "mcc" + std::to_string(key_base + i), "12", std::to_string(ts_value), std::to_string(ts1_value)};
            ::openmldb::api::PutRequest prequest;
            ::openmldb::api::Dimension* dim = prequest.add_dimensions();
            dim->set_idx(0);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(1);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(2);
            dim->set_key("mcc" + std::to_string(key_base + i));
            auto value = prequest.mutable_value();
            sdk_codec.EncodeRow(row, value);
            prequest.set_tid(id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    ::openmldb::api::TraverseRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_idx_name("card");
    sr.set_limit(100);
    ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(14, (signed)srp->count());
    ASSERT_EQ("card21006", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(33, (signed)srp->count());
    ASSERT_EQ("card21023", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(13, (signed)srp->count());
    ASSERT_EQ("card21029", srp->pk());
    ASSERT_TRUE(srp->is_finish());

    sr.clear_pk();
    sr.clear_ts();
    sr.set_idx_name("card1");
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(15, (signed)srp->count());
    ASSERT_EQ("card21006", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(33, (signed)srp->count());
    ASSERT_EQ("card21023", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(12, (signed)srp->count());
    ASSERT_EQ("card21029", srp->pk());
    ASSERT_TRUE(srp->is_finish());

    sr.clear_pk();
    sr.clear_ts();
    sr.set_idx_name("mcc");
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(34, (signed)srp->count());
    ASSERT_EQ("mcc10016", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(26, (signed)srp->count());
    ASSERT_EQ("mcc21008", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(0, (signed)srp->count());
    ASSERT_TRUE(srp->is_finish());
    FLAGS_max_traverse_cnt = old_max_traverse;
}

TEST_P(TabletImplTest, ScanWithLimit) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kAbsoluteTime, storage_mode, &tablet));
    MockClosure closure;
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9527);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9528);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9529);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(9530);
    sr.set_et(9526);
    sr.set_limit(2);
    ::openmldb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(2, (signed)srp.count());
}

TEST_P(TabletImplTest, Scan) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kAbsoluteTime, storage_mode, &tablet));
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(2);
    sr.set_pk("test1");
    sr.set_st(9528);
    sr.set_et(9527);
    sr.set_limit(10);
    ::openmldb::api::ScanResponse srp;
    MockClosure closure;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, (signed)srp.pairs().size());
    ASSERT_EQ(100, srp.code());

    sr.set_tid(id);
    sr.set_pid(1);
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(0, (signed)srp.count());

    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9527);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(2);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);

        ASSERT_EQ(100, presponse.code());
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);

        ASSERT_EQ(0, presponse.code());
    }
    {
        ::openmldb::api::PutRequest prequest;
        prequest.set_time(9528);
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(2);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);

        ASSERT_EQ(100, presponse.code());
        prequest.set_tid(id);
        prequest.set_pid(1);

        tablet.Put(NULL, &prequest, &presponse, &closure);

        ASSERT_EQ(0, presponse.code());
    }
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(1, (signed)srp.count());
}


TEST_P(TabletImplTest, GCWithUpdateLatest) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int32_t old_gc_interval = FLAGS_gc_interval;
    int32_t old_disk_gc_interval = FLAGS_disk_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    FLAGS_disk_gc_interval = 1;
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 3, kLatestTime, storage_mode, &tablet));
    // version 1
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test1"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // version 2
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test2"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(2);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // version 3
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test3"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(3);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // get version 1
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.ts());
        ASSERT_EQ("test1", ::openmldb::test::DecodeV(response.value()));
    }

    // update ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(1);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::kLatestTime);
        ttl->set_lat_ttl(2);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // get version 1 again
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // sleep 70s
    sleep(70);

    // get version 1 again
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
        ASSERT_EQ("key not found", response.msg());
    }
    // revert ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(1);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::kLatestTime);
        ttl->set_lat_ttl(3);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // make sure that gc has clean the data
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
        ASSERT_EQ("key not found", response.msg());
    }

    FLAGS_gc_interval = old_gc_interval;
    FLAGS_disk_gc_interval = old_disk_gc_interval;
}

TEST_P(TabletImplTest, GC) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 3, 0, kAbsoluteTime, storage_mode, &tablet));

    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_time(9527);
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::openmldb::api::PutResponse presponse;
    MockClosure closure;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    prequest.set_time(now);
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(now);
    sr.set_et(9527);
    sr.set_limit(10);
    ::openmldb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(1, (signed)srp.count());
}

TEST_P(TabletImplTest, DropTable) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::openmldb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, storage_mode, &tablet));

    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_time(9527);
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::openmldb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, storage_mode, &tablet));
}

TEST_F(TabletImplTest, DropTableNoRecycleMem) {
    bool tmp_recycle_bin_enabled = FLAGS_recycle_bin_enabled;
    std::string tmp_db_root_path = FLAGS_db_root_path;
    std::string tmp_recycle_bin_root_path = FLAGS_recycle_bin_root_path;
    std::vector<std::string> file_vec;
    FLAGS_recycle_bin_enabled = false;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    FLAGS_recycle_bin_root_path = tmp_path.GetTempPath();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::openmldb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, openmldb::common::kMemory, &tablet));

    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_time(9527);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::openmldb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    ::openmldb::base::GetChildFileName(FLAGS_db_root_path, file_vec);
    ASSERT_TRUE(file_vec.empty());
    file_vec.clear();
    ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_TRUE(file_vec.empty());
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, openmldb::common::kMemory, &tablet));
    FLAGS_recycle_bin_enabled = tmp_recycle_bin_enabled;
    FLAGS_db_root_path = tmp_db_root_path;
    FLAGS_recycle_bin_root_path = tmp_recycle_bin_root_path;
}

TEST_F(TabletImplTest, DropTableNoRecycleDisk) {
    bool tmp_recycle_bin_enabled = FLAGS_recycle_bin_enabled;
    std::string tmp_hdd_root_path = FLAGS_hdd_root_path;
    std::string tmp_recycle_bin_hdd_root_path = FLAGS_recycle_bin_hdd_root_path;
    std::vector<std::string> file_vec;
    FLAGS_recycle_bin_enabled = false;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_hdd_root_path = tmp_path.GetTempPath();
    FLAGS_recycle_bin_hdd_root_path = tmp_path.GetTempPath();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::openmldb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, openmldb::common::kHDD, &tablet));

    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_time(9527);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::openmldb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    ::openmldb::base::GetChildFileName(FLAGS_hdd_root_path, file_vec);
    ASSERT_TRUE(file_vec.empty());
    file_vec.clear();
    ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_hdd_root_path, file_vec);
    ASSERT_TRUE(file_vec.empty());
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, openmldb::common::kHDD, &tablet));
    FLAGS_recycle_bin_enabled = tmp_recycle_bin_enabled;
    FLAGS_hdd_root_path = tmp_hdd_root_path;
    FLAGS_recycle_bin_hdd_root_path = tmp_recycle_bin_hdd_root_path;
}

TEST_P(TabletImplTest, Recover) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_term(1024);
        table_meta->add_replicas("127.0.0.1:9527");
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_time(9527);
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // recover
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::LoadTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_seg_cnt(64);
        table_meta->set_storage_mode(storage_mode);
        table_meta->add_replicas("127.0.0.1:9530");
        table_meta->add_replicas("127.0.0.1:9531");
        ::openmldb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        std::string file;
        if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
            file = FLAGS_db_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        } else if (storage_mode == ::openmldb::common::StorageMode::kSSD) {
            file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        } else {
            file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        }
        int fd = open(file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::TableMeta table_meta_test;
        google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
        if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_EQ(table_meta_test.seg_cnt(), 64);
        }
        ASSERT_EQ(table_meta_test.term(), 1024lu);
        ASSERT_EQ(table_meta_test.replicas_size(), 2);
        ASSERT_STREQ(table_meta_test.replicas(0).c_str(), "127.0.0.1:9530");

        sleep(1);
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("test1");
        sr.set_st(9530);
        sr.set_et(9526);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
        ::openmldb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        grq.set_storage_mode(storage_mode);
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9528);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test1"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        sleep(2);
    }

    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::LoadTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("test1");
        sr.set_st(9530);
        sr.set_et(9526);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
    }
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
        // Some functions in tablet_impl only support memtable now
        // refer to issue #1438
        if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_EQ(0, deleteindex_response.code());
        } else {
            ASSERT_EQ(701, deleteindex_response.code());
        }
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
        // Some functions in tablet_impl only support memtable now
        // refer to issue #1438
        if (storage_mode == ::openmldb::common::kMemory) {
            ASSERT_EQ(108, srp.code());
        } else {
            ASSERT_EQ(0, srp.code());
        }
        sr.set_pk("card0");
        sr.set_idx_name("card");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ((signed)srp.count(), 1);
    }
}

TEST_P(TabletImplTest, LoadWithIncompleteBinlog) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int old_offset = FLAGS_make_snapshot_threshold_offset;
    int old_interval = FLAGS_binlog_delete_interval;
    FLAGS_binlog_delete_interval = 1000;
    FLAGS_make_snapshot_threshold_offset = 0;
    uint32_t tid = counter++;
    ::openmldb::storage::LogParts* log_part = new ::openmldb::storage::LogParts(12, 4, scmp);
    std::string binlog_dir;

    if (storage_mode == ::openmldb::common::kMemory) {
        binlog_dir = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog/";
    } else if (storage_mode == ::openmldb::common::kSSD) {
        binlog_dir = FLAGS_ssd_root_path + "/" + std::to_string(tid) + "_0/binlog/";
    } else {
        binlog_dir = FLAGS_hdd_root_path + "/" + std::to_string(tid) + "_0/binlog/";
    }
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    ::openmldb::storage::WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 1;
    for (; count <= 10; count++) {
        offset++;
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key";
        ::openmldb::test::AddDimension(0, key, &entry);
        entry.set_ts(count);
        entry.set_value(::openmldb::test::EncodeKV(key, "value" + std::to_string(count)));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    // not set end falg
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    // no record binlog
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    // empty binlog
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    count = 1;
    for (; count <= 20; count++) {
        offset++;
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key_new";
        ::openmldb::test::AddDimension(0, key, &entry);
        entry.set_ts(count);
        entry.set_value(::openmldb::test::EncodeKV(key, "value_new" + std::to_string(count)));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    binlog_index++;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    count = 1;
    for (; count <= 30; count++) {
        offset++;
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key_xxx";
        ::openmldb::test::AddDimension(0, key, &entry);
        entry.set_ts(count);
        entry.set_value(::openmldb::test::EncodeKV(key, "value_xxx" + std::to_string(count)));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    delete wh;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::LoadTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_storage_mode(storage_mode);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::GeneralResponse response;
        MockClosure closure;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(tid);
        sr.set_pid(0);
        sr.set_pk("key_new");
        sr.set_st(50);
        sr.set_et(0);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(20, (signed)srp.count());

        sr.set_pk("key");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());

        sr.set_pk("key_xxx");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(30, (signed)srp.count());

        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test1"));
        prequest.set_time(9528);
        prequest.set_tid(tid);
        prequest.set_pid(0);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        ::openmldb::api::GeneralRequest grq;
        grq.set_tid(tid);
        grq.set_pid(0);
        grq.set_storage_mode(storage_mode);
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file;

        if (storage_mode == ::openmldb::common::kMemory) {
            manifest_file = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/snapshot/MANIFEST";
        } else if (storage_mode == ::openmldb::common::kSSD) {
            manifest_file = FLAGS_ssd_root_path + "/" + std::to_string(tid) + "_0/snapshot/MANIFEST";
        } else {
            manifest_file = FLAGS_hdd_root_path + "/" + std::to_string(tid) + "_0/snapshot/MANIFEST";
        }

        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(61, (signed)manifest.offset());

        sleep(10);
        std::vector<std::string> vec;
        std::string binlog_path;

        if (storage_mode == ::openmldb::common::kMemory) {
            binlog_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog";
        } else if (storage_mode == ::openmldb::common::kSSD) {
            binlog_path = FLAGS_ssd_root_path + "/" + std::to_string(tid) + "_0/binlog";
        } else {
            binlog_path = FLAGS_hdd_root_path + "/" + std::to_string(tid) + "_0/binlog";
        }

        ::openmldb::base::GetFileName(binlog_path, vec);
        ASSERT_EQ(4, (signed)vec.size());
        std::sort(vec.begin(), vec.end());
        std::string file_name = binlog_path + "/00000001.log";
        ASSERT_STREQ(file_name.c_str(), vec[0].c_str());
        std::string file_name1 = binlog_path + "/00000007.log";
        ASSERT_STREQ(file_name1.c_str(), vec[3].c_str());
    }
    FLAGS_make_snapshot_threshold_offset = old_offset;
    FLAGS_binlog_delete_interval = old_interval;
}

TEST_P(TabletImplTest, GCWithUpdateTTL) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int32_t old_gc_interval = FLAGS_gc_interval;
    int32_t old_disk_gc_interval = FLAGS_disk_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    FLAGS_disk_gc_interval = 1;
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 3, 0, kAbsoluteTime, storage_mode, &tablet));
    // version 1
    //
    uint64_t now1 = ::baidu::common::timer::get_micros() / 1000;
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test1"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        prequest.set_time(now1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }
    // 1 minute before
    uint64_t now2 = now1 - 60 * 1000;
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test2"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        prequest.set_time(now2);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // 2 minute before
    uint64_t now3 = now1 - 2 * 60 * 1000;
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test3"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        prequest.set_time(now3);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // get now3
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(now3);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(now3, response.ts());
        ASSERT_EQ("test3", ::openmldb::test::DecodeV(response.value()));
    }

    // update ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(1);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
        ttl->set_abs_ttl(1);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    sleep(70);
    // get now3
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(now3);
        ::openmldb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(307, response.code());
        ASSERT_EQ("invalid args", response.msg());
    }
    FLAGS_gc_interval = old_gc_interval;
    FLAGS_disk_gc_interval = old_disk_gc_interval;
}

TEST_P(TabletImplTest, DropTableFollower) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::openmldb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_storage_mode(storage_mode);
    AddDefaultSchema(1, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    table_meta->set_mode(::openmldb::api::TableMode::kTableFollower);
    table_meta->add_replicas("127.0.0.1:9527");
    ::openmldb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_time(9527);
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::openmldb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    // ReadOnly
    ASSERT_EQ(103, presponse.code());

    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    prequest.set_time(9527);
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(100, presponse.code());
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
}

TEST_P(TabletImplTest, TestGetType) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 4, kLatestTime, storage_mode, &tablet));
    MockClosure closure;
    // 1
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_time(1);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test1"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 2
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test2"));
        prequest.set_time(2);
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 3
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test3"));
        prequest.set_time(3);
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 6
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test6"));
        prequest.set_time(6);
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // eq
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(1);
        request.set_type(::openmldb::api::GetType::kSubKeyEq);

        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.ts());
        ASSERT_EQ("test1", ::openmldb::test::DecodeV(response.value()));
    }
    // le
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(5);
        request.set_type(::openmldb::api::GetType::kSubKeyLe);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(3, (signed)response.ts());
        ASSERT_EQ("test3", ::openmldb::test::DecodeV(response.value()));
    }
    // lt
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(3);
        request.set_type(::openmldb::api::GetType::kSubKeyLt);

        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(2, (signed)response.ts());
        ASSERT_EQ("test2", ::openmldb::test::DecodeV(response.value()));
    }
    // gt
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(2);
        request.set_type(::openmldb::api::GetType::kSubKeyGt);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(6, (signed)response.ts());
        ASSERT_EQ("test6", ::openmldb::test::DecodeV(response.value()));
    }
    // ge
    {
        ::openmldb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(1);
        request.set_type(::openmldb::api::GetType::kSubKeyGe);
        ::openmldb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(6, (signed)response.ts());
        ASSERT_EQ("test6", ::openmldb::test::DecodeV(response.value()));
    }
}

TEST_P(TabletImplTest, Snapshot) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kAbsoluteTime, storage_mode, &tablet));

    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_time(9527);
    prequest.set_tid(id);
    prequest.set_pid(2);
    ::openmldb::api::PutResponse presponse;
    MockClosure closure;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(100, presponse.code());
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());

    ::openmldb::api::GeneralRequest grequest;
    ::openmldb::api::GeneralResponse gresponse;
    grequest.set_tid(id);
    grequest.set_pid(1);
    grequest.set_storage_mode(storage_mode);
    tablet.PauseSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());

    tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(105, gresponse.code());

    tablet.RecoverSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());

    tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
}

TEST_P(TabletImplTest, CreateTableLatestTestDefault) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kLatestTime, storage_mode, &tablet));
    // get table status
    {
        ::openmldb::api::GetTableStatusRequest request;
        ::openmldb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        if (storage_mode == ::openmldb::common::kMemory) {
            ASSERT_EQ(1, (signed)ts.skiplist_height());
        }
    }
}

TEST_P(TabletImplTest, CreateTableLatestTestSpecify) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::kTableLeader);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
        table_meta->set_key_entry_max_height(2);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::openmldb::api::GetTableStatusRequest request;
        ::openmldb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        if (storage_mode == ::openmldb::common::kMemory) {
            ASSERT_EQ(2, (signed)ts.skiplist_height());
        }
    }
}

TEST_P(TabletImplTest, CreateTableAbsoluteTestDefault) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_mode(::openmldb::api::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::openmldb::api::GetTableStatusRequest request;
        ::openmldb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        if (storage_mode == ::openmldb::common::kMemory) {
            ASSERT_EQ(4, (signed)ts.skiplist_height());
        }
    }
}

TEST_P(TabletImplTest, CreateTableAbsoluteTestSpecify) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::kTableLeader);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_key_entry_max_height(8);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::openmldb::api::GetTableStatusRequest request;
        ::openmldb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        if (storage_mode == ::openmldb::common::kMemory) {
            ASSERT_EQ(8, (signed)ts.skiplist_height());
        }
    }
}

TEST_P(TabletImplTest, CreateTableAbsAndLatTest) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 10, 20, kAbsAndLat, storage_mode, &tablet));
    // get table status
    {
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 1, "", &cur_ttl));
        ASSERT_EQ(10, cur_ttl.abs_ttl());
        ASSERT_EQ(20, cur_ttl.lat_ttl());
        ASSERT_EQ(::openmldb::type::TTLType::kAbsAndLat, cur_ttl.ttl_type());
    }
}

TEST_P(TabletImplTest, CreateTableAbsAndOrTest) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 10, 20, kAbsOrLat, storage_mode, &tablet));
    // get table status
    {
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 1, "", &cur_ttl));
        ASSERT_EQ(10, cur_ttl.abs_ttl());
        ASSERT_EQ(20, cur_ttl.lat_ttl());
        ASSERT_EQ(::openmldb::type::TTLType::kAbsOrLat, cur_ttl.ttl_type());
    }
}

TEST_P(TabletImplTest, CreateTableAbsAndLatTestSpecify) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::kTableLeader);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_key_entry_max_height(8);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::openmldb::api::GetTableStatusRequest request;
        ::openmldb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        if (storage_mode == ::openmldb::common::kMemory) {
            ASSERT_EQ(8, (signed)ts.skiplist_height());
        }
    }
}

TEST_P(TabletImplTest, GetTermPair) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    uint32_t id = counter++;
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();
    int offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    MockClosure closure;
    {
        TabletImpl tablet;
        tablet.Init("");
        ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kAbsoluteTime, storage_mode, &tablet));

        ::openmldb::api::PutRequest prequest;
        ::openmldb::api::PutResponse presponse;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        ::openmldb::api::GeneralRequest grequest;
        ::openmldb::api::GeneralResponse gresponse;
        grequest.set_tid(id);
        grequest.set_pid(1);
        grequest.set_storage_mode(storage_mode);
        tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        sleep(1);

        ::openmldb::api::GetTermPairRequest pair_request;
        ::openmldb::api::GetTermPairResponse pair_response;
        pair_request.set_tid(id);
        pair_request.set_pid(1);
        pair_request.set_storage_mode(storage_mode);
        tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
        ASSERT_EQ(0, pair_response.code());
        ASSERT_TRUE(pair_response.has_table());
        ASSERT_EQ(1, (signed)pair_response.offset());
    }
    TabletImpl tablet;
    tablet.Init("");
    ::openmldb::api::GetTermPairRequest pair_request;
    ::openmldb::api::GetTermPairResponse pair_response;
    pair_request.set_tid(id);
    pair_request.set_pid(1);
    pair_request.set_storage_mode(storage_mode);
    tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(1, (signed)pair_response.offset());

    std::string manifest_file;

    if (storage_mode == ::openmldb::common::kMemory) {
        manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
    } else if (storage_mode == ::openmldb::common::kSSD) {
        manifest_file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
    } else {
        manifest_file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
    }

    int fd = open(manifest_file.c_str(), O_RDONLY);
    ASSERT_GT(fd, 0);
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    ::openmldb::api::Manifest manifest;
    google::protobuf::TextFormat::Parse(&fileInput, &manifest);

    std::string snapshot_file;

    if (storage_mode == ::openmldb::common::kMemory) {
        snapshot_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
    } else if (storage_mode == ::openmldb::common::kSSD) {
        snapshot_file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
    } else {
        snapshot_file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
    }

    // for memtable snapshot is a file
    // for disktable snapshot is a directory
    if (storage_mode == openmldb::common::kMemory) {
        unlink(snapshot_file.c_str());
    } else {
        ::openmldb::base::RemoveDirRecursive(snapshot_file.c_str());
    }

    tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(0, (signed)pair_response.offset());
    FLAGS_make_snapshot_threshold_offset = offset;

    if (storage_mode == openmldb::common::kMemory) {
        unlink(manifest_file.c_str());
    }
}

TEST_P(TabletImplTest, MakeSnapshotThreshold) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    int offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 1, 0, kAbsoluteTime, storage_mode, &tablet));
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test1", &prequest);
        prequest.set_time(9527);
        prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::openmldb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        grq.set_storage_mode(storage_mode);
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file;

        if (storage_mode == ::openmldb::common::kMemory) {
            manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        } else if (storage_mode == ::openmldb::common::kSSD) {
            manifest_file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        } else {
            manifest_file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        }
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(1, (signed)manifest.offset());
    }
    FLAGS_make_snapshot_threshold_offset = 5;
    {
        ::openmldb::api::PutRequest prequest;
        PackDefaultDimension("test2", &prequest);
        prequest.set_time(9527);
        prequest.set_value(::openmldb::test::EncodeKV("test2", "test1"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::openmldb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        prequest.clear_dimensions();
        PackDefaultDimension("test2", &prequest);
        prequest.set_time(9527);
        prequest.set_value(::openmldb::test::EncodeKV("test2", "test2"));
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        ::openmldb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        grq.set_storage_mode(storage_mode);
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file;

        if (storage_mode == ::openmldb::common::kMemory) {
            manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        } else if (storage_mode == ::openmldb::common::kSSD) {
            manifest_file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        } else {
            manifest_file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        }
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(1, (signed)manifest.offset());
        std::string snapshot_file;

        if (storage_mode == ::openmldb::common::kMemory) {
            manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
        } else if (storage_mode == ::openmldb::common::kSSD) {
            manifest_file = FLAGS_ssd_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
        } else {
            manifest_file = FLAGS_hdd_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
        }
        unlink(snapshot_file.c_str());
        FLAGS_make_snapshot_threshold_offset = offset;
    }
}

TEST_P(TabletImplTest, UpdateTTLAbsAndLat) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int32_t old_gc_interval = FLAGS_gc_interval;
    int32_t old_disk_gc_interval = FLAGS_disk_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    FLAGS_disk_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    // create table
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 100, 50, kAbsAndLat, storage_mode, &tablet));
    // table not exist
    ASSERT_EQ(100, UpdateTTL(0, 0, ::openmldb::type::kAbsAndLat, 10, 5, &tablet));
    // bigger than max ttl, tablet side will not check
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsAndLat, 60 * 24 * 365 * 30 * 2, 5, &tablet));
    // bigger than max ttl, tablet side will not check
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsAndLat, 30, 20000, &tablet));
    // ttl type mismatch is allowed
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kLatestTime, 10, 5, &tablet));
    // normal case
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::api::PutResponse presponse;
    ::openmldb::api::PutRequest prequest;
    {
        PackDefaultDimension("test", &prequest);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test9"));
        prequest.set_time(now - 60 * 60 * 1000);
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        prequest.set_time(now - 70 * 60 * 1000);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test8"));
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    MockClosure closure;
    ::openmldb::api::GetRequest grequest;
    grequest.set_tid(id);
    grequest.set_pid(0);
    grequest.set_key("test");
    grequest.set_ts(0);
    ::openmldb::api::GetResponse gresponse;
    tablet.Get(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
    ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));
    // UpdateTTLRequest

    // ExecuteGcRequest
    ::openmldb::api::ExecuteGcRequest request_execute;
    ::openmldb::api::GeneralResponse response_execute;
    request_execute.set_tid(id);
    request_execute.set_pid(0);
    // etTableStatusRequest
    ::openmldb::api::GetTableStatusRequest gr;
    ::openmldb::api::GetTableStatusResponse gres;
    // ttl update to zero
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsAndLat, 0, 0, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(0, (signed)cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 0, 0);

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
    }
    // ttl update from zero to no zero
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsAndLat, 50, 1, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(50, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(1, (signed)cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 50, 1);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));
    }
    // update from 50 to 100
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsAndLat, 100, 2, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(2, (signed)cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 100, 2);
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
    }
    FLAGS_gc_interval = old_gc_interval;
    FLAGS_disk_gc_interval = old_disk_gc_interval;
}

TEST_P(TabletImplTest, UpdateTTLAbsOrLat) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    int32_t old_gc_interval = FLAGS_gc_interval;
    int32_t old_disk_gc_interval = FLAGS_disk_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    FLAGS_disk_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 100, 50, kAbsOrLat, storage_mode, &tablet));
    // table not exist
    ASSERT_EQ(100, UpdateTTL(0, 0, ::openmldb::type::kAbsOrLat, 10, 5, &tablet));
    // bigger than max ttl
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsOrLat, 60 * 24 * 365 * 30 * 2, 5, &tablet));
    // bigger than max ttl
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsOrLat, 30, 20000, &tablet));
    // ttl type mismatch
    ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kLatestTime, 30, 20000, &tablet));
    // normal case
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::api::PutRequest prequest;
    ::openmldb::api::PutResponse presponse;
    {
        PackDefaultDimension("test", &prequest);
        prequest.set_time(now - 60 * 60 * 1000);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test9"));
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        prequest.clear_dimensions();
        PackDefaultDimension("test", &prequest);
        prequest.set_time(now - 70 * 60 * 1000);
        prequest.set_value(::openmldb::test::EncodeKV("test", "test8"));
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    ::openmldb::api::GetRequest grequest;
    grequest.set_tid(id);
    grequest.set_pid(0);
    grequest.set_key("test");
    grequest.set_ts(0);
    ::openmldb::api::GetResponse gresponse;
    MockClosure closure;
    tablet.Get(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
    ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

    // ExecuteGcRequest
    ::openmldb::api::ExecuteGcRequest request_execute;
    ::openmldb::api::GeneralResponse response_execute;
    request_execute.set_tid(id);
    request_execute.set_pid(0);
    // etTableStatusRequest
    ::openmldb::api::GetTableStatusRequest gr;
    ::openmldb::api::GetTableStatusResponse gres;
    // ttl update to zero
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsOrLat, 0, 0, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(0, (signed)cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 0, 0);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
    }
    // ttl update from zero to no zero
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsOrLat, 10, 1, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(10, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(1, (signed)cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 10, 1);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());
    }
    // update from 10 to 100
    {
        ASSERT_EQ(0, UpdateTTL(id, 0, ::openmldb::type::kAbsOrLat, 100, 2, &tablet));
        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(2, (signed)cur_ttl.lat_ttl());
        CheckTTLFromMeta(id, 0, storage_mode, "", 100, 2);
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
    }
    FLAGS_gc_interval = old_gc_interval;
    FLAGS_disk_gc_interval = old_disk_gc_interval;
}

TEST_P(TabletImplTest, AbsAndLat) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = counter++;
    ::openmldb::api::CreateTableRequest request;
    auto table_meta = request.mutable_table_meta();
    {
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_storage_mode(storage_mode);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "test", ::openmldb::type::kString);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts1", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts2", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts3", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts4", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts5", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts6", ::openmldb::type::kBigInt);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "index0", "test", "ts1", ::openmldb::type::kAbsAndLat, 100,
                              10);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "index1", "test", "ts2", ::openmldb::type::kAbsAndLat, 50,
                              8);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "index2", "test", "ts3", ::openmldb::type::kAbsAndLat, 70,
                              5);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "index3", "test", "ts4", ::openmldb::type::kAbsAndLat, 0,
                              5);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "index4", "test", "ts5", ::openmldb::type::kAbsAndLat, 50,
                              0);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "index5", "test", "ts6", ::openmldb::type::kAbsAndLat, 0,
                              0);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    for (int i = 0; i < 100; ++i) {
        ::openmldb::api::PutResponse presponse;
        ::openmldb::api::PutRequest prequest;
        for (int idx = 0; idx < 6; idx++) {
            auto dim = prequest.add_dimensions();
            dim->set_idx(idx);
            dim->set_key("test" + std::to_string(i % 10));
        }
        uint64_t ts = now - (99 - i) * 60 * 1000;
        std::string ts_str = std::to_string(ts);
        std::vector<std::string> row = {"test" + std::to_string(i % 10), ts_str, ts_str,
            ts_str, ts_str, ts_str, ts_str};
        auto value = prequest.mutable_value();
        sdk_codec.EncodeRow(row, value);
        prequest.set_tid(id);
        prequest.set_pid(0);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    //// Traverse test
    // ts1 has no expire data traverse return 100
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("index0");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // ts2 has 20 expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("index1");
        ::openmldb::api::TraverseResponse srp;
        tablet.Traverse(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(80, (signed)srp.count());
    }
    // ts3 has 30 expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("index2");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(70, (signed)srp->count());
    }
    // ts4 has no expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("index3");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // ts5 has no expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("index4");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // ts6 has no expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("index5");
        ::openmldb::api::TraverseResponse srp;
        tablet.Traverse(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(100, (signed)srp.count());
    }
    // //// Scan Count test
    ::openmldb::api::ScanRequest sr;
    ::openmldb::api::ScanResponse srp;
    ::openmldb::api::CountRequest cr;
    ::openmldb::api::CountResponse crp;
    cr.set_filter_expired_data(true);
    //     time    cnt
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000 + 100);
        sr.set_idx_name("index0");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_idx_name("index0");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(10, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   valid
    // et  expire  valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 60 * 60 * 1000 + 100);
        sr.set_idx_name("index1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 60 * 60 * 1000 + 100);
        cr.set_idx_name("index1");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(6, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   valid
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 60 * 60 * 1000 + 100);
        sr.set_idx_name("index2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 60 * 60 * 1000 + 100);
        cr.set_idx_name("index2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(6, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   valid
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(7, (signed)crp.count());
    }
    //     time    cnt
    // st  expire  valid
    // et  expire  valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 50 * 60 * 1000 + 100);
        sr.set_et(now - 70 * 60 * 1000 + 100);
        sr.set_idx_name("index1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 50 * 60 * 1000 + 100);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_idx_name("index1");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   expire
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 50 * 60 * 1000 + 100);
        sr.set_et(now - 70 * 60 * 1000 + 100);
        sr.set_idx_name("index2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 50 * 60 * 1000 + 100);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_idx_name("index2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //     time    cnt
    // st  expire  valid
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 90 * 60 * 1000 + 100);
        sr.set_idx_name("index1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 90 * 60 * 1000 + 100);
        cr.set_idx_name("index1");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   expire
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(1, (signed)crp.count());
    }
    //     time    cnt
    // st  expire  expire
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 80 * 60 * 1000 + 100);
        sr.set_et(now - 100 * 60 * 1000 + 100);
        sr.set_idx_name("index2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 80 * 60 * 1000 + 100);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_idx_name("index2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       !0
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000 + 100);
        sr.set_idx_name("index3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_idx_name("index3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(4, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       !0
    // st  valid   valid
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(8, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(8, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       !0
    // st  valid   expire
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //     time    cnt
    // ttl !0       0
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000 + 100);
        sr.set_idx_name("index4");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_idx_name("index4");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(4, (signed)crp.count());
    }
    //     time    cnt
    // ttl !0       0
    // st  valid   valid
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index4");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(8, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index4");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(8, (signed)crp.count());
    }
    //     time    cnt
    // ttl !0       0
    // st  expire  valid
    // et  expire  valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index4");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index4");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       0
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("index5");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("index5");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //// Get test
    ::openmldb::api::GetRequest gr;
    ::openmldb::api::GetResponse grp;
    gr.set_type(::openmldb::api::kSubKeyLe);
    //     time    cnt
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_idx_name("index0");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ts  expire  valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 70 * 60 * 1000 + 100);
        gr.set_idx_name("index1");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ts  valid   expire
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 60 * 60 * 1000 + 100);
        gr.set_idx_name("index2");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ts  expire  expire
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 90 * 60 * 1000 + 100);
        gr.set_idx_name("index2");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(109, grp.code());
    }
    //     time    cnt
    // ttl 0       !0
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 10 * 60 * 1000 + 100);
        gr.set_idx_name("index3");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ttl 0       !0
    // ts  valid   expire
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_idx_name("index3");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ttl !0      0
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 10 * 60 * 1000 + 100);
        gr.set_idx_name("index4");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ttl !0      0
    // ts  expire  valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_idx_name("index4");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ttl 0      0
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 10 * 60 * 1000 + 100);
        gr.set_idx_name("index5");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(8, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index2");
        sr.set_limit(9);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
    }
}

TEST_P(TabletImplTest, AbsOrLat) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = counter++;
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    {
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_storage_mode(storage_mode);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "test", ::openmldb::type::kString);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts1", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts2", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts3", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts4", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts5", ::openmldb::type::kBigInt);
        SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts6", ::openmldb::type::kBigInt);

        SchemaCodec::SetIndex(table_meta->add_column_key(), "ts1", "test", "ts1", ::openmldb::type::kAbsOrLat, 100, 10);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "ts2", "test", "ts2", ::openmldb::type::kAbsOrLat, 50, 8);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "ts3", "test", "ts3", ::openmldb::type::kAbsOrLat, 70, 6);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "ts4", "test", "ts4", ::openmldb::type::kAbsOrLat, 0, 5);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "ts5", "test", "ts5", ::openmldb::type::kAbsOrLat, 50, 0);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "ts6", "test", "ts6", ::openmldb::type::kAbsOrLat, 0, 0);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 100; ++i) {
        ::openmldb::api::PutResponse presponse;
        ::openmldb::api::PutRequest prequest;
        for (int idx = 0; idx < 6; idx++) {
            auto dim = prequest.add_dimensions();
            dim->set_idx(idx);
            dim->set_key("test" + std::to_string(i % 10));
        }
        std::string ts_str = std::to_string(now - (99 - i) * 60 * 1000);
        std::vector<std::string> row = {"test" + std::to_string(i % 10), ts_str, ts_str,
                ts_str, ts_str, ts_str, ts_str};
        auto value = prequest.mutable_value();
        sdk_codec.EncodeRow(row, value);
        prequest.set_tid(id);
        prequest.set_pid(0);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("ts1");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("ts2");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(50, (signed)srp->count());
    }
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("ts3");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(60, (signed)srp->count());
    }
    // ts4 has 50 expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("ts4");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(50, (signed)srp->count());
    }
    // ts5 has 50 expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("ts5");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(50, (signed)srp->count());
    }
    // ts6 has no expire
    {
        ::openmldb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_idx_name("ts6");
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }

    // //// Scan Count test
    ::openmldb::api::ScanRequest sr;
    ::openmldb::api::ScanResponse srp;
    ::openmldb::api::CountRequest cr;
    ::openmldb::api::CountResponse crp;
    cr.set_filter_expired_data(true);
    //     time    cnt
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000 + 100);
        sr.set_idx_name("ts1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_idx_name("ts1");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(10, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   valid
    // et  expire  valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 60 * 60 * 1000 + 100);
        sr.set_idx_name("ts2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 60 * 60 * 1000 + 100);
        cr.set_idx_name("ts2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(5, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   valid
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 70 * 60 * 1000 + 100);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_idx_name("ts3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(6, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   valid
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(6, (signed)crp.count());
    }
    //     time    cnt
    // st  expire  valid
    // et  expire  valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(307, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   expire
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 70 * 60 * 1000 + 100);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_idx_name("ts3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // st  expire  valid
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 90 * 60 * 1000 + 100);
        sr.set_idx_name("ts2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 90 * 60 * 1000 + 100);
        cr.set_idx_name("ts2");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(307, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // st  valid   expire
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // st  expire  expire
    // et  expire  expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 80 * 60 * 1000 + 100);
        sr.set_et(now - 100 * 60 * 1000 + 100);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 80 * 60 * 1000 + 100);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_idx_name("ts3");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(307, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       !0
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000 + 100);
        sr.set_idx_name("ts4");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_idx_name("ts4");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(4, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       !0
    // st  valid   valid
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts4");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts4");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(5, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       !0
    // st  valid   expire
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts4");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts4");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // ttl !0       0
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000 + 100);
        sr.set_idx_name("ts5");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_idx_name("ts5");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(4, (signed)crp.count());
    }
    //     time    cnt
    // ttl !0       0
    // st  valid   valid
    // et  valid   expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts5");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts5");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(5, (signed)crp.count());
    }
    //     time    cnt
    // ttl !0       0
    // st  expire  valid
    // et  expire  valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts5");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts5");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(307, crp.code());
        ASSERT_EQ(0, (signed)crp.count());
    }
    //     time    cnt
    // ttl 0       0
    // st  valid   valid
    // et  valid   valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 60 * 60 * 1000 + 100);
        sr.set_et(now - 80 * 60 * 1000 + 100);
        sr.set_idx_name("ts6");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_idx_name("ts6");
        cr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //// Get test
    ::openmldb::api::GetRequest gr;
    ::openmldb::api::GetResponse grp;
    gr.set_type(::openmldb::api::kSubKeyLe);
    //     time    cnt
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_idx_name("ts1");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ts  expire  valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 70 * 60 * 1000 + 100);
        gr.set_idx_name("ts2");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(307, grp.code());
    }
    //     time    cnt
    // ts  valid   expire
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 60 * 60 * 1000 + 100);
        gr.set_idx_name("ts3");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(109, grp.code());
    }
    //     time    cnt
    // ts  expire  expire
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 90 * 60 * 1000 + 100);
        gr.set_idx_name("ts3");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(307, grp.code());
    }
    //     time    cnt
    // ttl 0       !0
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 10 * 60 * 1000 + 100);
        gr.set_idx_name("ts4");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ttl 0       !0
    // ts  valid   expire
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_idx_name("ts4");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(109, grp.code());
    }
    //     time    cnt
    // ttl !0      0
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 10 * 60 * 1000 + 100);
        gr.set_idx_name("ts5");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    //     time    cnt
    // ttl !0      0
    // ts  expire  valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_idx_name("ts5");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(307, grp.code());
    }
    //     time    cnt
    // ttl 0      0
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 10 * 60 * 1000 + 100);
        gr.set_idx_name("ts6");
        tablet.Get(NULL, &gr, &grp, &closure);
        ASSERT_EQ(0, grp.code());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts2");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts3");
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        sr.set_limit(4);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts2");
        sr.set_limit(4);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts3");
        sr.set_limit(4);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts2");
        sr.set_limit(9);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts3");
        sr.set_limit(9);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
}

TEST_F(TabletImplTest, DelRecycleMem) {
    std::string tmp_recycle_bin_root_path = FLAGS_recycle_bin_root_path;
    FLAGS_recycle_ttl = 1;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_recycle_bin_root_path = tmp_path.GetTempPath("recycle");
    ::openmldb::base::MkdirRecur(FLAGS_recycle_bin_root_path + "/99_1_binlog_20191111070955/binlog/");
    ::openmldb::base::MkdirRecur(FLAGS_recycle_bin_root_path + "/100_2_20191111115149/binlog/");
    TabletImpl tablet;
    tablet.Init("");

    {
        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
        ASSERT_EQ(2, (signed)file_vec.size());
    }

    {
        LOG(INFO) << "sleep for 30s" << std::endl;
        sleep(30);
        std::string now_time = ::openmldb::base::GetNowTime();
        ::openmldb::base::MkdirRecur(absl::StrCat(FLAGS_recycle_bin_root_path, "/99_3_", now_time, "/binlog/"));
        ::openmldb::base::MkdirRecur(absl::StrCat(FLAGS_recycle_bin_root_path, "/100_4_binlog_", now_time, "/binlog/"));
        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
        ASSERT_EQ(4, (signed)file_vec.size());
    }

    {
        LOG(INFO) << "sleep for 35s" << std::endl;
        sleep(35);
        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
        ASSERT_EQ(2, (signed)file_vec.size());
    }

    {
        LOG(INFO) << "sleep for 60s" << std::endl;
        sleep(60);
        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
        ASSERT_EQ(0, (signed)file_vec.size());
    }
}

TEST_F(TabletImplTest, DelRecycleDisk) {
    std::string tmp_recycle_bin_hdd_root_path = FLAGS_recycle_bin_hdd_root_path;
    FLAGS_recycle_ttl = 1;
    ::openmldb::test::TempPath tmp_path;
    FLAGS_recycle_bin_hdd_root_path = tmp_path.GetTempPath("recycle");
    ::openmldb::base::MkdirRecur(absl::StrCat(FLAGS_recycle_bin_hdd_root_path, "/99_1_binlog_20191111070955/binlog/"));
    ::openmldb::base::MkdirRecur(absl::StrCat(FLAGS_recycle_bin_hdd_root_path, "/100_2_20191111115149/binlog/"));
    TabletImpl tablet;
    tablet.Init("");

    {
        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_hdd_root_path, file_vec);
        ASSERT_EQ(2, (signed)file_vec.size());
    }
    {
        LOG(INFO) << "sleep for 30s" << std::endl;
        sleep(30);

        std::string now_time = ::openmldb::base::GetNowTime();
        ::openmldb::base::MkdirRecur(absl::StrCat(FLAGS_recycle_bin_hdd_root_path, "/99_3_", now_time, "/binlog/"));
        ::openmldb::base::MkdirRecur(absl::StrCat(FLAGS_recycle_bin_hdd_root_path,
                    "/100_4_binlog_", now_time, "/binlog/"));

        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_hdd_root_path, file_vec);
        ASSERT_EQ(4, (signed)file_vec.size());
    }

    {
        LOG(INFO) << "sleep for 35s" << std::endl;
        sleep(35);

        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_hdd_root_path, file_vec);
        ASSERT_EQ(2, (signed)file_vec.size());
    }

    {
        LOG(INFO) << "sleep for 60s" << std::endl;
        sleep(60);

        std::vector<std::string> file_vec;
        ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_hdd_root_path, file_vec);
        ASSERT_EQ(0, (signed)file_vec.size());
    }
}

TEST_P(TabletImplTest,  SendIndexData) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    // only support Memtable now
    if (storage_mode != openmldb::common::kMemory) {
        GTEST_SKIP();
    }
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = counter++;
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 0, 0, 0, kLatestTime, storage_mode, &tablet));
    ASSERT_EQ(0, CreateDefaultTable("", "t0", id, 1, 0, 0, kLatestTime, storage_mode, &tablet));
    std::string index_file_path = FLAGS_db_root_path + "/" + std::to_string(id) + "_0/index/";
    ::openmldb::base::MkdirRecur(index_file_path);
    std::string index_file = index_file_path + "0_1_index.data";
    FILE* f = fopen(index_file.c_str(), "w+");
    ASSERT_TRUE(f != NULL);
    for (int i = 0; i < 1000; ++i) {
        fputc('6', f);
    }
    fclose(f);
    uint64_t src_size = 0;
    ::openmldb::base::GetFileSize(index_file, src_size);
    ::openmldb::api::SendIndexDataRequest request;
    request.set_tid(id);
    request.set_pid(0);
    ::openmldb::api::SendIndexDataRequest_EndpointPair* pair = request.add_pairs();
    pair->set_pid(1);
    pair->set_endpoint(FLAGS_endpoint);
    ::openmldb::api::GeneralResponse response;
    tablet.SendIndexData(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    sleep(2);
    std::string des_index_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/index/0_1_index.data";
    uint64_t des_size = 0;
    ::openmldb::base::GetFileSize(des_index_file, des_size);
    ASSERT_TRUE(::openmldb::base::IsExists(des_index_file));
    ASSERT_EQ(src_size, des_size);
}

TEST_P(TabletImplTest, BulkLoad) {
    ::openmldb::common::StorageMode storage_mode = GetParam();

    // only support Memtable now
    if (storage_mode != openmldb::common::kMemory) {
        GTEST_SKIP();
    }
    // create table, empty data
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        auto column = table_meta->add_column_desc();
        column->set_name("card");
        column->set_data_type(::openmldb::type::kString);
        column = table_meta->add_column_desc();
        column->set_name("amt");
        column->set_data_type(::openmldb::type::kString);
        column = table_meta->add_column_desc();
        column->set_name("apprv_cde");
        column->set_data_type(::openmldb::type::kInt);
        column = table_meta->add_column_desc();
        column->set_name("ts");
        column->set_data_type(::openmldb::type::kTimestamp);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "ts", ::openmldb::type::kAbsoluteTime, 0,
                              0);
        SchemaCodec::SetIndex(table_meta->add_column_key(), "amt", "amt", "ts", ::openmldb::type::kAbsoluteTime, 0, 0);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // get bulk load info
    {
        ::openmldb::api::BulkLoadInfoRequest request;
        request.set_tid(id);
        request.set_pid(1);
        ::openmldb::api::BulkLoadInfoResponse response;
        MockClosure closure;
        brpc::Controller cntl;
        tablet.GetBulkLoadInfo(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code()) << response.msg();
        LOG(INFO) << "info: " << response.DebugString();
    }

    // hash test
    {
        const uint32_t SEED = 0xe17a1465;
        std::vector<std::string> keys = {"2|1", "1|1", "1|4", "2/6", "4", "6", "1"};
        for (auto key : keys) {
            LOG(INFO) << "hash(" << key << ") = " << ::openmldb::base::hash(key.data(), key.size(), SEED) % 8;
        }
    }

    // handle a bulk load request data part
    {
        ::openmldb::api::BulkLoadRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_part_id(0);
        auto block_info = request.add_block_info();
        block_info->set_ref_cnt(3);
        block_info->set_offset(0);
        block_info->set_length(3);
        auto binlog_info = request.add_binlog_info();
        binlog_info->set_block_id(0);
        ::openmldb::api::GeneralResponse response;
        MockClosure closure;
        brpc::Controller cntl;
        cntl.request_attachment().append("123");
        tablet.BulkLoad(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code()) << response.msg();
    }

    // index part
    {
        ::openmldb::api::BulkLoadRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_part_id(1);
        auto index1 = request.add_index_region();
        auto seg1 = index1->add_segment();
        auto entry = seg1->add_key_entries()->add_key_entry()->add_time_entry();
        entry->set_block_id(0);
        request.add_index_region();
        ::openmldb::api::GeneralResponse response;
        MockClosure closure;
        brpc::Controller cntl;
        tablet.BulkLoad(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code()) << response.msg();
    }

    // TODO(hw): bulk load meaningful data, and get data from the table
}

TEST_P(TabletImplTest, AddIndex) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_db("db1");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_seg_cnt(1);
    table_meta->set_storage_mode(storage_mode);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "card", ::openmldb::type::kVarchar);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "mcc", ::openmldb::type::kVarchar);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "", ::openmldb::type::kAbsoluteTime, 20, 0);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::openmldb::api::AddIndexRequest add_index_request;
    ::openmldb::api::GeneralResponse add_index_response;
    add_index_request.set_tid(id);
    add_index_request.set_pid(1);
    SchemaCodec::SetIndex(add_index_request.mutable_column_key(), "mcc", "mcc", "ts1",
            ::openmldb::type::kAbsoluteTime, 20, 0);
    tablet.AddIndex(NULL, &add_index_request, &add_index_response, &closure);
    // Some functions in tablet_impl only support memtable now
    // refer to issue #1438
    if (storage_mode != openmldb::common::kMemory) {
        ASSERT_EQ(701, add_index_response.code());
    } else {
        ASSERT_EQ(0, add_index_response.code());

        uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
        uint64_t key_base = 10000;
        ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
        for (int i = 0; i < 10; i++) {
            uint64_t ts_value = cur_time - 10 * 60 * 1000 - i;
            uint64_t ts1_value = cur_time - i;
            std::vector<std::string> row = {"card" + std::to_string(key_base + i), "mcc" + std::to_string(key_base + i),
                                            "12", std::to_string(ts_value), std::to_string(ts1_value)};
            ::openmldb::api::PutRequest prequest;
            ::openmldb::api::Dimension* dim = prequest.add_dimensions();
            dim->set_idx(0);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(1);
            dim->set_key("mcc" + std::to_string(key_base + i));
            auto value = prequest.mutable_value();
            sdk_codec.EncodeRow(row, value);
            prequest.set_tid(id);
            prequest.set_pid(1);
            prequest.set_time(cur_time);
            ::openmldb::api::PutResponse presponse;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("card10005");
        sr.set_idx_name("card");
        sr.set_st(0);
        sr.set_et(0);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());

        sr.set_pk("mcc10005");
        sr.set_idx_name("mcc");
        sr.set_st(0);
        sr.set_et(0);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
    }
}

TEST_P(TabletImplTest, CountWithFilterExpire) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    // create table
    MockClosure closure;
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(0, 5, ::openmldb::type::TTLType::kLatestTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        PrepareLatestTableData(tablet, id, 0);
    }

    {
        //
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (int32_t)response.count());
    }

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(5, (int32_t)response.count());
    }
}

TEST_P(TabletImplTest, PutCompress) {
    ::openmldb::common::StorageMode storage_mode = GetParam();
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_storage_mode(storage_mode);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        table_meta->set_compress_type(::openmldb::type::CompressType::kSnappy);
        AddDefaultSchema(0, 5, ::openmldb::type::TTLType::kLatestTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        PrepareLatestTableData(tablet, id, 0, true);
    }

    {
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (int32_t)response.count());
    }
}

INSTANTIATE_TEST_SUITE_P(TabletMemAndHDD, TabletImplTest,
                         ::testing::Values(::openmldb::common::kMemory, /*::openmldb::common::kSSD,*/
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
        DLOG(INFO) << "base_table_id: " << base_table_id << ", aggr_table_id: " << aggr_table_id;
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
        auto result = ScanFromTablet(aggr_table_id, 1, "id1", "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        ASSERT_EQ(0, result.second);
        auto aggrs = tablet.GetAggregators(base_table_id, 1);
        ASSERT_EQ(aggrs->size(), 1);
        auto aggr = aggrs->at(0);
        ::openmldb::storage::AggrBuffer* aggr_buffer;
        ASSERT_TRUE(aggr->GetAggrBuffer("id1", &aggr_buffer));
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
        auto result = ScanFromTablet(aggr_table_id, 1, "id1", "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        ASSERT_EQ(49, result.second);
        result = ScanFromTablet(aggr_table_id, 1, "id2", "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        // 50 = 49 (the number of aggr value) + 1 (the number of out-of-order put)
        ASSERT_EQ(50, result.second);
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

TEST_F(TabletImplTest, AggregatorConcurrentPut) {
    uint32_t aggr_table_id;
    uint32_t base_table_id;
    int max_counter = 1000;
    int thread_num = 8;
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
        ::openmldb::api::TableMeta* aggr_table_meta = request.mutable_table_meta();
        aggr_table_meta->Clear();
        aggr_table_meta->set_tid(id);
        AddDefaultAggregatorSchema(aggr_table_meta);
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

        auto put_data = [&](const std::string& key, std::atomic<int>* counter, int max_counter) {
            int i = (*counter)++;
            while (i <= max_counter) {
                ::openmldb::api::PutRequest prequest;
                ::openmldb::test::SetDimension(0, key, prequest.add_dimensions());
                prequest.set_time(i);
                prequest.set_value(EncodeAggrRow(key, i, i));
                prequest.set_tid(base_table_id);
                prequest.set_pid(1);
                ::openmldb::api::PutResponse presponse;
                MockClosure closure;
                tablet.Put(NULL, &prequest, &presponse, &closure);
                ASSERT_EQ(0, presponse.code());

                i = (*counter)++;
            }
        };

        std::atomic<int> id1_counter = 1;
        std::atomic<int> id2_counter = 1;
        std::vector<std::thread> threads;
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back(put_data, "id1", &id1_counter, max_counter);
        }
        for (int i = 0; i < thread_num; i++) {
            threads.emplace_back(put_data, "id2", &id2_counter, max_counter);
        }

        for (size_t i = 0; i < threads.size(); i++) {
            threads[i].join();
        }

        int64_t total_val = 0;
        int total_cnt = 0;
        uint64_t max_offset = 0;
        for (int i = 1; i <= 2; i++) {
            std::string key = absl::StrCat("id", i);
            ::openmldb::api::ScanRequest sr;
            sr.set_tid(aggr_table_id);
            sr.set_pid(1);
            sr.set_pk(key);
            sr.set_st(max_counter);
            sr.set_et(0);
            std::shared_ptr<::openmldb::api::ScanResponse> srp = std::make_shared<::openmldb::api::ScanResponse>();
            tablet.Scan(nullptr, &sr, srp.get(), &closure);
            ASSERT_EQ(0, srp->code());
            ASSERT_LE(max_counter / 2 - 1, (signed)srp->count());

            ::openmldb::base::ScanKvIterator kv_it(key, srp);
            codec::RowView row_view(aggr_table_meta->column_desc());
            uint64_t last_k = 0;
            while (kv_it.Valid()) {
                uint64_t k = kv_it.GetKey();
                const int8_t* row_ptr = reinterpret_cast<const int8_t*>(kv_it.GetValue().data());
                openmldb::storage::AggrBuffer buffer;
                row_view.GetValue(row_ptr, 1, openmldb::type::DataType::kTimestamp, &buffer.ts_begin_);
                row_view.GetValue(row_ptr, 2, openmldb::type::DataType::kTimestamp, &buffer.ts_end_);
                row_view.GetValue(row_ptr, 3, openmldb::type::DataType::kInt, &buffer.aggr_cnt_);
                char* aggr_val = nullptr;
                uint32_t ch_length = 0;
                row_view.GetValue(row_ptr, 4, &aggr_val, &ch_length);
                buffer.aggr_val_.vlong = *reinterpret_cast<int64_t*>(aggr_val);
                row_view.GetValue(row_ptr, 5, openmldb::type::DataType::kBigInt, &buffer.binlog_offset_);

                max_offset = std::max(max_offset, buffer.binlog_offset_);
                if (last_k != k) {
                    total_val += buffer.aggr_val_.vlong;
                    total_cnt += buffer.aggr_cnt_;
                    last_k = k;
                }

                kv_it.Next();
            }
            ASSERT_GE(max_offset, max_counter);

            auto aggrs = tablet.GetAggregators(base_table_id, 1);
            ASSERT_EQ(aggrs->size(), 1);
            auto aggr = aggrs->at(0);
            ::openmldb::storage::AggrBuffer* aggr_buffer;
            aggr->GetAggrBuffer(key, &aggr_buffer);

            max_offset = std::max(max_offset, aggr_buffer->binlog_offset_);
            total_val += aggr_buffer->aggr_val_.vlong;
            total_cnt += aggr_buffer->aggr_cnt_;
        }
        ASSERT_EQ(total_val, (1 + max_counter) * max_counter / 2 * 2);
        ASSERT_EQ(total_cnt, max_counter * 2);
        ASSERT_EQ(max_offset, max_counter * 2);

        ::openmldb::api::DropTableRequest dr;
        dr.set_tid(base_table_id);
        dr.set_pid(1);
        ::openmldb::api::DropTableResponse drs;
        tablet.DropTable(nullptr, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
        dr.set_tid(aggr_table_id);
        dr.set_pid(1);
        tablet.DropTable(nullptr, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
    }
}

TEST_F(TabletImplTest, AggregatorDeleteKey) {
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

        // put data to base table
        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            for (int32_t i = 1; i <= 100; i++) {
                ::openmldb::api::PutRequest prequest;
                ::openmldb::test::SetDimension(0, key, prequest.add_dimensions());
                prequest.set_time(i);
                prequest.set_value(EncodeAggrRow("id1", i, i));
                prequest.set_tid(base_table_id);
                prequest.set_pid(1);
                ::openmldb::api::PutResponse presponse;
                MockClosure closure;
                tablet.Put(NULL, &prequest, &presponse, &closure);
                ASSERT_EQ(0, presponse.code());
            }
        }

        // check the base table
        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            auto result = ScanFromTablet(base_table_id, 1, key, "", 100, 0, &tablet);
            ASSERT_EQ(0, result.first);
            ASSERT_EQ(100, result.second);
        }

        // check the pre-aggr table
        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            auto result = ScanFromTablet(aggr_table_id, 1, key, "", 100, 0, &tablet);
            ASSERT_EQ(0, result.first);
            ASSERT_EQ(49, result.second);

            auto aggrs = tablet.GetAggregators(base_table_id, 1);
            ASSERT_EQ(aggrs->size(), 1);
            auto aggr = aggrs->at(0);
            ::openmldb::storage::AggrBuffer* aggr_buffer;
            aggr->GetAggrBuffer(key, &aggr_buffer);
            ASSERT_EQ(aggr_buffer->aggr_cnt_, 2);
            ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 199);
            ASSERT_EQ(aggr_buffer->binlog_offset_, 100 * k);
        }

        // delete key id1
        ::openmldb::api::DeleteRequest dr;
        ::openmldb::api::GeneralResponse res;
        dr.set_tid(base_table_id);
        dr.set_pid(1);
        dr.set_key("id1");
        dr.set_idx_name("idx1");
        tablet.Delete(NULL, &dr, &res, &closure);
        ASSERT_EQ(0, res.code());

        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            auto result = ScanFromTablet(base_table_id, 1, key, "", 100, 0, &tablet);
            ASSERT_EQ(0, result.first);
            ASSERT_EQ(k == 1 ? 0 : 100, result.second);
        }

        // check the pre-aggr table
        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            auto result = ScanFromTablet(aggr_table_id, 1, key, "", 100, 0, &tablet);
            ASSERT_EQ(0, result.first);
            auto aggrs = tablet.GetAggregators(base_table_id, 1);
            ASSERT_EQ(aggrs->size(), 1);
            auto aggr = aggrs->at(0);
            ::openmldb::storage::AggrBuffer* aggr_buffer = nullptr;
            if (k == 1) {
                ASSERT_EQ(0, result.second);
                ASSERT_FALSE(aggr->GetAggrBuffer(key, &aggr_buffer));
                ASSERT_EQ(nullptr, aggr_buffer);
            } else {
                ASSERT_EQ(49, result.second);
                aggr->GetAggrBuffer(key, &aggr_buffer);
                ASSERT_EQ(aggr_buffer->aggr_cnt_, 2);
                ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 199);
                ASSERT_EQ(aggr_buffer->binlog_offset_, 100 * k);
            }
        }
    }

    // after re-start, the states are still correct
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

        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            auto result = ScanFromTablet(base_table_id, 1, key, "", 100, 0, &tablet);
            ASSERT_EQ(0, result.first);
            ASSERT_EQ(k == 1 ? 0 : 100, result.second);
        }

        // check the pre-aggr table
        for (int32_t k = 1; k <= 2; k++) {
            std::string key = absl::StrCat("id", k);
            auto result = ScanFromTablet(aggr_table_id, 1, key, "", 100, 0, &tablet);
            ASSERT_EQ(0, result.first);
            auto aggrs = tablet.GetAggregators(base_table_id, 1);
            ASSERT_EQ(aggrs->size(), 1);
            auto aggr = aggrs->at(0);
            ::openmldb::storage::AggrBuffer* aggr_buffer = nullptr;
            if (k == 1) {
                ASSERT_EQ(0, result.second);
                ASSERT_FALSE(aggr->GetAggrBuffer(key, &aggr_buffer));
                ASSERT_EQ(nullptr, aggr_buffer);
            } else {
                ASSERT_EQ(49, result.second);
                aggr->GetAggrBuffer(key, &aggr_buffer);
                ASSERT_EQ(aggr_buffer->aggr_cnt_, 2);
                ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 199);
                ASSERT_EQ(aggr_buffer->binlog_offset_, 100 * k);
            }
        }
    }
}

struct DeleteInputParm {
    DeleteInputParm() = default;
    DeleteInputParm(const std::string& pk, const std::optional<uint64_t>& start_ts_i,
            const std::optional<uint64_t>& end_ts_i) : key(pk), start_ts(start_ts_i), end_ts(end_ts_i) {}
    std::string key;
    std::optional<uint64_t> start_ts = std::nullopt;
    std::optional<uint64_t> end_ts = std::nullopt;
};

struct DeleteExpectParm {
    DeleteExpectParm() = default;
    DeleteExpectParm(uint64_t base_t_cnt, uint64_t agg_t_cnt, uint64_t agg_cnt, uint64_t value, uint64_t t_value) :
        base_table_cnt(base_t_cnt), aggr_table_cnt(agg_t_cnt), aggr_cnt(agg_cnt),
        aggr_buffer_value(value), aggr_table_value(t_value) {}
    uint64_t base_table_cnt = 0;
    uint64_t aggr_table_cnt = 0;
    uint32_t aggr_cnt = 0;
    uint64_t aggr_buffer_value = 0;
    uint64_t aggr_table_value = 0;
};

struct DeleteParm {
    DeleteParm(const DeleteInputParm& input_p, const DeleteExpectParm& expect_p) : input(input_p), expect(expect_p) {}
    DeleteInputParm input;
    DeleteExpectParm expect;
};

class AggregatorDeleteTest : public ::testing::TestWithParam<DeleteParm> {};

TEST_P(AggregatorDeleteTest, AggregatorDeleteRange) {
    uint32_t aggr_table_id = 0;
    uint32_t base_table_id = 0;
    const auto& parm = GetParam();
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
    ::openmldb::api::TableMeta agg_table_meta;
    table_meta = request.mutable_table_meta();
    table_meta->Clear();
    table_meta->set_tid(id);
    AddDefaultAggregatorSchema(table_meta);
    agg_table_meta.CopyFrom(*table_meta);
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
    aggr_request.set_bucket_size("5");
    ::openmldb::api::CreateAggregatorResponse aggr_response;
    tablet.CreateAggregator(NULL, &aggr_request, &aggr_response, &closure);
    ASSERT_EQ(0, response.code());

    // put data to base table
    for (int32_t k = 1; k <= 2; k++) {
        std::string key = absl::StrCat("id", k);
        for (int32_t i = 1; i <= 100; i++) {
            ::openmldb::api::PutRequest prequest;
            ::openmldb::test::SetDimension(0, key, prequest.add_dimensions());
            prequest.set_time(i);
            prequest.set_value(EncodeAggrRow("id1", i, i));
            prequest.set_tid(base_table_id);
            prequest.set_pid(1);
            ::openmldb::api::PutResponse presponse;
            MockClosure closure;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }

    // check the base table
    for (int32_t k = 1; k <= 2; k++) {
        std::string key = absl::StrCat("id", k);
        auto result = ScanFromTablet(base_table_id, 1, key, "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        ASSERT_EQ(100, result.second);
    }

    // check the pre-aggr table
    for (int32_t k = 1; k <= 2; k++) {
        std::string key = absl::StrCat("id", k);
        auto result = ScanFromTablet(aggr_table_id, 1, key, "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        ASSERT_EQ(19, result.second);

        auto aggrs = tablet.GetAggregators(base_table_id, 1);
        ASSERT_EQ(aggrs->size(), 1);
        auto aggr = aggrs->at(0);
        ::openmldb::storage::AggrBuffer* aggr_buffer;
        aggr->GetAggrBuffer(key, &aggr_buffer);
        ASSERT_EQ(aggr_buffer->aggr_cnt_, 5);
        ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 490);
        ASSERT_EQ(aggr_buffer->binlog_offset_, 100 * k);
    }

    // delete key id1
    ::openmldb::api::DeleteRequest dr;
    ::openmldb::api::GeneralResponse res;
    dr.set_tid(base_table_id);
    dr.set_pid(1);
    auto dim = dr.add_dimensions();
    dim->set_idx(0);
    dim->set_key(parm.input.key);
    if (parm.input.start_ts.has_value()) {
        dr.set_ts(parm.input.start_ts.value());
    }
    if (parm.input.end_ts.has_value()) {
        dr.set_end_ts(parm.input.end_ts.value());
    }
    tablet.Delete(NULL, &dr, &res, &closure);
    ASSERT_EQ(0, res.code());

    for (int32_t k = 1; k <= 2; k++) {
        std::string key = absl::StrCat("id", k);
        auto result = ScanFromTablet(base_table_id, 1, key, "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        if (k == 1) {
            ASSERT_EQ(result.second, parm.expect.base_table_cnt);
        } else {
            ASSERT_EQ(result.second, 100);
        }
    }

    // check the pre-aggr table
    for (int32_t k = 1; k <= 2; k++) {
        std::string key = absl::StrCat("id", k);
        auto result = ScanFromTablet(aggr_table_id, 1, key, "", 100, 0, &tablet);
        ASSERT_EQ(0, result.first);
        auto aggrs = tablet.GetAggregators(base_table_id, 1);
        ASSERT_EQ(aggrs->size(), 1);
        auto aggr = aggrs->at(0);
        ::openmldb::storage::AggrBuffer* aggr_buffer = nullptr;
        if (k == 1) {
            ASSERT_EQ(result.second, parm.expect.aggr_table_cnt);
            ASSERT_TRUE(aggr->GetAggrBuffer(key, &aggr_buffer));
            ASSERT_EQ(aggr_buffer->aggr_cnt_, parm.expect.aggr_cnt);
            ASSERT_EQ(aggr_buffer->aggr_val_.vlong, parm.expect.aggr_buffer_value);
        } else {
            ASSERT_EQ(19, result.second);
            aggr->GetAggrBuffer(key, &aggr_buffer);
            ASSERT_EQ(aggr_buffer->aggr_cnt_, 5);
            ASSERT_EQ(aggr_buffer->aggr_val_.vlong, 490);
            ASSERT_EQ(aggr_buffer->binlog_offset_, 100 * k);
        }
    }
    for (int i = 1; i <= 2; i++) {
        std::string key = absl::StrCat("id", i);
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(aggr_table_id);
        sr.set_pid(1);
        sr.set_pk(key);
        sr.set_st(100);
        sr.set_et(0);
        std::shared_ptr<::openmldb::api::ScanResponse> srp = std::make_shared<::openmldb::api::ScanResponse>();
        tablet.Scan(nullptr, &sr, srp.get(), &closure);
        ASSERT_EQ(0, srp->code());

        ::openmldb::base::ScanKvIterator kv_it(key, srp);
        codec::RowView row_view(agg_table_meta.column_desc());
        uint64_t last_k = 0;
        int64_t total_val = 0;
        while (kv_it.Valid()) {
            uint64_t k = kv_it.GetKey();
            if (last_k != k) {
                const int8_t* row_ptr = reinterpret_cast<const int8_t*>(kv_it.GetValue().data());
                char* aggr_val = nullptr;
                uint32_t ch_length = 0;
                ASSERT_EQ(row_view.GetValue(row_ptr, 4, &aggr_val, &ch_length), 0);
                int64_t val = *reinterpret_cast<int64_t*>(aggr_val);
                total_val += val;
                last_k = k;
            }
            kv_it.Next();
        }
        if (i == 1) {
            ASSERT_EQ(total_val, parm.expect.aggr_table_value);
        } else {
            ASSERT_EQ(total_val, 4560);
        }
    }
}

// [st, et]
uint64_t ComputeAgg(uint64_t st, uint64_t et) {
    uint64_t val = 0;
    for (auto i = st; i <= et; i++) {
        val += i;
    }
    return val;
}

std::vector<DeleteParm> delete_cases = {
    /*0*/ DeleteParm(DeleteInputParm("id1", std::nullopt, 200),
            DeleteExpectParm(100, 19, 5, ComputeAgg(96, 100), ComputeAgg(1, 95))),
    /*1*/ DeleteParm(DeleteInputParm("id1", std::nullopt, 100),
            DeleteExpectParm(100, 19, 5, ComputeAgg(96, 100), ComputeAgg(1, 95))),
    /*2*/ DeleteParm(DeleteInputParm("id1", 200, 100),
            DeleteExpectParm(100, 19, 5, ComputeAgg(96, 100), ComputeAgg(1, 95))),
    /*3*/ DeleteParm(DeleteInputParm("id1", 200, 99),
            DeleteExpectParm(99, 19, 4, ComputeAgg(96, 99), ComputeAgg(1, 95))),
    /*4*/ DeleteParm(DeleteInputParm("id1", 200, 98),
            DeleteExpectParm(98, 19, 3, ComputeAgg(96, 98), ComputeAgg(1, 95))),
    /*5*/ DeleteParm(DeleteInputParm("id1", 99, 97),
            DeleteExpectParm(98, 19, 3, 100 + 96 + 97, ComputeAgg(1, 95))),
    /*6*/ DeleteParm(DeleteInputParm("id1", 98, 96),
            DeleteExpectParm(98, 19, 3, 100 + 99 + 96, ComputeAgg(1, 95))),
    /*7*/ DeleteParm(DeleteInputParm("id1", 98, 95),
            DeleteExpectParm(97, 19, 2, 100 + 99, ComputeAgg(1, 95))),
    /*8*/ DeleteParm(DeleteInputParm("id1", 95, 94),
            DeleteExpectParm(99, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 94))),
    /*9*/ DeleteParm(DeleteInputParm("id1", 95, 91),
            DeleteExpectParm(96, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 91))),
    /*10*/ DeleteParm(DeleteInputParm("id1", 95, 90),
            DeleteExpectParm(95, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 90))),
    /*11*/ DeleteParm(DeleteInputParm("id1", 95, 89),
            DeleteExpectParm(94, 21, 5, ComputeAgg(96, 100), ComputeAgg(1, 89))),
    /*12*/ DeleteParm(DeleteInputParm("id1", 95, 86),
            DeleteExpectParm(91, 21, 5, ComputeAgg(96, 100), ComputeAgg(1, 86))),
    /*13*/ DeleteParm(DeleteInputParm("id1", 95, 85),
            DeleteExpectParm(90, 19, 5, ComputeAgg(96, 100), ComputeAgg(1, 85))),
    /*14*/ DeleteParm(DeleteInputParm("id1", 95, 84),
            DeleteExpectParm(89, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 84))),
    /*15*/ DeleteParm(DeleteInputParm("id1", 95, 81),
            DeleteExpectParm(86, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 81))),
    /*16*/ DeleteParm(DeleteInputParm("id1", 95, 80),
            DeleteExpectParm(85, 18, 5, ComputeAgg(96, 100), ComputeAgg(1, 80))),
    /*17*/ DeleteParm(DeleteInputParm("id1", 95, 79),
            DeleteExpectParm(84, 19, 5, ComputeAgg(96, 100), ComputeAgg(1, 79))),
    /*18*/ DeleteParm(DeleteInputParm("id1", 78, 76),
            DeleteExpectParm(98, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 95) - 78 - 77)),
    /*19*/ DeleteParm(DeleteInputParm("id1", 80, 75),
            DeleteExpectParm(95, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 95) - ComputeAgg(76, 80))),
    /*20*/ DeleteParm(DeleteInputParm("id1", 80, 74),
            DeleteExpectParm(94, 21, 5, ComputeAgg(96, 100), ComputeAgg(1, 95) - ComputeAgg(75, 80))),
    /*21*/ DeleteParm(DeleteInputParm("id1", 80, 68),
            DeleteExpectParm(88, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 68) + ComputeAgg(81, 95))),
    /*22*/ DeleteParm(DeleteInputParm("id1", 80, 58),
            DeleteExpectParm(78, 18, 5, ComputeAgg(96, 100), ComputeAgg(1, 58) + ComputeAgg(81, 95))),
    /*23*/ DeleteParm(DeleteInputParm("id1", 100, 94), DeleteExpectParm(94, 20, 0, 0, ComputeAgg(1, 94))),
    /*24*/ DeleteParm(DeleteInputParm("id1", 100, 91), DeleteExpectParm(91, 20, 0, 0, ComputeAgg(1, 91))),
    /*25*/ DeleteParm(DeleteInputParm("id1", 100, 90), DeleteExpectParm(90, 20, 0, 0, ComputeAgg(1, 90))),
    /*26*/ DeleteParm(DeleteInputParm("id1", 100, 89), DeleteExpectParm(89, 21, 0, 0, ComputeAgg(1, 89))),
    /*27*/ DeleteParm(DeleteInputParm("id1", 100, 85), DeleteExpectParm(85, 19, 0, 0, ComputeAgg(1, 85))),
    /*28*/ DeleteParm(DeleteInputParm("id1", 100, 84), DeleteExpectParm(84, 20, 0, 0, ComputeAgg(1, 84))),
    /*29*/ DeleteParm(DeleteInputParm("id1", 99, 84), DeleteExpectParm(85, 20, 1, 100, ComputeAgg(1, 84))),
    /*30*/ DeleteParm(DeleteInputParm("id1", 96, 84),
            DeleteExpectParm(88, 20, 4, ComputeAgg(97, 100), ComputeAgg(1, 84))),
    /*31*/ DeleteParm(DeleteInputParm("id1", 2, 1),
            DeleteExpectParm(99, 20, 5, ComputeAgg(96, 100), ComputeAgg(1, 95) - 2)),
    /*32*/ DeleteParm(DeleteInputParm("id1", 2, std::nullopt),
            DeleteExpectParm(98, 20, 5, ComputeAgg(96, 100), ComputeAgg(3, 95))),
    /*33*/ DeleteParm(DeleteInputParm("id1", 5, std::nullopt),
            DeleteExpectParm(95, 20, 5, ComputeAgg(96, 100), ComputeAgg(6, 95))),
    /*34*/ DeleteParm(DeleteInputParm("id1", 6, std::nullopt),
            DeleteExpectParm(94, 19, 5, ComputeAgg(96, 100), ComputeAgg(7, 95))),
    /*35*/ DeleteParm(DeleteInputParm("id1", 6, 0),
            DeleteExpectParm(94, 19, 5, ComputeAgg(96, 100), ComputeAgg(7, 95))),
    /*36*/ DeleteParm(DeleteInputParm("id1", 6, 1),
            DeleteExpectParm(95, 21, 5, ComputeAgg(96, 100), ComputeAgg(7, 95) + 1)),
    /*37*/ DeleteParm(DeleteInputParm("id1", 10, 1),
            DeleteExpectParm(91, 21, 5, ComputeAgg(96, 100), ComputeAgg(11, 95) + 1)),
    /*38*/ DeleteParm(DeleteInputParm("id1", 11, 1),
            DeleteExpectParm(90, 20, 5, ComputeAgg(96, 100), ComputeAgg(12, 95) + 1)),
    /*39*/ DeleteParm(DeleteInputParm("id1", 11, 0),
            DeleteExpectParm(89, 18, 5, ComputeAgg(96, 100), ComputeAgg(12, 95))),
    /*40*/ DeleteParm(DeleteInputParm("id1", 11, std::nullopt),
            DeleteExpectParm(89, 18, 5, ComputeAgg(96, 100), ComputeAgg(12, 95))),
    /*41*/ DeleteParm(DeleteInputParm("id1", 100, std::nullopt), DeleteExpectParm(0, 2, 0, 0, 0)),
    /*42*/ DeleteParm(DeleteInputParm("id1", 100, 0), DeleteExpectParm(0, 2, 0, 0, 0)),
    /*43*/ DeleteParm(DeleteInputParm("id1", std::nullopt, 0), DeleteExpectParm(0, 2, 0, 0, 0)),
};

INSTANTIATE_TEST_SUITE_P(AggregatorTest, AggregatorDeleteTest, testing::ValuesIn(delete_cases));

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::AddGlobalTestEnvironment(new ::openmldb::tablet::DiskTestEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    FLAGS_ssd_root_path = tmp_path.GetTempPath("ssd");
    FLAGS_hdd_root_path = tmp_path.GetTempPath("hdd");
    FLAGS_recycle_bin_root_path = tmp_path.GetTempPath("recycle");
    FLAGS_recycle_bin_ssd_root_path = tmp_path.GetTempPath("recycle");
    FLAGS_recycle_bin_hdd_root_path = tmp_path.GetTempPath("recycle");
    FLAGS_recycle_bin_enabled = true;
    return RUN_ALL_TESTS();
}
