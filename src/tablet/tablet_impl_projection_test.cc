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
#include "base/glog_wrapper.h"
#include "base/kv_iterator.h"
#include "base/strings.h"
#include "brpc/channel.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "storage/mem_table.h"
#include "storage/ticket.h"
#include "tablet/tablet_impl.h"
#include "vm/engine.h"

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

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

using ::openmldb::api::TableStatus;

struct TestArgs {
    openmldb::common::StorageMode storage_mode;
    Schema schema;
    common::ColumnKey ckey;
    std::string pk;
    uint64_t ts;
    codec::ProjectList plist;
    std::string input_row;
    std::string output_row;
    Schema output_schema;
    ::openmldb::common::TTLSt ttl_desc;
    TestArgs() : schema(), plist(), output_schema() {}
    ~TestArgs() {}
};

class DiskTestEnvironment : public ::testing::Environment{
    virtual void SetUp() {
        std::vector<std::string> file_path;
        ::openmldb::base::SplitString(FLAGS_hdd_root_path, ",", file_path);
        for (uint32_t i = 0; i < file_path.size(); i++) {
            ::openmldb::base::RemoveDirRecursive(file_path[i]);
        }
        ::openmldb::base::SplitString(FLAGS_recycle_bin_hdd_root_path, ",", file_path);
        for (uint32_t i = 0; i < file_path.size(); i++) {
            ::openmldb::base::RemoveDirRecursive(file_path[i]);
        }
    }
    virtual void TearDown() {
        std::vector<std::string> file_path;
        ::openmldb::base::SplitString(FLAGS_hdd_root_path, ",", file_path);
        for (uint32_t i = 0; i < file_path.size(); i++) {
            ::openmldb::base::RemoveDirRecursive(file_path[i]);
        }
        ::openmldb::base::SplitString(FLAGS_recycle_bin_hdd_root_path, ",", file_path);
        for (uint32_t i = 0; i < file_path.size(); i++) {
            ::openmldb::base::RemoveDirRecursive(file_path[i]);
        }
    }
};

class TabletProjectTest : public ::testing::TestWithParam<TestArgs*> {
 public:
    TabletProjectTest() {}
    ~TabletProjectTest() {}
    void SetUp() { tablet_.Init(""); }

 public:
    ::openmldb::tablet::TabletImpl tablet_;
};

std::vector<TestArgs*> GenCommonCase() {
    std::vector<TestArgs*> args;
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kString);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col2", ::openmldb::type::kBigInt);
        testargs->ckey.set_index_name("col1");
        testargs->ckey.add_col_name("col1");
        testargs->ckey.set_ts_name("col2");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"hello", "1000", "32"}, testargs->schema, 1, testargs->input_row);
        codec::RowCodec::EncodeRow({"1000"}, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 1;
        testargs->storage_mode = openmldb::common::kMemory;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kString);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col2", ::openmldb::type::kBigInt);
        testargs->ckey.set_index_name("col1");
        testargs->ckey.add_col_name("col1");
        testargs->ckey.set_ts_name("col2");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"hello", "1000", "32"}, testargs->schema, 1, testargs->input_row);
        codec::RowCodec::EncodeRow({"1000"}, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 1;
        testargs->storage_mode = openmldb::common::kHDD;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col4", ::openmldb::type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "2", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        codec::RowCodec::EncodeRow({"hello"}, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        testargs->storage_mode = openmldb::common::kMemory;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col4", ::openmldb::type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "2", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        codec::RowCodec::EncodeRow({"hello"}, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        testargs->storage_mode = openmldb::common::kHDD;
        args.push_back(testargs);
    }

    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col3", ::openmldb::type::kBigInt);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "2", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        std::vector<std::string> values = {"hello", "1000"};
        codec::RowCodec::EncodeRow(values, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->storage_mode = openmldb::common::kMemory;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col3", ::openmldb::type::kBigInt);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "2", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        std::vector<std::string> values = {"hello", "1000"};
        codec::RowCodec::EncodeRow(values, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->storage_mode = openmldb::common::kHDD;
        args.push_back(testargs);
    }
    // add null
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col2", ::openmldb::type::kInt);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "null", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        std::vector<std::string> values = {"hello", "null"};
        codec::RowCodec::EncodeRow(values, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 1;
        testargs->storage_mode = openmldb::common::kMemory;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col2", ::openmldb::type::kInt);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);

        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "null", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        std::vector<std::string> values = {"hello", "null"};
        codec::RowCodec::EncodeRow(values, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 1;
        testargs->storage_mode = openmldb::common::kHDD;
        args.push_back(testargs);
    }
    // add null str
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col2", ::openmldb::type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);
        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "null", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        std::vector<std::string> values = {"1000", "null"};
        codec::RowCodec::EncodeRow(values, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 2;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 1;
        testargs->storage_mode = openmldb::common::kMemory;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col1", ::openmldb::type::kSmallInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col2", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->schema.Add(), "col4", ::openmldb::type::kVarchar);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col3", ::openmldb::type::kBigInt);
        codec::SchemaCodec::SetColumnDesc(testargs->output_schema.Add(), "col2", ::openmldb::type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.set_ts_name("col3");
        auto ttl = testargs->ckey.mutable_ttl();
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);
        testargs->pk = "hello";
        testargs->ts = 1000l;
        codec::RowCodec::EncodeRow({"1", "null", "1000", "hello"}, testargs->schema, 1, testargs->input_row);
        std::vector<std::string> values = {"1000", "null"};
        codec::RowCodec::EncodeRow(values, testargs->output_schema, 1, testargs->output_row);

        uint32_t* idx = testargs->plist.Add();
        *idx = 2;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 1;
        testargs->storage_mode = openmldb::common::kHDD;
        args.push_back(testargs);
    }
    return args;
}
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

void CompareRow(codec::RowView* left, codec::RowView* right, const Schema& schema) {
    for (int32_t i = 0; i < schema.size(); i++) {
        uint32_t idx = (uint32_t)i;
        const common::ColumnDesc& column = schema.Get(i);
        ASSERT_EQ(left->IsNULL(idx), right->IsNULL(i));
        if (left->IsNULL(idx)) continue;
        int32_t ret = 0;
        switch (column.data_type()) {
            case ::openmldb::type::kBool: {
                bool left_val = false;
                bool right_val = false;
                ret = left->GetBool(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetBool(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }

            case ::openmldb::type::kSmallInt: {
                int16_t left_val = 0;
                int16_t right_val = 0;
                ret = left->GetInt16(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt16(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }

            case ::openmldb::type::kInt: {
                int32_t left_val = 0;
                int32_t right_val = 0;
                ret = left->GetInt32(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt32(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kTimestamp:
            case ::openmldb::type::kBigInt: {
                int64_t left_val = 0;
                int64_t right_val = 0;
                ret = left->GetInt64(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt64(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kFloat: {
                float left_val = 0;
                float right_val = 0;
                ret = left->GetFloat(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetFloat(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kDouble: {
                double left_val = 0;
                double right_val = 0;
                ret = left->GetDouble(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetDouble(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kVarchar: {
                char* left_val = NULL;
                uint32_t left_size = 0;
                char* right_val = NULL;
                uint32_t right_size = 0;
                ret = left->GetString(idx, &left_val, &left_size);
                ASSERT_EQ(0, ret);
                ret = right->GetString(idx, &right_val, &right_size);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_size, right_size);
                std::string left_str(left_val, left_size);
                std::string right_str(right_val, right_size);
                ASSERT_EQ(left_str, right_str);
                break;
            }
            default: {
                PDLOG(WARNING, "not supported type");
            }
        }
    }
}

TEST_P(TabletProjectTest, get_case) {
    auto args = GetParam();
    // create table
    std::string name = ::openmldb::tablet::GenRand();
    int tid = rand() % 100000;  // NOLINT
    MockClosure closure;
    // create a table
    {
        ::openmldb::api::CreateTableRequest crequest;
        ::openmldb::api::TableMeta* table_meta = crequest.mutable_table_meta();
        table_meta->set_name(name);
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        table_meta->set_key_entry_max_height(8);
        table_meta->set_format_version(1);
        table_meta->set_storage_mode(args->storage_mode);
        Schema* schema = table_meta->mutable_column_desc();
        schema->CopyFrom(args->schema);
        ::openmldb::common::ColumnKey* ck = table_meta->add_column_key();
        ck->CopyFrom(args->ckey);
        ::openmldb::api::CreateTableResponse cresponse;
        tablet_.CreateTable(NULL, &crequest, &cresponse, &closure);
        ASSERT_EQ(0, cresponse.code());
    }
    // put a record
    {
        ::openmldb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(0);
        request.set_format_version(1);
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string key = args->pk;
        dim->set_key(key);
        request.set_value(args->input_row);
        ::openmldb::api::PutResponse response;
        tablet_.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get with projectlist
    {
        ::openmldb::api::GetRequest sr;
        sr.set_tid(tid);
        sr.set_pid(0);
        sr.set_key(args->pk);
        sr.set_ts(args->ts);
        sr.set_et(0);
        sr.mutable_projection()->CopyFrom(args->plist);
        ::openmldb::api::GetResponse srp;
        tablet_.Get(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(srp.value().size(), args->output_row.size());
        codec::RowView left(args->output_schema);
        left.Reset(reinterpret_cast<const int8_t*>(srp.value().c_str()), srp.value().size());
        codec::RowView right(args->output_schema);
        right.Reset(reinterpret_cast<int8_t*>(args->output_row.data()), args->output_row.size());
        CompareRow(&left, &right, args->output_schema);
    }
}
TEST_P(TabletProjectTest, sql_case) {
    auto args = GetParam();
    // create table
    std::string name = "t" + ::openmldb::tablet::GenRand();
    std::string db = "db" + name;
    int tid = rand() % 10000000;  // NOLINT
    MockClosure closure;
    // create a table
    {
        ::openmldb::api::CreateTableRequest crequest;
        ::openmldb::api::TableMeta* table_meta = crequest.mutable_table_meta();
        table_meta->set_db(db);
        table_meta->set_name(name);
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        table_meta->set_key_entry_max_height(8);
        table_meta->set_storage_mode(args->storage_mode);
        table_meta->set_format_version(1);
        Schema* schema = table_meta->mutable_column_desc();
        schema->CopyFrom(args->schema);
        ::openmldb::common::ColumnKey* ck = table_meta->add_column_key();
        ck->CopyFrom(args->ckey);
        ::openmldb::api::CreateTableResponse cresponse;
        tablet_.CreateTable(NULL, &crequest, &cresponse, &closure);
        ASSERT_EQ(0, cresponse.code());
    }
    // put a record
    {
        ::openmldb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(0);
        request.set_format_version(1);
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string key = args->pk;
        dim->set_key(key);
        request.set_value(args->input_row);
        ::openmldb::api::PutResponse response;
        tablet_.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    {
        ::openmldb::api::QueryRequest request;
        request.set_db(db);
        std::string sql = "select col1 from " + name + ";";
        request.set_sql(sql);
        request.set_is_batch(true);
        brpc::Controller cntl;
        ::openmldb::api::QueryResponse response;
        tablet_.Query(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (int32_t)response.count());
    }
}

TEST_P(TabletProjectTest, scan_case) {
    auto args = GetParam();
    // create table
    std::string name = ::openmldb::tablet::GenRand();
    int tid = rand() % 10000000;  // NOLINT
    MockClosure closure;
    // create a table
    {
        ::openmldb::api::CreateTableRequest crequest;
        ::openmldb::api::TableMeta* table_meta = crequest.mutable_table_meta();
        table_meta->set_name(name);
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        table_meta->set_key_entry_max_height(8);
        table_meta->set_format_version(1);
        table_meta->set_storage_mode(args->storage_mode);
        Schema* schema = table_meta->mutable_column_desc();
        schema->CopyFrom(args->schema);
        ::openmldb::common::ColumnKey* ck = table_meta->add_column_key();
        ck->CopyFrom(args->ckey);
        ::openmldb::api::CreateTableResponse cresponse;
        tablet_.CreateTable(NULL, &crequest, &cresponse, &closure);
        ASSERT_EQ(0, cresponse.code());
    }
    // put a record
    {
        ::openmldb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(0);
        request.set_format_version(1);
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string key = args->pk;
        dim->set_key(key);
        request.set_value(args->input_row);
        ::openmldb::api::PutResponse response;
        tablet_.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // scan with projectlist
    {
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(tid);
        sr.set_pid(0);
        sr.set_pk(args->pk);
        sr.set_st(args->ts);
        sr.set_et(0);
        sr.mutable_projection()->CopyFrom(args->plist);
        auto srp = std::make_shared<::openmldb::api::ScanResponse>();
        tablet_.Scan(NULL, &sr, srp.get(), &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(1, (int64_t)srp->count());
        ::openmldb::base::ScanKvIterator kv_it(args->pk, srp);
        ASSERT_TRUE(kv_it.Valid());
        ASSERT_EQ(kv_it.GetValue().size(), args->output_row.size());
        codec::RowView left(args->output_schema);
        left.Reset(reinterpret_cast<const int8_t*>(kv_it.GetValue().data()), kv_it.GetValue().size());
        codec::RowView right(args->output_schema);
        right.Reset(reinterpret_cast<int8_t*>(args->output_row.data()), args->output_row.size());
        CompareRow(&left, &right, args->output_schema);
    }
}

INSTANTIATE_TEST_SUITE_P(TabletProjectPrefix, TabletProjectTest, testing::ValuesIn(GenCommonCase()));

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::AddGlobalTestEnvironment(new ::openmldb::tablet::DiskTestEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    std::string k1 = ::openmldb::tablet::GenRand();
    std::string k2 = ::openmldb::tablet::GenRand();
    FLAGS_db_root_path = "/tmp/db" + k1 + ",/tmp/db" + k2;
    FLAGS_hdd_root_path = "/tmp/hdd/db" + k1 + ",/tmp/hdd/db" + k2;
    FLAGS_recycle_bin_root_path = "/tmp/recycle" + k1 + ",/tmp/recycle" + k2;
    FLAGS_recycle_bin_hdd_root_path = "/tmp/hdd/recycle" + k1 + ",/tmp/hdd/recycle" + k2;
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
