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

#include <boost/lexical_cast.hpp>

#include "base/file_util.h"
#include "base/glog_wapper.h"  //NOLINT
#include "base/kv_iterator.h"
#include "base/strings.h"
#include "brpc/channel.h"
#include "codec/flat_array.h"
#include "codec/schema_codec.h"
#include "gtest/gtest.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "storage/mem_table.h"
#include "storage/ticket.h"
#include "tablet/tablet_impl.h"
#include "timer.h"  // NOLINT
#include "vm/engine.h"

DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(recycle_ssd_bin_root_path);
DECLARE_string(recycle_hdd_bin_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(gc_interval);
DECLARE_int32(gc_safe_offset);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_int32(binlog_delete_interval);

namespace fedb {
namespace tablet {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

using ::fedb::api::TableStatus;

struct TestArgs {
    Schema schema;
    common::ColumnKey ckey;
    std::string pk;
    uint64_t ts;
    codec::ProjectList plist;
    void* row_ptr;
    uint32_t row_size;
    void* out_ptr;
    uint32_t out_size;
    Schema output_schema;
    api::TTLDesc ttl_desc;
    TestArgs()
        : schema(),
          plist(),
          row_ptr(NULL),
          row_size(0),
          out_ptr(NULL),
          out_size(0),
          output_schema() {}
    ~TestArgs() {}
};

class TabletProjectTest : public ::testing::TestWithParam<TestArgs*> {
 public:
    TabletProjectTest() {}
    ~TabletProjectTest() {}
    void SetUp() { tablet_.Init(""); }

 public:
    ::fedb::tablet::TabletImpl tablet_;
};

std::vector<TestArgs*> GenCommonCase() {
    std::vector<TestArgs*> args;
    {
        TestArgs* testargs = new TestArgs();

        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kVarchar);

        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kBigInt);
        column2->set_lat_ttl(0);
        column2->set_is_ts_col(true);
        column2->set_type("int64");

        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kInt);

        common::ColumnDesc* column4 = testargs->output_schema.Add();
        column4->set_name("col2");
        column4->set_data_type(type::kBigInt);

        testargs->ckey.set_index_name("col1");
        testargs->ckey.add_col_name("col1");
        testargs->ckey.add_ts_name("col2");

        testargs->pk = "hello";
        testargs->ts = 1000l;

        codec::RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(testargs->pk.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr),
                           input_row_size);
        input_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());
        input_rb.AppendInt64(testargs->ts);
        int32_t i = 32;
        input_rb.AppendInt32(i);

        codec::RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(0);
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr),
                            output_row_size);
        output_rb.AppendInt64(testargs->ts);
        uint32_t* idx = testargs->plist.Add();
        *idx = 1;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        column3->set_lat_ttl(0);
        column3->set_is_ts_col(true);
        column3->set_type("int64");
        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.add_ts_name("col3");

        testargs->pk = "hello";
        testargs->ts = 1000l;

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        codec::RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(testargs->pk.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr),
                           input_row_size);
        int16_t c1 = 1;
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);
        input_rb.AppendInt64(testargs->ts);
        input_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());
        codec::RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size =
            output_rb.CalTotalLength(testargs->pk.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr),
                            output_row_size);
        output_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }

    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        column3->set_lat_ttl(0);
        column3->set_is_ts_col(true);
        column3->set_type("int64");

        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.add_ts_name("col3");

        testargs->pk = "hello";
        testargs->ts = 1000l;

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        common::ColumnDesc* column6 = testargs->output_schema.Add();
        column6->set_name("col3");
        column6->set_data_type(type::kBigInt);

        codec::RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(testargs->pk.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr),
                           input_row_size);
        int16_t c1 = 1;
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);
        input_rb.AppendInt64(testargs->ts);
        input_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());

        codec::RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size =
            output_rb.CalTotalLength(testargs->pk.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr),
                            output_row_size);
        output_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());
        output_rb.AppendInt64(testargs->ts);
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    // add null
    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        column3->set_lat_ttl(0);
        column3->set_is_ts_col(true);
        column3->set_type("int64");

        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.add_ts_name("col3");

        testargs->pk = "hello";
        testargs->ts = 1000l;

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        common::ColumnDesc* column6 = testargs->output_schema.Add();
        column6->set_name("col3");
        column6->set_data_type(type::kBigInt);

        codec::RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(testargs->pk.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr),
                           input_row_size);
        int16_t c1 = 1;
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);
        input_rb.AppendNULL();
        input_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());

        codec::RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size =
            output_rb.CalTotalLength(testargs->pk.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr),
                            output_row_size);
        output_rb.AppendString(testargs->pk.c_str(), testargs->pk.size());
        output_rb.AppendNULL();
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    // add null str
    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        column3->set_lat_ttl(0);
        column3->set_is_ts_col(true);
        column3->set_type("int64");

        testargs->ckey.set_index_name("col4");
        testargs->ckey.add_col_name("col4");
        testargs->ckey.add_ts_name("col3");
        testargs->pk = "hello";
        testargs->ts = 1000l;

        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        common::ColumnDesc* column6 = testargs->output_schema.Add();
        column6->set_name("col3");
        column6->set_data_type(type::kBigInt);

        codec::RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(0);
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr),
                           input_row_size);
        int16_t c1 = 1;
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);
        input_rb.AppendInt64(testargs->ts);
        input_rb.AppendNULL();

        codec::RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(0);
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr),
                            output_row_size);
        output_rb.AppendNULL();
        output_rb.AppendInt64(testargs->ts);
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    return args;
}
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1); // NOLINT
}

void CompareRow(codec::RowView* left, codec::RowView* right,
                const Schema& schema) {
    for (int32_t i = 0; i < schema.size(); i++) {
        uint32_t idx = (uint32_t)i;
        const common::ColumnDesc& column = schema.Get(i);
        ASSERT_EQ(left->IsNULL(idx), right->IsNULL(i));
        if (left->IsNULL(idx)) continue;
        int32_t ret = 0;
        switch (column.data_type()) {
            case ::fedb::type::kBool: {
                bool left_val = false;
                bool right_val = false;
                ret = left->GetBool(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetBool(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }

            case ::fedb::type::kSmallInt: {
                int16_t left_val = 0;
                int16_t right_val = 0;
                ret = left->GetInt16(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt16(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }

            case ::fedb::type::kInt: {
                int32_t left_val = 0;
                int32_t right_val = 0;
                ret = left->GetInt32(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt32(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::fedb::type::kTimestamp:
            case ::fedb::type::kBigInt: {
                int64_t left_val = 0;
                int64_t right_val = 0;
                ret = left->GetInt64(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt64(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::fedb::type::kFloat: {
                float left_val = 0;
                float right_val = 0;
                ret = left->GetFloat(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetFloat(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::fedb::type::kDouble: {
                double left_val = 0;
                double right_val = 0;
                ret = left->GetDouble(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetDouble(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::fedb::type::kVarchar: {
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
    std::string name = ::fedb::tablet::GenRand();
    int tid = rand() % 100000;  // NOLINT
    MockClosure closure;
    // create a table
    {
        ::fedb::api::CreateTableRequest crequest;
        ::fedb::api::TableMeta* table_meta = crequest.mutable_table_meta();
        table_meta->set_name(name);
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        table_meta->set_key_entry_max_height(8);
        table_meta->set_format_version(1);
        Schema* schema = table_meta->mutable_column_desc();
        schema->CopyFrom(args->schema);
        ::fedb::common::ColumnKey* ck = table_meta->add_column_key();
        ck->CopyFrom(args->ckey);
        ::fedb::api::CreateTableResponse cresponse;
        tablet_.CreateTable(NULL, &crequest, &cresponse, &closure);
        ASSERT_EQ(0, cresponse.code());
    }
    // put a record
    {
        ::fedb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(0);
        request.set_format_version(1);
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string key = args->pk;
        dim->set_key(key);
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(args->ts);
        request.set_value(reinterpret_cast<char*>(args->row_ptr),
                          args->row_size);
        ::fedb::api::PutResponse response;
        tablet_.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get with projectlist
    {
        ::fedb::api::GetRequest sr;
        sr.set_tid(tid);
        sr.set_pid(0);
        sr.set_key(args->pk);
        sr.set_ts(args->ts);
        sr.set_et(0);
        sr.mutable_projection()->CopyFrom(args->plist);
        ::fedb::api::GetResponse srp;
        tablet_.Get(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(srp.value().size(), args->out_size);
        codec::RowView left(args->output_schema);
        left.Reset(reinterpret_cast<const int8_t*>(srp.value().c_str()),
                   srp.value().size());
        codec::RowView right(args->output_schema);
        right.Reset(reinterpret_cast<int8_t*>(args->out_ptr), args->out_size);
        CompareRow(&left, &right, args->output_schema);
    }
}
TEST_P(TabletProjectTest, sql_case) {
    auto args = GetParam();
    // create table
    std::string name = "t" + ::fedb::tablet::GenRand();
    std::string db = "db" + name;
    int tid = rand() % 10000000;  // NOLINT
    MockClosure closure;
    // create a table
    {
        ::fedb::api::CreateTableRequest crequest;
        ::fedb::api::TableMeta* table_meta = crequest.mutable_table_meta();
        table_meta->set_db(db);
        table_meta->set_name(name);
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        table_meta->set_key_entry_max_height(8);
        table_meta->set_format_version(1);
        Schema* schema = table_meta->mutable_column_desc();
        schema->CopyFrom(args->schema);
        ::fedb::common::ColumnKey* ck = table_meta->add_column_key();
        ck->CopyFrom(args->ckey);
        ::fedb::api::CreateTableResponse cresponse;
        tablet_.CreateTable(NULL, &crequest, &cresponse, &closure);
        ASSERT_EQ(0, cresponse.code());
    }
    // put a record
    {
        ::fedb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(0);
        request.set_format_version(1);
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string key = args->pk;
        dim->set_key(key);
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(args->ts);
        request.set_value(reinterpret_cast<char*>(args->row_ptr),
                          args->row_size);
        ::fedb::api::PutResponse response;
        tablet_.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    {
        ::fedb::api::QueryRequest request;
        request.set_db(db);
        std::string sql = "select col1 from " + name + ";";
        request.set_sql(sql);
        request.set_is_batch(true);
        brpc::Controller cntl;
        ::fedb::api::QueryResponse response;
        tablet_.Query(&cntl, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (int32_t)response.count());
    }
}

TEST_P(TabletProjectTest, scan_case) {
    auto args = GetParam();
    // create table
    std::string name = ::fedb::tablet::GenRand();
    int tid = rand() % 10000000;  // NOLINT
    MockClosure closure;
    // create a table
    {
        ::fedb::api::CreateTableRequest crequest;
        ::fedb::api::TableMeta* table_meta = crequest.mutable_table_meta();
        table_meta->set_name(name);
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        table_meta->set_key_entry_max_height(8);
        table_meta->set_format_version(1);
        Schema* schema = table_meta->mutable_column_desc();
        schema->CopyFrom(args->schema);
        ::fedb::common::ColumnKey* ck = table_meta->add_column_key();
        ck->CopyFrom(args->ckey);
        ::fedb::api::CreateTableResponse cresponse;
        tablet_.CreateTable(NULL, &crequest, &cresponse, &closure);
        ASSERT_EQ(0, cresponse.code());
    }
    // put a record
    {
        ::fedb::api::PutRequest request;
        request.set_tid(tid);
        request.set_pid(0);
        request.set_format_version(1);
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        std::string key = args->pk;
        dim->set_key(key);
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(args->ts);
        request.set_value(reinterpret_cast<char*>(args->row_ptr),
                          args->row_size);
        ::fedb::api::PutResponse response;
        tablet_.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // scan with projectlist
    {
        ::fedb::api::ScanRequest sr;
        sr.set_tid(tid);
        sr.set_pid(0);
        sr.set_pk(args->pk);
        sr.set_st(args->ts);
        sr.set_et(0);
        sr.mutable_projection()->CopyFrom(args->plist);
        ::fedb::api::ScanResponse srp;
        tablet_.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (int64_t)srp.count());
        ::fedb::base::KvIterator* kv_it = new ::fedb::base::KvIterator(&srp);
        ASSERT_TRUE(kv_it->Valid());
        ASSERT_EQ(kv_it->GetValue().size(), args->out_size);
        codec::RowView left(args->output_schema);
        left.Reset(reinterpret_cast<const int8_t*>(kv_it->GetValue().data()),
                   kv_it->GetValue().size());
        codec::RowView right(args->output_schema);
        right.Reset(reinterpret_cast<int8_t*>(args->out_ptr), args->out_size);
        CompareRow(&left, &right, args->output_schema);
    }
}

INSTANTIATE_TEST_SUITE_P(TabletProjectPrefix, TabletProjectTest,
                        testing::ValuesIn(GenCommonCase()));

}  // namespace tablet
}  // namespace fedb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    std::string k1 = ::fedb::tablet::GenRand();
    std::string k2 = ::fedb::tablet::GenRand();
    FLAGS_ssd_root_path = "/tmp/ssd" + k1 + ",/tmp/ssd" + k2;
    FLAGS_db_root_path = "/tmp/db" + k1 + ",/tmp/db" + k2;
    FLAGS_hdd_root_path = "/tmp/hdd" + k1 + ",/tmp/hdd" + k2;
    FLAGS_recycle_bin_root_path = "/tmp/recycle" + k1 + ",/tmp/recycle" + k2;
    FLAGS_recycle_ssd_bin_root_path =
        "/tmp/ssd_recycle" + k1 + ",/tmp/ssd_recycle" + k2;
    FLAGS_recycle_hdd_bin_root_path =
        "/tmp/hdd_recycle" + k1 + ",/tmp/hdd_recycle" + k2;
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
