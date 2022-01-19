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
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(gc_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_int32(binlog_delete_interval);
DECLARE_uint32(max_traverse_cnt);
DECLARE_bool(recycle_bin_enabled);
DECLARE_string(db_root_path);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(endpoint);
DECLARE_uint32(recycle_ttl);

namespace openmldb {
namespace tablet {

using ::openmldb::api::TableStatus;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;
using ::openmldb::codec::SchemaCodec;

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

class TabletImplTest : public ::testing::Test {
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

void PackDefaultDimension(const std::string& key, ::openmldb::api::PutRequest* request) {
    auto dimension = request->add_dimensions();
    dimension->set_key(key);
    dimension->set_idx(0);
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

TEST_F(TabletImplTest, Count_Latest_Table) {
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
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
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
        ASSERT_EQ(10u, response.count());
    }

    {
        //
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
        //
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(100u, response.count());
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

TEST_F(TabletImplTest, Count_Time_Table) {
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
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
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
        //
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
        //
        ::openmldb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        ::openmldb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(100, (signed)response.count());
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

TEST_F(TabletImplTest, SCAN_latest_table) {
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
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(0, 5, ::openmldb::type::TTLType::kLatestTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        PrepareLatestTableData(tablet, id, 0);
    }

    // scan with default type
    {
        std::string key = "1";
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk(key);
        sr.set_st(92);
        sr.set_et(90);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
        ::openmldb::base::KvIterator* kv_it = new ::openmldb::base::KvIterator(&srp);
        ASSERT_TRUE(kv_it->Valid());
        ASSERT_EQ(92l, (signed)kv_it->GetKey());
        ASSERT_STREQ("91", ::openmldb::test::DecodeV(kv_it->GetValue().ToString()).c_str());
        kv_it->Next();
        ASSERT_FALSE(kv_it->Valid());
    }

    // scan with default et ge
    {
        ::openmldb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("1");
        sr.set_st(92);
        sr.set_et(0);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        ::openmldb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        ::openmldb::base::KvIterator* kv_it = new ::openmldb::base::KvIterator(&srp);
        ASSERT_TRUE(kv_it->Valid());
        ASSERT_EQ(92l, (signed)kv_it->GetKey());
        ASSERT_STREQ("91", ::openmldb::test::DecodeV(kv_it->GetValue().ToString()).c_str());
        kv_it->Next();
        ASSERT_TRUE(kv_it->Valid());
    }
}

TEST_F(TabletImplTest, Get) {
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
    // create table
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(1, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
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
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t1");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(0, 5, ::openmldb::type::TTLType::kLatestTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
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

TEST_F(TabletImplTest, UpdateTTLAbsoluteTime) {
    int32_t old_gc_interval = FLAGS_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    // create table
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        AddDefaultSchema(100, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
        ttl->set_abs_ttl(0);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // bigger than max ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
        ttl->set_abs_ttl(60 * 24 * 365 * 30 * 2);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
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
    // UpdateTTLRequest
    ::openmldb::api::UpdateTTLRequest request;
    request.set_tid(id);
    request.set_pid(0);
    ::openmldb::api::UpdateTTLResponse response;
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
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
        ttl->set_abs_ttl(0);
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, cur_ttl.abs_ttl());
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, cur_ttl.abs_ttl());
    }
    // ttl update from zero to no zero
    {
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
        ttl->set_abs_ttl(50);
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, cur_ttl.abs_ttl());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(50, cur_ttl.abs_ttl());
    }
    // update from 50 to 100
    {
        auto ttl = request.mutable_ttl();
        ttl->set_abs_ttl(100);
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(50, cur_ttl.abs_ttl());
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, cur_ttl.abs_ttl());
    }
    FLAGS_gc_interval = old_gc_interval;
}

TEST_F(TabletImplTest, UpdateTTLLatest) {
    int32_t old_gc_interval = FLAGS_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    // create table
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        AddDefaultSchema(0, 1, ::openmldb::type::TTLType::kLatestTime, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::kLatestTime);
        ttl->set_lat_ttl(0);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // reach the max ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::kLatestTime);
        ttl->set_lat_ttl(20000);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);
        ttl->set_abs_ttl(0);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
    // normal case
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::kLatestTime);
        ttl->set_lat_ttl(2);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(70);
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(2, cur_ttl.lat_ttl());
    }
    FLAGS_gc_interval = old_gc_interval;
}

TEST_F(TabletImplTest, CreateTableWithSchema) {
    TabletImpl tablet;
    tablet.Init("");
    {
        uint32_t id = counter++;
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
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

TEST_F(TabletImplTest, MultiGet) {
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
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
    ASSERT_EQ(142, deleteindex_response.code());
    // delete other index
    deleteindex_request.set_idx_name("amt");
    deleteindex_request.set_tid(id);
    tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response, &closure);
    ASSERT_EQ(0, deleteindex_response.code());

    // get index not found
    get_request.set_tid(id);
    get_request.set_pid(1);
    get_request.set_key("abcd2");
    get_request.set_ts(1100);
    get_request.set_idx_name("amt");
    tablet.Get(NULL, &get_request, &get_response, &closure);
    ASSERT_EQ(108, get_response.code());

    // scan index not found
    ::openmldb::api::ScanRequest scan_request;
    ::openmldb::api::ScanResponse scan_response;
    scan_request.set_tid(id);
    scan_request.set_pid(1);
    scan_request.set_pk("abcd2");
    scan_request.set_st(1100);
    scan_request.set_idx_name("amt");
    tablet.Scan(NULL, &scan_request, &scan_response, &closure);
    ASSERT_EQ(108, scan_response.code());
}

TEST_F(TabletImplTest, CreateTable) {
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
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        std::string file = FLAGS_db_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        int fd = open(file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::TableMeta table_meta_test;
        google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
        ASSERT_EQ(table_meta_test.tid(), (signed)id);
        ASSERT_STREQ(table_meta_test.name().c_str(), "t0");

        table_meta->set_name("");
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(129, response.code());
    }
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(129, response.code());
    }
}

TEST_F(TabletImplTest, Scan_with_duplicate_skip) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
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

TEST_F(TabletImplTest, Scan_with_latestN) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
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
    ::openmldb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(2, (signed)srp.count());
    ::openmldb::base::KvIterator* kv_it = new ::openmldb::base::KvIterator(&srp, false);
    ASSERT_EQ(9539, (signed)kv_it->GetKey());
    ASSERT_STREQ("test9539", ::openmldb::test::DecodeV(kv_it->GetValue().ToString()).c_str());
    kv_it->Next();
    ASSERT_EQ(9538, (signed)kv_it->GetKey());
    ASSERT_STREQ("test9538", ::openmldb::test::DecodeV(kv_it->GetValue().ToString()).c_str());
    kv_it->Next();
    ASSERT_FALSE(kv_it->Valid());
    delete kv_it;
}

TEST_F(TabletImplTest, Traverse) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
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
    ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(13, (signed)srp->count());
    ::openmldb::base::KvIterator* kv_it = new ::openmldb::base::KvIterator(srp);
    for (int cnt = 0; cnt < 13; cnt++) {
        uint64_t cur_ts = 9539 - cnt;
        ASSERT_EQ(cur_ts, kv_it->GetKey());
        ASSERT_STREQ(std::string("test" + std::to_string(cur_ts)).c_str(),
                ::openmldb::test::DecodeV(kv_it->GetValue().ToString()).c_str());
        kv_it->Next();
    }
    ASSERT_FALSE(kv_it->Valid());
    delete kv_it;
}

TEST_F(TabletImplTest, TraverseTTL) {
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

TEST_F(TabletImplTest, TraverseTTLTS) {
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
    table_meta->set_seg_cnt(1);
    table_meta->set_format_version(1);
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
    ASSERT_EQ("mcc21009", srp->pk());
    ASSERT_FALSE(srp->is_finish());
    sr.set_pk(srp->pk());
    sr.set_ts(srp->ts());
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(0, (signed)srp->count());
    ASSERT_TRUE(srp->is_finish());
    FLAGS_max_traverse_cnt = old_max_traverse;
}

TEST_F(TabletImplTest, Scan_with_limit) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
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

TEST_F(TabletImplTest, Scan) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ::openmldb::api::ScanRequest sr;
    sr.set_tid(2);
    sr.set_pk("test1");
    sr.set_st(9528);
    sr.set_et(9527);
    sr.set_limit(10);
    ::openmldb::api::ScanResponse srp;
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

TEST_F(TabletImplTest, GC_WITH_UPDATE_LATEST) {
    int32_t old_gc_interval = FLAGS_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    // create a latest table
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        AddDefaultSchema(0, 3, ::openmldb::type::TTLType::kLatestTime, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

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
}

TEST_F(TabletImplTest, GC) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(3, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
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

TEST_F(TabletImplTest, DropTable) {
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

    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(1, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
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
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
}

TEST_F(TabletImplTest, DropTableNoRecycle) {
    bool tmp_recycle_bin_enabled = FLAGS_recycle_bin_enabled;
    std::string tmp_db_root_path = FLAGS_db_root_path;
    std::string tmp_recycle_bin_root_path = FLAGS_recycle_bin_root_path;
    std::vector<std::string> file_vec;
    FLAGS_recycle_bin_enabled = false;
    FLAGS_db_root_path = "/tmp/gtest/db";
    FLAGS_recycle_bin_root_path = "/tmp/gtest/recycle";
    ::openmldb::base::RemoveDirRecursive("/tmp/gtest");
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

    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(1, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    ::openmldb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

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
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ::openmldb::base::RemoveDirRecursive("/tmp/gtest");
    FLAGS_recycle_bin_enabled = tmp_recycle_bin_enabled;
    FLAGS_db_root_path = tmp_db_root_path;
    FLAGS_recycle_bin_root_path = tmp_recycle_bin_root_path;
}

TEST_F(TabletImplTest, Recover) {
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
        table_meta->add_replicas("127.0.0.1:9530");
        table_meta->add_replicas("127.0.0.1:9531");
        ::openmldb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        std::string file = FLAGS_db_root_path + "/" + std::to_string(id) + "_" + std::to_string(1) + "/table_meta.txt";
        int fd = open(file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::TableMeta table_meta_test;
        google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
        ASSERT_EQ(table_meta_test.seg_cnt(), 64);
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

TEST_F(TabletImplTest, LoadWithDeletedKey) {
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

TEST_F(TabletImplTest, Load_with_incomplete_binlog) {
    int old_offset = FLAGS_make_snapshot_threshold_offset;
    int old_interval = FLAGS_binlog_delete_interval;
    FLAGS_binlog_delete_interval = 1000;
    FLAGS_make_snapshot_threshold_offset = 0;
    uint32_t tid = counter++;
    ::openmldb::storage::LogParts* log_part = new ::openmldb::storage::LogParts(12, 4, scmp);
    std::string binlog_dir = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog/";
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
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/snapshot/MANIFEST";
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(61, (signed)manifest.offset());

        sleep(10);
        std::vector<std::string> vec;
        std::string binlog_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog";
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

TEST_F(TabletImplTest, GC_WITH_UPDATE_TTL) {
    int32_t old_gc_interval = FLAGS_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    // create a latest table
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        // 3 minutes
        AddDefaultSchema(3, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
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
}

TEST_F(TabletImplTest, DropTableFollower) {
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

TEST_F(TabletImplTest, TestGetType) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 4, ::openmldb::type::TTLType::kLatestTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
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

TEST_F(TabletImplTest, Snapshot) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
    ::openmldb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::openmldb::api::PutRequest prequest;
    PackDefaultDimension("test1", &prequest);
    prequest.set_value(::openmldb::test::EncodeKV("test1", "test0"));
    prequest.set_time(9527);
    prequest.set_tid(id);
    prequest.set_pid(2);
    ::openmldb::api::PutResponse presponse;
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
    tablet.PauseSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());

    tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(105, gresponse.code());

    tablet.RecoverSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());

    tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
}

TEST_F(TabletImplTest, CreateTableLatestTest_Default) {
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    // no height specify
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::openmldb::api::kTableLeader);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
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
        ASSERT_EQ(1, (signed)ts.skiplist_height());
    }
}

TEST_F(TabletImplTest, CreateTableLatestTest_Specify) {
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
        ASSERT_EQ(2, (signed)ts.skiplist_height());
    }
}

TEST_F(TabletImplTest, CreateTableAbsoluteTest_Default) {
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
        ASSERT_EQ(4, (signed)ts.skiplist_height());
    }
}

TEST_F(TabletImplTest, CreateTableAbsoluteTest_Specify) {
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
        ASSERT_EQ(8, (signed)ts.skiplist_height());
    }
}

TEST_F(TabletImplTest, CreateTableAbsAndLatTest) {
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
        table_meta->set_mode(::openmldb::api::kTableLeader);
        AddDefaultSchema(10, 20, ::openmldb::type::TTLType::kAbsAndLat, table_meta);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 1, "", &cur_ttl));
        ASSERT_EQ(10, cur_ttl.abs_ttl());
        ASSERT_EQ(20, cur_ttl.lat_ttl());
        ASSERT_EQ(::openmldb::type::TTLType::kAbsAndLat, cur_ttl.ttl_type());
    }
}

TEST_F(TabletImplTest, CreateTableAbsAndOrTest) {
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
        table_meta->set_mode(::openmldb::api::kTableLeader);
        AddDefaultSchema(10, 20, ::openmldb::type::TTLType::kAbsOrLat, table_meta);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 1, "", &cur_ttl));
        ASSERT_EQ(10, cur_ttl.abs_ttl());
        ASSERT_EQ(20, cur_ttl.lat_ttl());
        ASSERT_EQ(::openmldb::type::TTLType::kAbsOrLat, cur_ttl.ttl_type());
    }
}

TEST_F(TabletImplTest, CreateTableAbsAndLatTest_Specify) {
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
        ASSERT_EQ(8, (signed)ts.skiplist_height());
    }
}

TEST_F(TabletImplTest, GetTermPair) {
    uint32_t id = counter++;
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + GenRand();
    int offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    MockClosure closure;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_mode(::openmldb::api::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

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
        tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        sleep(1);

        ::openmldb::api::GetTermPairRequest pair_request;
        ::openmldb::api::GetTermPairResponse pair_response;
        pair_request.set_tid(id);
        pair_request.set_pid(1);
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
    tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(1, (signed)pair_response.offset());

    std::string manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
    int fd = open(manifest_file.c_str(), O_RDONLY);
    ASSERT_GT(fd, 0);
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    ::openmldb::api::Manifest manifest;
    google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    std::string snapshot_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
    unlink(snapshot_file.c_str());
    tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(0, (signed)pair_response.offset());
    FLAGS_make_snapshot_threshold_offset = offset;
}

TEST_F(TabletImplTest, MakeSnapshotThreshold) {
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    int offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    // create table
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        AddDefaultSchema(1, 0, ::openmldb::type::TTLType::kAbsoluteTime, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
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
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
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
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::openmldb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(1, (signed)manifest.offset());
        std::string snapshot_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
        unlink(snapshot_file.c_str());
        FLAGS_make_snapshot_threshold_offset = offset;
    }
}

TEST_F(TabletImplTest, UpdateTTLAbsAndLat) {
    int32_t old_gc_interval = FLAGS_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    // create table
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        AddDefaultSchema(100, 50, ::openmldb::type::TTLType::kAbsAndLat, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
        ttl->set_abs_ttl(10);
        ttl->set_lat_ttl(5);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // bigger than max ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
        ttl->set_abs_ttl(60 * 24 * 365 * 30 * 2);
        ttl->set_lat_ttl(5);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // bigger than max ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
        ttl->set_abs_ttl(30);
        ttl->set_lat_ttl(20000);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
        ttl->set_abs_ttl(10);
        ttl->set_lat_ttl(5);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
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
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
        ttl->set_abs_ttl(0);
        ttl->set_lat_ttl(0);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(50, (signed)cur_ttl.lat_ttl());
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(0, (signed)cur_ttl.lat_ttl());
    }
    // ttl update from zero to no zero
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(50);
        ttl_desc->set_lat_ttl(1);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(0, (signed)cur_ttl.lat_ttl());
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(50, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(1, (signed)cur_ttl.lat_ttl());
    }
    // update from 50 to 100
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl = request.mutable_ttl();
        ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
        ttl->set_abs_ttl(100);
        ttl->set_lat_ttl(2);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(50, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(1, (signed)cur_ttl.lat_ttl());

        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(2, (signed)cur_ttl.lat_ttl());
    }
    FLAGS_gc_interval = old_gc_interval;
}

TEST_F(TabletImplTest, UpdateTTLAbsOrLat) {
    int32_t old_gc_interval = FLAGS_gc_interval;
    // 1 minute
    FLAGS_gc_interval = 1;
    TabletImpl tablet;
    tablet.Init("");
    // create table
    uint32_t id = counter++;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        AddDefaultSchema(100, 50, ::openmldb::type::TTLType::kAbsOrLat, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(5);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // bigger than max ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(60 * 24 * 365 * 30 * 2);
        ttl_desc->set_lat_ttl(5);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // bigger than max ttl
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(30);
        ttl_desc->set_lat_ttl(20000);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(5);
        ::openmldb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
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
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(0);
        ttl_desc->set_lat_ttl(0);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(50, (signed)cur_ttl.lat_ttl());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(0, (signed)cur_ttl.lat_ttl());
    }
    // ttl update from zero to no zero
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(1);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", ::openmldb::test::DecodeV(gresponse.value()));

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(0, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(0, (signed)cur_ttl.lat_ttl());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(10, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(1, (signed)cur_ttl.lat_ttl());
    }
    // update from 10 to 100
    {
        ::openmldb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        auto ttl_desc = request.mutable_ttl();
        ttl_desc->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(2);
        ::openmldb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        ::openmldb::common::TTLSt cur_ttl;
        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(10, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(1, (signed)cur_ttl.lat_ttl());

        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        ASSERT_EQ(0, GetTTL(tablet, id, 0, "", &cur_ttl));
        ASSERT_EQ(100, (signed)cur_ttl.abs_ttl());
        ASSERT_EQ(2, (signed)cur_ttl.lat_ttl());
    }
    FLAGS_gc_interval = old_gc_interval;
}

TEST_F(TabletImplTest, ScanAtLeast) {
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = 100;
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kAbsAndLat, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::api::PutResponse presponse;
    ::openmldb::api::PutRequest prequest;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "test" + std::to_string(i % 10);
        prequest.clear_dimensions();
        PackDefaultDimension(key, &prequest);
        prequest.set_time(now - i * 60 * 1000);
        prequest.set_value(::openmldb::test::EncodeKV(key, "test" + std::to_string(i % 10)));
        prequest.set_tid(id);
        prequest.set_pid(0);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::openmldb::api::ScanRequest sr;
    ::openmldb::api::ScanResponse srp;
    // test atleast more than et
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 500 * 60 * 1000);
        sr.set_atleast(80);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(80, (signed)srp.count());
    }
    // test atleast less than et
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 700 * 60 * 1000 + 1000);
        sr.set_atleast(50);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(70, (signed)srp.count());
    }
    // test atleast and limit
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 700 * 60 * 1000 + 1000);
        sr.set_atleast(50);
        sr.set_limit(60);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(60, (signed)srp.count());
    }
    // test atleast more than limit
    sr.set_tid(id);
    sr.set_pid(0);
    sr.set_pk("test" + std::to_string(0));
    sr.set_st(now);
    sr.set_et(now - 700 * 60 * 1000 + 1000);
    sr.set_atleast(70);
    sr.set_limit(60);
    sr.set_et_type(::openmldb::api::kSubKeyGe);
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(307, srp.code());
    // test atleast more than count
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 1100 * 60 * 1000);
        sr.set_atleast(120);
        sr.set_limit(0);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(100, (signed)srp.count());
    }
}

TEST_F(TabletImplTest, AbsAndLat) {
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = 101;
    ::openmldb::api::CreateTableRequest request;
    auto table_meta = request.mutable_table_meta();
    {
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_format_version(1);
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
        ::openmldb::api::Dimension* dim = prequest.add_dimensions();
        dim->set_idx(0);
        dim->set_key("test" + std::to_string(i % 10));
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
        ::openmldb::api::TraverseResponse* srp = new ::openmldb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(80, (signed)srp->count());
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
        sr.set_idx_name("index0");
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
    // test atleast more than et and no ttl
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 50 * 60 * 1000);
        sr.set_idx_name("index0");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
    }
    // test atleast more than et and expire and with ttl
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 50 * 60 * 1000);
        sr.set_idx_name("index1");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(8, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 50 * 60 * 1000);
        sr.set_idx_name("index2");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
    }
    // test et less than expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index0");
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_idx_name("index1");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
    }
    // test atleast less than expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index0");
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_idx_name("index1");
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
    }
    // test atleast and limit ls than valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index0");
        sr.set_atleast(5);
        sr.set_limit(6);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_idx_name("index1");
        sr.set_atleast(5);
        sr.set_limit(6);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_idx_name("index2");
        sr.set_atleast(5);
        sr.set_limit(6);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
    // test atleast and limit more than valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index0");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(9, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("index1");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now - 30 * 60 * 1000);
        sr.set_et(now - 50 * 60 * 1000);
        sr.set_idx_name("index0");
        sr.set_limit(7);
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
    }
}  // NOLINT

TEST_F(TabletImplTest, AbsOrLat) {
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = 102;
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    {
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_format_version(1);
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
        ::openmldb::api::Dimension* dim = prequest.add_dimensions();
        dim->set_idx(0);
        dim->set_key("test" + std::to_string(i % 10));
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
    // test atleast more than et and no ttl
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000);
        sr.set_idx_name("ts1");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
    }
    // test atleast more than et and expire and with ttl
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000);
        sr.set_idx_name("ts2");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 40 * 60 * 1000);
        sr.set_idx_name("ts3");
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
    // test et less than expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(10);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
    // test atleast less than expire
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(5);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
    // test atleast and limit ls than valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        sr.set_atleast(3);
        sr.set_limit(4);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(3);
        sr.set_limit(4);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(3);
        sr.set_limit(4);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
    }
    // test atleast and limit more than valid
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts1");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(9, (signed)srp.count());
    }
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 100 * 60 * 1000);
        sr.set_idx_name("ts2");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
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
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::openmldb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
    }
}  // NOLINT

TEST_F(TabletImplTest, DelRecycle) {
    uint32_t tmp_recycle_ttl = FLAGS_recycle_ttl;
    std::string tmp_recycle_bin_root_path = FLAGS_recycle_bin_root_path;
    FLAGS_recycle_ttl = 1;
    FLAGS_recycle_bin_root_path = "/tmp/gtest/recycle";
    std::string tmp_recycle_path = "/tmp/gtest/recycle";
    ::openmldb::base::RemoveDirRecursive(FLAGS_recycle_bin_root_path);
    ::openmldb::base::MkdirRecur("/tmp/gtest/recycle/99_1_binlog_20191111070955/binlog/");
    ::openmldb::base::MkdirRecur("/tmp/gtest/recycle/100_2_20191111115149/binlog/");
    TabletImpl tablet;
    tablet.Init("");

    std::vector<std::string> file_vec;
    ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(2, (signed)file_vec.size());
    std::cout << "sleep for 30s" << std::endl;
    sleep(30);

    std::string now_time = ::openmldb::base::GetNowTime();
    ::openmldb::base::MkdirRecur("/tmp/gtest/recycle/99_3_" + now_time + "/binlog/");
    ::openmldb::base::MkdirRecur("/tmp/gtest/recycle/100_4_binlog_" + now_time + "/binlog/");
    file_vec.clear();
    ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(4, (signed)file_vec.size());

    std::cout << "sleep for 35s" << std::endl;
    sleep(35);

    file_vec.clear();
    ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(2, (signed)file_vec.size());

    std::cout << "sleep for 65s" << std::endl;
    sleep(65);

    file_vec.clear();
    ::openmldb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(0, (signed)file_vec.size());

    ::openmldb::base::RemoveDirRecursive("/tmp/gtest");
    FLAGS_recycle_ttl = tmp_recycle_ttl;
    FLAGS_recycle_bin_root_path = tmp_recycle_bin_root_path;
}

TEST_F(TabletImplTest, DumpIndex) {
    int old_offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    ::openmldb::api::CreateTableRequest request;
    ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
    ::openmldb::common::ColumnDesc* desc = table_meta->add_column_desc();
    desc->set_name("card");
    desc->set_data_type(::openmldb::type::kString);
    desc = table_meta->add_column_desc();
    desc->set_name("mcc");
    desc->set_data_type(::openmldb::type::kString);
    desc = table_meta->add_column_desc();
    desc->set_name("price");
    desc->set_data_type(::openmldb::type::kBigInt);
    desc = table_meta->add_column_desc();
    desc->set_name("ts1");
    desc->set_data_type(::openmldb::type::kBigInt);
    desc = table_meta->add_column_desc();
    desc->set_name("ts2");
    desc->set_data_type(::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "index1", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "index2", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);

    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    ::openmldb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    for (int i = 0; i < 10; i++) {
        std::vector<std::string> input;
        input.push_back("card" + std::to_string(i));
        input.push_back("mcc" + std::to_string(i));
        input.push_back(std::to_string(i));
        input.push_back(std::to_string(i + 100));
        input.push_back(std::to_string(i + 10000));
        std::string value;
        ::openmldb::codec::RowCodec::EncodeRow(input, table_meta->column_desc(), 1, value);
        ::openmldb::api::PutRequest request;
        request.set_value(value);
        request.set_tid(id);
        request.set_pid(1);

        ::openmldb::api::Dimension* d = request.add_dimensions();
        d->set_key(input[0]);
        d->set_idx(0);
        ::openmldb::api::TSDimension* tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 100);
        tsd->set_idx(0);
        tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 10000);
        tsd->set_idx(1);
        ::openmldb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    ::openmldb::api::GeneralRequest grq;
    grq.set_tid(id);
    grq.set_pid(1);
    ::openmldb::api::GeneralResponse grp;
    grp.set_code(-1);
    tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
    sleep(2);
    for (int i = 0; i < 10; i++) {
        std::vector<std::string> input;
        input.push_back("card" + std::to_string(i));
        input.push_back("mcc" + std::to_string(i));
        input.push_back(std::to_string(i));
        input.push_back(std::to_string(i + 200));
        input.push_back(std::to_string(i + 20000));
        std::string value;
        ::openmldb::codec::RowCodec::EncodeRow(input, table_meta->column_desc(), 1, value);
        ::openmldb::api::PutRequest request;
        request.set_value(value);
        request.set_tid(id);
        request.set_pid(1);
        ::openmldb::api::Dimension* d = request.add_dimensions();
        d->set_key(input[0]);
        d->set_idx(0);
        ::openmldb::api::TSDimension* tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 100);
        tsd->set_idx(0);
        tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 10000);
        tsd->set_idx(1);
        ::openmldb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        ::openmldb::api::DumpIndexDataRequest dump_request;
        dump_request.set_tid(id);
        dump_request.set_pid(1);
        dump_request.set_partition_num(8);
        dump_request.set_idx(1);
        auto column_key = dump_request.mutable_column_key();
        column_key->set_index_name("card|mcc");
        column_key->add_col_name("card");
        column_key->add_col_name("mcc");
        column_key->set_ts_name("ts2");
        ::openmldb::api::GeneralResponse dump_response;
        tablet.DumpIndexData(NULL, &dump_request, &dump_response, &closure);
        ASSERT_EQ(0, dump_response.code());
    }
    FLAGS_make_snapshot_threshold_offset = old_offset;
}

TEST_F(TabletImplTest, SendIndexData) {
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
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        ::openmldb::api::CreateTableRequest request;
        ::openmldb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        AddDefaultSchema(0, 0, ::openmldb::type::TTLType::kLatestTime, table_meta);
        table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
        ::openmldb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
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
    ::openmldb::base::RemoveDirRecursive(FLAGS_db_root_path);
}

TEST_F(TabletImplTest, BulkLoad) {
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

TEST_F(TabletImplTest, AddIndex) {
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
    table_meta->set_format_version(1);
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
    ASSERT_EQ(0, response.code());

    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t key_base = 10000;
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    for (int i = 0; i < 10; i++) {
        uint64_t ts_value = cur_time - 10 * 60 * 1000 - i;
        uint64_t ts1_value = cur_time - i;
        std::vector<std::string> row = {"card" + std::to_string(key_base + i),
                "mcc" + std::to_string(key_base + i), "12", std::to_string(ts_value), std::to_string(ts1_value)};
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

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::openmldb::tablet::GenRand();
    return RUN_ALL_TESTS();
}
