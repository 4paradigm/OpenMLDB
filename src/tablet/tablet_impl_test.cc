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
#include "base/kv_iterator.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "codec/codec.h"
#include "codec/flat_array.h"
#include "codec/schema_codec.h"
#include "gtest/gtest.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "base/glog_wapper.h" // NOLINT
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "timer.h"  // NOLINT

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

namespace fedb {
namespace tablet {

using ::fedb::api::TableStatus;
using Schema =
    ::google::protobuf::RepeatedPtrField<::fedb::common::ColumnDesc>;

uint32_t counter = 10;
static const ::fedb::base::DefaultComparator scmp;

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

bool RollWLogFile(::fedb::storage::WriteHandle** wh,
                  ::fedb::storage::LogParts* logs, const std::string& log_path,
                  uint32_t& binlog_index, uint64_t offset,  // NOLINT
                  bool append_end = true) {
    if (*wh != NULL) {
        if (append_end) {
            (*wh)->EndLog();
        }
        delete *wh;
        *wh = NULL;
    }
    std::string name = ::fedb::base::FormatToString(binlog_index, 8) + ".log";
    ::fedb::base::MkdirRecur(log_path);
    std::string full_path = log_path + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    logs->Insert(binlog_index, offset);
    *wh = new ::fedb::storage::WriteHandle("off", name, fd);
    binlog_index++;
    return true;
}

int MultiDimensionEncode(
    const std::vector<::fedb::codec::ColumnDesc>& colum_desc,
    const std::vector<std::string>& input,
    std::vector<std::pair<std::string, uint32_t>>& dimensions,  // NOLINT
    std::string& buffer) {                                      // NOLINT
    uint32_t cnt = input.size();
    if (cnt != colum_desc.size()) {
        std::cout << "Input value mismatch schema" << std::endl;
        return -1;
    }
    ::fedb::codec::FlatArrayCodec codec(&buffer, (uint8_t)cnt);
    uint32_t idx_cnt = 0;
    for (uint32_t i = 0; i < input.size(); i++) {
        if (colum_desc[i].add_ts_idx) {
            dimensions.push_back(std::make_pair(input[i], idx_cnt));
            idx_cnt++;
        }
        bool codec_ok = false;
        if (colum_desc[i].type == ::fedb::codec::ColType::kInt32) {
            codec_ok = codec.Append(boost::lexical_cast<int32_t>(input[i]));
        } else if (colum_desc[i].type == ::fedb::codec::ColType::kInt64) {
            codec_ok = codec.Append(boost::lexical_cast<int64_t>(input[i]));
        } else if (colum_desc[i].type == ::fedb::codec::ColType::kUInt32) {
            codec_ok = codec.Append(boost::lexical_cast<uint32_t>(input[i]));
        } else if (colum_desc[i].type == ::fedb::codec::ColType::kUInt64) {
            codec_ok = codec.Append(boost::lexical_cast<uint64_t>(input[i]));
        } else if (colum_desc[i].type == ::fedb::codec::ColType::kFloat) {
            codec_ok = codec.Append(boost::lexical_cast<float>(input[i]));
        } else if (colum_desc[i].type == ::fedb::codec::ColType::kDouble) {
            codec_ok = codec.Append(boost::lexical_cast<double>(input[i]));
        } else if (colum_desc[i].type == ::fedb::codec::ColType::kString) {
            codec_ok = codec.Append(input[i]);
        }
        if (!codec_ok) {
            std::cout << "Failed invalid value " << input[i] << std::endl;
            return -1;
        }
    }
    codec.Build();
    return 0;
}

void MultiDimensionDecode(const std::string& value,
                          std::vector<std::string>& output,  // NOLINT
                          uint16_t column_num) {
    fedb::codec::FlatArrayIterator fit(value.c_str(), value.size(),
                                        column_num);
    while (fit.Valid()) {
        std::string col;
        if (fit.GetType() == ::fedb::codec::ColType::kString) {
            fit.GetString(&col);
        } else if (fit.GetType() == ::fedb::codec::ColType::kInt32) {
            int32_t int32_col = 0;
            fit.GetInt32(&int32_col);
            col = boost::lexical_cast<std::string>(int32_col);
        } else if (fit.GetType() == ::fedb::codec::ColType::kInt64) {
            int64_t int64_col = 0;
            fit.GetInt64(&int64_col);
            col = boost::lexical_cast<std::string>(int64_col);
        } else if (fit.GetType() == ::fedb::codec::ColType::kUInt32) {
            uint32_t uint32_col = 0;
            fit.GetUInt32(&uint32_col);
            col = boost::lexical_cast<std::string>(uint32_col);
        } else if (fit.GetType() == ::fedb::codec::ColType::kUInt64) {
            uint64_t uint64_col = 0;
            fit.GetUInt64(&uint64_col);
            col = boost::lexical_cast<std::string>(uint64_col);
        } else if (fit.GetType() == ::fedb::codec::ColType::kDouble) {
            double double_col = 0.0;
            fit.GetDouble(&double_col);
            col = boost::lexical_cast<std::string>(double_col);
        } else if (fit.GetType() == ::fedb::codec::ColType::kFloat) {
            float float_col = 0.0f;
            fit.GetFloat(&float_col);
            col = boost::lexical_cast<std::string>(float_col);
        }
        fit.Next();
        output.push_back(col);
    }
}

void PrepareLatestTableData(TabletImpl& tablet, int32_t tid,  // NOLINT
                            int32_t pid) {
    for (int32_t i = 0; i < 100; i++) {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk(boost::lexical_cast<std::string>(i % 10));
        prequest.set_time(i + 1);
        prequest.set_value(boost::lexical_cast<std::string>(i));
        prequest.set_tid(tid);
        prequest.set_pid(pid);
        ::fedb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    for (int32_t i = 0; i < 100; i++) {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("10");
        prequest.set_time(i % 10 + 1);
        prequest.set_value(boost::lexical_cast<std::string>(i));
        prequest.set_tid(tid);
        prequest.set_pid(pid);
        ::fedb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
}

TEST_F(TabletImplTest, Count_Latest_Table) {
    TabletImpl tablet;
    tablet.Init("");
    // create table
    MockClosure closure;
    uint32_t id = counter++;
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        PrepareLatestTableData(tablet, id, 0);
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(100u, response.count());
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        request.set_enable_remove_duplicated_record(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        // default
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10u, response.count());
    }

    {
        // default st et type
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1u, response.count());
    }

    {
        // st type=le et type=ge
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(2u, response.count());
    }

    {
        // st type=le et type=ge
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(90);
        request.set_et(81);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        PrepareLatestTableData(tablet, id, 0);
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (int32_t)response.count());
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (int64_t)(response.count()));
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(100, (signed)response.count());
    }

    {
        //
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("10");
        request.set_filter_expired_data(true);
        request.set_enable_remove_duplicated_record(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (signed)response.count());
    }

    {
        // default
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(10, (signed)response.count());
    }

    {
        // default st et type
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.count());
    }

    {
        // st type=le et type=ge
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(91);
        request.set_et(81);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
        tablet.Count(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(2, (signed)response.count());
    }

    {
        // st type=le et type=ge
        ::fedb::api::CountRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_key("0");
        request.set_st(90);
        request.set_et(81);
        request.set_et_type(::fedb::api::GetType::kSubKeyGe);
        request.set_filter_expired_data(true);
        ::fedb::api::CountResponse response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_ttl(5);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        PrepareLatestTableData(tablet, id, 0);
    }

    // scan with default type
    {
        std::string key = "1";
        ::fedb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk(key);
        sr.set_st(92);
        sr.set_et(90);
        ::fedb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
        ::fedb::base::KvIterator* kv_it = new ::fedb::base::KvIterator(&srp);
        ASSERT_TRUE(kv_it->Valid());
        ASSERT_EQ(92l, (signed)kv_it->GetKey());
        ASSERT_STREQ("91", kv_it->GetValue().ToString().c_str());
        kv_it->Next();
        ASSERT_FALSE(kv_it->Valid());
    }

    // scan with default et ge
    {
        ::fedb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("1");
        sr.set_st(92);
        sr.set_et(0);
        sr.set_et_type(::fedb::api::kSubKeyGe);
        ::fedb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        ::fedb::base::KvIterator* kv_it = new ::fedb::base::KvIterator(&srp);
        ASSERT_TRUE(kv_it->Valid());
        ASSERT_EQ(92l, (signed)kv_it->GetKey());
        ASSERT_STREQ("91", kv_it->GetValue().ToString().c_str());
        kv_it->Next();
        ASSERT_TRUE(kv_it->Valid());
    }
}

TEST_F(TabletImplTest, Get) {
    TabletImpl tablet;
    tablet.Init("");
    // table not found
    {
        ::fedb::api::GetRequest request;
        request.set_tid(1);
        request.set_pid(0);
        request.set_key("test");
        request.set_ts(0);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // create table
    uint32_t id = counter++;
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_ttl(1);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    // key not found
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
    }
    // put/get expired key
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test0");
        prequest.set_time(now - 2 * 60 * 1000);
        prequest.set_value("value0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test0");
        request.set_ts(0);
        ::fedb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
    }
    // put some key
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(now);
        prequest.set_value("test10");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(now - 2);
        prequest.set_value("test9");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        prequest.set_time(now - 120 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 2);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test9", response.value());
        request.set_ts(0);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test10", response.value());
    }
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 1);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
    }
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 2);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test9", response.value());
    }
    {
        // get expired key
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 120 * 1000);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(307, response.code());
    }
    // create latest ttl table
    id = counter++;
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t1");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(5);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    int num = 10;
    while (num) {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(num);
        prequest.set_value("test" + std::to_string(num));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        num--;
    }
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(5);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_NE(0, response.code());
        request.set_ts(6);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test6", response.value());
        request.set_ts(0);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test10", response.value());
        request.set_ts(7);
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test7", response.value());
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_wal(true);
        table_meta->set_ttl(100);
        table_meta->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        request.set_type(::fedb::api::TTLType::kAbsoluteTime);
        request.set_value(0);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // bigger than max ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_type(::fedb::api::TTLType::kAbsoluteTime);
        request.set_value(60 * 24 * 365 * 30 * 2);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_type(::fedb::api::kLatestTime);
        request.set_value(0);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
    // normal case
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test");
    prequest.set_time(now - 60 * 60 * 1000);
    prequest.set_value("test9");
    prequest.set_tid(id);
    prequest.set_pid(0);
    ::fedb::api::PutResponse presponse;
    MockClosure closure;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
    ::fedb::api::GetRequest grequest;
    grequest.set_tid(id);
    grequest.set_pid(0);
    grequest.set_key("test");
    grequest.set_ts(0);
    ::fedb::api::GetResponse gresponse;
    tablet.Get(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
    ASSERT_EQ("test9", gresponse.value());
    // UpdateTTLRequest
    ::fedb::api::UpdateTTLRequest request;
    request.set_tid(id);
    request.set_pid(0);
    request.set_type(::fedb::api::TTLType::kAbsoluteTime);
    ::fedb::api::UpdateTTLResponse response;
    // ExecuteGcRequest
    ::fedb::api::ExecuteGcRequest request_execute;
    ::fedb::api::GeneralResponse response_execute;
    request_execute.set_tid(id);
    request_execute.set_pid(0);
    // etTableStatusRequest
    ::fedb::api::GetTableStatusRequest gr;
    ::fedb::api::GetTableStatusResponse gres;
    // ttl update to zero
    {
        request.set_value(0);
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(100, (int64_t)ts.ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(0, (int64_t)(ts.ttl()));
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    // ttl update from zero to no zero
    {
        request.set_value(50);
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(0, (signed)ts.ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(50, (signed)ts.ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    // update from 50 to 100
    {
        request.set_value(100);
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(50, (signed)ts.ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(100, (signed)ts.ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_wal(true);
        table_meta->set_ttl(1);
        table_meta->set_ttl_type(::fedb::api::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        request.set_type(::fedb::api::kLatestTime);
        request.set_value(0);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // reach the max ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_type(::fedb::api::kLatestTime);
        request.set_value(20000);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_type(::fedb::api::TTLType::kAbsoluteTime);
        request.set_value(0);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
    // normal case
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        request.set_type(::fedb::api::kLatestTime);
        request.set_value(2);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(70);
        ::fedb::api::GetTableStatusRequest gr;
        ::fedb::api::GetTableStatusResponse gres;
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(2, (signed)ts.ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    FLAGS_gc_interval = old_gc_interval;
}

TEST_F(TabletImplTest, CreateTableWithSchema) {
    TabletImpl tablet;
    tablet.Init("");
    {
        uint32_t id = counter++;
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        // get schema
        ::fedb::api::GetTableSchemaRequest request0;
        request0.set_tid(id);
        request0.set_pid(1);
        ::fedb::api::GetTableSchemaResponse response0;
        tablet.GetTableSchema(NULL, &request0, &response0, &closure);
        ASSERT_EQ("", response0.schema());
    }
    {
        std::vector<::fedb::codec::ColumnDesc> columns;
        ::fedb::codec::ColumnDesc desc1;
        desc1.type = ::fedb::codec::ColType::kString;
        desc1.name = "card";
        desc1.add_ts_idx = true;
        columns.push_back(desc1);

        ::fedb::codec::ColumnDesc desc2;
        desc2.type = ::fedb::codec::ColType::kDouble;
        desc2.name = "amt";
        desc2.add_ts_idx = false;
        columns.push_back(desc2);

        ::fedb::codec::ColumnDesc desc3;
        desc3.type = ::fedb::codec::ColType::kInt32;
        desc3.name = "apprv_cde";
        desc3.add_ts_idx = false;
        columns.push_back(desc3);

        ::fedb::codec::SchemaCodec codec;
        std::string buffer;
        codec.Encode(columns, buffer);
        uint32_t id = counter++;
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        table_meta->set_schema(buffer);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        ::fedb::api::GetTableSchemaRequest request0;
        request0.set_tid(id);
        request0.set_pid(1);
        ::fedb::api::GetTableSchemaResponse response0;
        tablet.GetTableSchema(NULL, &request0, &response0, &closure);
        ASSERT_TRUE(response0.schema().size() != 0);  // NOLINT

        std::vector<::fedb::codec::ColumnDesc> ncolumns;
        codec.Decode(response0.schema(), ncolumns);
        ASSERT_EQ(3, (signed)ncolumns.size());
        ASSERT_EQ(::fedb::codec::ColType::kString, ncolumns[0].type);
        ASSERT_EQ("card", ncolumns[0].name);
        ASSERT_EQ(::fedb::codec::ColType::kDouble, ncolumns[1].type);
        ASSERT_EQ("amt", ncolumns[1].name);
        ASSERT_EQ(::fedb::codec::ColType::kInt32, ncolumns[2].type);
        ASSERT_EQ("apprv_cde", ncolumns[2].name);
    }
}

TEST_F(TabletImplTest, MultiGet) {
    TabletImpl tablet;
    tablet.Init("");
    uint32_t id = counter++;
    std::vector<::fedb::codec::ColumnDesc> columns;
    ::fedb::codec::ColumnDesc desc1;
    desc1.type = ::fedb::codec::ColType::kString;
    desc1.name = "pk";
    desc1.add_ts_idx = true;
    columns.push_back(desc1);

    ::fedb::codec::ColumnDesc desc2;
    desc2.type = ::fedb::codec::ColType::kString;
    desc2.name = "amt";
    desc2.add_ts_idx = true;
    columns.push_back(desc2);

    ::fedb::codec::ColumnDesc desc3;
    desc3.type = ::fedb::codec::ColType::kInt32;
    desc3.name = "apprv_cde";
    desc3.add_ts_idx = false;
    columns.push_back(desc3);

    ::fedb::codec::SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
    table_meta->set_schema(buffer);
    for (uint32_t i = 0; i < columns.size(); i++) {
        if (columns[i].add_ts_idx) {
            table_meta->add_dimensions(columns[i].name);
        }
    }
    ::fedb::api::CreateTableResponse response;
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
        std::vector<std::pair<std::string, uint32_t>> dimensions;
        MultiDimensionEncode(columns, input, dimensions, value);
        ::fedb::api::PutRequest request;
        request.set_time(1100);
        request.set_value(value);
        request.set_tid(id);
        request.set_pid(1);
        for (size_t i = 0; i < dimensions.size(); i++) {
            ::fedb::api::Dimension* d = request.add_dimensions();
            d->set_key(dimensions[i].first);
            d->set_idx(dimensions[i].second);
        }
        ::fedb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get
    ::fedb::api::GetRequest get_request;
    ::fedb::api::GetResponse get_response;
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
    MultiDimensionDecode(value, vec, columns.size());
    ASSERT_EQ(3, (signed)vec.size());
    ASSERT_STREQ("test2", vec[0].c_str());
    ASSERT_STREQ("abcd2", vec[1].c_str());
    ASSERT_STREQ("12122", vec[2].c_str());

    // delete index
    ::fedb::api::DeleteIndexRequest deleteindex_request;
    ::fedb::api::GeneralResponse deleteindex_response;
    // delete first index should fail
    deleteindex_request.set_idx_name("pk");
    deleteindex_request.set_tid(id);
    deleteindex_request.set_pid(1);
    tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response,
                       &closure);
    ASSERT_EQ(142, deleteindex_response.code());
    // delete other index
    deleteindex_request.set_idx_name("amt");
    deleteindex_request.set_tid(id);
    tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response,
                       &closure);
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
    ::fedb::api::ScanRequest scan_request;
    ::fedb::api::ScanResponse scan_response;
    scan_request.set_tid(id);
    scan_request.set_pid(1);
    scan_request.set_pk("abcd2");
    scan_request.set_st(1100);
    scan_request.set_idx_name("amt");
    tablet.Scan(NULL, &scan_request, &scan_response, &closure);
    ASSERT_EQ(108, scan_response.code());
}

TEST_F(TabletImplTest, TTL) {
    uint32_t id = counter++;
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    TabletImpl tablet;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_wal(true);
    table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
    // 1 minutes
    table_meta->set_ttl(1);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(now);
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(now - 2 * 60 * 1000);
        prequest.set_value("test2");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
}

TEST_F(TabletImplTest, CreateTable) {
    uint32_t id = counter++;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_ttl(0);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        std::string file = FLAGS_db_root_path + "/" + std::to_string(id) + "_" +
                           std::to_string(1) + "/table_meta.txt";
        int fd = open(file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::fedb::api::TableMeta table_meta_test;
        google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
        ASSERT_EQ(table_meta_test.tid(), (signed)id);
        ASSERT_STREQ(table_meta_test.name().c_str(), "t0");

        table_meta->set_name("");
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(129, response.code());
    }
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_ttl(0);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(129, response.code());
    }
}

TEST_F(TabletImplTest, Put) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(2);
    prequest.set_pid(2);
    ::fedb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(100, presponse.code());
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
}

TEST_F(TabletImplTest, Scan_with_duplicate_skip) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9529);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::fedb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(9530);
    sr.set_et(0);
    sr.set_enable_remove_duplicated_record(true);
    ::fedb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(3, (signed)srp.count());
}

TEST_F(TabletImplTest, Scan_with_latestN) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_ttl_type(::fedb::api::kLatestTime);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    for (int ts = 9527; ts < 9540; ts++) {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(ts);
        prequest.set_value("test" + std::to_string(ts));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::fedb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(0);
    sr.set_et(0);
    sr.set_limit(2);
    ::fedb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(2, (signed)srp.count());
    ::fedb::base::KvIterator* kv_it =
        new ::fedb::base::KvIterator(&srp, false);
    ASSERT_EQ(9539, (signed)kv_it->GetKey());
    ASSERT_STREQ("test9539", kv_it->GetValue().ToString().c_str());
    kv_it->Next();
    ASSERT_EQ(9538, (signed)kv_it->GetKey());
    ASSERT_STREQ("test9538", kv_it->GetValue().ToString().c_str());
    kv_it->Next();
    ASSERT_FALSE(kv_it->Valid());
    delete kv_it;
}

TEST_F(TabletImplTest, Traverse) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    for (int ts = 9527; ts < 9540; ts++) {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(ts);
        prequest.set_value("test" + std::to_string(ts));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::fedb::api::TraverseRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_limit(100);
    ::fedb::api::TraverseResponse* srp = new ::fedb::api::TraverseResponse();
    tablet.Traverse(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(13, (signed)srp->count());
    ::fedb::base::KvIterator* kv_it = new ::fedb::base::KvIterator(srp);
    for (int cnt = 0; cnt < 13; cnt++) {
        uint64_t cur_ts = 9539 - cnt;
        ASSERT_EQ(cur_ts, kv_it->GetKey());
        ASSERT_STREQ(std::string("test" + std::to_string(cur_ts)).c_str(),
                     kv_it->GetValue().ToString().c_str());
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
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(5);
    table_meta->set_seg_cnt(1);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t key_base = 10000;
    for (int i = 0; i < 100; i++) {
        uint64_t ts = cur_time - 10 * 60 * 1000 + i;
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test" + std::to_string(key_base + i));
        prequest.set_time(ts);
        prequest.set_value("test" + std::to_string(ts));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    key_base = 21000;
    for (int i = 0; i < 60; i++) {
        uint64_t ts = cur_time + i;
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test" + std::to_string(key_base + i));
        prequest.set_time(ts);
        prequest.set_value("test" + std::to_string(ts));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::fedb::api::TraverseRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_limit(100);
    ::fedb::api::TraverseResponse* srp = new ::fedb::api::TraverseResponse();
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
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(5);
    table_meta->set_seg_cnt(1);
    ::fedb::common::ColumnDesc* desc = table_meta->add_column_desc();
    desc->set_name("card");
    desc->set_type("string");
    desc->set_add_ts_idx(true);
    desc = table_meta->add_column_desc();
    desc->set_name("mcc");
    desc->set_type("string");
    desc->set_add_ts_idx(true);
    desc = table_meta->add_column_desc();
    desc->set_name("price");
    desc->set_type("int64");
    desc->set_add_ts_idx(false);
    desc = table_meta->add_column_desc();
    desc->set_name("ts1");
    desc->set_type("int64");
    desc->set_add_ts_idx(false);
    desc->set_is_ts_col(true);
    desc = table_meta->add_column_desc();
    desc->set_name("ts2");
    desc->set_type("int64");
    desc->set_add_ts_idx(false);
    desc->set_is_ts_col(true);
    ::fedb::common::ColumnKey* column_key = table_meta->add_column_key();
    column_key->set_index_name("card");
    column_key->add_ts_name("ts1");
    column_key->add_ts_name("ts2");
    column_key = table_meta->add_column_key();
    column_key->set_index_name("mcc");
    column_key->add_ts_name("ts2");
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t key_base = 10000;
    for (int i = 0; i < 30; i++) {
        for (int idx = 0; idx < 2; idx++) {
            uint64_t ts_value = cur_time - 10 * 60 * 1000 - idx;
            uint64_t ts1_value = cur_time - idx;
            ::fedb::api::PutRequest prequest;
            ::fedb::api::Dimension* dim = prequest.add_dimensions();
            dim->set_idx(0);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(1);
            dim->set_key("mcc" + std::to_string(key_base + i));
            ::fedb::api::TSDimension* ts = prequest.add_ts_dimensions();
            ts->set_idx(0);
            ts->set_ts(ts_value);
            ts = prequest.add_ts_dimensions();
            ts->set_idx(1);
            ts->set_ts(ts1_value);
            std::string value = "value" + std::to_string(i);
            prequest.set_tid(id);
            prequest.set_pid(1);
            ::fedb::api::PutResponse presponse;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    key_base = 21000;
    for (int i = 0; i < 30; i++) {
        for (int idx = 0; idx < 2; idx++) {
            uint64_t ts1_value = cur_time - 10 * 60 * 1000 - idx;
            uint64_t ts_value = cur_time - idx;
            ::fedb::api::PutRequest prequest;
            ::fedb::api::Dimension* dim = prequest.add_dimensions();
            dim->set_idx(0);
            dim->set_key("card" + std::to_string(key_base + i));
            dim = prequest.add_dimensions();
            dim->set_idx(1);
            dim->set_key("mcc" + std::to_string(key_base + i));
            ::fedb::api::TSDimension* ts = prequest.add_ts_dimensions();
            ts->set_idx(0);
            ts->set_ts(ts_value);
            ts = prequest.add_ts_dimensions();
            ts->set_idx(1);
            ts->set_ts(ts1_value);
            std::string value = "value" + std::to_string(i);
            prequest.set_tid(id);
            prequest.set_pid(1);
            ::fedb::api::PutResponse presponse;
            tablet.Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    ::fedb::api::TraverseRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_idx_name("card");
    sr.set_limit(100);
    ::fedb::api::TraverseResponse* srp = new ::fedb::api::TraverseResponse();
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
    sr.set_ts_name("ts2");
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
    sr.set_ts_name("ts2");
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
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9529);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::fedb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(9530);
    sr.set_et(9526);
    sr.set_limit(2);
    ::fedb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(2, (signed)srp.count());
}

TEST_F(TabletImplTest, Scan) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ::fedb::api::ScanRequest sr;
    sr.set_tid(2);
    sr.set_pk("test1");
    sr.set_st(9528);
    sr.set_et(9527);
    sr.set_limit(10);
    ::fedb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, (signed)srp.pairs().size());
    ASSERT_EQ(100, srp.code());

    sr.set_tid(id);
    sr.set_pid(1);
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(0, (signed)srp.count());

    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(2);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);

        ASSERT_EQ(100, presponse.code());
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);

        ASSERT_EQ(0, presponse.code());
    }
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test0");
        prequest.set_tid(2);
        ::fedb::api::PutResponse presponse;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(3);
        table_meta->set_ttl_type(::fedb::api::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // version 1
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // version 2
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_value("test2");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(2);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // version 3
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_value("test3");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(3);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // get version 1
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::fedb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.ts());
        ASSERT_EQ("test1", response.value());
    }

    // update ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_type(::fedb::api::kLatestTime);
        request.set_value(2);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // get version 1 again
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::fedb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // sleep 70s
    sleep(70);

    // get version 1 again
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::fedb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(109, response.code());
        ASSERT_EQ("key not found", response.msg());
    }
    // revert ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_type(::fedb::api::kLatestTime);
        request.set_value(3);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    // make sure that gc has clean the data
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(1);
        ::fedb::api::GetResponse response;
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
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_wal(true);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::fedb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    prequest.set_time(now);
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ::fedb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(now);
    sr.set_et(9527);
    sr.set_limit(10);
    ::fedb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(1, (signed)srp.count());
}

TEST_F(TabletImplTest, DropTable) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ::fedb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::fedb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
    ::fedb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::fedb::api::PutResponse presponse;
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
    ::fedb::base::RemoveDirRecursive("/tmp/gtest");
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    MockClosure closure;
    ::fedb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::fedb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
    ::fedb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::fedb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    ::fedb::base::GetChildFileName(FLAGS_db_root_path, file_vec);
    ASSERT_TRUE(file_vec.empty());
    file_vec.clear();
    ::fedb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_TRUE(file_vec.empty());
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ::fedb::base::RemoveDirRecursive("/tmp/gtest");
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_seg_cnt(128);
        table_meta->set_term(1024);
        table_meta->add_replicas("127.0.0.1:9527");
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // recover
    {
        TabletImpl tablet;
        tablet.Init("");
        ::fedb::api::LoadTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_seg_cnt(64);
        table_meta->add_replicas("127.0.0.1:9530");
        table_meta->add_replicas("127.0.0.1:9531");
        ::fedb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        std::string file = FLAGS_db_root_path + "/" + std::to_string(id) + "_" +
                           std::to_string(1) + "/table_meta.txt";
        int fd = open(file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::fedb::api::TableMeta table_meta_test;
        google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
        ASSERT_EQ(table_meta_test.seg_cnt(), 64);
        ASSERT_EQ(table_meta_test.term(), 1024lu);
        ASSERT_EQ(table_meta_test.replicas_size(), 2);
        ASSERT_STREQ(table_meta_test.replicas(0).c_str(), "127.0.0.1:9530");

        sleep(1);
        ::fedb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("test1");
        sr.set_st(9530);
        sr.set_et(9526);
        ::fedb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
        ::fedb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        ::fedb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        sleep(2);
    }

    {
        TabletImpl tablet;
        tablet.Init("");
        ::fedb::api::LoadTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::fedb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("test1");
        sr.set_st(9530);
        sr.set_et(9526);
        ::fedb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
    }
}

TEST_F(TabletImplTest, LoadWithDeletedKey) {
    uint32_t id = counter++;
    MockClosure closure;
    ::fedb::api::TableMeta table_meta_test;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_seg_cnt(8);
        table_meta->set_term(1024);

        ::fedb::common::ColumnDesc* column_desc1 =
            table_meta->add_column_desc();
        column_desc1->set_name("card");
        column_desc1->set_type("string");
        column_desc1->set_add_ts_idx(true);
        ::fedb::common::ColumnDesc* column_desc2 =
            table_meta->add_column_desc();
        column_desc2->set_name("mcc");
        column_desc2->set_type("string");
        column_desc2->set_add_ts_idx(true);

        table_meta->add_replicas("127.0.0.1:9527");
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        for (int i = 0; i < 5; i++) {
            ::fedb::api::PutRequest request;
            request.set_time(1100);
            request.set_value("test");
            request.set_tid(id);
            request.set_pid(1);
            ::fedb::api::Dimension* d1 = request.add_dimensions();
            d1->set_idx(0);
            d1->set_key("card" + std::to_string(i));
            ::fedb::api::Dimension* d2 = request.add_dimensions();
            d2->set_idx(1);
            d2->set_key("mcc" + std::to_string(i));
            ::fedb::api::PutResponse response;
            MockClosure closure;
            tablet.Put(NULL, &request, &response, &closure);
            ASSERT_EQ(0, response.code());
        }
        ::fedb::api::DeleteIndexRequest deleteindex_request;
        ::fedb::api::GeneralResponse deleteindex_response;
        // delete first index should fail
        deleteindex_request.set_idx_name("mcc");
        deleteindex_request.set_tid(id);
        deleteindex_request.set_pid(1);
        tablet.DeleteIndex(NULL, &deleteindex_request, &deleteindex_response,
                           &closure);
        ASSERT_EQ(0, deleteindex_response.code());
    }
    // load
    {
        TabletImpl tablet;
        tablet.Init("");
        ::fedb::api::LoadTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        ::fedb::api::GeneralResponse response;
        MockClosure closure;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::fedb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("mcc0");
        sr.set_idx_name("mcc");
        sr.set_st(1200);
        sr.set_et(1000);
        ::fedb::api::ScanResponse srp;
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
    ::fedb::storage::LogParts* log_part =
        new ::fedb::storage::LogParts(12, 4, scmp);
    std::string binlog_dir =
        FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog/";
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    ::fedb::storage::WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 1;
    for (; count <= 10; count++) {
        offset++;
        ::fedb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::fedb::base::Slice slice(buffer);
        ::fedb::base::Status status = wh->Write(slice);
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
        ::fedb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key_new";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value_new" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::fedb::base::Slice slice(buffer);
        ::fedb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    binlog_index++;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    count = 1;
    for (; count <= 30; count++) {
        offset++;
        ::fedb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key_xxx";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value_xxx" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::fedb::base::Slice slice(buffer);
        ::fedb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    delete wh;
    {
        TabletImpl tablet;
        tablet.Init("");
        ::fedb::api::LoadTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(tid);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::GeneralResponse response;
        MockClosure closure;
        tablet.LoadTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::fedb::api::ScanRequest sr;
        sr.set_tid(tid);
        sr.set_pid(0);
        sr.set_pk("key_new");
        sr.set_st(50);
        sr.set_et(0);
        ::fedb::api::ScanResponse srp;
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

        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test1");
        prequest.set_tid(tid);
        prequest.set_pid(0);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        ::fedb::api::GeneralRequest grq;
        grq.set_tid(tid);
        grq.set_pid(0);
        ::fedb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file = FLAGS_db_root_path + "/" +
                                    std::to_string(tid) +
                                    "_0/snapshot/MANIFEST";
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::fedb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(61, (signed)manifest.offset());

        sleep(10);
        std::vector<std::string> vec;
        std::string binlog_path =
            FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog";
        ::fedb::base::GetFileName(binlog_path, vec);
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        // 3 minutes
        table_meta->set_ttl(3);
        table_meta->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // version 1
    //
    uint64_t now1 = ::baidu::common::timer::get_micros() / 1000;
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        prequest.set_time(now1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }
    // 1 minute before
    uint64_t now2 = now1 - 60 * 1000;
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_value("test2");
        prequest.set_tid(id);
        prequest.set_pid(1);
        prequest.set_time(now2);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // 2 minute before
    uint64_t now3 = now1 - 2 * 60 * 1000;
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_value("test3");
        prequest.set_tid(id);
        prequest.set_pid(1);
        prequest.set_time(now3);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        prequest.set_time(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
    }

    // get now3
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(now3);
        ::fedb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(now3, response.ts());
        ASSERT_EQ("test3", response.value());
    }

    // update ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_type(::fedb::api::TTLType::kAbsoluteTime);
        request.set_value(1);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }

    sleep(70);
    // get now3
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test1");
        request.set_ts(now3);
        ::fedb::api::GetResponse response;
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
    ::fedb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::fedb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(100, drs.code());

    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_mode(::fedb::api::TableMode::kTableFollower);
    table_meta->add_replicas("127.0.0.1:9527");
    ::fedb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::fedb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    // ReadOnly
    ASSERT_EQ(103, presponse.code());

    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
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
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(4);
    table_meta->set_wal(true);
    table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    // 1
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(1);
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 2
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(2);
        prequest.set_value("test2");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 3
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(3);
        prequest.set_value("test3");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 6
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(6);
        prequest.set_value("test6");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // eq
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(1);
        request.set_type(::fedb::api::GetType::kSubKeyEq);

        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, (signed)response.ts());
        ASSERT_EQ("test1", response.value());
    }
    // le
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(5);
        request.set_type(::fedb::api::GetType::kSubKeyLe);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(3, (signed)response.ts());
        ASSERT_EQ("test3", response.value());
    }
    // lt
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(3);
        request.set_type(::fedb::api::GetType::kSubKeyLt);

        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(2, (signed)response.ts());
        ASSERT_EQ("test2", response.value());
    }
    // gt
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(2);
        request.set_type(::fedb::api::GetType::kSubKeyGt);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(6, (signed)response.ts());
        ASSERT_EQ("test6", response.value());
    }
    // ge
    {
        ::fedb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(1);
        request.set_type(::fedb::api::GetType::kSubKeyGe);
        ::fedb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(6, (signed)response.ts());
        ASSERT_EQ("test6", response.value());
    }
}

TEST_F(TabletImplTest, Snapshot) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init("");
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    ::fedb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());

    ::fedb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(2);
    ::fedb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(100, presponse.code());
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse, &closure);
    ASSERT_EQ(0, presponse.code());

    ::fedb::api::GeneralRequest grequest;
    ::fedb::api::GeneralResponse gresponse;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        table_meta->set_key_entry_max_height(2);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        table_meta->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        table_meta->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        table_meta->set_key_entry_max_height(8);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        ASSERT_EQ(8, (signed)ts.skiplist_height());
    }
}

TEST_F(TabletImplTest, CreateTableAbsoluteTest_TTlDesc) {
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        ttl_desc->set_abs_ttl(10);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        ASSERT_EQ(10, (signed)ts.ttl_desc().abs_ttl());
        ASSERT_EQ(0, (signed)ts.ttl_desc().lat_ttl());
        ASSERT_EQ(::fedb::api::TTLType::kAbsoluteTime,
                  ts.ttl_desc().ttl_type());
    }
}

TEST_F(TabletImplTest, CreateTableLatestTest_TTlDesc) {
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        ttl_desc->set_lat_ttl(10);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        ASSERT_EQ(0, (signed)ts.ttl_desc().abs_ttl());
        ASSERT_EQ(10, (signed)ts.ttl_desc().lat_ttl());
        ASSERT_EQ(::fedb::api::TTLType::kLatestTime, ts.ttl_desc().ttl_type());
    }
}

TEST_F(TabletImplTest, CreateTableAbsAndLatTest) {
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(20);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        ASSERT_EQ(10, (signed)ts.ttl_desc().abs_ttl());
        ASSERT_EQ(20, (signed)ts.ttl_desc().lat_ttl());
        ASSERT_EQ(::fedb::api::TTLType::kAbsAndLat, ts.ttl_desc().ttl_type());
    }
}

TEST_F(TabletImplTest, CreateTableAbsAndOrTest) {
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(20);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
        tablet.GetTableStatus(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        const TableStatus& ts = response.all_table_status(0);
        ASSERT_EQ(10, (signed)ts.ttl_desc().abs_ttl());
        ASSERT_EQ(20, (signed)ts.ttl_desc().lat_ttl());
        ASSERT_EQ(::fedb::api::TTLType::kAbsOrLat, ts.ttl_desc().ttl_type());
    }
}

TEST_F(TabletImplTest, CreateTableAbsAndLatTest_Specify) {
    uint32_t id = counter++;
    MockClosure closure;
    TabletImpl tablet;
    tablet.Init("");
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        table_meta->set_ttl_type(::fedb::api::TTLType::kAbsoluteTime);
        table_meta->set_key_entry_max_height(8);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // get table status
    {
        ::fedb::api::GetTableStatusRequest request;
        ::fedb::api::GetTableStatusResponse response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::fedb::api::kTableLeader);
        table_meta->set_wal(true);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        ::fedb::api::PutRequest prequest;
        ::fedb::api::PutResponse presponse;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        ::fedb::api::GeneralRequest grequest;
        ::fedb::api::GeneralResponse gresponse;
        grequest.set_tid(id);
        grequest.set_pid(1);
        tablet.MakeSnapshot(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        sleep(1);

        ::fedb::api::GetTermPairRequest pair_request;
        ::fedb::api::GetTermPairResponse pair_response;
        pair_request.set_tid(id);
        pair_request.set_pid(1);
        tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
        ASSERT_EQ(0, pair_response.code());
        ASSERT_TRUE(pair_response.has_table());
        ASSERT_EQ(1, (signed)pair_response.offset());
    }
    TabletImpl tablet;
    tablet.Init("");
    ::fedb::api::GetTermPairRequest pair_request;
    ::fedb::api::GetTermPairResponse pair_response;
    pair_request.set_tid(id);
    pair_request.set_pid(1);
    tablet.GetTermPair(NULL, &pair_request, &pair_response, &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(1, (signed)pair_response.offset());

    std::string manifest_file =
        FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
    int fd = open(manifest_file.c_str(), O_RDONLY);
    ASSERT_GT(fd, 0);
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    ::fedb::api::Manifest manifest;
    google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    std::string snapshot_file = FLAGS_db_root_path + "/" + std::to_string(id) +
                                "_1/snapshot/" + manifest.name();
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_ttl(1);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::fedb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        ::fedb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file = FLAGS_db_root_path + "/" +
                                    std::to_string(id) + "_1/snapshot/MANIFEST";
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::fedb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(1, (signed)manifest.offset());
    }
    FLAGS_make_snapshot_threshold_offset = 5;
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test2");
        prequest.set_time(9527);
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        prequest.set_pk("test3");
        prequest.set_time(9527);
        prequest.set_value("test2");
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        ::fedb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        ::fedb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        sleep(1);
        std::string manifest_file = FLAGS_db_root_path + "/" +
                                    std::to_string(id) + "_1/snapshot/MANIFEST";
        int fd = open(manifest_file.c_str(), O_RDONLY);
        ASSERT_GT(fd, 0);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::fedb::api::Manifest manifest;
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        ASSERT_EQ(1, (signed)manifest.offset());
        std::string snapshot_file = FLAGS_db_root_path + "/" +
                                    std::to_string(id) + "_1/snapshot/" +
                                    manifest.name();
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(50);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(5);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // bigger than max ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(60 * 24 * 365 * 30 * 2);
        ttl_desc->set_lat_ttl(5);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // bigger than max ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(30);
        ttl_desc->set_lat_ttl(20000);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(5);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
    // normal case
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::fedb::api::PutResponse presponse;
    ::fedb::api::PutRequest prequest;
    {
        prequest.set_pk("test");
        prequest.set_time(now - 60 * 60 * 1000);
        prequest.set_value("test9");
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        prequest.set_pk("test");
        prequest.set_time(now - 70 * 60 * 1000);
        prequest.set_value("test8");
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    MockClosure closure;
    ::fedb::api::GetRequest grequest;
    grequest.set_tid(id);
    grequest.set_pid(0);
    grequest.set_key("test");
    grequest.set_ts(0);
    ::fedb::api::GetResponse gresponse;
    tablet.Get(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
    ASSERT_EQ("test9", gresponse.value());
    // UpdateTTLRequest

    // ExecuteGcRequest
    ::fedb::api::ExecuteGcRequest request_execute;
    ::fedb::api::GeneralResponse response_execute;
    request_execute.set_tid(id);
    request_execute.set_pid(0);
    // etTableStatusRequest
    ::fedb::api::GetTableStatusRequest gr;
    ::fedb::api::GetTableStatusResponse gres;
    // ttl update to zero
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(0);
        ttl_desc->set_lat_ttl(0);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(100, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(50, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(0, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(0, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    // ttl update from zero to no zero
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(50);
        ttl_desc->set_lat_ttl(1);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(0, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(0, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(50, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(1, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    // update from 50 to 100
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(2);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(50, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(1, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(100, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(2, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(50);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // table not exist
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(0);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(5);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(100, response.code());
    }
    // bigger than max ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(60 * 24 * 365 * 30 * 2);
        ttl_desc->set_lat_ttl(5);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // bigger than max ttl
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(30);
        ttl_desc->set_lat_ttl(20000);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(132, response.code());
    }
    // ttl type mismatch
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(5);
        ::fedb::api::UpdateTTLResponse response;
        MockClosure closure;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(112, response.code());
    }
    // normal case
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::fedb::api::PutRequest prequest;
    ::fedb::api::PutResponse presponse;
    {
        prequest.set_pk("test");
        prequest.set_time(now - 60 * 60 * 1000);
        prequest.set_value("test9");
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        prequest.set_pk("test");
        prequest.set_time(now - 70 * 60 * 1000);
        prequest.set_value("test8");
        prequest.set_tid(id);
        prequest.set_pid(0);
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    ::fedb::api::GetRequest grequest;
    grequest.set_tid(id);
    grequest.set_pid(0);
    grequest.set_key("test");
    grequest.set_ts(0);
    ::fedb::api::GetResponse gresponse;
    MockClosure closure;
    tablet.Get(NULL, &grequest, &gresponse, &closure);
    ASSERT_EQ(0, gresponse.code());
    ASSERT_EQ("test9", gresponse.value());

    // ExecuteGcRequest
    ::fedb::api::ExecuteGcRequest request_execute;
    ::fedb::api::GeneralResponse response_execute;
    request_execute.set_tid(id);
    request_execute.set_pid(0);
    // etTableStatusRequest
    ::fedb::api::GetTableStatusRequest gr;
    ::fedb::api::GetTableStatusResponse gres;
    // ttl update to zero
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(0);
        ttl_desc->set_lat_ttl(0);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(100, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(50, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(0, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(0, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    // ttl update from zero to no zero
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(10);
        ttl_desc->set_lat_ttl(1);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());
        ASSERT_EQ("test9", gresponse.value());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(0, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(0, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(10, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(1, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    // update from 10 to 100
    {
        ::fedb::api::UpdateTTLRequest request;
        request.set_tid(id);
        request.set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(2);
        ::fedb::api::UpdateTTLResponse response;
        tablet.UpdateTTL(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(109, gresponse.code());

        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        bool checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(10, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(1, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
        prequest.set_time(now - 10 * 60 * 1000);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());

        tablet.ExecuteGc(NULL, &request_execute, &response_execute, &closure);
        sleep(3);

        gresponse.Clear();
        tablet.Get(NULL, &grequest, &gresponse, &closure);
        ASSERT_EQ(0, gresponse.code());

        gres.Clear();
        tablet.GetTableStatus(NULL, &gr, &gres, &closure);
        ASSERT_EQ(0, gres.code());
        checked = false;
        for (int32_t i = 0; i < gres.all_table_status_size(); i++) {
            const ::fedb::api::TableStatus& ts = gres.all_table_status(i);
            if (ts.tid() == id) {
                ASSERT_EQ(100, (signed)ts.ttl_desc().abs_ttl());
                ASSERT_EQ(2, (signed)ts.ttl_desc().lat_ttl());
                checked = true;
            }
        }
        ASSERT_TRUE(checked);
    }
    FLAGS_gc_interval = old_gc_interval;
}

TEST_F(TabletImplTest, ScanAtLeast) {
    TabletImpl tablet;
    tablet.Init("");
    MockClosure closure;
    uint32_t id = 100;
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_wal(true);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(0);
        ttl_desc->set_lat_ttl(0);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    ::fedb::api::PutResponse presponse;
    ::fedb::api::PutRequest prequest;
    for (int i = 0; i < 1000; ++i) {
        prequest.set_pk("test" + std::to_string(i % 10));
        prequest.set_time(now - i * 60 * 1000);
        prequest.set_value("test" + std::to_string(i % 10));
        prequest.set_tid(id);
        prequest.set_pid(0);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::fedb::api::ScanRequest sr;
    ::fedb::api::ScanResponse srp;
    // test atleast more than et
    for (int i = 0; i < 10; ++i) {
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_pk("test" + std::to_string(i));
        sr.set_st(now);
        sr.set_et(now - 500 * 60 * 1000);
        sr.set_atleast(80);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
    sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsAndLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(10);
        ::fedb::common::ColumnDesc* desc = table_meta->add_column_desc();
        desc->set_name("test");
        desc->set_type("string");
        desc->set_add_ts_idx(true);
        desc->set_is_ts_col(false);
        desc = table_meta->add_column_desc();
        desc->set_name("ts1");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc = table_meta->add_column_desc();
        desc->set_name("ts2");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(50);
        desc->set_lat_ttl(8);
        desc = table_meta->add_column_desc();
        desc->set_name("ts3");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(70);
        desc->set_lat_ttl(5);
        desc = table_meta->add_column_desc();
        desc->set_name("ts4");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(0);
        desc->set_lat_ttl(5);
        desc = table_meta->add_column_desc();
        desc->set_name("ts5");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(50);
        desc->set_lat_ttl(0);
        desc = table_meta->add_column_desc();
        desc->set_name("ts6");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(0);
        desc->set_lat_ttl(0);
        ::fedb::common::ColumnKey* column_key = table_meta->add_column_key();
        column_key->set_index_name("test");
        column_key->add_ts_name("ts1");
        column_key->add_ts_name("ts2");
        column_key->add_ts_name("ts3");
        column_key->add_ts_name("ts4");
        column_key->add_ts_name("ts5");
        column_key->add_ts_name("ts6");
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 100; ++i) {
        ::fedb::api::PutResponse presponse;
        ::fedb::api::PutRequest prequest;
        ::fedb::api::Dimension* dim = prequest.add_dimensions();
        dim->set_idx(0);
        dim->set_key("test" + std::to_string(i % 10));
        ::fedb::api::TSDimension* ts = prequest.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(2);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(3);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(4);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(5);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        prequest.set_tid(id);
        prequest.set_pid(0);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    //// Traverse test
    // ts1 has no expire data traverse return 100
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts1");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // ts2 has 20 expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts2");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(80, (signed)srp->count());
    }
    // ts3 has 30 expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts3");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(70, (signed)srp->count());
    }
    // ts4 has no expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts4");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // ts5 has no expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts5");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // ts6 has no expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts6");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    // //// Scan Count test
    ::fedb::api::ScanRequest sr;
    ::fedb::api::ScanResponse srp;
    ::fedb::api::CountRequest cr;
    ::fedb::api::CountResponse crp;
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
        sr.set_ts_name("ts1");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_ts_name("ts1");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 60 * 60 * 1000 + 100);
        cr.set_ts_name("ts2");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 60 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(7, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 50 * 60 * 1000 + 100);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_ts_name("ts2");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 50 * 60 * 1000 + 100);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 90 * 60 * 1000 + 100);
        cr.set_ts_name("ts2");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 80 * 60 * 1000 + 100);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts4");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_ts_name("ts4");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts4");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(8, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts4");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts4");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts4");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts5");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_ts_name("ts5");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts5");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(8, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts5");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts5");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts5");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts6");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts6");
        cr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //// Get test
    ::fedb::api::GetRequest gr;
    ::fedb::api::GetResponse grp;
    gr.set_type(::fedb::api::kSubKeyLe);
    //     time    cnt
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_ts_name("ts1");
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
        gr.set_ts_name("ts2");
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
        gr.set_ts_name("ts3");
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
        gr.set_ts_name("ts3");
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
        gr.set_ts_name("ts4");
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
        gr.set_ts_name("ts4");
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
        gr.set_ts_name("ts5");
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
        gr.set_ts_name("ts5");
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
        gr.set_ts_name("ts6");
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
        sr.set_ts_name("ts1");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(5);
        sr.set_limit(6);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(5);
        sr.set_limit(6);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(5);
        sr.set_limit(6);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_limit(7);
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        ::fedb::api::TTLDesc* ttl_desc = table_meta->mutable_ttl_desc();
        ttl_desc->set_ttl_type(::fedb::api::TTLType::kAbsOrLat);
        ttl_desc->set_abs_ttl(100);
        ttl_desc->set_lat_ttl(10);
        ::fedb::common::ColumnDesc* desc = table_meta->add_column_desc();
        desc->set_name("test");
        desc->set_type("string");
        desc->set_add_ts_idx(true);
        desc->set_is_ts_col(false);
        desc = table_meta->add_column_desc();
        desc->set_name("ts1");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc = table_meta->add_column_desc();
        desc->set_name("ts2");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(50);
        desc->set_lat_ttl(8);
        desc = table_meta->add_column_desc();
        desc->set_name("ts3");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(70);
        desc->set_lat_ttl(6);
        desc = table_meta->add_column_desc();
        desc->set_name("ts4");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(0);
        desc->set_lat_ttl(5);
        desc = table_meta->add_column_desc();
        desc->set_name("ts5");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(50);
        desc->set_lat_ttl(0);
        desc = table_meta->add_column_desc();
        desc->set_name("ts6");
        desc->set_type("int64");
        desc->set_add_ts_idx(false);
        desc->set_is_ts_col(true);
        desc->set_abs_ttl(0);
        desc->set_lat_ttl(0);

        ::fedb::common::ColumnKey* column_key = table_meta->add_column_key();
        column_key->set_index_name("test");
        column_key->add_ts_name("ts1");
        column_key->add_ts_name("ts2");
        column_key->add_ts_name("ts3");
        column_key->add_ts_name("ts4");
        column_key->add_ts_name("ts5");
        column_key->add_ts_name("ts6");
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 100; ++i) {
        ::fedb::api::PutResponse presponse;
        ::fedb::api::PutRequest prequest;
        ::fedb::api::Dimension* dim = prequest.add_dimensions();
        dim->set_idx(0);
        dim->set_key("test" + std::to_string(i % 10));
        ::fedb::api::TSDimension* ts = prequest.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(2);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(3);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(4);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        ts = prequest.add_ts_dimensions();
        ts->set_idx(5);
        ts->set_ts(now - (99 - i) * 60 * 1000);
        prequest.set_tid(id);
        prequest.set_pid(0);
        tablet.Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts1");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts2");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(50, (signed)srp->count());
    }
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts3");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(60, (signed)srp->count());
    }
    // ts4 has 50 expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts4");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(50, (signed)srp->count());
    }
    // ts5 has 50 expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts5");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(50, (signed)srp->count());
    }
    // ts6 has no expire
    {
        ::fedb::api::TraverseRequest sr;
        sr.set_tid(id);
        sr.set_pid(0);
        sr.set_limit(100);
        sr.set_ts_name("ts6");
        ::fedb::api::TraverseResponse* srp =
            new ::fedb::api::TraverseResponse();
        tablet.Traverse(NULL, &sr, srp, &closure);
        ASSERT_EQ(0, srp->code());
        ASSERT_EQ(100, (signed)srp->count());
    }

    // //// Scan Count test
    ::fedb::api::ScanRequest sr;
    ::fedb::api::ScanResponse srp;
    ::fedb::api::CountRequest cr;
    ::fedb::api::CountResponse crp;
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
        sr.set_ts_name("ts1");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(10, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_ts_name("ts1");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 60 * 60 * 1000 + 100);
        cr.set_ts_name("ts2");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(6, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts2");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 70 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 90 * 60 * 1000 + 100);
        cr.set_ts_name("ts2");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 80 * 60 * 1000 + 100);
        cr.set_et(now - 100 * 60 * 1000 + 100);
        cr.set_ts_name("ts3");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts4");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_ts_name("ts4");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts4");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts4");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts4");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts4");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts5");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(4, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 40 * 60 * 1000 + 100);
        cr.set_ts_name("ts5");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts5");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(5, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts5");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts5");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(307, srp.code());
        ASSERT_EQ(0, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts5");
        cr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts6");
        sr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, (signed)srp.count());
        cr.set_tid(id);
        cr.set_pid(0);
        cr.set_key("test" + std::to_string(i));
        cr.set_st(now - 60 * 60 * 1000 + 100);
        cr.set_et(now - 80 * 60 * 1000 + 100);
        cr.set_ts_name("ts6");
        cr.set_et_type(::fedb::api::kSubKeyGe);
        tablet.Count(NULL, &cr, &crp, &closure);
        ASSERT_EQ(0, crp.code());
        ASSERT_EQ(2, (signed)crp.count());
    }
    //// Get test
    ::fedb::api::GetRequest gr;
    ::fedb::api::GetResponse grp;
    gr.set_type(::fedb::api::kSubKeyLe);
    //     time    cnt
    // ts  valid   valid
    for (int i = 0; i < 10; ++i) {
        gr.set_tid(id);
        gr.set_pid(0);
        gr.set_key("test" + std::to_string(i));
        gr.set_ts(now - 80 * 60 * 1000 + 100);
        gr.set_ts_name("ts1");
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
        gr.set_ts_name("ts2");
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
        gr.set_ts_name("ts3");
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
        gr.set_ts_name("ts3");
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
        gr.set_ts_name("ts4");
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
        gr.set_ts_name("ts4");
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
        gr.set_ts_name("ts5");
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
        gr.set_ts_name("ts5");
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
        gr.set_ts_name("ts6");
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
        sr.set_ts_name("ts1");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(10);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(5);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(3);
        sr.set_limit(4);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(3);
        sr.set_limit(4);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(3);
        sr.set_limit(4);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts1");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts2");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
        sr.set_ts_name("ts3");
        sr.set_atleast(9);
        sr.set_limit(9);
        sr.set_et_type(::fedb::api::kSubKeyGe);
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
    ::fedb::base::RemoveDirRecursive(FLAGS_recycle_bin_root_path);
    ::fedb::base::MkdirRecur(
        "/tmp/gtest/recycle/99_1_binlog_20191111070955/binlog/");
    ::fedb::base::MkdirRecur(
        "/tmp/gtest/recycle/100_2_20191111115149/binlog/");
    TabletImpl tablet;
    tablet.Init("");

    std::vector<std::string> file_vec;
    ::fedb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(2, (signed)file_vec.size());
    std::cout << "sleep for 30s" << std::endl;
    sleep(30);

    std::string now_time = ::fedb::base::GetNowTime();
    ::fedb::base::MkdirRecur("/tmp/gtest/recycle/99_3_" + now_time +
                              "/binlog/");
    ::fedb::base::MkdirRecur("/tmp/gtest/recycle/100_4_binlog_" + now_time +
                              "/binlog/");
    file_vec.clear();
    ::fedb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(4, (signed)file_vec.size());

    std::cout << "sleep for 35s" << std::endl;
    sleep(35);

    file_vec.clear();
    ::fedb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(2, (signed)file_vec.size());

    std::cout << "sleep for 65s" << std::endl;
    sleep(65);

    file_vec.clear();
    ::fedb::base::GetChildFileName(FLAGS_recycle_bin_root_path, file_vec);
    ASSERT_EQ(0, (signed)file_vec.size());

    ::fedb::base::RemoveDirRecursive("/tmp/gtest");
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
    ::fedb::api::CreateTableRequest request;
    ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
    ::fedb::common::ColumnDesc* desc = table_meta->add_column_desc();
    desc->set_name("card");
    desc->set_type("string");
    desc->set_add_ts_idx(true);
    desc = table_meta->add_column_desc();
    desc->set_name("mcc");
    desc->set_type("string");
    desc->set_add_ts_idx(true);
    desc = table_meta->add_column_desc();
    desc->set_name("price");
    desc->set_type("int64");
    desc->set_add_ts_idx(false);
    desc = table_meta->add_column_desc();
    desc->set_name("ts1");
    desc->set_type("int64");
    desc->set_add_ts_idx(false);
    desc->set_is_ts_col(true);
    desc = table_meta->add_column_desc();
    desc->set_name("ts2");
    desc->set_type("int64");
    desc->set_add_ts_idx(false);
    desc->set_is_ts_col(true);
    ::fedb::common::ColumnKey* column_key = table_meta->add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->add_ts_name("ts1");
    column_key->add_ts_name("ts2");

    std::vector<::fedb::codec::ColumnDesc> columns;
    ::fedb::codec::SchemaCodec::ConvertColumnDesc(table_meta->column_desc(),
                                                   columns);
    ::fedb::codec::SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    table_meta->set_schema(buffer);
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    ::fedb::api::CreateTableResponse response;
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
        std::vector<std::pair<std::string, uint32_t>> dimensions;
        MultiDimensionEncode(columns, input, dimensions, value);
        ::fedb::api::PutRequest request;
        request.set_value(value);
        request.set_tid(id);
        request.set_pid(1);

        ::fedb::api::Dimension* d = request.add_dimensions();
        d->set_key(input[0]);
        d->set_idx(0);
        ::fedb::api::TSDimension* tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 100);
        tsd->set_idx(0);
        tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 10000);
        tsd->set_idx(1);
        ::fedb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    ::fedb::api::GeneralRequest grq;
    grq.set_tid(id);
    grq.set_pid(1);
    ::fedb::api::GeneralResponse grp;
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
        std::vector<std::pair<std::string, uint32_t>> dimensions;
        MultiDimensionEncode(columns, input, dimensions, value);
        ::fedb::api::PutRequest request;
        request.set_value(value);
        request.set_tid(id);
        request.set_pid(1);
        ::fedb::api::Dimension* d = request.add_dimensions();
        d->set_key(input[0]);
        d->set_idx(0);
        ::fedb::api::TSDimension* tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 100);
        tsd->set_idx(0);
        tsd = request.add_ts_dimensions();
        tsd->set_ts(i + 10000);
        tsd->set_idx(1);
        ::fedb::api::PutResponse response;
        MockClosure closure;
        tablet.Put(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        column_key = table_meta->add_column_key();
        column_key->set_index_name("card|mcc");
        column_key->add_col_name("card");
        column_key->add_col_name("mcc");
        column_key->add_ts_name("ts2");
        ::fedb::api::DumpIndexDataRequest dump_request;
        dump_request.set_tid(id);
        dump_request.set_pid(1);
        dump_request.set_partition_num(8);
        dump_request.set_idx(1);
        dump_request.mutable_column_key()->CopyFrom(*column_key);
        ::fedb::api::GeneralResponse dump_response;
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
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(0);
        table_meta->set_ttl(0);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_ttl_type(::fedb::api::TTLType::kLatestTime);
        table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    std::string index_file_path =
        FLAGS_db_root_path + "/" + std::to_string(id) + "_0/index/";
    ::fedb::base::MkdirRecur(index_file_path);
    std::string index_file = index_file_path + "0_1_index.data";
    FILE* f = fopen(index_file.c_str(), "w+");
    ASSERT_TRUE(f != NULL);
    for (int i = 0; i < 1000; ++i) {
        fputc('6', f);
    }
    fclose(f);
    uint64_t src_size = 0;
    ::fedb::base::GetFileSize(index_file, src_size);
    ::fedb::api::SendIndexDataRequest request;
    request.set_tid(id);
    request.set_pid(0);
    ::fedb::api::SendIndexDataRequest_EndpointPair* pair = request.add_pairs();
    pair->set_pid(1);
    pair->set_endpoint(FLAGS_endpoint);
    ::fedb::api::GeneralResponse response;
    tablet.SendIndexData(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    sleep(2);
    std::string des_index_file = FLAGS_db_root_path + "/" + std::to_string(id) +
                                 "_1/index/0_1_index.data";
    uint64_t des_size = 0;
    ::fedb::base::GetFileSize(des_index_file, des_size);
    ASSERT_TRUE(::fedb::base::IsExists(des_index_file));
    ASSERT_EQ(src_size, des_size);
    ::fedb::base::RemoveDirRecursive(FLAGS_db_root_path);
}

}  // namespace tablet
}  // namespace fedb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::fedb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::fedb::tablet::GenRand();
    return RUN_ALL_TESTS();
}
