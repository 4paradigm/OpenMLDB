//
// tablet_impl_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-05
//

#include "tablet/tablet_impl.h"
#include "proto/tablet.pb.h"
#include "base/kv_iterator.h"
#include "gtest/gtest.h"
#include "logging.h"
#include "timer.h"
#include "base/schema_codec.h"
#include "base/flat_array.h"
#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <sys/stat.h> 
#include <fcntl.h>

DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);

namespace rtidb {
namespace tablet {

uint32_t counter = 10;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);
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

int MultiDimensionEncode(const std::vector<::rtidb::base::ColumnDesc>& colum_desc, 
            const std::vector<std::string>& input, 
            std::vector<std::pair<std::string, uint32_t>>& dimensions,
            std::string& buffer) {
    uint32_t cnt = input.size();
    if (cnt != colum_desc.size()) {
        std::cout << "Input value mismatch schema" << std::endl;
        return -1;
    }
    ::rtidb::base::FlatArrayCodec codec(&buffer, (uint8_t) cnt);
    uint32_t idx_cnt = 0;
    for (uint32_t i = 0; i < input.size(); i++) {
        if (colum_desc[i].add_ts_idx) {
            dimensions.push_back(std::make_pair(input[i], idx_cnt));
            idx_cnt ++;
        }
        bool codec_ok = false;
        if (colum_desc[i].type == ::rtidb::base::ColType::kInt32) {
            codec_ok = codec.Append(boost::lexical_cast<int32_t>(input[i]));
        } else if (colum_desc[i].type == ::rtidb::base::ColType::kInt64) {
            codec_ok = codec.Append(boost::lexical_cast<int64_t>(input[i]));
        } else if (colum_desc[i].type == ::rtidb::base::ColType::kUInt32) {
            codec_ok = codec.Append(boost::lexical_cast<uint32_t>(input[i]));
        } else if (colum_desc[i].type == ::rtidb::base::ColType::kUInt64) {
            codec_ok = codec.Append(boost::lexical_cast<uint64_t>(input[i]));
        } else if (colum_desc[i].type == ::rtidb::base::ColType::kFloat) {
            codec_ok = codec.Append(boost::lexical_cast<float>(input[i]));
        } else if (colum_desc[i].type == ::rtidb::base::ColType::kDouble) {
            codec_ok = codec.Append(boost::lexical_cast<double>(input[i]));
        } else if (colum_desc[i].type == ::rtidb::base::ColType::kString) {
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
                  std::vector<std::string>& output) {
    rtidb::base::FlatArrayIterator fit(value.c_str(), value.size());
    while (fit.Valid()) {
        std::string col;
        if (fit.GetType() == ::rtidb::base::ColType::kString) {
            fit.GetString(&col);
        }else if (fit.GetType() == ::rtidb::base::ColType::kInt32) {
            int32_t int32_col = 0;
            fit.GetInt32(&int32_col);
            col = boost::lexical_cast<std::string>(int32_col);
        }else if (fit.GetType() == ::rtidb::base::ColType::kInt64) {
            int64_t int64_col = 0;
            fit.GetInt64(&int64_col);
            col = boost::lexical_cast<std::string>(int64_col);
        }else if (fit.GetType() == ::rtidb::base::ColType::kUInt32) {
            uint32_t uint32_col = 0;
            fit.GetUInt32(&uint32_col);
            col = boost::lexical_cast<std::string>(uint32_col);
        }else if (fit.GetType() == ::rtidb::base::ColType::kUInt64) {
            uint64_t uint64_col = 0;
            fit.GetUInt64(&uint64_col);
            col = boost::lexical_cast<std::string>(uint64_col);
        }else if (fit.GetType() == ::rtidb::base::ColType::kDouble) {
            double double_col = 0.0d;
            fit.GetDouble(&double_col);
            col = boost::lexical_cast<std::string>(double_col);
        }else if (fit.GetType() == ::rtidb::base::ColType::kFloat) {
            float float_col = 0.0f;
            fit.GetFloat(&float_col);
            col = boost::lexical_cast<std::string>(float_col);
        }
        fit.Next();
        output.push_back(col);
    }
}

TEST_F(TabletImplTest, Get) {
    TabletImpl tablet;
    tablet.Init();
    // table not found
    {
        ::rtidb::api::GetRequest request;
        request.set_tid(1);
        request.set_pid(0);
        request.set_key("test");
        request.set_ts(0);
        ::rtidb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(-1, response.code());
    }
    // create table
    uint32_t id = counter++;
    {
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_ttl(1);
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
    }
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    // key not found
    {
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now);
        ::rtidb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(1, response.code());
    }
    // put/get expired key
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test0");
        prequest.set_time(now - 2 * 60 * 1000);
        prequest.set_value("value0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test0");
        request.set_ts(0);
        ::rtidb::api::GetResponse response;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(1, response.code());
    }
    // put some key
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(now);
        prequest.set_value("test10");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(now - 2);
        prequest.set_value("test9");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());

        prequest.set_time(now - 120 * 1000);
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {        
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 2);
        ::rtidb::api::GetResponse response;
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
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 1);
        ::rtidb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(1, response.code());
    }
    {        
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 2);
        ::rtidb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ("test9", response.value());
    }
    {        
        // get expired key
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(now - 120 * 1000);
        ::rtidb::api::GetResponse response;
        MockClosure closure;
        tablet.Get(NULL, &request, &response, &closure);
        ASSERT_EQ(1, response.code());
    }

    // create latest ttl table
    id = counter++;
    {
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t1");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(5);
        table_meta->set_ttl_type(::rtidb::api::TTLType::kLatestTime);
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
    }
    int num = 10;
    while (num) {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test");
        prequest.set_time(num);
        prequest.set_value("test" + std::to_string(num));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        MockClosure closure;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
        num--;
    }    
    {        
        ::rtidb::api::GetRequest request;
        request.set_tid(id);
        request.set_pid(1);
        request.set_key("test");
        request.set_ts(5);
        ::rtidb::api::GetResponse response;
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

TEST_F(TabletImplTest, CreateTableWithSchema) {
    
    TabletImpl tablet;
    tablet.Init();
    {
        uint32_t id = counter++;
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
        

        //get schema
        ::rtidb::api::GetTableSchemaRequest request0;
        request0.set_tid(id);
        request0.set_pid(1);
        ::rtidb::api::GetTableSchemaResponse response0;
        tablet.GetTableSchema(NULL, &request0, &response0, &closure);
        ASSERT_EQ("", response0.schema());
        
    }
    {
        std::vector<::rtidb::base::ColumnDesc> columns;
        ::rtidb::base::ColumnDesc desc1;
        desc1.type = ::rtidb::base::ColType::kString;
        desc1.name = "card";
        desc1.add_ts_idx = true;
        columns.push_back(desc1);

        ::rtidb::base::ColumnDesc desc2;
        desc2.type = ::rtidb::base::ColType::kDouble;
        desc2.name = "amt";
        desc2.add_ts_idx = false;
        columns.push_back(desc2);

        ::rtidb::base::ColumnDesc desc3;
        desc3.type = ::rtidb::base::ColType::kInt32;
        desc3.name = "apprv_cde";
        desc3.add_ts_idx = false;
        columns.push_back(desc3);

        ::rtidb::base::SchemaCodec codec;
        std::string buffer;
        codec.Encode(columns, buffer);
        uint32_t id = counter++;
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        table_meta->set_schema(buffer);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());

        ::rtidb::api::GetTableSchemaRequest request0;
        request0.set_tid(id);
        request0.set_pid(1);
        ::rtidb::api::GetTableSchemaResponse response0;
        tablet.GetTableSchema(NULL, &request0, &response0, &closure);
        ASSERT_TRUE(response0.schema().size() != 0);

        std::vector<::rtidb::base::ColumnDesc> ncolumns;
        codec.Decode(response0.schema(), ncolumns);
        ASSERT_EQ(3, ncolumns.size());
        ASSERT_EQ(::rtidb::base::ColType::kString, ncolumns[0].type);
        ASSERT_EQ("card", ncolumns[0].name);
        ASSERT_EQ(::rtidb::base::ColType::kDouble, ncolumns[1].type);
        ASSERT_EQ("amt", ncolumns[1].name);
        ASSERT_EQ(::rtidb::base::ColType::kInt32, ncolumns[2].type);
        ASSERT_EQ("apprv_cde", ncolumns[2].name);
    }
    
}

TEST_F(TabletImplTest, MultiGet) {
    TabletImpl tablet;
    tablet.Init();
    uint32_t id = counter++;
    std::vector<::rtidb::base::ColumnDesc> columns;
    ::rtidb::base::ColumnDesc desc1;
    desc1.type = ::rtidb::base::ColType::kString;
    desc1.name = "pk";
    desc1.add_ts_idx = true;
    columns.push_back(desc1);

    ::rtidb::base::ColumnDesc desc2;
    desc2.type = ::rtidb::base::ColType::kString;
    desc2.name = "amt";
    desc2.add_ts_idx = true;
    columns.push_back(desc2);

    ::rtidb::base::ColumnDesc desc3;
    desc3.type = ::rtidb::base::ColType::kInt32;
    desc3.name = "apprv_cde";
    desc3.add_ts_idx = false;
    columns.push_back(desc3);

    ::rtidb::base::SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    table_meta->set_schema(buffer);
    for (uint32_t i = 0; i < columns.size(); i++) {
        if (columns[i].add_ts_idx) {
            table_meta->add_dimensions(columns[i].name);
        }
    }
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    
	// put
    for (int i = 0; i < 5; i++) {
        std::vector<std::string> input;
        input.push_back("test" + std::to_string(i));
        input.push_back("abcd" + std::to_string(i));
        input.push_back("1212"+ std::to_string(i));
        std::string value;
        std::vector<std::pair<std::string, uint32_t>> dimensions;
        MultiDimensionEncode(columns, input, dimensions, value);
		::rtidb::api::PutRequest request;
		request.set_time(1100);
		request.set_value(value);
		request.set_tid(id);
		request.set_pid(1);
		for (size_t i = 0; i < dimensions.size(); i++) {
			::rtidb::api::Dimension* d = request.add_dimensions();
			d->set_key(dimensions[i].first);
			d->set_idx(dimensions[i].second);
		}
		::rtidb::api::PutResponse response;
		MockClosure closure;
		tablet.Put(NULL, &request, &response, &closure);
    	ASSERT_EQ(0, response.code());
    }
	// get
    ::rtidb::api::GetRequest get_request;
    ::rtidb::api::GetResponse get_response;
	get_request.set_tid(id);
    get_request.set_pid(1);
    get_request.set_key("abcd2");
    get_request.set_ts(1100);
    get_request.set_idx_name("amt");
	
	tablet.Get(NULL, &get_request, &get_response, &closure);
  	ASSERT_EQ(0, get_response.code());
  	ASSERT_EQ(1100, get_response.ts());
	std::string value(get_response.value());
	std::vector<std::string> vec;
    MultiDimensionDecode(value, vec);
  	ASSERT_EQ(3, vec.size());
	ASSERT_STREQ("test2", vec[0].c_str());
	ASSERT_STREQ("abcd2", vec[1].c_str());
	ASSERT_STREQ("12122", vec[2].c_str());
}    

TEST_F(TabletImplTest, TTL) {
    uint32_t id = counter++;
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    TabletImpl tablet;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_wal(true);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    // 1 minutes
    table_meta->set_ttl(1);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(now);
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());

    }
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(now - 2 * 60 * 1000);
        prequest.set_value("test2");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }

}

TEST_F(TabletImplTest, CreateTable) {
    uint32_t id = counter++;
    TabletImpl tablet;
    tablet.Init();
    {
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_wal(true);
        table_meta->set_ttl(0);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
		std::string file = FLAGS_db_root_path + "/" + std::to_string(id) +"_" + std::to_string(1) + "/table_meta.txt";
        int fd = open(file.c_str(), O_RDONLY);
		ASSERT_GT(fd, 0);
		google::protobuf::io::FileInputStream fileInput(fd);
		fileInput.SetCloseOnDelete(true);
		::rtidb::api::TableMeta table_meta_test;
		google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
		ASSERT_EQ(table_meta_test.tid(), id);
		ASSERT_STREQ(table_meta_test.name().c_str(), "t0");
		
        table_meta->set_name("");
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(8, response.code());
    }
    {
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_ttl(0);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(8, response.code());
    }

}

TEST_F(TabletImplTest, Put) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());

    ::rtidb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(2);
    prequest.set_pid(2);
    ::rtidb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ASSERT_EQ(10, presponse.code());
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ASSERT_EQ(0, presponse.code());
}

TEST_F(TabletImplTest, Scan_with_duplicate_skip) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9529);
        prequest.set_value("testx");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::rtidb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(9530);
    sr.set_et(0);
    sr.set_enable_remove_duplicated_record(true);
    ::rtidb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(3, srp.count());
}

TEST_F(TabletImplTest, Scan_with_latestN) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
    for (int ts = 9527; ts < 9540; ts++) {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(ts);
        prequest.set_value("test" + std::to_string(ts));
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::rtidb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(0);
    sr.set_et(0);
    sr.set_limit(2);
    ::rtidb::api::ScanResponse* srp = new ::rtidb::api::ScanResponse();
    tablet.Scan(NULL, &sr, srp, &closure);
    ASSERT_EQ(0, srp->code());
    ASSERT_EQ(2, srp->count());
    ::rtidb::base::KvIterator* kv_it = new ::rtidb::base::KvIterator(srp);
    ASSERT_EQ(9539, kv_it->GetKey());
    ASSERT_STREQ("test9539", kv_it->GetValue().ToString().c_str());
    kv_it->Next();
    ASSERT_EQ(9538, kv_it->GetKey());
    ASSERT_STREQ("test9538", kv_it->GetValue().ToString().c_str());
    kv_it->Next();
    ASSERT_FALSE(kv_it->Valid());
    delete kv_it;
}

TEST_F(TabletImplTest, Scan_with_limit) {
    TabletImpl tablet;
    uint32_t id = counter++;

    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }

    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9529);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    ::rtidb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(9530);
    sr.set_et(9526);
    sr.set_limit(2);
    ::rtidb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(2, srp.count());
}

TEST_F(TabletImplTest, Scan) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
    ::rtidb::api::ScanRequest sr;
    sr.set_tid(2);
    sr.set_pk("test1");
    sr.set_st(9528);
    sr.set_et(9527);
    sr.set_limit(10);
    ::rtidb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.pairs().size());
    ASSERT_EQ(10, srp.code());

    sr.set_tid(id);
    sr.set_pid(1);
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(0, srp.count());

    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(2);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);

        ASSERT_EQ(10, presponse.code());
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse,
                &closure);

        ASSERT_EQ(0, presponse.code());

    }
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test0");
        prequest.set_tid(2);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);

        ASSERT_EQ(10, presponse.code());
        prequest.set_tid(id);
        prequest.set_pid(1);

        tablet.Put(NULL, &prequest, &presponse,
                &closure);

        ASSERT_EQ(0, presponse.code());

    }
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(1, srp.count());

}

TEST_F(TabletImplTest, GC) {
    TabletImpl tablet;
    uint32_t id = counter ++;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_wal(true);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());

    ::rtidb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::rtidb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    prequest.set_time(now);
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ::rtidb::api::ScanRequest sr;
    sr.set_tid(id);
    sr.set_pid(1);
    sr.set_pk("test1");
    sr.set_st(now);
    sr.set_et(9527);
    sr.set_limit(10);
    ::rtidb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(1, srp.count());
}

TEST_F(TabletImplTest, DropTable) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init();
    MockClosure closure;
    ::rtidb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::rtidb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(-1, drs.code());

    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
    ::rtidb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());

    ::rtidb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::rtidb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ASSERT_EQ(0, presponse.code());
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
}

/*TEST_F(TabletImplTest, Recover) {
    uint32_t id = counter++;
    MockClosure closure;
    {
        TabletImpl tablet;
        tablet.Init();
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_seg_cnt(128);
        table_meta->set_term(1024);
        table_meta->add_replicas("127.0.0.1:9527");
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // recover
    {
        TabletImpl tablet;
        tablet.Init();
        ::rtidb::api::LoadTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_seg_cnt(64);
        table_meta->add_replicas("127.0.0.1:9530");
        table_meta->add_replicas("127.0.0.1:9531");
        ::rtidb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());

		std::string file = FLAGS_db_root_path + "/" + std::to_string(id) +"_" + std::to_string(1) + "/table_meta.txt";
        int fd = open(file.c_str(), O_RDONLY);
		ASSERT_GT(fd, 0);
		google::protobuf::io::FileInputStream fileInput(fd);
		fileInput.SetCloseOnDelete(true);
		::rtidb::api::TableMeta table_meta_test;
		google::protobuf::TextFormat::Parse(&fileInput, &table_meta_test);
		ASSERT_EQ(table_meta_test.seg_cnt(), 64);
		ASSERT_EQ(table_meta_test.term(), 1024);
		ASSERT_EQ(table_meta_test.replicas_size(), 2);
		ASSERT_STREQ(table_meta_test.replicas(0).c_str(), "127.0.0.1:9530");

        sleep(1);
        ::rtidb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("test1");
        sr.set_st(9530);
        sr.set_et(9526);
        ::rtidb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(1, srp.count());
        ::rtidb::api::GeneralRequest grq;
        grq.set_tid(id);
        grq.set_pid(1);
        ::rtidb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet.MakeSnapshot(NULL, &grq, &grp, &closure);
        ASSERT_EQ(0, grp.code());
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9528);
        prequest.set_value("test1");
        prequest.set_tid(id);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
        sleep(2);
    }

    {
        TabletImpl tablet;
        tablet.Init();
        ::rtidb::api::LoadTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
        sleep(1);
        ::rtidb::api::ScanRequest sr;
        sr.set_tid(id);
        sr.set_pid(1);
        sr.set_pk("test1");
        sr.set_st(9530);
        sr.set_et(9526);
        ::rtidb::api::ScanResponse srp;
        tablet.Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(0, srp.code());
        ASSERT_EQ(2, srp.count());
    }

}*/

TEST_F(TabletImplTest, DropTableFollower) {
    uint32_t id = counter++;
    TabletImpl tablet;
    tablet.Init();
    MockClosure closure;
    ::rtidb::api::DropTableRequest dr;
    dr.set_tid(id);
    dr.set_pid(1);
    ::rtidb::api::DropTableResponse drs;
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(-1, drs.code());

    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(1);
    table_meta->set_mode(::rtidb::api::TableMode::kTableFollower);
    table_meta->add_replicas("127.0.0.1:9527");
    ::rtidb::api::CreateTableResponse response;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
    ::rtidb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    ::rtidb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    //ReadOnly
    ASSERT_EQ(20, presponse.code());

    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
    sleep(1);
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ASSERT_EQ(10, presponse.code());
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());

}

TEST_F(TabletImplTest, Snapshot) {
    TabletImpl tablet;
    uint32_t id = counter++;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_name("t0");
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_ttl(0);
    table_meta->set_wal(true);
    ::rtidb::api::CreateTableResponse response;
    MockClosure closure;
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());

    ::rtidb::api::PutRequest prequest;
    prequest.set_pk("test1");
    prequest.set_time(9527);
    prequest.set_value("test0");
    prequest.set_tid(id);
    prequest.set_pid(2);
    ::rtidb::api::PutResponse presponse;
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ASSERT_EQ(10, presponse.code());
    prequest.set_tid(id);
    prequest.set_pid(1);
    tablet.Put(NULL, &prequest, &presponse,
            &closure);
    ASSERT_EQ(0, presponse.code());

    ::rtidb::api::GeneralRequest grequest;
    ::rtidb::api::GeneralResponse gresponse;
    grequest.set_tid(id);
    grequest.set_pid(1);
    tablet.PauseSnapshot(NULL, &grequest, &gresponse,
            &closure);
    ASSERT_EQ(0, gresponse.code());

    tablet.MakeSnapshot(NULL, &grequest, &gresponse,
            &closure);
    ASSERT_EQ(-1, gresponse.code());

    tablet.RecoverSnapshot(NULL, &grequest, &gresponse,
            &closure);
    ASSERT_EQ(0, gresponse.code());

    tablet.MakeSnapshot(NULL, &grequest, &gresponse,
            &closure);
    ASSERT_EQ(0, gresponse.code());
}

TEST_F(TabletImplTest, GetTermPair) {
    uint32_t id = counter++;
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path="/rtidb3" + GenRand();
    MockClosure closure;
    {
        TabletImpl tablet;
        tablet.Init();
        ::rtidb::api::CreateTableRequest request;
        ::rtidb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(id);
        table_meta->set_pid(1);
        table_meta->set_ttl(0);
        table_meta->set_mode(::rtidb::api::kTableLeader);
        table_meta->set_wal(true);
        ::rtidb::api::CreateTableResponse response;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());

        ::rtidb::api::PutRequest prequest;
        ::rtidb::api::PutResponse presponse;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test0");
        prequest.set_tid(id);
        prequest.set_pid(1);
        tablet.Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());

        ::rtidb::api::GeneralRequest grequest;
        ::rtidb::api::GeneralResponse gresponse;
        grequest.set_tid(id);
        grequest.set_pid(1);
        tablet.MakeSnapshot(NULL, &grequest, &gresponse,
                &closure);
        ASSERT_EQ(0, gresponse.code());
        sleep(1);

        ::rtidb::api::GetTermPairRequest pair_request;
        ::rtidb::api::GetTermPairResponse pair_response;
        pair_request.set_tid(id);
        pair_request.set_pid(1);
        tablet.GetTermPair(NULL, &pair_request, &pair_response,
                &closure);
        ASSERT_EQ(0, pair_response.code());
        ASSERT_TRUE(pair_response.has_table());
        ASSERT_EQ(1, pair_response.offset());
    }
    TabletImpl tablet;
    tablet.Init();
    ::rtidb::api::GetTermPairRequest pair_request;
    ::rtidb::api::GetTermPairResponse pair_response;
    pair_request.set_tid(id);
    pair_request.set_pid(1);
    tablet.GetTermPair(NULL, &pair_request, &pair_response,
            &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(1, pair_response.offset());

    std::string manifest_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/MANIFEST";
    int fd = open(manifest_file.c_str(), O_RDONLY);
    ASSERT_GT(fd, 0);
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    ::rtidb::api::Manifest manifest;
    google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    std::string snapshot_file = FLAGS_db_root_path + "/" + std::to_string(id) + "_1/snapshot/" + manifest.name();
    unlink(snapshot_file.c_str());
    tablet.GetTermPair(NULL, &pair_request, &pair_response,
            &closure);
    ASSERT_EQ(0, pair_response.code());
    ASSERT_FALSE(pair_response.has_table());
    ASSERT_EQ(0, pair_response.offset());
}



}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::rtidb::tablet::GenRand();
    return RUN_ALL_TESTS();
}



