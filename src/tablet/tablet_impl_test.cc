//
// tablet_impl_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-05
//

#include "tablet/tablet_impl.h"
#include "proto/tablet.pb.h"
#include "base/kv_iterator.h"
#include <boost/lexical_cast.hpp>
#include "gtest/gtest.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>

DECLARE_string(db_root_path);

namespace rtidb {
namespace tablet {

uint32_t counter = 10;

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
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


TEST_F(TabletImplTest, TTL) {
    uint32_t id = counter++;
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    TabletImpl tablet;
    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_wal(true);
    request.set_mode(::rtidb::api::TableMode::kTableLeader);
    // 1 minutes
    request.set_ttl(1);
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
        request.set_name("t0");
        request.set_tid(id);
        request.set_pid(1);
        request.set_wal(true);
        request.set_ttl(0);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
        request.set_name("");
        tablet.CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(8, response.code());
    }
    {
        ::rtidb::api::CreateTableRequest request;
        request.set_name("t0");
        request.set_ttl(0);
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
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(0);
    request.set_wal(true);
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
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(0);
    request.set_wal(true);
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
    sr.set_enable_remove_duplicated_record(true);
    ::rtidb::api::ScanResponse srp;
    tablet.Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(0, srp.code());
    ASSERT_EQ(3, srp.count());
}

TEST_F(TabletImplTest, Scan_with_limit) {
    TabletImpl tablet;
    uint32_t id = counter++;

    tablet.Init();
    ::rtidb::api::CreateTableRequest request;
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(0);
    request.set_wal(true);
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
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(0);
    request.set_wal(true);
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
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(1);
    request.set_wal(true);
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
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(1);
    request.set_mode(::rtidb::api::TableMode::kTableLeader);
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
    tablet.CreateTable(NULL, &request, &response,
            &closure);
    ASSERT_EQ(0, response.code());
}

TEST_F(TabletImplTest, Recover) {
    uint32_t id = counter++;
    MockClosure closure;
    {
        TabletImpl tablet;
        tablet.Init();
        ::rtidb::api::CreateTableRequest request;
        request.set_name("t0");
        request.set_tid(id);
        request.set_pid(1);
        request.set_ttl(0);
        request.set_mode(::rtidb::api::TableMode::kTableLeader);
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
        request.set_name("t0");
        request.set_tid(id);
        request.set_pid(1);
        request.set_ttl(0);
        request.set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
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
    }

    {
        TabletImpl tablet;
        tablet.Init();
        ::rtidb::api::LoadTableRequest request;
        request.set_name("t0");
        request.set_tid(id);
        request.set_pid(1);
        request.set_ttl(0);
        request.set_mode(::rtidb::api::TableMode::kTableLeader);
        ::rtidb::api::GeneralResponse response;
        tablet.LoadTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
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

}

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
    request.set_name("t0");
    request.set_tid(id);
    request.set_pid(1);
    request.set_ttl(1);
    request.set_mode(::rtidb::api::TableMode::kTableFollower);
    request.add_replicas("127.0.0.1:9527");
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

    //fix slave drop fails bugs
    ::rtidb::api::AppendEntriesRequest arequest;
    request.set_tid(id);
    request.set_pid(1);
    ::rtidb::api::AppendEntriesResponse aresponse;
    tablet.AppendEntries(NULL, &arequest, &aresponse, &closure);
    tablet.DropTable(NULL, &dr, &drs, &closure);
    ASSERT_EQ(0, drs.code());
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



}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::rtidb::tablet::GenRand();
    return RUN_ALL_TESTS();
}



