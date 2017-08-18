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
#include "gflags/gflags.h"
#include "config.h"
#include "logging.h"
#include "timer.h"
#ifdef TCMALLOC_ENABLE
#include "gperftools/heap-checker.h"
#endif
#include <gflags/gflags.h>

DECLARE_string(snapshot_root_path);
DECLARE_string(binlog_root_path);


namespace rtidb {
namespace tablet {

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}


class MockClosure : public ::google::protobuf::Closure {

public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}

};

class TabletImplMemTest : public ::testing::Test {

public:
    TabletImplMemTest() {}
    ~TabletImplMemTest() {}
};

TEST_F(TabletImplMemTest, TestMem) {
#ifdef TCMALLOC_ENABLE
    MockClosure closure;
    HeapLeakChecker checker("test_mem");
    TabletImpl* tablet = new TabletImpl();
    tablet->Init();
    // create table
    {
        ::rtidb::api::CreateTableRequest request;
        request.set_name("t0");
        request.set_tid(1);
        request.set_pid(1);
        // 1 minutes
        request.set_ttl(0);
        ::rtidb::api::CreateTableResponse response;
        MockClosure closure;
        tablet->CreateTable(NULL, &request, &response,
                &closure);
        ASSERT_EQ(0, response.code());
    }
    // put once
    {
        ::rtidb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test2");
        prequest.set_tid(1);
        prequest.set_pid(1);
        ::rtidb::api::PutResponse presponse;
        tablet->Put(NULL, &prequest, &presponse,
                &closure);
        ASSERT_EQ(0, presponse.code());
    }
    // 
    {
        for (uint32_t i = 0; i < 100; i++) {
            ::rtidb::api::PutRequest prequest;
            prequest.set_pk("test3");
            prequest.set_time(i);
            prequest.set_value("test2");
            prequest.set_tid(1);
            prequest.set_pid(1);
            ::rtidb::api::PutResponse presponse;
            tablet->Put(NULL, &prequest, &presponse,
                    &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    // scan
    {
        ::rtidb::api::ScanRequest sr;
        sr.set_tid(1);
        sr.set_pid(1);
        sr.set_pk("test3");
        sr.set_st(10000);
        sr.set_et(0);
        ::rtidb::api::ScanResponse srp;
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(99, srp.count());
    }

    // drop table
    {
        ::rtidb::api::DropTableRequest dr;
        dr.set_tid(1);
        dr.set_pid(1);
        ::rtidb::api::DropTableResponse drs;
        tablet->DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
    }
    delete tablet;
    ASSERT_EQ(true, checker.NoLeaks());
#endif
}

}
}

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    FLAGS_snapshot_root_path = "/tmp/" + ::rtidb::tablet::GenRand();
    FLAGS_binlog_root_path = "/tmp/" + ::rtidb::tablet::GenRand();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



