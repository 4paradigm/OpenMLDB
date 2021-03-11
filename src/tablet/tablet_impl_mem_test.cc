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


#include <gflags/gflags.h>
#include "base/kv_iterator.h"
#include "config.h" // NOLINT
#include "gtest/gtest.h"
#include "base/glog_wapper.h" // NOLINT
#include "proto/tablet.pb.h"
#include "tablet/tablet_impl.h"
#include "timer.h" // NOLINT
#ifdef TCMALLOC_ENABLE
#include "gperftools/heap-checker.h"
#endif

DECLARE_string(db_root_path);

namespace fedb {
namespace tablet {

inline std::string GenRand() { return std::to_string(rand() % 10000000 + 1); } // NOLINT

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
#ifndef __APPLE__
#ifdef TCMALLOC_ENABLE
    MockClosure closure;
    HeapLeakChecker checker("test_mem");
    TabletImpl* tablet = new TabletImpl();
    tablet->Init("");
    // create table
    {
        ::fedb::api::CreateTableRequest request;
        ::fedb::api::TableMeta* table_meta = request.mutable_table_meta();
        table_meta->set_name("t0");
        table_meta->set_tid(1);
        table_meta->set_pid(1);
        // 1 minutes
        table_meta->set_ttl(0);
        ::fedb::api::CreateTableResponse response;
        MockClosure closure;
        tablet->CreateTable(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    // put once
    {
        ::fedb::api::PutRequest prequest;
        prequest.set_pk("test1");
        prequest.set_time(9527);
        prequest.set_value("test2");
        prequest.set_tid(1);
        prequest.set_pid(1);
        ::fedb::api::PutResponse presponse;
        tablet->Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(0, presponse.code());
        prequest.set_time(0);
        tablet->Put(NULL, &prequest, &presponse, &closure);
        ASSERT_EQ(114, presponse.code());
    }
    //
    {
        for (uint32_t i = 0; i < 100; i++) {
            ::fedb::api::PutRequest prequest;
            prequest.set_pk("test3");
            prequest.set_time(i + 1);
            prequest.set_value("test2");
            prequest.set_tid(1);
            prequest.set_pid(1);
            ::fedb::api::PutResponse presponse;
            tablet->Put(NULL, &prequest, &presponse, &closure);
            ASSERT_EQ(0, presponse.code());
        }
    }
    // scan
    {
        ::fedb::api::ScanRequest sr;
        sr.set_tid(1);
        sr.set_pid(1);
        sr.set_pk("test3");
        sr.set_st(10000);
        sr.set_et(0);
        ::fedb::api::ScanResponse srp;
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(100, (int32_t)srp.count());
    }

    // drop table
    {
        ::fedb::api::DropTableRequest dr;
        dr.set_tid(1);
        dr.set_pid(1);
        ::fedb::api::DropTableResponse drs;
        tablet->DropTable(NULL, &dr, &drs, &closure);
        ASSERT_EQ(0, drs.code());
    }
    delete tablet;
    ASSERT_EQ(true, checker.NoLeaks());
#endif
#endif
}

}  // namespace tablet
}  // namespace fedb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::fedb::base::SetLogLevel(INFO);
    FLAGS_db_root_path = "/tmp/" + ::fedb::tablet::GenRand();
    return RUN_ALL_TESTS();
}
