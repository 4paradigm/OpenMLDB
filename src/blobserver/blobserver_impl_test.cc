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


#include "blobserver/blobserver_impl.h"
#include <gflags/gflags.h>
#include <google/protobuf/stubs/common.h>
#include "client/bs_client.h"
#include "gtest/gtest.h"
#include "base/glog_wapper.h"

DECLARE_string(hdd_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_string(endpoint);

namespace rtidb {
namespace blobserver {

uint32_t counter = 10;

inline std::string GenRand() { return std::to_string(rand() % 10000000 + 1); } // NOLINT

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class BlobServerImplTest : public ::testing::Test {
 public:
    BlobServerImplTest() {}
    ~BlobServerImplTest() {}
};

TEST_F(BlobServerImplTest, Basic_Test) {
    BlobServerImpl* server = new BlobServerImpl();
    server->Init("");
    FLAGS_endpoint = "127.0.0.1:19572";
    brpc::Server brpc_server;
    {
        int code = brpc_server.AddService(server, brpc::SERVER_OWNS_SERVICE);
        ASSERT_EQ(0, code);
        brpc::ServerOptions options;
        code = brpc_server.Start(FLAGS_endpoint.c_str(), &options);
        ASSERT_EQ(0, code);
    }

    uint32_t tid = counter++, pid = 0;
    ::rtidb::client::BsClient client(FLAGS_endpoint, "");
    ASSERT_EQ(client.Init(), 0);
    std::string err_msg;
    {
        ::rtidb::blobserver::TableMeta meta;
        meta.set_tid(tid);
        meta.set_pid(pid);
        meta.set_storage_mode(::rtidb::common::StorageMode::kHDD);
        meta.set_table_type(::rtidb::type::kObjectStore);
        bool ok = client.CreateTable(meta, &err_msg);
        ASSERT_TRUE(ok);
    }
    int64_t key = 10010;
    std::string value = "testvalue1";
    bool ok = client.Put(tid, pid, key, value, &err_msg);
    ASSERT_TRUE(ok);
    std::string get_value;
    ok = client.Get(tid, pid, key, &get_value, &err_msg);
    ASSERT_TRUE(ok);
    {
        int code = memcmp(value.data(), get_value.data(), value.length());
        ASSERT_EQ(0, code);
    }
    int64_t auto_gen_key;
    std::string value2 = "testvalue2";
    ok = client.Put(tid, pid, value2, &auto_gen_key, &err_msg);
    ASSERT_TRUE(ok);
    get_value.clear();
    ok = client.Get(tid, pid, auto_gen_key, &get_value, &err_msg);
    ASSERT_TRUE(ok);
    {
        int code = memcmp(value2.data(), get_value.data(), value2.length());
        ASSERT_EQ(0, code);
    }
    ok = client.Delete(tid, pid, auto_gen_key, &err_msg);
    ASSERT_TRUE(ok);
    ok = client.Get(tid, pid, auto_gen_key, &get_value, &err_msg);
    ASSERT_FALSE(ok);
}

}  // namespace blobserver
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::rtidb::base::SetLogLevel(INFO);
    FLAGS_hdd_root_path =
        "/tmp/test_blobserver" + ::rtidb::blobserver::GenRand();
    return RUN_ALL_TESTS();
}
