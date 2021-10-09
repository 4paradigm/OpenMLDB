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

#include "sdk/cluster_sdk.h"

#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "codec/schema_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "sdk/mini_cluster.h"

namespace openmldb::sdk {

using ::openmldb::codec::SchemaCodec;

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() = default;
    ~MockClosure() override = default;
    void Run() override {}
};

class ClusterSDKTest : public ::testing::Test {
 public:
    ClusterSDKTest() : mc_(new MiniCluster(6181)) {}
    ~ClusterSDKTest() override { delete mc_; }
    void SetUp() override {
        bool ok = mc_->SetUp();
        ASSERT_TRUE(ok);
    }
    void TearDown() override { mc_->Close(); }

 public:
    MiniCluster* mc_;
};

TEST_F(ClusterSDKTest, smoke_empty_cluster) {
    ClusterOptions option;
    option.zk_cluster = mc_->GetZkCluster();
    option.zk_path = mc_->GetZkPath();
    NormalClusterSDK sdk(option);
    ASSERT_TRUE(sdk.Init());
}

TEST_F(ClusterSDKTest, smoketest) {
    ClusterOptions option;
    option.zk_cluster = mc_->GetZkCluster();
    option.zk_path = mc_->GetZkPath();
    NormalClusterSDK sdk(option);
    ASSERT_TRUE(sdk.Init());
    ::openmldb::nameserver::TableInfo table_info;
    table_info.set_format_version(1);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    auto ns_client = mc_->GetNsClient();
    std::string error;
    bool ok = ns_client->CreateDatabase(db, error);
    ASSERT_TRUE(ok);
    table_info.set_name(name);
    table_info.set_db(db);
    SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "col1", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "col2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_info.add_column_key(), "index0", "col1", "col2", ::openmldb::type::kAbsoluteTime, 0, 0);
    ok = ns_client->CreateTable(table_info, error);
    ASSERT_TRUE(ok);
    sleep(5);
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablet;
    ok = sdk.GetTablet(db, name, &tablet);
    ASSERT_TRUE(ok);
    ASSERT_EQ(8u, tablet.size());
    uint32_t tid = sdk.GetTableId(db, name);
    ASSERT_NE(tid, 0u);
    auto table_ptr = sdk.GetTableInfo(db, name);
    ASSERT_EQ(table_ptr->db(), db);
    ASSERT_EQ(table_ptr->name(), name);
    auto ns_ptr = sdk.GetNsClient();
    if (!ns_ptr) {
        ASSERT_TRUE(false);
    }
    ASSERT_EQ(ns_ptr->GetEndpoint(), ns_client->GetEndpoint());
    ASSERT_TRUE(sdk.Refresh());
}

TEST_F(ClusterSDKTest, standAloneMode) {
    // mini cluster endpoints' ports are random, so we get the ns first
    auto ns = mc_->GetNsClient()->GetRealEndpoint();
    LOG(INFO) << "nameserver address: " << ns;
    auto sep = ns.find(':');
    ASSERT_TRUE(sep != std::string::npos);
    auto host = ns.substr(0, sep);
    auto port = ns.substr(sep + 1);
    StandAloneClusterSDK sdk(host, std::stoi(port));
    ASSERT_TRUE(sdk.Init());
}

}  // namespace openmldb::sdk

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(nullptr));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
