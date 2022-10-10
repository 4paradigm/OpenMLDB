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

#include "sdk/db_sdk.h"

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

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() = default;
    ~MockClosure() override = default;
    void Run() override {}
};

class DBSDKTest : public ::testing::Test {
 public:
    DBSDKTest() : mc_(new MiniCluster(6181)) {}
    ~DBSDKTest() override { delete mc_; }
    void SetUp() override {
        ASSERT_TRUE(mc_->SetUp());
    }
    void TearDown() override { mc_->Close(); }

    void CreateTable() {
        table_name_ = "test" + GenRand();
        db_name_ = "db" + GenRand();
        auto ns_client = mc_->GetNsClient();
        ASSERT_TRUE(ns_client);
        std::string error;
        ASSERT_TRUE(ns_client->CreateDatabase(db_name_, error));

        ::openmldb::nameserver::TableInfo table_info;
        table_info.set_format_version(1);
        table_info.set_db(db_name_);
        table_info.set_name(table_name_);
        SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "col1", ::openmldb::type::kString);
        SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "col2", ::openmldb::type::kBigInt);
        SchemaCodec::SetIndex(table_info.add_column_key(), "index0", "col1", "col2", ::openmldb::type::kAbsoluteTime, 0,
                              0);
        ASSERT_TRUE(ns_client->CreateTable(table_info, false, error));
    }

 public:
    MiniCluster* mc_;
    std::string db_name_;
    std::string table_name_;
};

TEST_F(DBSDKTest, smokeEmptyCluster) {
    ClusterOptions option;
    option.zk_cluster = mc_->GetZkCluster();
    option.zk_path = mc_->GetZkPath();
    ClusterSDK sdk(option);
    ASSERT_TRUE(sdk.Init());
}

TEST_F(DBSDKTest, smokeTest) {
    ClusterOptions option;
    option.zk_cluster = mc_->GetZkCluster();
    option.zk_path = mc_->GetZkPath();
    ClusterSDK sdk(option);
    ASSERT_TRUE(sdk.Init());

    CreateTable();
    sleep(5);  // let sdk find the new table

    ASSERT_EQ(2u, sdk.GetAllTablet().size());
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablet;
    ASSERT_TRUE(sdk.GetTablet(db_name_, table_name_, &tablet));
    ASSERT_EQ(8u, tablet.size());
    uint32_t tid = sdk.GetTableId(db_name_, table_name_);
    ASSERT_NE(tid, 0u);
    auto table_ptr = sdk.GetTableInfo(db_name_, table_name_);
    ASSERT_EQ(table_ptr->db(), db_name_);
    ASSERT_EQ(table_ptr->name(), table_name_);

    auto ns_ptr = sdk.GetNsClient();
    ASSERT_TRUE(ns_ptr);
    ASSERT_EQ(ns_ptr->GetEndpoint(), mc_->GetNsClient()->GetEndpoint());
    ASSERT_TRUE(sdk.Refresh());
}

// TODO(hw): StandAlone sdk can access cluster, but it's not a good test. Better to access StandAlone server.
TEST_F(DBSDKTest, standAloneMode) {
    // mini cluster endpoints' ports are random, so we get the ns address first
    auto ns = mc_->GetNsClient()->GetRealEndpoint();
    LOG(INFO) << "nameserver address: " << ns;
    auto sep = ns.find(':');
    ASSERT_TRUE(sep != std::string::npos);
    auto host = ns.substr(0, sep);
    auto port = ns.substr(sep + 1);
    StandAloneSDK sdk(host, std::stoi(port));
    ASSERT_TRUE(sdk.Init());

    CreateTable();
    sleep(3);  // sdk refresh time 2s, sleep for update

    // check get tablet
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablet;
    ASSERT_TRUE(sdk.GetTablet(db_name_, table_name_, &tablet));
    ASSERT_EQ(8u, tablet.size());
    uint32_t tid = sdk.GetTableId(db_name_, table_name_);
    ASSERT_NE(tid, 0u);
    auto table_ptr = sdk.GetTableInfo(db_name_, table_name_);
    ASSERT_EQ(table_ptr->db(), db_name_);
    ASSERT_EQ(table_ptr->name(), table_name_);

    // TODO(hw): procedure test, but it's hard to add sp to server here.
}

}  // namespace openmldb::sdk

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_zk_session_timeout = 100000;
    srand(time(nullptr));
    ::openmldb::base::SetupGlog(true);
    return RUN_ALL_TESTS();
}
