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

#include "catalog/client_manager.h"

#include "gtest/gtest.h"

namespace openmldb {
namespace catalog {

class ClientManagerTest : public ::testing::Test {};

TEST_F(ClientManagerTest, NormalTest) {
    ::openmldb::nameserver::TableInfo table_info;
    table_info.set_name("t1");
    table_info.set_db("db1");
    table_info.set_tid(1);
    for (int i = 0; i < 8; i++) {
        auto pt = table_info.add_table_partition();
        pt->set_pid(i);
        for (int j = 0; j < 3; j++) {
            auto meta = pt->add_partition_meta();
            if (j == 0) {
                meta->set_is_leader(true);
            } else {
                meta->set_is_leader(false);
            }
            meta->set_is_alive(true);
            meta->set_endpoint("name" + std::to_string(j));
        }
    }
    ::openmldb::storage::TableSt table_st(table_info);
    ASSERT_EQ(8u, table_st.GetPartitionNum());
    ASSERT_EQ("name0", table_st.GetPartition(1).GetLeader());
    ASSERT_EQ(2u, table_st.GetPartition(1).GetFollower().size());

    std::map<std::string, std::shared_ptr<::openmldb::client::TabletClient>> tablet_clients;
    auto client0 = std::make_shared<::openmldb::client::TabletClient>("name0", "endpoint0");
    auto client1 = std::make_shared<::openmldb::client::TabletClient>("name1", "endpoint1");
    auto client2 = std::make_shared<::openmldb::client::TabletClient>("name2", "endpoint2");
    tablet_clients.emplace("name0", client0);
    tablet_clients.emplace("name1", client1);
    tablet_clients.emplace("name2", client2);
    ClientManager manager;
    manager.UpdateClient(tablet_clients);
    ASSERT_EQ("name0", manager.GetTablet("name0")->GetClient()->GetEndpoint());
    ASSERT_EQ("endpoint0", manager.GetTablet("name0")->GetClient()->GetRealEndpoint());

    TableClientManager table_client_manager(table_st, manager);
    ASSERT_EQ("name0", table_client_manager.GetPartitionClientManager(0)->GetLeader()->GetClient()->GetEndpoint());
    ASSERT_EQ("endpoint0",
              table_client_manager.GetPartitionClientManager(0)->GetLeader()->GetClient()->GetRealEndpoint());

    auto client3 = std::make_shared<::openmldb::client::TabletClient>("name0", "endpoint3");
    tablet_clients["name0"] = client3;
    manager.UpdateClient(tablet_clients);
    ASSERT_EQ("name0", table_client_manager.GetPartitionClientManager(0)->GetLeader()->GetClient()->GetEndpoint());
    ASSERT_EQ("endpoint3",
              table_client_manager.GetPartitionClientManager(0)->GetLeader()->GetClient()->GetRealEndpoint());
}

}  // namespace catalog
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
