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

#include <set>
#include <vector>

#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "gtest/gtest-param-test.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "plan/plan_api.h"
#include "sdk/node_adapter.h"

namespace openmldb {
namespace sdk {

struct TestInfo {
    std::string distribution;
    bool parse_flag;
    bool transfer_flag;
    std::vector<std::vector<std::string>> expect_distribution;
};

class NodeAdapterTest : public ::testing::TestWithParam<TestInfo> {
 public:
    NodeAdapterTest() {}
    ~NodeAdapterTest() {}
};

void CheckTablePartition(const ::openmldb::nameserver::TableInfo& table_info,
        const std::vector<std::vector<std::string>>& endpoints_vec) {
    ASSERT_EQ(table_info.partition_num(), endpoints_vec.size());
    ASSERT_EQ(table_info.table_partition_size(), endpoints_vec.size());
    for (const auto& endpoints : endpoints_vec) {
        ASSERT_EQ(table_info.replica_num(), endpoints.size());
    }
    for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
        const auto& table_partition = table_info.table_partition(idx);
        ASSERT_EQ(table_partition.partition_meta_size(), endpoints_vec.at(idx).size());
        std::string leader = endpoints_vec[idx][0];
        std::set<std::string> follower;
        for (auto iter = endpoints_vec[idx].begin() + 1; iter != endpoints_vec[idx].end(); iter++) {
            follower.insert(*iter);
        }
        for (int pos = 0; pos < table_partition.partition_meta_size(); pos++) {
            if (table_partition.partition_meta(pos).is_leader()) {
                ASSERT_EQ(table_partition.partition_meta(pos).endpoint(), leader);
            } else {
                ASSERT_EQ(follower.count(table_partition.partition_meta(pos).endpoint()), 1);
            }
        }
    }
}

TEST_P(NodeAdapterTest, TransformToTableInfo) {
    std::string base_sql = "CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time)) "
        "OPTIONS (";
    auto& c = GetParam();
    std::string sql = base_sql + c.distribution + ");";
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    if (plan_trees.empty() || sql_status.code != 0) {
        ASSERT_FALSE(c.parse_flag);
        return;
    }
    ASSERT_TRUE(c.parse_flag);
    hybridse::node::PlanNode* node = plan_trees[0];
    auto create_node = dynamic_cast<hybridse::node::CreatePlanNode*>(node);
    ::openmldb::nameserver::TableInfo table_info;
    bool ret = NodeAdapter::TransformToTableDef(create_node, &table_info, 3, true, &sql_status);
    ASSERT_EQ(ret, c.transfer_flag);
    // std::string table_meta_info;
    // google::protobuf::TextFormat::PrintToString(table_info, &table_meta_info);
    // printf("%s\n", table_meta_info.c_str());
    if (c.transfer_flag) {
        CheckTablePartition(table_info, c.expect_distribution);
    }
}

static std::vector<TestInfo> cases = {
    { "DISTRIBUTION=[('127.0.0.1:6527')]", true, true, {{"127.0.0.1:6527"}} },
    { "DISTRIBUTION=[('127.0.0.1:6527', [])]", true, true, {{"127.0.0.1:6527"}} },
    { "DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6528'])]", true, true, {{"127.0.0.1:6527", "127.0.0.1:6528"}}},
    { "DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6528','127.0.0.1:6529'])]", true, true,
        {{"127.0.0.1:6527", "127.0.0.1:6528", "127.0.0.1:6529"}} },
    { "DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6528','127.0.0.1:6529']), "
        "('127.0.0.1:6528', ['127.0.0.1:6527','127.0.0.1:6529'])]", true, true,
        {{"127.0.0.1:6527", "127.0.0.1:6528", "127.0.0.1:6529"},
            {"127.0.0.1:6528", "127.0.0.1:6527", "127.0.0.1:6529"}} },
    { "DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6527'])]", true, false, {} },
    { "DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6527')]", false, false, {} },
    { "DISTRIBUTION=[()]", false, false, {{}} },
    { "DISTRIBUTION=[]", false, false, {{}} },
    { "DISTRIBUTION=['127.0.0.1:6527']", true, true, {{"127.0.0.1:6527"}} },
    { "DISTRIBUTION=[('127.0.0.1:6527', '127.0.0.1:6527')]", false, false, {} },
    { "DISTRIBUTION=['127.0.0.1:6527', '127.0.0.1:6528']", true, true, {{"127.0.0.1:6527"}, {"127.0.0.1:6528"}} },
    { "DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6528','127.0.0.1:6528'])]", true, false, {} },
    { "REPLICANUM=2, DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6528','127.0.0.1:6529'])]", true, false, {} },
    { "PARTITIONNUM=2, DISTRIBUTION=[('127.0.0.1:6527', ['127.0.0.1:6528','127.0.0.1:6529'])]", true, false, {} },
    { "REPLICANUM=2, PARTITIONNUM=0", true, false, {} },
    { "REPLICANUM=0, PARTITIONNUM=8", true, false, {} },
};

INSTANTIATE_TEST_SUITE_P(NodeAdapter, NodeAdapterTest, testing::ValuesIn(cases));

}  // namespace sdk
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
