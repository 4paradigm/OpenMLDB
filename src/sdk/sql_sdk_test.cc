/*
 * sql_sdk_test.cc
 * Copyright (C) 4paradigm.com 2020 chenjing <chenjing@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sched.h>
#include <timer.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"  // NOLINT
#include "boost/algorithm/string.hpp"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/base_test.h"
#include "vm/catalog.h"

namespace rtidb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class SQLSDKTest : public rtidb::test::SQLCaseTest {
 public:
    SQLSDKTest()
        : rtidb::test::SQLCaseTest(), cluster_init_(false), mc_(6181) {}
    ~SQLSDKTest() {}
    void SetUp() {
        if (cluster_init_) {
            return;
        }
        LOG(INFO) << "SetUp";
        bool ok = mc_.SetUp();
        cluster_init_ = true;
        ASSERT_TRUE(ok);
    }
    void TearDown() {
        mc_.Close();
        LOG(INFO) << "TearDown";
    }

 public:
    bool cluster_init_;
    MiniCluster mc_;
};
INSTANTIATE_TEST_CASE_P(SQLSDKTestSelectSample, SQLSDKTest,
                        testing::ValuesIn(rtidb::test::InitCases(
                            "/cases/integration/v1/test_select_sample.yaml")));
INSTANTIATE_TEST_CASE_P(SQLSDKTestCreate, SQLSDKTest,
                        testing::ValuesIn(rtidb::test::InitCases(
                            "/cases/integration/v1/test_create.yaml")));
TEST_P(SQLSDKTest, sql_integration_query_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_.GetZkCluster();
    sql_opt.zk_path = mc_.GetZkPath();
    sql_opt.enbale_debug = true;
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) ASSERT_TRUE(false);
    fesql::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(sql_case.db(), &status));
    // create and insert inputs
    for (auto i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(fesql::sqlcase::SQLCase::GenRand("auto_t"),
                                    i);
        }
        std::string create;
        ASSERT_TRUE(sql_case.BuildCreateSQLFromInput(i, &create));
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(create, placeholder, sql_case.inputs()[i].name_);
        LOG(INFO) << create;
        if (!create.empty()) {
            ASSERT_TRUE(router->ExecuteDDL(sql_case.db(), create, &status));
        }

        ASSERT_TRUE(router->RefreshCatalog());
        std::string insert;
        ASSERT_TRUE(sql_case.BuildInsertSQLFromInput(i, &insert));
        boost::replace_all(insert, placeholder, sql_case.inputs()[i].name_);
        LOG(INFO) << insert;
        if (!insert.empty()) {
            ASSERT_TRUE(router->ExecuteInsert(sql_case.db(), insert, &status));
            ASSERT_TRUE(router->RefreshCatalog());
        }
        ASSERT_TRUE(router->RefreshCatalog());
    }

    std::string sql = sql_case.sql_str();
    for (auto i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}",
                       fesql::sqlcase::SQLCase::GenRand("auto_t"));
    LOG(INFO) << sql;

    if (boost::algorithm::starts_with(sql, "select")) {
        auto rs = router->ExecuteSQL(sql_case.db(), sql, &status);
        if (!sql_case.expect().success_) {
            if ((rs)) {
                FAIL() << "sql case expect success == false";
            }
            return;
        }

        if (!rs) FAIL() << "sql case expect success == true";
        std::vector<fesql::codec::Row> rows;
        fesql::type::TableDef output_table;
        if (!sql_case.expect().schema_.empty() ||
            !sql_case.expect().columns_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
            rtidb::test::CheckSchema(output_table.columns(),
                                     *(rs->GetSchema()));
        }

        if (!sql_case.expect().data_.empty() ||
            !sql_case.expect().rows_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputData(rows));
            rtidb::test::CheckRows(output_table.columns(),
                                   sql_case.expect().order_, rows, rs);
        }

        if (!sql_case.expect().count_ > 0) {
            ASSERT_EQ(sql_case.expect().count_,
                      static_cast<int64_t>(rs->Size()));
        }
    } else if (boost::algorithm::starts_with(sql, "create")) {
        ASSERT_EQ(sql_case.expect().success_,
                  router->ExecuteDDL(sql_case.db(), sql, &status));
        router->RefreshCatalog();
    } else if (boost::algorithm::starts_with(sql, "insert")) {
        ASSERT_EQ(sql_case.expect().success_,
                  router->ExecuteInsert(sql_case.db(), sql, &status));
        router->RefreshCatalog();
    }
}

}  // namespace sdk
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
