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
TEST_P(SQLSDKTest, sql_integration_query_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_.GetZkCluster();
    sql_opt.zk_path = mc_.GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) ASSERT_TRUE(false);
    fesql::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(sql_case.db(), &status));

    // Load create sql string list
    std::vector<std::string> create_sqls;
    for ( size_t i = 0 ; i < sql_case.inputs().size(); i++) {
        std::string create = "";
        fesql::type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        sql_case.BuildCreateSQLFromSchema(table_def, &create);
        create_sqls.push_back(create);
    }
    create_sqls.push_back(sql_case.create_str());

    // Load insert sql string list
    std::vector<std::string> insert_sqls;
    for ( size_t i = 0 ; i < sql_case.inputs().size(); i++) {
        std::string create = "";
        fesql::type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);

        if (!sql_case.inputs()[i].data_.empty()) {
            std::vector<std::string> row_vec;
            boost::split(row_vec, sql_case.inputs()[i].data_.empty(), boost::is_any_of("\n"),
                         boost::token_compress_on);
            sql_case.BuildInsertSQLFromRow(table_def, , &create);
        }
        create_sqls.push_back(create);
    }
    insert_sqls.push_back(sql_case.insert_str());
    ASSERT_TRUE(router->ExecuteDDL(sql_case.db(), create, &status));
    //    std::string name = "test" + GenRand();
    //    std::string db = "db" + GenRand();
    //    bool ok = router->CreateDB(db, &status);
    //    ASSERT_TRUE(ok);
    //    std::string ddl = "create table " + name +
    //                      "("
    //                      "col1 string, col2 timestamp, col3 date,"
    //                      "index(key=col1, ts=col2));";
    //    ok = router->ExecuteDDL(db, ddl, &status);
    //    ASSERT_TRUE(ok);
    //
    //    ASSERT_TRUE(router->RefreshCatalog());
    //    std::string insert = "insert into " + name +
    //                         " values('hello', 1591174600000l,
    //                         '2020-06-03');";
    //    ok = router->ExecuteInsert(db, insert, &status);
    //    ASSERT_TRUE(ok);
    //    ASSERT_TRUE(router->RefreshCatalog());
    //    std::string sql_select = "select * from " + name + " ;";
    //    auto rs = router->ExecuteSQL(db, sql_select, &status);
    //    if (!rs) ASSERT_TRUE(false);
    //    ASSERT_EQ(1, rs->Size());
    //    ASSERT_EQ(3, rs->GetSchema()->GetColumnCnt());
    //    ASSERT_TRUE(rs->Next());
    //    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    //    ASSERT_EQ(1591174600000l, rs->GetTimeUnsafe(1));
    //    int32_t year = 0;
    //    int32_t month = 0;
    //    int32_t day = 0;
    //    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    //    ASSERT_EQ(2020, year);
    //    ASSERT_EQ(6, month);
    //    ASSERT_EQ(3, day);
    //    ASSERT_FALSE(rs->Next());
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
