/*
 * Copyright 2021 4Paradigm
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

#include "vm/engine_test.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "vm/core_api.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {

class EngineTest : public ::testing::TestWithParam<SQLCase> {
 public:
    EngineTest() {}
    virtual ~EngineTest() {}
};

class BatchRequestEngineTest : public ::testing::TestWithParam<SQLCase> {
 public:
    BatchRequestEngineTest() {}
    virtual ~BatchRequestEngineTest() {}
};

INSTANTIATE_TEST_CASE_P(
    EngineFailQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/fail_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestFzTest, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/fz_sql.yaml")));

// INSTANTIATE_TEST_CASE_P(
//     EngineTestFzTempTest, EngineTest,
//     testing::ValuesIn(InitCases("/cases/query/fz_temp.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineSimpleQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineConstQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/const_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineUdfQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/udf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineOperatorQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/operator_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineUdafQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineExtreamQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/extream_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineLastJoinQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineLastJoinWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineWindowWithUnionQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_with_union_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchGroupQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/group_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestWindowRowQuery, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/window/test_window_row.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestWindowRowsRangeQuery, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/window/test_window_row_range.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestWindowUnion, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/window/test_window_union.yaml")));
INSTANTIATE_TEST_CASE_P(EngineTestWindowMaxSize, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/window/test_maxsize.yaml")));

INSTANTIATE_TEST_CASE_P(EngineTestLast_Join, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/join/test_last_join.yaml")));
INSTANTIATE_TEST_CASE_P(EngineTestLastJoin, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/join/test_lastjoin.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestArithmetic, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/expression/test_arithmetic.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestCompare, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/expression/test_compare.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestCondition, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/expression/test_condition.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestLogic, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/expression/test_logic.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestType, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/expression/test_type.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestSubSelect, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/select/test_sub_select.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestUdfFunction, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/function/test_udf_function.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestUdafFunction, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/function/test_udaf_function.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestCalculateFunction, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/function/test_calculate.yaml")));
INSTANTIATE_TEST_CASE_P(EngineTestDateFunction, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/function/test_date.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestStringFunction, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/function/test_string.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestSelectSample, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/select/test_select_sample.yaml")));
INSTANTIATE_TEST_CASE_P(EngineTestWhere, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/select/test_where.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestFzFunction, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/test_feature_zero_function.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestFzSQLFunction, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/v1/test_fz_sql.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestClusterWindowAndLastJoin, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/cluster/window_and_lastjoin.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestClusterWindowRow, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestClusterWindowRowRange, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/cluster/test_window_row_range.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestWindowExcludeCurrentTime, EngineTest,
    testing::ValuesIn(InitCases(
        "/cases/integration/v1/test_window_exclude_current_time.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestIndexOptimized, EngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/test_index_optimized.yaml")));
INSTANTIATE_TEST_CASE_P(EngineTestErrorWindow, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/window/error_window.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestDebugFzBenchmark, EngineTest,
    testing::ValuesIn(InitCases("/cases/debug/fz_benchmark_debug.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestDebugIssues, EngineTest,
    testing::ValuesIn(InitCases("/cases/debug/issues_case.yaml")));

// myhug 场景正确性验证
INSTANTIATE_TEST_CASE_P(
    EngineTestFzMyhug, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/fz_ddl/test_myhug.yaml")));

// luoji 场景正确性验证
INSTANTIATE_TEST_CASE_P(
    EngineTestFzLuoji, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/fz_ddl/test_luoji.yaml")));
// bank 场景正确性验证
INSTANTIATE_TEST_CASE_P(
    EngineTestFzBank, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/fz_ddl/test_bank.yaml")));
// TODO(qiliguo) #229 sql 语句加一个大 select, 选取其中几列，
//   添加到 expect 中的做验证
// imported from spark offline test
// 单表反欺诈场景
INSTANTIATE_TEST_CASE_P(EngineTestSparkFQZ, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/spark/test_fqz_studio.yaml")));
// 单表-广告场景
INSTANTIATE_TEST_CASE_P(
    EngineTestSparkAds, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/spark/test_ads.yaml")));
// 单表-新闻场景
INSTANTIATE_TEST_CASE_P(
    EngineTestSparkNews, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/spark/test_news.yaml")));
// 多表-京东数据场景
INSTANTIATE_TEST_CASE_P(
    EngineTestSparkJD, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/spark/test_jd.yaml")));
// 多表-信用卡用户转借记卡预测场景
INSTANTIATE_TEST_CASE_P(
    EngineTestSparkCredit, EngineTest,
    testing::ValuesIn(InitCases("/cases/integration/spark/test_credit.yaml")));

TEST_P(EngineTest, test_request_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport")) {
        EngineCheck(sql_case, options, kRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_batch_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "batch-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-batch-unsupport")) {
        EngineCheck(sql_case, options, kBatchMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_batch_request_engine_for_last_row) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "batch-request-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_cluster_request_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    options.set_cluster_optimized(true);
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        EngineCheck(sql_case, options, kRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_cluster_batch_request_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    options.set_cluster_optimized(true);
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "batch-request-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
INSTANTIATE_TEST_CASE_P(BatchRequestEngineTest, BatchRequestEngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/test_batch_request.yaml")));

TEST_P(BatchRequestEngineTest, test_batch_request_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    EngineOptions options;
    options.set_cluster_optimized(false);
    if (!boost::contains(sql_case.mode(), "batch-request-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(BatchRequestEngineTest, test_cluster_batch_request_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    EngineOptions options;
    options.set_cluster_optimized(true);
    if (!boost::contains(sql_case.mode(), "batch-request-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_F(EngineTest, EngineCacheTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;
    BuildTableDef(table_def);
    BuildTableDef(table_def2);
    table_def.set_name("t1");
    table_def2.set_name("t2");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);
    EngineOptions options;
    options.set_compile_only(true);
    Engine engine(catalog, options);
    std::string sql = "select col1, col2 from t1;";
    {
        base::Status get_status;
        BatchRunSession bsession1;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession1, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        BatchRunSession bsession2;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_EQ(bsession1.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
        RequestRunSession rsession;
        ASSERT_TRUE(engine.Get(sql, table_def.catalog(), rsession, get_status));
        ASSERT_NE(rsession.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
    }
    {
        base::Status get_status;
        BatchRunSession bsession1;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession1, get_status));
        ASSERT_EQ(get_status.code, common::kOk);

        RequestRunSession rsession1;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), rsession1, get_status));
        ASSERT_EQ(get_status.code, common::kOk);

        // clear wrong db
        engine.ClearCacheLocked("wrong_db");
        BatchRunSession bsession2;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        RequestRunSession rsession2;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), rsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);

        ASSERT_EQ(bsession1.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
        ASSERT_EQ(rsession1.GetCompileInfo().get(),
                  rsession2.GetCompileInfo().get());

        // clear right db
        engine.ClearCacheLocked(table_def.catalog());

        BatchRunSession bsession3;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        RequestRunSession rsession3;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), rsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(),
                  bsession3.GetCompileInfo().get());
        ASSERT_NE(rsession1.GetCompileInfo().get(),
                  rsession3.GetCompileInfo().get());
    }
}

TEST_F(EngineTest, EngineLRUCacheTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;
    BuildTableDef(table_def);
    BuildTableDef(table_def2);
    table_def.set_name("t1");
    table_def2.set_name("t2");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);
    EngineOptions options;
    options.set_compile_only(true);
    options.set_max_sql_cache_size(1);
    Engine engine(catalog, options);

    std::string sql = "select col1, col2 from t1;";
    std::string sql2 = "select col1, col2 as cl2 from t1;";
    {
        base::Status get_status;
        BatchRunSession bsession1;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession1, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        BatchRunSession bsession2;
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_EQ(bsession1.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
        BatchRunSession bsession3;
        ASSERT_TRUE(
            engine.Get(sql2, table_def.catalog(), bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_TRUE(
            engine.Get(sql, table_def.catalog(), bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
    }
}

TEST_F(EngineTest, EngineCompileOnlyTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;
    BuildTableDef(table_def);
    BuildTableDef(table_def2);
    table_def.set_name("t1");
    table_def2.set_name("t2");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 last join t2 "
            "order by t2.col5 on t1.col1 = t2.col2;"};
        EngineOptions options;
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_TRUE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;"};
        EngineOptions options;
        options.set_performance_sensitive(false);
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_FALSE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }
}

TEST_F(EngineTest, EngineGetDependentTableTest) {
    {
        std::vector<std::pair<std::string, std::set<std::string>>> pairs;
        pairs.push_back(std::make_pair("SELECT col1, col2 from t1;",
                                       std::set<std::string>({"t1"})));
        pairs.push_back(
            std::make_pair("SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 "
                           "last join t2 "
                           "order by t2.col5 on t1.col1 = t2.col2;",
                           std::set<std::string>({"t1", "t2"})));

        pairs.push_back(
            std::make_pair("SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 "
                           "last join t2 "
                           "order by t2.col5 on t1.col1 = t2.col2;",
                           std::set<std::string>({"t1", "t2"})));
        pairs.push_back(std::make_pair(
            "SELECT t1.col1 as id, t1.col2 as t1_col2, t1.col5 as t1_col5,\n"
            "      test_sum(t1.col1) OVER w1 as w1_col1_sum, sum(t1.col3) OVER "
            "w1 as w1_col3_sum,\n"
            "      sum(t2.col4) OVER w1 as w1_t2_col4_sum, sum(t2.col2) OVER "
            "w1 as w1_t2_col2_sum,\n"
            "      sum(t1.col5) OVER w1 as w1_col5_sum,\n"
            "      str1 as t2_str1 FROM t1\n"
            "      last join t2 order by t2.col5 on t1.col1=t2.col1 and "
            "t1.col5 = t2.col5\n"
            "      WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            std::set<std::string>({"t1", "t2"})));

        for (auto pair : pairs) {
            base::Status get_status;
            EngineOptions options;
            Engine engine(std::shared_ptr<Catalog>(), options);
            std::string sqlstr = pair.first;
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            std::set<std::string> tables;
            ASSERT_TRUE(engine.GetDependentTables(sqlstr, "db", kBatchMode,
                                                  &tables, get_status));
            ASSERT_EQ(tables, pair.second);
        }

        for (auto pair : pairs) {
            base::Status get_status;
            EngineOptions options;
            Engine engine(std::shared_ptr<Catalog>(), options);
            std::string sqlstr = pair.first;
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            std::set<std::string> tables;
            ASSERT_TRUE(engine.GetDependentTables(sqlstr, "db", kRequestMode,
                                                  &tables, get_status));
            ASSERT_EQ(tables, pair.second);
        }
    }

    // const select
    {
        std::vector<std::pair<std::string, std::set<std::string>>> pairs;
        pairs.push_back(std::make_pair("SELECT substr(\"hello world\", 3, 6);",
                                       std::set<std::string>()));
        for (auto pair : pairs) {
            base::Status get_status;
            EngineOptions options;
            Engine engine(std::shared_ptr<Catalog>(), options);
            std::string sqlstr = pair.first;
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            std::set<std::string> tables;
            ASSERT_TRUE(engine.GetDependentTables(sqlstr, "db", kBatchMode,
                                                  &tables, get_status));
            ASSERT_EQ(tables, pair.second);
        }
    }
}

TEST_F(EngineTest, RouterTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col5");
    index = table_def.add_indexes();
    index->set_name("index2");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    {
        std::string sql =
            "select col2, sum(col1) over w1 from t1 \n"
            "window w1 as (partition by col2 \n"
            "order by col5 rows between 3 preceding and current row);";
        EngineOptions options;
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        Engine engine(catalog, options);
        ExplainOutput explain_output;
        base::Status status;
        ASSERT_TRUE(engine.Explain(sql, "db", kBatchRequestMode,
                                   &explain_output, &status));
        ASSERT_EQ(explain_output.router.GetMainTable(), "t1");
        ASSERT_EQ(explain_output.router.GetRouterCol(), "col2");
    }
}

TEST_F(EngineTest, ExplainBatchRequestTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col5");
    index = table_def.add_indexes();
    index->set_name("index2");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);

    std::set<size_t> common_column_indices({2, 3, 5});
    std::string sql =
        "select col0, col1, col2, sum(col1) over w1, \n"
        "sum(col2) over w1, sum(col5) over w1 from t1 \n"
        "window w1 as (partition by col2 \n"
        "order by col5 rows between 3 preceding and current row);";
    EngineOptions options;
    options.set_compile_only(true);
    options.set_performance_sensitive(false);
    Engine engine(catalog, options);
    ExplainOutput explain_output;
    base::Status status;
    ASSERT_TRUE(engine.Explain(sql, "db", kBatchRequestMode,
                               common_column_indices, &explain_output,
                               &status));
    ASSERT_TRUE(status.isOK()) << status;
    auto& output_schema = explain_output.output_schema;
    ASSERT_EQ(false, output_schema.Get(0).is_constant());
    ASSERT_EQ(false, output_schema.Get(1).is_constant());
    ASSERT_EQ(true, output_schema.Get(2).is_constant());
    ASSERT_EQ(false, output_schema.Get(3).is_constant());
    ASSERT_EQ(true, output_schema.Get(4).is_constant());
    ASSERT_EQ(true, output_schema.Get(5).is_constant());
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    // ::fesql::vm::CoreAPI::EnableSignalTraceback();
    return RUN_ALL_TESTS();
}
