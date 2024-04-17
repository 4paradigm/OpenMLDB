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

#include "passes/physical/batch_request_optimize.h"
#include "gtest/gtest.h"
#include "testing/engine_test_base.h"
#include "vm/sql_compiler.h"
#include "vm/engine.h"

namespace hybridse {
namespace vm {

class BatchRequestOptimizeTest : public ::testing::TestWithParam<SqlCase> {
 public:
    BatchRequestOptimizeTest() {}
};

INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestFzTest, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/fz_sql.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestSimpleQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/simple_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestConstQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/const_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestUdfQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/udf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestLimitQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/limit.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestOperatorQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/operator_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestUdafQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestFeatureSignatureQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/feature_signature_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestExtreamQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/extream_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestLastJoinQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestLeftJoin, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/left_join.yml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestLastJoinWindowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestRequestLastJoinWindowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestWindowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestWindowWithUnionQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/window_with_union_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestBatchGroupQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/group_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestBatchWhereGroupQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/where_group_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestBatchHavingQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/query/having_query.yaml")));
INSTANTIATE_TEST_SUITE_P(WithClause, BatchRequestOptimizeTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/query/with.yaml")));

INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestWindowRowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/window/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestWindowRowsRangeQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/window/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestWindowUnion, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/window/test_window_union.yaml")));
INSTANTIATE_TEST_SUITE_P(BatchRequestTestLastJoinSimple, BatchRequestOptimizeTest,
                        testing::ValuesIn(sqlcase::InitCases(
                            "cases/function/join/test_lastjoin_simple.yaml")));
INSTANTIATE_TEST_SUITE_P(BatchRequestTestLastJoinComplex, BatchRequestOptimizeTest,
                        testing::ValuesIn(sqlcase::InitCases(
                            "cases/function/join/test_lastjoin_complex.yaml")));

INSTANTIATE_TEST_SUITE_P(BatchRequestTestMultipleDatabases, BatchRequestOptimizeTest,
                         testing::ValuesIn(sqlcase::InitCases(
                             "cases/function/multiple_databases/test_multiple_databases.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestSelectSample, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/select/test_select_sample.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestSubSelect, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/select/test_sub_select.yaml")));
INSTANTIATE_TEST_SUITE_P(BatchRequestTestWhere, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases(
    "cases/function/select/test_where.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestUdfFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/function/test_udf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestUdafFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/function/test_udaf_function.yaml")));

INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestFzFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/test_feature_zero_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestFzSqlFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(sqlcase::InitCases("cases/function/test_fz_sql.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestClusterWindowAndLastJoin, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/cluster/window_and_lastjoin.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestClusterWindowRow, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    BatchRequestTestClusterWindowRowRange, BatchRequestOptimizeTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/function/cluster/test_window_row_range.yaml")));

void CheckOptimizePlan(const SqlCase& sql_case_org,
                       const std::set<size_t> common_column_indices,
                       bool unchanged) {
    SqlCase sql_case = sql_case_org;
    if (boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "zetasql-unsupport") ||
        boost::contains(sql_case.mode(), "performance-sensitive-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode();
        return;
    } else if (!sql_case.expect().success_) {
        LOG(INFO) << "Skip failure expecting case";
        return;
    }

    auto catalog = std::make_shared<SimpleCatalog>(true);
    InitSimpleCataLogFromSqlCase(sql_case, catalog);
    EngineOptions options;
    options.SetCompileOnly(true);
    auto engine = std::make_shared<vm::Engine>(catalog, options);
    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < sql_case.CountInputs(); ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        boost::replace_all(sql_str, placeholder, sql_case.inputs_[j].name_);
    }
    LOG(INFO) << "Compile SQL:\n" << sql_str;

    Status status;
    RequestRunSession session;
    bool ok = engine->Get(sql_str, sql_case.db(), session, status);
    ASSERT_TRUE(ok) << status;
    auto origin_plan =
        std::dynamic_pointer_cast<SqlCompileInfo>(session.GetCompileInfo())
            ->get_sql_context()
            .physical_plan;
    LOG(INFO) << "Original plan:\n" << origin_plan->GetTreeString();

    if (!common_column_indices.empty()) {
        std::stringstream ss;
        for (size_t idx : common_column_indices) {
            ss << idx << ", ";
        }
        LOG(INFO) << "Common column indices: " << ss.str();
    }

    BatchRequestRunSession batch_request_session;
    for (size_t idx : common_column_indices) {
        batch_request_session.AddCommonColumnIdx(idx);
    }
    ok = engine->Get(sql_str, sql_case.db(), batch_request_session, status);
    ASSERT_TRUE(ok) << status;
    auto optimized_plan = std::dynamic_pointer_cast<SqlCompileInfo>(
                              batch_request_session.GetCompileInfo())
                              ->get_sql_context()
                              .physical_plan;
    LOG(INFO) << "Optimized plan:\n" << optimized_plan->GetTreeString();

    if (unchanged) {
        ASSERT_EQ(origin_plan->GetTreeString(),
                  optimized_plan->GetTreeString());
    }
}

TEST_P(BatchRequestOptimizeTest, test_without_common_column) {
    const SqlCase& sql_case = GetParam();
    CheckOptimizePlan(sql_case, {}, true);
}

TEST_P(BatchRequestOptimizeTest, test_with_all_common_columns) {
    const SqlCase& sql_case = GetParam();
    type::TableDef request_table;
    if (!sql_case.inputs().empty()) {
        sql_case.ExtractInputTableDef(request_table, 0);
    }

    std::set<size_t> common_column_indices;
    size_t column_num = request_table.columns_size();
    for (size_t i = 0; i < column_num; ++i) {
        common_column_indices.insert(i);
    }
    CheckOptimizePlan(sql_case, common_column_indices, true);
}

TEST_P(BatchRequestOptimizeTest, test_with_common_columns) {
    const SqlCase& sql_case = GetParam();
    type::TableDef request_table;
    if (!sql_case.inputs().empty()) {
        sql_case.ExtractInputTableDef(request_table, 0);
    }

    std::set<size_t> common_column_indices;
    size_t column_num = request_table.columns_size();
    for (size_t i = 0; i < column_num; i += 2) {
        common_column_indices.insert(i);
    }
    CheckOptimizePlan(sql_case, common_column_indices, false);

    common_column_indices.clear();
    for (size_t i = 1; i < column_num; i += 2) {
        common_column_indices.insert(i);
    }
    CheckOptimizePlan(sql_case, common_column_indices, false);
}

}  // namespace vm
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
