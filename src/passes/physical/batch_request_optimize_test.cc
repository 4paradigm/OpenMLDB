/*
 * batch_request_optimize_test.cc
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
#include "vm/engine_test.h"

namespace fesql {
namespace vm {

class BatchRequestOptimizeTest : public ::testing::TestWithParam<SQLCase> {
 public:
    BatchRequestOptimizeTest() {}
};

INSTANTIATE_TEST_CASE_P(
    BatchRequestTestFzTest, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/fz_sql.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestSimpleQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestConstQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/const_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestUdfQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/udf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestOperatorQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/operator_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestUdafQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestExtreamQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/extream_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestLastJoinQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestLastJoinWindowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestRequestLastJoinWindowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestWindowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/window_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestWindowWithUnionQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/window_with_union_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestBatchGroupQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/query/group_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestWindowRowQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/window/test_window_row.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestWindowRowsRangeQuery, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/window/test_window_row_range.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestWindowUnion, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/window/test_window_union.yaml")));
INSTANTIATE_TEST_CASE_P(BatchRequestTestLast_Join, BatchRequestOptimizeTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/join/test_last_join.yaml")));
INSTANTIATE_TEST_CASE_P(BatchRequestTestLastJoin, BatchRequestOptimizeTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/join/test_lastjoin.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestSelectSample, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/select/test_select_sample.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestSubSelect, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/select/test_sub_select.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestUdfFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/function/test_udf_function.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestUdafFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/function/test_udaf_function.yaml")));
INSTANTIATE_TEST_CASE_P(BatchRequestTestWhere, BatchRequestOptimizeTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/select/test_where.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestFzFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/test_feature_zero_function.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestFzSQLFunction, BatchRequestOptimizeTest,
    testing::ValuesIn(InitCases("/cases/integration/v1/test_fz_sql.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestClusterWindowAndLastJoin, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/cluster/window_and_lastjoin.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestClusterWindowRow, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_CASE_P(
    BatchRequestTestClusterWindowRowRange, BatchRequestOptimizeTest,
    testing::ValuesIn(
        InitCases("/cases/integration/cluster/test_window_row_range.yaml")));

void CheckOptimizePlan(const SQLCase& sql_case,
                       const std::set<size_t> common_column_indices,
                       bool unchanged) {
    if (boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-unsupport")) {
        LOG(INFO) << "Skip mode " << sql_case.mode();
        return;
    } else if (!sql_case.expect().success_) {
        LOG(INFO) << "Skip failure expecting case";
        return;
    }

    auto catalog = std::make_shared<tablet::TabletCatalog>();
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>> table_dict;
    std::map<size_t, std::string> idx_to_table_dict;
    EngineOptions options;
    options.set_compile_only(true);
    auto engine = std::make_shared<vm::Engine>(catalog, options);
    InitEngineCatalog(sql_case, options, table_dict, idx_to_table_dict, engine,
                      catalog);

    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < sql_case.CountInputs(); ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        boost::replace_all(sql_str, placeholder, idx_to_table_dict[j]);
    }
    LOG(INFO) << "Compile SQL:\n" << sql_str;

    Status status;
    RequestRunSession session;
    bool ok = engine->Get(sql_str, sql_case.db(), session, status);
    ASSERT_TRUE(ok) << status;
    auto origin_plan =
        session.GetCompileInfo()->get_sql_context().physical_plan;
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
    auto optimized_plan =
        batch_request_session.GetCompileInfo()->get_sql_context().physical_plan;
    LOG(INFO) << "Optimized plan:\n" << optimized_plan->GetTreeString();

    if (unchanged) {
        ASSERT_EQ(origin_plan->GetTreeString(),
                  optimized_plan->GetTreeString());
    }
}

TEST_P(BatchRequestOptimizeTest, test_without_common_column) {
    const SQLCase& sql_case = GetParam();
    CheckOptimizePlan(sql_case, {}, true);
}

TEST_P(BatchRequestOptimizeTest, test_with_all_common_columns) {
    const SQLCase& sql_case = GetParam();
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
    const SQLCase& sql_case = GetParam();
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
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
