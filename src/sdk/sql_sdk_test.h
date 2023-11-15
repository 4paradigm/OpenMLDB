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

#ifndef SRC_SDK_SQL_SDK_TEST_H_
#define SRC_SDK_SQL_SDK_TEST_H_

#include <string>
#include "base/glog_wrapper.h"
#include "sdk/sql_sdk_base_test.h"

namespace openmldb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> PBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(SQLSDKTest);

INSTANTIATE_TEST_SUITE_P(SQLSDKTestCreate, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("cases/function/ddl/test_create.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("cases/function/dml/test_insert.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestMultiRowsInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("cases/function/dml/multi_insert.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestBugTest, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/debug/bug.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestConstsSelect, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/const_query.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKHavingQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/having_query.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKLastJoinQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/last_join_query.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKLeftJoin, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/left_join.yml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKLastJoinWindowQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKLastJoinSubqueryWindow, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/last_join_subquery_window.yml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKLastJoinWhere, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/last_join_where.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKParameterizedQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/parameterized_query.yaml")));

// Test Cluster
INSTANTIATE_TEST_SUITE_P(
    SQLSDKClusterCaseWindowRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKClusterCaseWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/cluster/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKClusterCaseWindowAndLastJoin, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/cluster/window_and_lastjoin.yaml")));

// Test Expression
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestArithmetic, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/expression/test_arithmetic.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCompare, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/expression/test_predicate.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCondition, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/expression/test_condition.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLogic, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/expression/test_logic.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestType, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/expression/test_type.yaml")));

// Test Function
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCalulateFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/function/test_calculate.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestDateFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/function/test_date.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestStringFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/function/test_string.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDAFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/function/test_udaf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/function/test_udf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    UdfQuery, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/udf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    UdafQuery, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    LimitClauseQuery, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/limit.yaml")));

// Test Fz DDL
INSTANTIATE_TEST_SUITE_P(SQLSDKTestFzBank, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/fz_ddl/test_bank.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestFzMyhug, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/fz_ddl/test_myhug.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestFzLuoji, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/fz_ddl/test_luoji.yaml")));

// Test Join
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLastJoinComplex, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/join/test_lastjoin_complex.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLastJoinSimple, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/join/test_lastjoin_simple.yaml")));

// Test Select
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSelectSample, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/select/test_select_sample.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSubSelect, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/select/test_sub_select.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestWhere, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/select/test_where.yaml")));
INSTANTIATE_TEST_SUITE_P(WithClause, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/with.yaml")));

// Test Multiple Databases
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestMultipleDatabases, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/multiple_databases/test_multiple_databases.yaml")));
// Test Window
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestErrorWindow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/error_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowMaxSize, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_maxsize.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowAttributes, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/window_attributes.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowExcludeCurrentTime, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_exclude_current_time.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowUnion, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_union.yaml")));
INSTANTIATE_TEST_SUITE_P(
    WindowUnion, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/window_with_union_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    WindowTestCurrentRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_current_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestStandaloneBatchGroupQuery, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/group_query.yaml")));


INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestFZFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/test_feature_zero_function.yaml")));

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(SQLSDKBatchRequestQueryTest);
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestBatchRequest, SQLSDKBatchRequestQueryTest,
    testing::ValuesIn(SQLSDKBatchRequestQueryTest::InitCases("cases/function/test_batch_request.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestIndexOptimized, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/test_index_optimized.yaml")));


GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(SQLSDKClusterOnlineBatchQueryTest);
INSTANTIATE_TEST_SUITE_P(SQLSDKTestConstsSelect, SQLSDKClusterOnlineBatchQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/const_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSelectSample, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/select/test_select_sample.yaml")));
INSTANTIATE_TEST_SUITE_P(
SQLSDKTestClusterBatch, SQLSDKClusterOnlineBatchQueryTest,
testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/cluster/test_cluster_batch.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowRow, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchRowRange, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/cluster/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowRow2, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchRowRange2, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowUnion, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_union.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowQuery, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowUnionQuery, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/window_with_union_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchErrorWindow, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/error_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchCurrentRow, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_current_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchMaxSize, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_maxsize.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindow, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowExcludeCurrentTime, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/test_window_exclude_current_time.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWindowAttributes, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/function/window/window_attributes.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchGroupQuery, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/group_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchHavingQuery, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/having_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchWhereGroupQuery, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/where_group_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestClusterBatchUdafQuery, SQLSDKClusterOnlineBatchQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("cases/query/udaf_query.yaml")));
}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_SQL_SDK_TEST_H_
