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

#include <sched.h>
#include <unistd.h>

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <unordered_set>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "boost/algorithm/string.hpp"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/base_test.h"
#include "vm/catalog.h"
namespace openmldb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

enum InsertRule {
    kNotInsertFirstInput,
    kNotInsertLastRowOfFirstInput,
    kInsertAllInputs,
};
class SQLSDKTest : public openmldb::test::SQLCaseTest {
 public:
    SQLSDKTest() : openmldb::test::SQLCaseTest() {}
    ~SQLSDKTest() {}
    void SetUp() { LOG(INFO) << "SQLSDKTest TearDown"; }
    void TearDown() { LOG(INFO) << "SQLSDKTest TearDown"; }

    static void CreateDB(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                         std::shared_ptr<SQLRouter> router);
    static void CreateTables(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router, int partition_num = 1);

    static void DropTables(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                           std::shared_ptr<SQLRouter> router);
    static void InsertTables(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router, InsertRule insert_rule);

    static void CovertHybridSERowToRequestRow(hybridse::codec::RowView* row_view,
                                              std::shared_ptr<openmldb::sdk::SQLRequestRow> request_row);
    static void BatchExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints);
    static void RunBatchModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints);
    static void CreateProcedure(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router, bool is_batch = false);
    static void DropProcedure(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router);
};

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(SQLSDKTest);

INSTANTIATE_TEST_SUITE_P(SQLSDKTestCreate, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/function/ddl/test_create.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/function/dml/test_insert.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestMultiRowsInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/function/dml/multi_insert.yaml")));

class SQLSDKQueryTest : public SQLSDKTest {
 public:
    SQLSDKQueryTest() : SQLSDKTest() {}
    ~SQLSDKQueryTest() {}
    static void RequestExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router, bool has_batch_request, bool is_procedure = false,
                                  bool is_asyn = false);
    static void RunRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router);
    static void DistributeRunRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                            std::shared_ptr<SQLRouter> router, int32_t partition_num = 8);
    void RunRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                    std::shared_ptr<SQLRouter> router, bool is_asyn);
    void DistributeRunRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                              std::shared_ptr<SQLRouter> router, int32_t partition_num, bool is_asyn);

    static void BatchRequestExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                       std::shared_ptr<SQLRouter> router, bool has_batch_request, bool is_procedure,
                                       bool is_asy);
    static void BatchRequestExecuteSQLWithCommonColumnIndices(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                              std::shared_ptr<SQLRouter> router,
                                                              const std::set<size_t>& common_column_indices,
                                                              bool is_procedure = false, bool is_asyn = false);
    static void RunBatchRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                       std::shared_ptr<SQLRouter> router);
    static void DistributeRunBatchRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                 std::shared_ptr<SQLRouter> router, int32_t partition_num = 8);
};

class SQLSDKBatchRequestQueryTest : public SQLSDKQueryTest {
 public:
    SQLSDKBatchRequestQueryTest() : SQLSDKQueryTest() {}
    ~SQLSDKBatchRequestQueryTest() {}

    static void DistributeRunBatchRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                          std::shared_ptr<SQLRouter> router, int32_t partition_num,
                                                          bool is_asyn);
    static void RunBatchRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                std::shared_ptr<SQLRouter> router, bool is_asyn);
};

INSTANTIATE_TEST_SUITE_P(SQLSDKTestConstsSelect, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/query/const_query.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKLastJoinWindowQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKParameterizedQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/query/parameterized_query.yaml")));

// Test Cluster
INSTANTIATE_TEST_SUITE_P(
    SQLSDKClusterCaseWindowRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKClusterCaseWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/cluster/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKClusterCaseWindowAndLastJoin, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/cluster/window_and_lastjoin.yaml")));

// Test Expression
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestArithmetic, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/expression/test_arithmetic.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCompare, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/expression/test_predicate.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCondition, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/expression/test_condition.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLogic, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/expression/test_logic.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestType, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/expression/test_type.yaml")));

// Test Function
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCalulateFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/function/test_calculate.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestDateFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/function/test_date.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestStringFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/function/test_string.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDAFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/function/test_udaf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/function/test_udf_function.yaml")));

// Test Fz DDL
INSTANTIATE_TEST_SUITE_P(SQLSDKTestFzBank, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/fz_ddl/test_bank.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestFzMyhug, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/fz_ddl/test_myhug.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestFzLuoji, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/fz_ddl/test_luoji.yaml")));

// Test Join
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLastJoinComplex, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/join/test_lastjoin_complex.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLastJoinSimple, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/join/test_lastjoin_simple.yaml")));

// Test Select
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSelectSample, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/select/test_select_sample.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSubSelect, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/select/test_sub_select.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestWhere, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/select/test_where.yaml")));
// Test Multiple Databases
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestMultipleDatabases, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/multiple_databases/test_multiple_databases.yaml")));
// Test Window
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestErrorWindow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/error_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowMaxSize, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/test_maxsize.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/test_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowExcludeCurrentTime, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/test_window_exclude_current_time.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowUnion, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/window/test_window_union.yaml")));




INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestFZFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/test_feature_zero_function.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestBatchRequest, SQLSDKBatchRequestQueryTest,
    testing::ValuesIn(SQLSDKBatchRequestQueryTest::InitCases("/cases/function/test_batch_request.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestIndexOptimized, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/function/test_index_optimized.yaml")));

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_SQL_SDK_TEST_H_
