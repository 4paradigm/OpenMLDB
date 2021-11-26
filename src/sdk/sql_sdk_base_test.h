#ifndef SRC_SDK_SQL_SDK_BASE_TEST_H_
#define SRC_SDK_SQL_SDK_BASE_TEST_H_

#include <sched.h>
#include <unistd.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "sdk/sql_router.h"
#include "test/base_test.h"

namespace openmldb {
namespace sdk {

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

} // namespace sdk
} // namespace openmldb

#endif /* ifndef SRC_SDK_SQL_SDK_BASE_TEST_H_ */
