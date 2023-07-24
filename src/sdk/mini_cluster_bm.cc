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

#include "sdk/mini_cluster_bm.h"

#include <gflags/gflags.h>
#include <stdio.h>

#include "absl/strings/ascii.h"
#include "absl/strings/str_replace.h"
//#include "boost/algorithm/string.hpp"
#include "codec/fe_row_codec.h"
#include "sdk/base.h"
#include "sdk/sql_router.h"
#include "sdk/sql_sdk_test.h"
#include "test/base_test.h"
#include "vm/catalog.h"


typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> PBSchema;
// batch request rows size == 1
void BM_RequestQuery(benchmark::State& state, hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                     ::openmldb::sdk::MiniCluster* mc) {                             // NOLINT
    const bool is_procedure = hybridse::sqlcase::SqlCase::IsProcedure();
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    if (hybridse::sqlcase::SqlCase::IsDebug()) {
        sql_opt.enable_debug = true;
    } else {
        sql_opt.enable_debug = false;
    }
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    hybridse::sdk::Status status;
    openmldb::sdk::SQLSDKTest::CreateDB(sql_case, router);
    openmldb::sdk::SQLSDKTest::CreateTables(sql_case, router, 8);
    openmldb::sdk::SQLSDKTest::InsertTables(sql_case, router, openmldb::sdk::kInsertAllInputs);
    if (is_procedure) {
        openmldb::sdk::SQLSDKTest::CreateProcedure(sql_case, router);
    }
    {
        // execute SQL
        std::string sql = sql_case.sql_str();
        for (size_t i = 0; i < sql_case.inputs().size(); i++) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            absl::StrReplaceAll(sql, {{placeholder, sql_case.inputs()[i].name_}});
        }
        absl::AsciiStrToLower(sql);
        LOG(INFO) << sql;
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
        if (status.code != 0) {
            state.SkipWithError("benchmark error: hybridse case compile fail");
            return;
        }
        // success check

        hybridse::type::TableDef request_table;
        if (!sql_case.ExtractInputTableDef(sql_case.batch_request_, request_table)) {
            state.SkipWithError("benchmark error: hybridse case input schema invalid");
            return;
        }

        std::vector<hybridse::codec::Row> request_rows;
        if (!sql_case.ExtractInputData(sql_case.batch_request_, request_rows)) {
            state.SkipWithError("benchmark error: hybridse case input data invalid");
            return;
        }

        if (hybridse::sqlcase::SqlCase::IsDebug()) {
            openmldb::sdk::SQLSDKTest::CheckSchema(request_table.columns(), *(request_row->GetSchema().get()));
        }

        hybridse::codec::RowView row_view(request_table.columns());
        if (1 != request_rows.size()) {
            state.SkipWithError("benmark error: request rows size should be 1");
            return;
        }
        row_view.Reset(request_rows[0].buf());
        openmldb::sdk::SQLSDKTest::CovertHybridSERowToRequestRow(&row_view, request_row);

        if (!hybridse::sqlcase::SqlCase::IsDebug()) {
            for (int i = 0; i < 10; i++) {
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallProcedure(sql_case.db(), sql_case.sp_name_, request_row, &status);
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                } else {
                    auto rs = router->ExecuteSQLRequest(sql_case.db(), sql, request_row, &status);
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                }
            }
            LOG(INFO) << "------------WARMUP FINISHED ------------\n\n";
        }
        if (hybridse::sqlcase::SqlCase::IsDebug() || hybridse::sqlcase::SqlCase::IS_PERF()) {
            for (auto _ : state) {
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallProcedure(sql_case.db(), sql_case.sp_name_, request_row, &status);
                    if (!rs) FAIL() << "sql case expect success == true";
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                    hybridse::type::TableDef output_table;
                    std::vector<hybridse::codec::Row> rows;
                    if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
                        openmldb::sdk::SQLSDKTest::CheckSchema(output_table.columns(), *(rs->GetSchema()));
                    }

                    if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
                        openmldb::sdk::SQLSDKTest::CheckRows(output_table.columns(), sql_case.expect().order_, rows,
                                                             rs);
                    }
                    state.SkipWithError("BENCHMARK DEBUG");
                } else {
                    auto rs = router->ExecuteSQLRequest(sql_case.db(), sql, request_row, &status);
                    if (!rs) FAIL() << "sql case expect success == true";
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                    hybridse::type::TableDef output_table;
                    std::vector<hybridse::codec::Row> rows;
                    if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
                        openmldb::sdk::SQLSDKTest::CheckSchema(output_table.columns(), *(rs->GetSchema()));
                    }

                    if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
                        openmldb::sdk::SQLSDKTest::CheckRows(output_table.columns(), sql_case.expect().order_, rows,
                                                             rs);
                    }
                    state.SkipWithError("BENCHMARK DEBUG");
                }
                break;
            }
        } else {
            if (is_procedure) {
                for (auto _ : state) {
                    benchmark::DoNotOptimize(
                        router->CallProcedure(sql_case.db(), sql_case.sp_name_, request_row, &status));
                }
            } else {
                for (auto _ : state) {
                    benchmark::DoNotOptimize(router->ExecuteSQLRequest(sql_case.db(), sql, request_row, &status));
                }
            }
        }
    }
    openmldb::sdk::SQLSDKTest::DropProcedure(sql_case, router);
    openmldb::sdk::SQLSDKTest::DropTables(sql_case, router);
}

hybridse::sqlcase::SqlCase LoadSQLCaseWithID(const std::string& yaml, const std::string& case_id) {
    return hybridse::sqlcase::SqlCase::LoadSqlCaseWithID(openmldb::test::SQLCaseTest::GetYAMLBaseDir(), yaml, case_id);
}

void MiniBenchmarkOnCase(const std::string& yaml_path, const std::string& case_id, BmRunMode engine_mode,
                         ::openmldb::sdk::MiniCluster* mc, benchmark::State* state) {
    auto target_case = LoadSQLCaseWithID(yaml_path, case_id);
    if (target_case.id() != case_id) {
        LOG(WARNING) << "Fail to find case #" << case_id << " in " << yaml_path;
        state->SkipWithError("BENCHMARK CASE LOAD FAIL: fail to find case");
        return;
    }
    MiniBenchmarkOnCase(target_case, engine_mode, mc, state);
}
void MiniBenchmarkOnCase(hybridse::sqlcase::SqlCase& sql_case, BmRunMode engine_mode,  // NOLINT
                         ::openmldb::sdk::MiniCluster* mc, benchmark::State* state) {
    switch (engine_mode) {
        case kRequestMode: {
            BM_RequestQuery(*state, sql_case, mc);
            break;
        }
        case kBatchRequestMode: {
            BM_BatchRequestQuery(*state, sql_case, mc);
            break;
        }
        default: {
            FAIL() << "Unsupport Engine Mode " << engine_mode;
        }
    }
}
// batch request rows size >= 1
void BM_BatchRequestQuery(benchmark::State& state, hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                          ::openmldb::sdk::MiniCluster* mc) {
    if (sql_case.batch_request_.columns_.empty()) {
        FAIL() << "sql case should contain batch request columns: ";
        return;
    }
    const bool enable_request_batch_optimized = state.range(0) == 1;
    const bool is_procedure = hybridse::sqlcase::SqlCase::IsProcedure();
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    if (hybridse::sqlcase::SqlCase::IsDebug()) {
        sql_opt.enable_debug = true;
    } else {
        sql_opt.enable_debug = false;
    }
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    hybridse::sdk::Status status;
    openmldb::sdk::SQLSDKTest::CreateDB(sql_case, router);
    openmldb::sdk::SQLSDKTest::CreateTables(sql_case, router, 8);
    openmldb::sdk::SQLSDKTest::InsertTables(sql_case, router, openmldb::sdk::kInsertAllInputs);
    {
        // execute SQL
        std::string sql = sql_case.sql_str();
        for (size_t i = 0; i < sql_case.inputs().size(); i++) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            absl::StrReplaceAll(sql, {{placeholder, sql_case.inputs()[i].name_}});
        }
        absl::AsciiStrToLower(sql);
        LOG(INFO) << sql;
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
        if (status.code != 0) {
            state.SkipWithError("benchmark error: hybridse case compile fail");
            return;
        }
        // success check

        hybridse::type::TableDef request_table;
        if (!sql_case.ExtractInputTableDef(sql_case.batch_request_, request_table)) {
            state.SkipWithError("benchmark error: hybridse case input schema invalid");
            return;
        }

        std::vector<hybridse::codec::Row> request_rows;
        if (!sql_case.ExtractInputData(sql_case.batch_request_, request_rows)) {
            state.SkipWithError("benchmark error: hybridse case input data invalid");
            return;
        }

        if (hybridse::sqlcase::SqlCase::IsDebug()) {
            openmldb::sdk::SQLSDKTest::CheckSchema(request_table.columns(), *(request_row->GetSchema().get()));
        }

        auto common_column_indices = std::make_shared<openmldb::sdk::ColumnIndicesSet>(request_row->GetSchema());
        if (enable_request_batch_optimized) {
            for (size_t idx : sql_case.batch_request_.common_column_indices_) {
                common_column_indices->AddCommonColumnIdx(idx);
            }
        } else {
            // clear common column indices when disable batch_request_optimized
            sql_case.batch_request_.common_column_indices_.clear();
        }
        if (is_procedure) {
            openmldb::sdk::SQLSDKTest::CreateProcedure(sql_case, router);
        }

        hybridse::codec::RowView row_view(request_table.columns());

        auto row_batch =
            std::make_shared<openmldb::sdk::SQLRequestRowBatch>(request_row->GetSchema(), common_column_indices);

        LOG(INFO) << "Batch Request execute sql start!";
        for (size_t i = 0; i < request_rows.size(); i++) {
            row_view.Reset(request_rows[i].buf());
            openmldb::sdk::SQLSDKTest::CovertHybridSERowToRequestRow(&row_view, request_row);
            ASSERT_TRUE(row_batch->AddRow(request_row));
            if (hybridse::sqlcase::SqlCase::IsDebug()) {
                continue;
            }
            // don't repeat request in debug mode
            for (int repeat_idx = 1; repeat_idx < sql_case.batch_request_.repeat_; repeat_idx++) {
                ASSERT_TRUE(row_batch->AddRow(request_row));
            }
        }

        if (!hybridse::sqlcase::SqlCase::IsDebug()) {
            for (int i = 0; i < 10; i++) {
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.inputs()[0].name_, row_batch,
                                                                   &status);
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                } else {
                    auto rs = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                }
            }
            LOG(INFO) << "------------WARMUP FINISHED ------------\n\n";
        }
        if (hybridse::sqlcase::SqlCase::IsDebug() || hybridse::sqlcase::SqlCase::IS_PERF()) {
            for (auto _ : state) {
                state.SkipWithError("benchmark case debug");
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.inputs()[0].name_, row_batch,
                                                                   &status);
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                    if (!rs) FAIL() << "sql case expect success == true";
                    hybridse::type::TableDef output_table;
                    std::vector<hybridse::codec::Row> rows;
                    if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
                        openmldb::sdk::SQLSDKTest::CheckSchema(output_table.columns(), *(rs->GetSchema()));
                    }

                    if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
                        openmldb::sdk::SQLSDKTest::CheckRows(output_table.columns(), sql_case.expect().order_, rows,
                                                             rs);
                    }
                } else {
                    auto rs = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
                    if (!rs) FAIL() << "sql case expect success == true";
                    openmldb::sdk::SQLSDKTest::PrintResultSet(rs);
                    hybridse::type::TableDef output_table;
                    std::vector<hybridse::codec::Row> rows;
                    if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
                        openmldb::sdk::SQLSDKTest::CheckSchema(output_table.columns(), *(rs->GetSchema()));
                    }

                    if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
                        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
                        openmldb::sdk::SQLSDKTest::CheckRows(output_table.columns(), sql_case.expect().order_, rows,
                                                             rs);
                    }
                }
                break;
            }
        } else {
            if (is_procedure) {
                for (auto _ : state) {
                    benchmark::DoNotOptimize(router->CallSQLBatchRequestProcedure(
                        sql_case.db(), sql_case.inputs()[0].name_, row_batch, &status));
                }
            } else {
                for (auto _ : state) {
                    benchmark::DoNotOptimize(router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status));
                }
            }
        }
    }
    openmldb::sdk::SQLSDKTest::DropProcedure(sql_case, router);
    openmldb::sdk::SQLSDKTest::DropTables(sql_case, router);
}
