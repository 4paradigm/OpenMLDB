//
// Created by Chenjing on 2020-12-28.
//
/*
 * mini_cluster_microbenchmark.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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
#include <gflags/gflags.h>
#include <stdio.h>

#include "benchmark/benchmark.h"
#include "boost/algorithm/string.hpp"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "sdk/base.h"
#include "sdk/mini_cluster.h"
#include "sdk/mini_cluster_bm.h"
#include "sdk/sql_router.h"
#include "sdk/sql_sdk_test.h"
#include "test/base_test.h"
#include "vm/catalog.h"
DECLARE_bool(enable_distsql);
DECLARE_bool(enable_localtablet);

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey> RtiDBIndex;
// batch request rows size == 1
void BM_RequestQuery(benchmark::State& state, fesql::sqlcase::SQLCase& sql_case, ::rtidb::sdk::MiniCluster* mc) {  // NOLINT
    const bool is_procedure = fesql::sqlcase::SQLCase::IS_PROCEDURE();
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
        sql_opt.enable_debug = true;
    } else {
        sql_opt.enable_debug = false;
    }
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    fesql::sdk::Status status;
    rtidb::sdk::SQLSDKTest::CreateDB(sql_case, router);
    rtidb::sdk::SQLSDKTest::CreateTables(sql_case, router, 8);
    rtidb::sdk::SQLSDKTest::InsertTables(sql_case, router, rtidb::sdk::kInsertAllInputs);
    if (is_procedure) {
        rtidb::sdk::SQLSDKTest::CreateProcedure(sql_case, router);
    }
    {
        // execute SQL
        std::string sql = sql_case.sql_str();
        for (size_t i = 0; i < sql_case.inputs().size(); i++) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
        }
        boost::to_lower(sql);
        LOG(INFO) << sql;
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
        if (status.code != 0) {
            state.SkipWithError("benchmark error: fesql case compile fail");
            return;
        }
        // success check

        fesql::type::TableDef request_table;
        if (!sql_case.ExtractInputTableDef(sql_case.batch_request_, request_table)) {
            state.SkipWithError("benchmark error: fesql case input schema invalid");
            return;
        }

        std::vector<fesql::codec::Row> request_rows;
        if (!sql_case.ExtractInputData(sql_case.batch_request_, request_rows)) {
            state.SkipWithError("benchmark error: fesql case input data invalid");
            return;
        }

        if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
            rtidb::sdk::SQLSDKTest::CheckSchema(request_table.columns(), *(request_row->GetSchema().get()));
        }

        fesql::codec::RowView row_view(request_table.columns());
        ASSERT_EQ(1, request_rows.size());
        row_view.Reset(request_rows[0].buf());
        rtidb::sdk::SQLSDKTest::CovertFesqlRowToRequestRow(&row_view, request_row);

        if (!fesql::sqlcase::SQLCase::IS_DEBUG()) {
            for (int i = 0; i < 10; i++) {
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallProcedure(sql_case.db(), sql_case.inputs()[0].name_, request_row, &status);
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                } else {
                    auto rs = router->ExecuteSQL(sql_case.db(), sql, request_row, &status);
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                }
            }
            LOG(INFO) << "------------WARMUP FINISHED ------------\n\n";
        }
        if (fesql::sqlcase::SQLCase::IS_DEBUG() || fesql::sqlcase::SQLCase::IS_PERF()) {
            for (auto _ : state) {
                state.SkipWithError("benchmark case debug");
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallProcedure(sql_case.db(), sql_case.inputs()[0].name_, request_row, &status);
                    if (!rs) FAIL() << "sql case expect success == true";
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                } else {
                    auto rs = router->ExecuteSQL(sql_case.db(), sql, request_row, &status);
                    if (!rs) FAIL() << "sql case expect success == true";
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                }
                break;
            }
        } else {
            if (is_procedure) {
                for (auto _ : state) {
                    benchmark::DoNotOptimize(
                        router->CallProcedure(sql_case.db(), sql_case.inputs()[0].name_, request_row, &status));
                }
            } else {
                for (auto _ : state) {
                    benchmark::DoNotOptimize(router->ExecuteSQL(sql_case.db(), sql, request_row, &status));
                }
            }
        }
    }
    rtidb::sdk::SQLSDKTest::DropProcedure(sql_case, router);
    rtidb::sdk::SQLSDKTest::DropTables(sql_case, router);
}
void MiniBenchmarkOnCase(const std::string& yaml_path,
                         const std::string& case_id,
                         BmRunMode engine_mode,
                         ::rtidb::sdk::MiniCluster* mc,
                         benchmark::State* state) {
    std::vector<fesql::sqlcase::SQLCase> cases;
    fesql::sqlcase::SQLCase::CreateSQLCasesFromYaml(fesql::sqlcase::FindFesqlDirPath(),
                                                    yaml_path, cases);
    fesql::sqlcase::SQLCase* target_case = nullptr;
    for (auto& sql_case : cases) {
        if (sql_case.id() == case_id) {
            target_case = &sql_case;
            break;
        }
    }
    if (target_case == nullptr) {
        LOG(WARNING) << "Fail to find case #" << case_id << " in " << yaml_path;
        return;
    }

    switch (engine_mode) {
        case kRequestMode: {
            BM_RequestQuery(*state, *target_case, mc);
            break;
        }
        case kBatchRequestMode: {
            BM_BatchRequestQuery(*state, *target_case, mc);
            break;
        }
        default: {
            FAIL() << "Unsupport Engine Mode " << engine_mode;
        }
    }

}
// batch request rows size >= 1
void BM_BatchRequestQuery(benchmark::State& state, fesql::sqlcase::SQLCase& sql_case,
                          ::rtidb::sdk::MiniCluster* mc) {  // NOLINT
    if (sql_case.batch_request_.columns_.empty()) {
        FAIL() << "sql case should contain batch request columns: ";
        return;
    }
    const bool is_procedure = fesql::sqlcase::SQLCase::IS_PROCEDURE();
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
        sql_opt.enable_debug = true;
    } else {
        sql_opt.enable_debug = false;
    }
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    fesql::sdk::Status status;
    rtidb::sdk::SQLSDKTest::CreateDB(sql_case, router);
    rtidb::sdk::SQLSDKTest::CreateTables(sql_case, router, 8);
    rtidb::sdk::SQLSDKTest::InsertTables(sql_case, router, rtidb::sdk::kInsertAllInputs);
    if (is_procedure) {
        rtidb::sdk::SQLSDKTest::CreateProcedure(sql_case, router);
    }
    {
        // execute SQL
        std::string sql = sql_case.sql_str();
        for (size_t i = 0; i < sql_case.inputs().size(); i++) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
        }
        boost::to_lower(sql);
        LOG(INFO) << sql;
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
        if (status.code != 0) {
            state.SkipWithError("benchmark error: fesql case compile fail");
            return;
        }
        // success check

        fesql::type::TableDef request_table;
        if (!sql_case.ExtractInputTableDef(sql_case.batch_request_, request_table)) {
            state.SkipWithError("benchmark error: fesql case input schema invalid");
            return;
        }

        std::vector<fesql::codec::Row> request_rows;
        if (!sql_case.ExtractInputData(sql_case.batch_request_, request_rows)) {
            state.SkipWithError("benchmark error: fesql case input data invalid");
            return;
        }

        if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
            rtidb::sdk::SQLSDKTest::CheckSchema(request_table.columns(), *(request_row->GetSchema().get()));
        }

        auto common_column_indices = std::make_shared<rtidb::sdk::ColumnIndicesSet>(request_row->GetSchema());
        for (size_t idx : sql_case.batch_request_.common_column_indices_) {
            common_column_indices->AddCommonColumnIdx(idx);
        }
        fesql::codec::RowView row_view(request_table.columns());

        auto row_batch =
            std::make_shared<rtidb::sdk::SQLRequestRowBatch>(request_row->GetSchema(), common_column_indices);

        LOG(INFO) << "Batch Request execute sql start!";
        for (size_t i = 0; i < request_rows.size(); i++) {
            row_view.Reset(request_rows[i].buf());
            rtidb::sdk::SQLSDKTest::CovertFesqlRowToRequestRow(&row_view, request_row);
            ASSERT_TRUE(row_batch->AddRow(request_row));
        }

        if (!fesql::sqlcase::SQLCase::IS_DEBUG()) {
            for (int i = 0; i < 10; i++) {
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.inputs()[0].name_, row_batch,
                                                                   &status);
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                } else {
                    auto rs = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                }
            }
            LOG(INFO) << "------------WARMUP FINISHED ------------\n\n";
        }
        if (fesql::sqlcase::SQLCase::IS_DEBUG() || fesql::sqlcase::SQLCase::IS_PERF()) {
            for (auto _ : state) {
                state.SkipWithError("benchmark case debug");
                if (is_procedure) {
                    LOG(INFO) << "--------syn procedure----------";
                    auto rs = router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.inputs()[0].name_, row_batch,
                                                                   &status);
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                    if (!rs) FAIL() << "sql case expect success == true";
                } else {
                    auto rs = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
                    rtidb::sdk::SQLSDKTest::PrintResultSet(rs);
                    if (!rs) FAIL() << "sql case expect success == true";
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
    rtidb::sdk::SQLSDKTest::DropProcedure(sql_case, router);
    rtidb::sdk::SQLSDKTest::DropTables(sql_case, router);
}
