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
#ifndef HYBRIDSE_SRC_TESTING_ENGINE_TEST_BASE_H_
#define HYBRIDSE_SRC_TESTING_ENGINE_TEST_BASE_H_

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/fe_row_selector.h"
#include "codec/list_iterator_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "plan/plan_api.h"
#include "sys/time.h"
#include "vm/engine.h"
#include "testing/test_base.h"
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

static const int ENGINE_TEST_RET_SUCCESS = 0;
static const int ENGINE_TEST_RET_INVALID_CASE = 1;
static const int ENGINE_TEST_RET_COMPILE_ERROR = 2;
static const int ENGINE_TEST_RET_EXECUTION_ERROR = 3;
static const int ENGINE_TEST_INIT_CATALOG_ERROR = 4;

namespace hybridse {
namespace vm {
using hybridse::base::Status;
using hybridse::codec::Row;
using hybridse::common::kTestEngineError;
using hybridse::sqlcase::SqlCase;
enum EngineRunMode { RUNBATCH, RUNONE };


bool IsNaN(float x);
bool IsNaN(double x);

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema);
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows);
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows);

std::string YamlTypeName(type::Type type);
void PrintYamlResult(const vm::Schema& schema, const std::vector<Row>& rows);

const std::vector<Row> SortRows(const vm::Schema& schema,
                                const std::vector<Row>& rows,
                                const std::string& order_col);

const std::string GenerateTableName(int32_t id);

void DoEngineCheckExpect(const SqlCase& sql_case,
                         std::shared_ptr<RunSession> session,
                         const std::vector<Row>& output);

class EngineTest : public ::testing::TestWithParam<SqlCase> {
 public:
    EngineTest() {}
    virtual ~EngineTest() {}
};

class BatchRequestEngineTest : public ::testing::TestWithParam<SqlCase> {
 public:
    BatchRequestEngineTest() {}
    virtual ~BatchRequestEngineTest() {}
};

class EngineTestRunner {
 public:
    explicit EngineTestRunner(const SqlCase& sql_case,
                              const EngineOptions options)
        : sql_case_(sql_case), options_(options), engine_() {
        InitSqlCase();
    }
    virtual ~EngineTestRunner() {}

    void SetSession(std::shared_ptr<RunSession> session) { session_ = session; }

    std::shared_ptr<RunSession> GetSession() const { return session_; }

    static Status ExtractTableInfoFromCreateString(
        const std::string& create, SqlCase::TableInfo* table_info);
    Status Compile();
    virtual void InitSqlCase();
    virtual bool InitEngineCatalog() = 0;
    virtual bool InitTable(const std::string& db, const std::string& table_name) = 0;
    virtual bool AddRowsIntoTable(const std::string& db, const std::string& table_name,
                                  const std::vector<Row>& rows) = 0;
    virtual bool AddRowIntoTable(const std::string& db, const std::string& table_name,
                                 const Row& rows) = 0;
    virtual Status PrepareParameter();
    virtual Status PrepareData() = 0;
    virtual Status Compute(std::vector<codec::Row>*) = 0;
    int return_code() const { return return_code_; }
    const SqlCase& sql_case() const { return sql_case_; }

    void RunCheck();
    void RunBenchmark(size_t iters);

 protected:
    SqlCase sql_case_;
    EngineOptions options_;
    std::shared_ptr<Engine> engine_ = nullptr;
    std::shared_ptr<RunSession> session_ = nullptr;
    codec::Schema parameter_schema_;
    std::vector<Row> parameter_rows_;
    int return_code_ = ENGINE_TEST_RET_INVALID_CASE;
};

class BatchEngineTestRunner : public EngineTestRunner {
 public:
    explicit BatchEngineTestRunner(const SqlCase& sql_case,
                                   const EngineOptions options)
        : EngineTestRunner(sql_case, options) {
        session_ = std::make_shared<BatchRunSession>();
    }
    Status PrepareData() override {
        for (int32_t i = 0; i < sql_case_.CountInputs(); i++) {
            auto input = sql_case_.inputs()[i];
            std::vector<Row> rows;
            sql_case_.ExtractInputData(rows, i);
            size_t repeat = sql_case_.inputs()[i].repeat_;
            if (repeat > 1) {
                size_t row_num = rows.size();
                rows.resize(row_num * repeat);
                size_t offset = row_num;
                for (size_t i = 0; i < repeat - 1; ++i) {
                    std::copy(rows.begin(), rows.begin() + row_num,
                              rows.begin() + offset);
                    offset += row_num;
                }
            }
            if (!rows.empty()) {
                std::string table_name = sql_case_.inputs_[i].name_;
                std::string table_db_name =
                    sql_case_.inputs_[i].db_.empty() ? sql_case_.db() : sql_case_.inputs_[i].db_;
                CHECK_TRUE(AddRowsIntoTable(table_db_name, table_name, rows),
                           common::kTablePutFailed, "Fail to add rows into table ",
                           table_name);
            }
        }
        return Status::OK();
    }

    Status Compute(std::vector<Row>* outputs) override {
        auto batch_session =
            std::dynamic_pointer_cast<BatchRunSession>(session_);
        CHECK_TRUE(batch_session != nullptr, common::kNullPointer);
        Row parameter = parameter_rows_.empty() ? Row() : parameter_rows_[0];
        int run_ret = batch_session->Run(parameter, *outputs);
        if (run_ret != 0) {
            return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
        }
        CHECK_TRUE(run_ret == 0, common::kRunError, "Run batch session failed");
        return Status::OK();
    }
};

class RequestEngineTestRunner : public EngineTestRunner {
 public:
    explicit RequestEngineTestRunner(const SqlCase& sql_case,
                                     const EngineOptions options)
        : EngineTestRunner(sql_case, options) {
        session_ = std::make_shared<RequestRunSession>();
    }

    Status PrepareData() override {
        request_rows_.clear();
        const bool has_batch_request =
            !sql_case_.batch_request_.columns_.empty();
        auto request_session =
            std::dynamic_pointer_cast<RequestRunSession>(session_);
        CHECK_TRUE(request_session != nullptr, common::kNullPointer);
        std::string request_name = request_session->GetRequestName();
        std::string request_db_name =
            request_session->GetRequestDbName().empty() ? sql_case_.db() : request_session->GetRequestDbName();

        if (has_batch_request) {
            CHECK_TRUE(1 <= sql_case_.batch_request_.rows_.size(), common::kSqlCaseError,
                       "RequestEngine can't handler emtpy rows batch requests");
            CHECK_TRUE(sql_case_.ExtractInputData(sql_case_.batch_request_,
                                                  request_rows_),
                       common::kSqlCaseError, "Extract case request rows failed");
        }
        for (int32_t i = 0; i < sql_case_.CountInputs(); i++) {
            std::string input_name = sql_case_.inputs_[i].name_;
            std::string table_db_name =
                sql_case_.inputs_[i].db_.empty() ? sql_case_.db() : sql_case_.inputs_[i].db_;

            if ((table_db_name == request_db_name) && (input_name == request_name) && !has_batch_request) {
                CHECK_TRUE(sql_case_.ExtractInputData(request_rows_, i),
                           common::kSqlCaseError, "Extract case request rows failed");
                continue;
            } else {
                std::vector<Row> rows;
                if (!sql_case_.inputs_[i].rows_.empty() ||
                    !sql_case_.inputs_[i].data_.empty()) {
                    CHECK_TRUE(sql_case_.ExtractInputData(rows, i), common::kSqlCaseError,
                               "Extract case request rows failed");
                }

                if (sql_case_.inputs()[i].repeat_ > 1) {
                    std::vector<Row> store_rows;
                    for (int64_t j = 0; j < sql_case_.inputs()[i].repeat_;
                         j++) {
                        for (auto row : rows) {
                            store_rows.push_back(row);
                        }
                    }
                    CHECK_TRUE(AddRowsIntoTable(table_db_name, input_name, store_rows),
                               common::kTablePutFailed,
                               "Fail to add rows into table ", input_name);

                } else {
                    CHECK_TRUE(AddRowsIntoTable(table_db_name, input_name, rows),
                               common::kTablePutFailed,
                               "Fail to add rows into table ", input_name);
                }
            }
        }
        return Status::OK();
    }

    Status Compute(std::vector<Row>* outputs) override {
        const bool has_batch_request =
            !sql_case_.batch_request_.columns_.empty() ||
            !sql_case_.batch_request_.schema_.empty();
        auto request_session =
            std::dynamic_pointer_cast<RequestRunSession>(session_);
        std::string request_name = request_session->GetRequestName();
        std::string request_db_name = request_session->GetRequestDbName();
        CHECK_TRUE(parameter_rows_.empty(), common::kUnSupport, "Request do not support parameterized query currently")
        Row parameter = parameter_rows_.empty() ? Row() : parameter_rows_[0];
        if (request_rows_.empty()) {
            // send empty request, trigger e.g const project in request mode
            CHECK_TRUE(request_name.empty() && request_db_name.empty(), common::kUnsupportSql,
                       "no request data for request table: <", request_db_name, ".", request_name, ">")
            request_rows_.push_back(Row());
        }
        for (auto in_row : request_rows_) {
            Row out_row;
            int run_ret = request_session->Run(in_row, &out_row);
            if (run_ret != 0) {
                return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
                return Status(common::kRunError, "Run request session failed");
            }
            if (!has_batch_request && !request_name.empty()) {
                CHECK_TRUE(AddRowIntoTable(request_db_name, request_name, in_row), common::kTablePutFailed,
                           "Fail add row into table ", request_db_name, ".", request_name);
            }
            outputs->push_back(out_row);
        }
        return Status::OK();
    }

 private:
    std::vector<Row> request_rows_;
};

class BatchRequestEngineTestRunner : public EngineTestRunner {
 public:
    BatchRequestEngineTestRunner(const SqlCase& sql_case,
                                 const EngineOptions options,
                                 const std::set<size_t>& common_column_indices)
        : EngineTestRunner(sql_case, options) {
        auto request_session = std::make_shared<BatchRequestRunSession>();
        for (size_t idx : common_column_indices) {
            request_session->AddCommonColumnIdx(idx);
        }
        session_ = request_session;
    }

    Status PrepareData() override {
        request_rows_.clear();
        auto request_session =
            std::dynamic_pointer_cast<BatchRequestRunSession>(session_);
        CHECK_TRUE(request_session != nullptr, common::kNullPointer);

        bool has_batch_request = !sql_case_.batch_request().columns_.empty();
        if (!has_batch_request) {
            LOG(WARNING) << "No batch request field in case, try use last row from primary input";
        }

        std::vector<Row> original_request_data;
        std::string request_name = request_session->GetRequestName();
        std::string request_db_name =
            request_session->GetRequestDbName().empty() ? sql_case_.db() : request_session->GetRequestDbName();
        auto& request_schema = request_session->GetRequestSchema();
        for (int32_t i = 0; i < sql_case_.CountInputs(); i++) {
            auto input = sql_case_.inputs()[i];
            std::vector<Row> rows;
            sql_case_.ExtractInputData(rows, i);
            if (!rows.empty()) {
                std::string table_name = sql_case_.inputs_[i].name_;
                std::string table_db_name =
                    sql_case_.inputs_[i].db_.empty() ? sql_case_.db() : sql_case_.inputs_[i].db_;
                if ((table_db_name == request_db_name && table_name == request_name) &&
                    !has_batch_request) {
                    original_request_data.push_back(rows.back());
                    rows.pop_back();
                }

                size_t repeat = sql_case_.inputs()[i].repeat_;
                if (repeat > 1) {
                    size_t row_num = rows.size();
                    rows.resize(row_num * repeat);
                    size_t offset = row_num;
                    for (size_t i = 0; i < repeat - 1; ++i) {
                        std::copy(rows.begin(), rows.begin() + row_num,
                                  rows.begin() + offset);
                        offset += row_num;
                    }
                }
                CHECK_TRUE(AddRowsIntoTable(table_db_name, table_name, rows),
                           common::kTablePutFailed, "Fail to add rows into table ",
                           table_db_name, ".", table_name);
            }
        }

        type::TableDef request_table;
        if (has_batch_request) {
            sql_case_.ExtractTableDef(sql_case_.batch_request().columns_,
                                      sql_case_.batch_request().indexs_,
                                      request_table);
            sql_case_.ExtractRows(request_table.columns(),
                                  sql_case_.batch_request().rows_,
                                  original_request_data);
        } else {
            sql_case_.ExtractInputTableDef(request_table, 0);
        }

        std::vector<size_t> common_column_indices;
        for (size_t idx : request_session->common_column_indices()) {
            common_column_indices.push_back(idx);
        }
        size_t request_schema_size = static_cast<size_t>(request_schema.size());
        if (common_column_indices.empty() ||
            common_column_indices.size() == request_schema_size ||
            !options_.IsBatchRequestOptimized()) {
            request_rows_ = original_request_data;
        } else {
            std::vector<size_t> non_common_column_indices;
            for (size_t i = 0; i < request_schema_size; ++i) {
                if (std::find(common_column_indices.begin(),
                              common_column_indices.end(),
                              i) == common_column_indices.end()) {
                    non_common_column_indices.push_back(i);
                }
            }
            codec::RowSelector left_selector(&request_table.columns(),
                                             common_column_indices);
            codec::RowSelector right_selector(&request_table.columns(),
                                              non_common_column_indices);

            bool left_selected = false;
            codec::RefCountedSlice left_slice;
            for (auto& original_row : original_request_data) {
                if (!left_selected) {
                    int8_t* left_buf;
                    size_t left_size;
                    left_selector.Select(original_row.buf(0),
                                         original_row.size(0), &left_buf,
                                         &left_size);
                    left_slice = codec::RefCountedSlice::CreateManaged(
                        left_buf, left_size);
                    left_selected = true;
                }
                int8_t* right_buf = nullptr;
                size_t right_size;
                right_selector.Select(original_row.buf(0), original_row.size(0),
                                      &right_buf, &right_size);
                codec::RefCountedSlice right_slice =
                    codec::RefCountedSlice::CreateManaged(right_buf,
                                                          right_size);
                request_rows_.emplace_back(codec::Row(
                    1, codec::Row(left_slice), 1, codec::Row(right_slice)));
            }
        }
        size_t repeat = sql_case_.batch_request().repeat_;
        if (repeat > 1) {
            size_t row_num = request_rows_.size();
            request_rows_.resize(row_num * repeat);
            size_t offset = row_num;
            for (size_t i = 0; i < repeat - 1; ++i) {
                std::copy(request_rows_.begin(),
                          request_rows_.begin() + row_num,
                          request_rows_.begin() + offset);
                offset += row_num;
            }
        }

        if (request_rows_.empty()) {
            // batch request rows will empty for const projects
            // workaround by add the one empty row
            request_rows_.push_back(Row());
        }
        return Status::OK();
    }

    Status Compute(std::vector<Row>* outputs) override {
        auto request_session =
            std::dynamic_pointer_cast<BatchRequestRunSession>(session_);
        CHECK_TRUE(request_session != nullptr, common::kNullPointer);
        // Currently parameterized query un-support currently
        CHECK_TRUE(parameter_rows_.empty(), common::kUnSupport,
                   "Batch request do not support parameterized query currently")
        int run_ret = request_session->Run(request_rows_, *outputs);
        if (run_ret != 0) {
            return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
            return Status(common::kTestRunSessionError, "Run batch request session failed");
        }
        return Status::OK();
    }

 private:
    std::vector<Row> request_rows_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_TESTING_ENGINE_TEST_BASE_H_
