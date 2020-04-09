/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * runner_bm_case.cc
 *
 * Author: chenjing
 * Date: 2020/4/9
 *--------------------------------------------------------------------------
 **/
#include "bm/runner_bm_case.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "bm/base_bm.h"
#include "gtest/gtest.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "vm/engine.h"

namespace fesql {
namespace bm {
using vm::DataHandler;
using vm::Engine;
using vm::RequestRunSession;
using vm::Runner;
using vm::RunnerContext;
using vm::TableHandler;

using namespace ::llvm;  // NOLINT

static int64_t DeleteData(std::shared_ptr<DataHandler> data_handler);
static Runner* GetRunner(Runner* root, int id);
static bool RunnerRun(
    Runner* runner, std::shared_ptr<TableHandler> table_handler,
    int64_t limit_cnt,
    std::vector<std::shared_ptr<DataHandler>>& result);  // NOLINT
static void RequestUnionRunnerCase(const std::string& sql, int runner_id,
                                   benchmark::State* state, MODE mode,
                                   int64_t limit_cnt, int64_t size);

static void RequestUnionRunnerCase(const std::string& sql, int runner_id,
                                   benchmark::State* state, MODE mode,
                                   int64_t limit_cnt, int64_t size) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    // prepare data into table
    auto catalog = Data_WindowCase1(size);
    Engine engine(catalog);
    RequestRunSession session;
    base::Status status;
    ASSERT_TRUE(engine.Get(sql, "db", session, status));
    auto table = catalog->GetTable("db", "t1");
    if (!table) {
        LOG(WARNING) << "table not exist";
        return;
    }

    std::ostringstream plan_oss;
    session.GetPhysicalPlan()->Print(plan_oss, "");
    LOG(INFO) << "physical plan:\n" << plan_oss.str() << std::endl;
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::unique_ptr<codec::RowView> row_view =
        std::move(std::unique_ptr<codec::RowView>(
            new codec::RowView(session.GetSchema())));

    auto start_runner = GetRunner(session.GetRunner(), runner_id);
    ASSERT_TRUE(nullptr != start_runner);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                std::vector<std::shared_ptr<DataHandler>> res;
                benchmark::DoNotOptimize(
                    RunnerRun(start_runner, table, limit_cnt, res));
            }
        }
        case TEST: {
            std::vector<std::shared_ptr<DataHandler>> res;
            ASSERT_TRUE(RunnerRun(start_runner, table, limit_cnt, res));
            ASSERT_EQ(res.size(), static_cast<size_t>(limit_cnt));
            for (auto data : res) {
//                ASSERT_EQ(static_cast<int64_t >(1 + size), data->GetCount());
                LOG(INFO) << "res size : " << data->GetCount();
            }
        }
    }
}

void IndexSeekRunnerCase(const std::string sql, int runner_id,
                         benchmark::State* state, MODE mode, int64_t limit_cnt,
                         int64_t data_size);

void AggRunnerCase(const std::string sql, int runner_id,
                   benchmark::State* state, MODE mode, int64_t limit_cnt,
                   int64_t data_size);

void IndexSeekRunnerCase(const std::string sql, int runner_id,
                         benchmark::State* state, MODE mode, int64_t limit_cnt,
                         int64_t size) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    // prepare data into table
    auto catalog = Data_WindowCase1(size);
    Engine engine(catalog);
    RequestRunSession session;
    base::Status status;
    ASSERT_TRUE(engine.Get(sql, "db", session, status));
    auto table = catalog->GetTable("db", "t1");
    if (!table) {
        LOG(WARNING) << "table not exist";
        return;
    }

    std::ostringstream plan_oss;
    session.GetPhysicalPlan()->Print(plan_oss, "");
    LOG(INFO) << "physical plan:\n" << plan_oss.str() << std::endl;
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::unique_ptr<codec::RowView> row_view =
        std::move(std::unique_ptr<codec::RowView>(
            new codec::RowView(session.GetSchema())));

    auto start_runner = GetRunner(session.GetRunner(), runner_id);
    ASSERT_TRUE(nullptr != start_runner);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                std::vector<std::shared_ptr<DataHandler>> res;
                benchmark::DoNotOptimize(
                    RunnerRun(start_runner, table, limit_cnt, res));
            }
        }
        case TEST: {
            std::vector<std::shared_ptr<DataHandler>> res;
            ASSERT_TRUE(RunnerRun(start_runner, table, limit_cnt, res));
        }
    }
}
void AggRunnerCase(const std::string sql, int runner_id,
                   benchmark::State* state, MODE mode, int64_t limit_cnt,
                   int64_t size) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    // prepare data into table
    auto catalog = Data_WindowCase1(size);
    Engine engine(catalog);
    RequestRunSession session;
    base::Status status;
    ASSERT_TRUE(engine.Get(sql, "db", session, status));
    auto table = catalog->GetTable("db", "t1");
    if (!table) {
        LOG(WARNING) << "table not exist";
        return;
    }

    std::ostringstream plan_oss;
    session.GetPhysicalPlan()->Print(plan_oss, "");
    LOG(INFO) << "physical plan:\n" << plan_oss.str() << std::endl;
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::unique_ptr<codec::RowView> row_view =
        std::move(std::unique_ptr<codec::RowView>(
            new codec::RowView(session.GetSchema())));

    auto start_runner = GetRunner(session.GetRunner(), runner_id);
    ASSERT_TRUE(nullptr != start_runner);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                std::vector<std::shared_ptr<DataHandler>> res;
                benchmark::DoNotOptimize(
                    RunnerRun(start_runner, table, limit_cnt, res));
                for (auto data : res) {
                    DeleteData(data);
                }
            }
        }
        case TEST: {
            std::vector<std::shared_ptr<DataHandler>> res;
            ASSERT_TRUE(RunnerRun(start_runner, table, limit_cnt, res));
            for (auto data : res) {
                DeleteData(data);
            }
        }
    }
}

static bool RunnerRun(
    Runner* runner, std::shared_ptr<TableHandler> table_handler,
    int64_t limit_cnt,
    std::vector<std::shared_ptr<DataHandler>>& result) {  // NOLINT
    auto iter = table_handler->GetIterator();
    int64_t cnt = 0;
    while (cnt < limit_cnt && iter->Valid()) {
        cnt++;
        RunnerContext ctx(iter->GetValue());
        auto data = runner->Run(ctx);
        iter->Next();
        result.push_back(data);
    }
    return true;
}

static int64_t DeleteData(std::shared_ptr<DataHandler> data_handler) {
    if (!data_handler) {
        return 0;
    }
    switch (data_handler->GetHanlderType()) {
        case vm::kRowHandler: {
            auto row =
                std::dynamic_pointer_cast<vm::MemRowHandler>(data_handler);
            delete row->GetValue().buf();
            return 1;
        }
        case vm::kTableHandler: {
            auto table =
                std::dynamic_pointer_cast<vm::MemTableHandler>(data_handler);
            auto iter = table->GetIterator();
            int64_t cnt = 0;
            while (iter->Valid()) {
                delete iter->GetValue().buf();
                iter->Next();
                cnt++;
            }
            return cnt;
        }
        case vm::kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<vm::MemPartitionHandler>(
                data_handler);
            auto iter = partition->GetWindowIterator();
            int64_t group_cnt = 0;
            while (iter->Valid()) {
                auto seg_iter = iter->GetValue();
                while (seg_iter->Valid()) {
                    delete seg_iter->GetValue().buf();
                    seg_iter->Next();
                }
                iter->Next();
                group_cnt++;
            }
            return group_cnt;
        }
    }
    return 0;
}
static Runner* GetRunner(Runner* root, int id) {
    if (nullptr == root) {
        return nullptr;
    }

    if (id == root->id_) {
        return root;
    } else {
        for (auto runner : root->GetProducers()) {
            auto res = GetRunner(runner, id);
            if (nullptr != res) {
                return res;
            }
        }
        return nullptr;
    }
}

void WindowSumFeature1_RequestUnion(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt, int64_t size) {
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    RequestUnionRunnerCase(sql, 4, state, mode, limit_cnt, size);
}

void WindowSumFeature1_IndexSeek(benchmark::State* state, MODE mode,
                                 int64_t limit_cnt, int64_t size) {
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    IndexSeekRunnerCase(sql, 3, state, mode, limit_cnt, size);
}
void WindowSumFeature1_Aggregation(benchmark::State* state, MODE mode,
                                   int64_t limit_cnt, int64_t size) {
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    AggRunnerCase(sql, 5, state, mode, limit_cnt, size);
}

}  // namespace bm
}  // namespace fesql
