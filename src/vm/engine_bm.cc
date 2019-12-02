/*
 * engine_mk.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "vm/engine.h"
#include "benchmark/benchmark.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "gtest/gtest.h"
#include "vm/table_mgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"


namespace fesql {
namespace vm {
using namespace ::llvm;
class TableMgrImpl : public TableMgr {

 public:
    TableMgrImpl(std::shared_ptr<TableStatus> status):status_(status) {}
    ~TableMgrImpl() {}
    std::shared_ptr<TableStatus> GetTableDef(const std::string&,
            const std::string&) {
        return  status_;
    }
    std::shared_ptr<TableStatus> GetTableDef(const std::string&, const uint32_t) {
        return status_;
    }
 private:
    std::shared_ptr<TableStatus> status_;
};


static void BM_EngineFn(benchmark::State& state) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    std::unique_ptr<::fesql::storage::Table> table(new ::fesql::storage::Table("t1", 1, 1, 1));
    ASSERT_TRUE(table->Init());
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *((int32_t*)(ptr + 2)) = 1;
    *((int16_t*)(ptr +2+4)) = 2;
    *((float*)(ptr +2+ 4 + 2)) = 3.1f;
    *((double*)(ptr +2+ 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr +2+ 4 + 2 + 4 + 8)) = 5;

    table->Put("k1", 1, (char*)ptr, 28);
    table->Put("k1", 2, (char*)ptr, 28);
    std::shared_ptr<TableStatus> status(new TableStatus());
    status->table = std::move(table);
    status->table_def.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col15");
    }
    TableMgrImpl table_mgr(status);
    const std::string sql = "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return d\nend\n%%sql\nSELECT test(col1,col1), col2 FROM t1 limit 10;";
    Engine engine(&table_mgr);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        session.Run(output, 2);
        int8_t* output1 = output[0];
        int8_t* output2 = output[1];
        free(output1);
        free(output2);
    }
}

BENCHMARK(BM_EngineFn);
}  // namespace vm
}  // namespace fesql

BENCHMARK_MAIN();


