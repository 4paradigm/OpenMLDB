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

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "base/texttable.h"
#include "boost/algorithm/string.hpp"
#include "codec/fe_row_codec.h"
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
#include "sys/time.h"
#include "vm/engine.h"
#include "testing/test_base.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)
namespace hybridse {
namespace cmd {

static const int SIMPLE_ENGINE_RET_SUCCESS = 0;
static const int SIMPLE_ENGINE_DATA_ERROR = 1;
static const int SIMPLE_ENGINE_COMPILE_ERROR = 2;
static const int SIMPLE_ENGINE_RUN_ERROR = 3;
static const int MAX_DEBUG_COLUMN_CNT = 20;
static const int MAX_DEBUG_LINES_CNT = 20;
using hybridse::codec::Row;
using hybridse::codec::RowView;
using hybridse::vm::BatchRunSession;
using hybridse::vm::Engine;
using hybridse::vm::EngineOptions;
using hybridse::vm::SimpleCatalog;

static void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    RowView row_view(schema);
    ::hybridse::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
    if (rows.empty()) {
        t.add("Empty set");
        t.end_of_row();
        oss << t << std::endl;
        LOG(INFO) << "\n" << oss.str() << "\n";
        return;
    }

    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            t.add(str);
            if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
                t.add("...");
                break;
            }
        }
        t.end_of_row();
        if (t.rows().size() >= MAX_DEBUG_LINES_CNT) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

int run() {
    // build Simple Catalog
    auto catalog = std::make_shared<SimpleCatalog>(true);
    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // prepare table t1 schema and data
    hybridse::type::TableDef table_def;
    {
        table_def.set_name("t1");
        table_def.set_catalog("db");
        {
            ::hybridse::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::hybridse::type::kVarchar);
            column->set_name("col0");
        }
        {
            ::hybridse::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("col1");
        }
        {
            ::hybridse::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::hybridse::type::kInt64);
            column->set_name("col2");
        }
    }
    *(db.add_tables()) = table_def;
    catalog->AddDatabase(db);

    // insert data into simple_db
    std::vector<Row> t1_rows;
    for (int i = 0; i < 10; ++i) {
        std::string str1 = "hello";
        codec::RowBuilder builder(table_def.columns());
        uint32_t total_size = builder.CalTotalLength(str1.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str1.c_str(), str1.size());
        builder.AppendInt32(i);
        builder.AppendInt64(1576571615000 - i);
        t1_rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    if (!catalog->InsertRows("simple_db", "t1", t1_rows)) {
        return SIMPLE_ENGINE_DATA_ERROR;
    }

    // build simple engine
    EngineOptions options;
    Engine engine(catalog, options);
    std::string sql = "select col0, col1, col2, col1+col2 as col12 from t1;";
    {
        base::Status get_status;
        BatchRunSession session;
        // compile sql
        if (!engine.Get(sql, "simple_db", session, get_status) ||
            get_status.code != common::kOk) {
            return SIMPLE_ENGINE_COMPILE_ERROR;
        }
        std::vector<Row> outputs;
        // run sql query
        if (0 != session.Run(outputs)) {
            return SIMPLE_ENGINE_RUN_ERROR;
        }
        PrintRows(session.GetSchema(), outputs);
    }
    return SIMPLE_ENGINE_RET_SUCCESS;
}

}  // namespace cmd
}  // namespace hybridse

int main(int argc, char** argv) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return hybridse::cmd::run();
}
