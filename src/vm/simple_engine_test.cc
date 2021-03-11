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

#include "case/case_data_mock.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "vm/engine_test_base.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)
namespace fesql {
namespace vm {
TEST_F(EngineTest, SimpleEngineTest) {
    // build Simple Catalog
    auto catalog = std::make_shared<SimpleCatalog>(true);
    // database simple_db
    fesql::type::Database db;
    db.set_name("simple_db");

    // prepare table t1 schema and data
    fesql::type::TableDef table_def;
    {
        table_def.set_name("t1");
        table_def.set_catalog("db");
        {
            ::fesql::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::fesql::type::kVarchar);
            column->set_name("col0");
        }
        {
            ::fesql::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::fesql::type::kInt32);
            column->set_name("col1");
        }
        {
            ::fesql::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::fesql::type::kInt64);
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
    catalog->InsertRows("simple_db", "t1", t1_rows);

    // build simple engine
    EngineOptions options;
    Engine engine(catalog, options);
    std::string sql =
        "select col0, col1, col2, col1+col2 as col12 from t1;";
    {
        base::Status get_status;
        BatchRunSession session;
        // compile sql
        ASSERT_TRUE(engine.Get(sql, "simple_db", session, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        std::vector<Row> outputs;
        // run sql query
        ASSERT_EQ(0, session.Run(outputs));
        LOG(INFO) << "output size " << outputs.size();
        PrintRows(session.GetSchema(), outputs);
    }
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    // ::fesql::vm::CoreAPI::EnableSignalTraceback();
    return RUN_ALL_TESTS();
}
