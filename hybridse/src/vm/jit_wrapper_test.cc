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

#include "vm/jit_wrapper.h"
#include "codec/fe_row_codec.h"
#include "gtest/gtest.h"
#include "udf/udf.h"
#include "vm/engine.h"
#include "vm/simple_catalog.h"
#include "vm/sql_compiler.h"

namespace hybridse {
namespace vm {

class JitWrapperTest : public ::testing::Test {};

std::shared_ptr<SimpleCatalog> GetTestCatalog() {
    hybridse::type::Database db;
    db.set_name("db");
    ::hybridse::type::TableDef *table = db.add_tables();
    table->set_name("t1");
    table->set_catalog("db");
    {
        ::hybridse::type::ColumnDef *column = table->add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->set_name("col_1");
    }
    {
        ::hybridse::type::ColumnDef *column = table->add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("col_2");
    }
    auto catalog = std::make_shared<SimpleCatalog>();
    catalog->AddDatabase(db);
    return catalog;
}

std::shared_ptr<SqlCompileInfo> Compile(
    const std::string &sql, const EngineOptions &options,
    std::shared_ptr<SimpleCatalog> catalog) {
    base::Status status;
    BatchRunSession session;
    Engine engine(catalog, options);
    if (!engine.Get(sql, "db", session, status)) {
        LOG(WARNING) << "Fail to compile sql. error " << status.msg;
        return nullptr;
    }
    return std::dynamic_pointer_cast<SqlCompileInfo>(session.GetCompileInfo());
}

std::shared_ptr<SqlCompileInfo> CompileNotPerformanceSensitive(
    const std::string &sql, const EngineOptions &options,
    std::shared_ptr<SimpleCatalog> catalog) {
    base::Status status;
    BatchRunSession session;
    Engine engine(catalog, options);
    if (!engine.Get(sql, "db", session, status)) {
        LOG(WARNING) << "Fail to compile sql";
        return nullptr;
    }
    return std::dynamic_pointer_cast<SqlCompileInfo>(session.GetCompileInfo());
}

void simple_test(const EngineOptions &options) {
    auto catalog = GetTestCatalog();
    std::string sql = "select col_1, col_2 from t1;";
    auto compile_info = Compile(sql, options, catalog);
    auto &sql_context = compile_info->get_sql_context();
    std::string ir_str = sql_context.ir;
    ASSERT_FALSE(ir_str.empty());
    HybridSeJitWrapper *jit = HybridSeJitWrapper::Create();
    ASSERT_TRUE(jit->Init());

    base::RawBuffer ir_buf(const_cast<char *>(ir_str.data()), ir_str.size());
    ASSERT_TRUE(jit->AddModuleFromBuffer(ir_buf));

    auto fn_name = sql_context.physical_plan->GetFnInfos()[0]->fn_name();
    auto fn = jit->FindFunction(fn_name);
    ASSERT_TRUE(fn != nullptr);

    int8_t buf[1024];
    auto schema = catalog->GetTable("db", "t1")->GetSchema();
    codec::RowBuilder row_builder(*schema);
    row_builder.SetBuffer(buf, 1024);
    row_builder.AppendDouble(3.14);
    row_builder.AppendInt64(42);

    hybridse::codec::Row empty_parameter;
    hybridse::codec::Row row(base::RefCountedSlice::Create(buf, 1024));
    hybridse::codec::Row output = CoreAPI::RowProject(fn, row, empty_parameter);
    codec::RowView row_view(*schema, output.buf(), output.size());
    double c1;
    int64_t c2;
    ASSERT_EQ(row_view.GetDouble(0, &c1), 0);
    ASSERT_EQ(row_view.GetInt64(1, &c2), 0);
    ASSERT_EQ(c1, 3.14);
    ASSERT_EQ(c2, 42);
    delete jit;
}

TEST_F(JitWrapperTest, test) {
    EngineOptions options;
    options.SetKeepIr(true);
    simple_test(options);
}

#ifdef LLVM_EXT_ENABLE
TEST_F(JitWrapperTest, test_mcjit) {
    EngineOptions options;
    options.SetKeepIr(true);
    options.jit_options().SetEnableMcjit(true);
    options.jit_options().SetEnableGdb(true);
    options.jit_options().SetEnablePerf(true);
    options.jit_options().SetEnableVtune(true);
    simple_test(options);
}
#endif

TEST_F(JitWrapperTest, test_window) {
    EngineOptions options;
    options.SetKeepIr(true);
    auto catalog = GetTestCatalog();
    auto compile_info = CompileNotPerformanceSensitive(
        "select col_1, sum(col_2) over w, "
        "distinct_count(col_2) over w "
        "from t1 "
        "window w as ("
        "PARTITION by col_2 ORDER BY col_2 "
        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW);",
        options, catalog);
    auto &sql_context = compile_info->get_sql_context();
    std::string ir_str = sql_context.ir;

    // clear this dict to ensure jit wrapper reinit all symbols
    // this should be removed by better symbol init utility

    ASSERT_FALSE(ir_str.empty());
    HybridSeJitWrapper *jit = HybridSeJitWrapper::Create();
    ASSERT_TRUE(jit->Init());

    base::RawBuffer ir_buf(const_cast<char *>(ir_str.data()), ir_str.size());
    ASSERT_TRUE(jit->AddModuleFromBuffer(ir_buf));

    auto fn_name = sql_context.physical_plan->GetFnInfos()[0]->fn_name();
    auto fn = jit->FindFunction(fn_name);
    ASSERT_TRUE(fn != nullptr);
    delete jit;
}

}  // namespace vm
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
