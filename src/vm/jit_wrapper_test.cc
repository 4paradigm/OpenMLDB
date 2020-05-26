/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * jit_wrapper_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "codec/fe_row_codec.h"
#include "vm/jit_wrapper.h"
#include "vm/engine.h"
#include "vm/simple_catalog.h"
#include "gtest/gtest.h"

namespace fesql {
namespace vm {

class JITWrapperTest : public ::testing::Test {};


std::shared_ptr<SimpleCatalog> GetTestCatalog() {
    fesql::type::Database db;
    db.set_name("db");
    ::fesql::type::TableDef *table = db.add_tables();
    table->set_name("t1");
    table->set_catalog("db");
    {
        ::fesql::type::ColumnDef *column = table->add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col_1");
    }
    {
        ::fesql::type::ColumnDef *column = table->add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col_2");
    }

    auto catalog = std::make_shared<SimpleCatalog>();
    catalog->AddDatabase(db);
    return catalog;
}


std::string GetModuleString(std::shared_ptr<SimpleCatalog> catalog) {
    EngineOptions options;
    options.set_keep_ir(true);

    base::Status status;
    BatchRunSession session;
    Engine engine(catalog, options);
    engine.Get("select col_1, col_2 from t1;", "db", session, status);
    auto compile_info = session.GetCompileInfo();
    return compile_info->get_sql_context().ir;
}


TEST_F(JITWrapperTest, test) {
    auto catalog = GetTestCatalog();
    std::string ir_str = GetModuleString(catalog);

    FeSQLJITWrapper jit;
    ASSERT_TRUE(jit.Init());

    base::RawBuffer ir_buf(const_cast<char*>(ir_str.data()), ir_str.size());
    ASSERT_TRUE(jit.AddModuleFromBuffer(ir_buf));

    auto fn = jit.FindFunction("__internal_sql_codegen_0");
    ASSERT_TRUE(fn != nullptr);

    int8_t buf[1024];
    auto schema = catalog->GetTable("db", "t1")->GetSchema();
    codec::RowBuilder row_builder(*schema);
    row_builder.SetBuffer(buf, 1024);
    row_builder.AppendDouble(3.14);
    row_builder.AppendInt32(42);

    fesql::codec::Row row(base::Slice::Create(buf, 1024));

    fesql::codec::Row output = CoreAPI::RowProject(fn, row);

    codec::RowView row_view(*schema, output.buf(), output.size());
    double c1;
    int32_t c2;
    ASSERT_EQ(row_view.GetDouble(0, &c1), 0);
    ASSERT_EQ(row_view.GetInt32(1, &c2), 0);
    ASSERT_EQ(c1, 3.14);
    ASSERT_EQ(c2, 42);
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    fesql::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
