/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * simple_catalog_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "gtest/gtest.h"
#include "vm/simple_catalog.h"

namespace fesql {
namespace vm {

TEST(SimpleCatalogTest, test) {
    fesql::type::Database db;
    db.set_name("db");
    ::fesql::type::TableDef* table = db.add_tables();
    table->set_name("t");
    table->set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table->add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table->add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }

	SimpleCatalog catalog;
	catalog.AddDatabase(db);

	ASSERT_TRUE(catalog.GetDatabase("db") != nullptr);
	ASSERT_TRUE(catalog.GetTable("db", "t_nonexist") == nullptr);

	auto tbl_handle = catalog.GetTable("db", "t");
	ASSERT_TRUE(tbl_handle);
	ASSERT_TRUE(tbl_handle->GetSchema() != nullptr);
	ASSERT_TRUE(tbl_handle->GetName() == "t");
	ASSERT_TRUE(tbl_handle->GetDatabase() == "db");

	ASSERT_TRUE(tbl_handle->GetWindowIterator("") == nullptr);
	ASSERT_TRUE(tbl_handle->GetCount() == 0);
   	ASSERT_TRUE(tbl_handle->GetIterator() == nullptr);
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
