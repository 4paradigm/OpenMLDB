/*
 * simple_catalog_test.cc
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * simple_catalog_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "vm/simple_catalog.h"
#include "gtest/gtest.h"

namespace fesql {
namespace vm {

class SimpleCatalogTest : public ::testing::Test {};

TEST_F(SimpleCatalogTest, test) {
    fesql::type::Database db;
    db.set_name("db");
    ::fesql::type::TableDef *table = db.add_tables();
    table->set_name("t");
    table->set_catalog("db");
    {
        ::fesql::type::ColumnDef *column = table->add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef *column = table->add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }

    SimpleCatalog catalog;
    catalog.AddDatabase(db);

    ASSERT_TRUE(catalog.GetDatabase("db") != nullptr);
    ASSERT_TRUE(catalog.GetTable("db", "t_nonexist") == nullptr);

    auto tbl_handle = catalog.GetTable("db", "t");
    ASSERT_TRUE(tbl_handle != nullptr);
    ASSERT_TRUE(tbl_handle->GetSchema() != nullptr);
    ASSERT_EQ(tbl_handle->GetName(), "t");
    ASSERT_EQ(tbl_handle->GetDatabase(), "db");

    ASSERT_EQ(tbl_handle->GetWindowIterator(""), nullptr);
    ASSERT_EQ(tbl_handle->GetCount(), 0);
    ASSERT_EQ(tbl_handle->GetIterator(), nullptr);
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
