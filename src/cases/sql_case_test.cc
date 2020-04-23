/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * sql_case_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "cases/sql_case.h"
#include "gtest/gtest.h"
namespace fesql {
namespace cases {

class SQLCaseTest : public ::testing::Test {};

TEST_F(SQLCaseTest, ExtractSchemaTest) {
    type::TableDef table;
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }

    {
        SQLCase sql_case;
        sql_case.schema_str_ =
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string";
        type::TableDef output_table;
        sql_case.ExtractSchema(output_table);
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(table.DebugString(), output_table.DebugString());
    }
    {
        SQLCase sql_case;
        sql_case.schema_str_ =
            "col0:string, col1:int, col2:smallint, col3:float, col4:double, "
            "col5:bigint, col6:string";
        type::TableDef output_table;
        sql_case.ExtractSchema(output_table);
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(table.DebugString(), output_table.DebugString());
    }
    {
        SQLCase sql_case;
        sql_case.schema_str_ =
            "col0:string, col1:i32, col2:i16, col3:float, col4:double, "
            "col5:i64, col6:string";
        type::TableDef output_table;
        sql_case.ExtractSchema(output_table);
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(table.DebugString(), output_table.DebugString());
    }

    // Invalid Type
    {
        SQLCase sql_case;
        sql_case.schema_str_ =
            "col0:str, col1:long";
        type::TableDef output_table;
        ASSERT_FALSE(sql_case.ExtractSchema(output_table));
    }

    {
        SQLCase sql_case;
        sql_case.schema_str_ =
            "col0:integer, col1:int32";
        type::TableDef output_table;
        ASSERT_FALSE(sql_case.ExtractSchema(output_table));
    }

    //InValid Column
    {
        SQLCase sql_case;
        sql_case.schema_str_ =
            "col0:str, col1:int32,,";
        type::TableDef output_table;
        ASSERT_FALSE(sql_case.ExtractSchema(output_table));
    }
}
}  // namespace cases
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
