/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * sql_case_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "case/sql_case.h"
#include <vector>
#include "boost/filesystem/operations.hpp"
#include "gtest/gtest.h"
#include "yaml-cpp/yaml.h"
namespace fesql {
namespace sqlcase {

class SQLCaseTest : public ::testing::Test {};

TEST_F(SQLCaseTest, ExtractTableDefTest) {
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
        type::TableDef exp_table = table;
        auto index = exp_table.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
        const std::string schema_str =
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string";
        type::TableDef output_table;
        ASSERT_TRUE(SQLCase::ExtractSchema(schema_str, output_table));
        const std::string index_str = "index1:col1:col5";
        ASSERT_TRUE(SQLCase::ExtractIndex(index_str, output_table));
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(exp_table.DebugString(), output_table.DebugString());
    }
    {
        type::TableDef exp_table = table;
        auto index = exp_table.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
        const std::string schema_str =
            "col0:string, col1:int, col2:smallint, col3:float, col4:double, "
            "col5:bigint, col6:string";
        type::TableDef output_table;
        ASSERT_TRUE(SQLCase::ExtractSchema(schema_str, output_table));
        const std::string index_str = "index1:col1|col2:col5";
        ASSERT_TRUE(SQLCase::ExtractIndex(index_str, output_table));
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(exp_table.DebugString(), output_table.DebugString());
    }
    {
        type::TableDef exp_table = table;
        {
            auto index = exp_table.add_indexes();
            index->set_name("index1");
            index->add_first_keys("col1");
            index->set_second_key("col5");
        }
        {
            auto index = exp_table.add_indexes();
            index->set_name("index2");
            index->add_first_keys("col1");
            index->add_first_keys("col2");
            index->set_second_key("col5");
        }

        const std::string schema_str =
            "col0:string\ncol1:i32\ncol2:i16\ncol3:float\ncol4:double\n"
            "col5:i64\ncol6:varchar";
        type::TableDef output_table;
        ASSERT_TRUE(SQLCase::ExtractSchema(schema_str, output_table));
        const std::string index_str = "index1:col1:col5, index2:col1|col2:col5";
        ASSERT_TRUE(SQLCase::ExtractIndex(index_str, output_table));
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(exp_table.DebugString(), output_table.DebugString());
    }

    type::TableDef table2;
    table2.set_name("t1");
    table2.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table2.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table2.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table2.add_columns();
        column->set_type(::fesql::type::kTimestamp);
        column->set_name("coltime");
    }

    // column with timestamp
    {
        const std::string schema_str =
            "col0:string, col1:int32, coltime:timestamp";
        type::TableDef output_table;
        output_table.set_name(table2.name());
        output_table.set_catalog(table2.catalog());
        ASSERT_TRUE(SQLCase::ExtractSchema(schema_str, output_table));
        ASSERT_EQ(table2.DebugString(), output_table.DebugString());
    }

    // Invalid Type
    {
        const std::string schema_str = "col0:str, col1:long";
        type::TableDef output_table;
        ASSERT_FALSE(SQLCase::ExtractSchema(schema_str, output_table));
    }

    {
        const std::string schema_str = "col0:integer, col1:int32";
        type::TableDef output_table;
        ASSERT_FALSE(SQLCase::ExtractSchema(schema_str, output_table));
    }

    // InValid Column
    {
        const std::string schema_str = "col0:str, col1:int32,,";
        type::TableDef output_table;
        ASSERT_FALSE(SQLCase::ExtractSchema(schema_str, output_table));
    }
}

TEST_F(SQLCaseTest, ExtractDataTest) {
    {
        std::string data_schema_str =
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string";
        std::string data_str =
            "0, 1, 5, 1.1, 11.1, 1, 1\n"
            "0, 2, 5, 2.2, 22.2, 2, 22\n"
            "0, 3, 55, 3.3, 33.3, 1, 333\n"
            "0, 4, 55, 4.4, 44.4, 2, 4444\n"
            "0, 5, 55, 5.5, 55.5, 3, "
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        type::TableDef output_table;
        ASSERT_TRUE(SQLCase::ExtractSchema(data_schema_str, output_table));
        std::vector<fesql::codec::Row> rows;
        ASSERT_TRUE(
            SQLCase::ExtractRows(output_table.columns(), data_str, rows));
        ASSERT_EQ(5u, rows.size());
        fesql::codec::RowView row_view(output_table.columns());

        {
            row_view.Reset(rows[0].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(1, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(5, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(1.1f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(11.1, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(1L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("1", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[1].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(2, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(5, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(2.2f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(22.2, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(2L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("22", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[2].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(3, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(55, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(3.3f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(33.3, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(1L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("333", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[3].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(4, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(55, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(4.4f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(44.4, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(2L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("4444", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[4].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(5, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(55, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(5.5f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(55.5, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(3L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                "aaaa",
                row_view.GetStringUnsafe(6));
        }
    }
}
TEST_F(SQLCaseTest, ExtractInsertSqlTest) {
    const std::string schema_str =
        "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
        "col5:int64, col6:string, col7:timestamp";

    std::string row_str = "0, 1, 5, 1.1, 11.1, 1, 1, 1587647803000\n";
    type::TableDef output_table;
    ASSERT_TRUE(SQLCase::ExtractSchema(schema_str, output_table));
    std::string create_sql;
    ASSERT_TRUE(
        SQLCase::BuildInsertSQLFromRow(output_table, row_str, &create_sql));
    ASSERT_EQ(
        "Insert into  values('0', 1, 5, 1.1, 11.1, 1, '1', 1587647803000)",
        create_sql);
}
TEST_F(SQLCaseTest, ExtractRowTest) {
    {
        const std::string schema_str =
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string, col7:timestamp";

        std::string row_str = "0, 1, 5, 1.1, 11.1, 1, 1, 1587647803000\n";
        type::TableDef output_table;
        ASSERT_TRUE(SQLCase::ExtractSchema(schema_str, output_table));
        std::vector<fesql::codec::Row> rows;
        int8_t* row_ptr = nullptr;
        int32_t row_size = 0;
        ASSERT_TRUE(SQLCase::ExtractRow(output_table.columns(), row_str,
                                        &row_ptr, &row_size));
        fesql::codec::RowView row_view(output_table.columns());

        {
            row_view.Reset(row_ptr);
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(1, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(5, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(1.1f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(11.1, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(1L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("1", row_view.GetStringUnsafe(6));
            ASSERT_EQ(1587647803000, row_view.GetTimestampUnsafe(7));
        }
    }
}

TEST_F(SQLCaseTest, ExtractSQLCase) {
    SQLCase sql_case;

    const std::string schema =
        "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
        "col5:int64, col6:string";
    const std::string index = "index1:col1:col5";
    const std::string data =
        "0, 1, 5, 1.1, 11.1, 1, 1\n"
        "0, 2, 5, 2.2, 22.2, 2, 22\n"
        "0, 3, 55, 3.3, 33.3, 1, 333\n"
        "0, 4, 55, 4.4, 44.4, 2, 4444\n"
        "0, 5, 55, 5.5, 55.5, 3, "
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "a";
    const std::string order = "col1";
    {
        SQLCase::TableInfo table_data = {.name_ = "",
                                         .schema_ = schema,
                                         .index_ = index,
                                         .data_ = data,
                                         .order_ = order};
        sql_case.AddInput(table_data);
    }

    {
        SQLCase::TableInfo table_data;
        table_data.name_ = "";
        table_data.schema_ =
            "f0:string, f1:float, f2:double, f3:int16, f4:int32, f5:int64, "
            "f6:timestamp";
        table_data.data_ =
            "A, 1.1, 2.2, 3, 4, 5, 1587647803000\n"
            "BB, 11.1, 22.2, 30, 40, 50, 1587647804000";
        sql_case.set_output(table_data);
    }

    // Check Data Schema
    {
        type::TableDef output_table;
        ASSERT_TRUE(sql_case.ExtractInputTableDef(output_table));
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
        auto index = table.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(table.DebugString(), output_table.DebugString());

        // Check Create SQL
        {
            std::string create_sql;
            ASSERT_TRUE(
                SQLCase::BuildCreateSQLFromSchema(output_table, &create_sql));
            LOG(INFO) << create_sql;
            ASSERT_EQ(
                "CREATE TABLE t1(\n"
                "col0 string,\n"
                "col1 int,\n"
                "col2 smallint,\n"
                "col3 float,\n"
                "col4 double,\n"
                "col5 bigint,\n"
                "col6 string,\n"
                "index(key=(col1), ts=col5)\n"
                ")",
                create_sql);
        }
    }

    // Check Data
    {
        type::TableDef output_table;
        std::vector<fesql::codec::Row> rows;
        ASSERT_TRUE(sql_case.ExtractInputData(rows));
        ASSERT_EQ(5u, rows.size());
        sql_case.ExtractInputTableDef(output_table);
        fesql::codec::RowView row_view(output_table.columns());

        {
            row_view.Reset(rows[0].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(1, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(5, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(1.1f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(11.1, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(1L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("1", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[1].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(2, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(5, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(2.2f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(22.2, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(2L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("22", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[2].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(3, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(55, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(3.3f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(33.3, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(1L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("333", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[3].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(4, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(55, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(4.4f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(44.4, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(2L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ("4444", row_view.GetStringUnsafe(6));
        }
        {
            row_view.Reset(rows[4].buf());
            ASSERT_EQ("0", row_view.GetAsString(0));
            ASSERT_EQ(5, row_view.GetInt32Unsafe(1));
            ASSERT_EQ(55, row_view.GetInt16Unsafe(2));
            ASSERT_EQ(5.5f, row_view.GetFloatUnsafe(3));
            ASSERT_EQ(55.5, row_view.GetDoubleUnsafe(4));
            ASSERT_EQ(3L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                "aaaa",
                row_view.GetStringUnsafe(6));
        }
    }

    // Check Data Schema
    {
        type::TableDef output_table;
        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
        type::TableDef table;
        table.set_name("t1");
        table.set_catalog("db");
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kVarchar);
            column->set_name("f0");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kFloat);
            column->set_name("f1");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kDouble);
            column->set_name("f2");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kInt16);
            column->set_name("f3");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kInt32);
            column->set_name("f4");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kInt64);
            column->set_name("f5");
        }

        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kTimestamp);
            column->set_name("f6");
        }
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(table.DebugString(), output_table.DebugString());
    }

    // Check Data
    {
        type::TableDef output_table;
        std::vector<fesql::codec::Row> rows;
        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
        ASSERT_EQ(2u, rows.size());
        sql_case.ExtractOutputSchema(output_table);
        fesql::codec::RowView row_view(output_table.columns());

        {
            row_view.Reset(rows[0].buf());
            ASSERT_EQ("A", row_view.GetAsString(0));
            ASSERT_EQ(1.1f, row_view.GetFloatUnsafe(1));
            ASSERT_EQ(2.2, row_view.GetDoubleUnsafe(2));
            ASSERT_EQ(3, row_view.GetInt16Unsafe(3));
            ASSERT_EQ(4, row_view.GetInt32Unsafe(4));
            ASSERT_EQ(5L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ(1587647803000L, row_view.GetTimestampUnsafe(6));
        }
        {
            row_view.Reset(rows[1].buf());
            ASSERT_EQ("BB", row_view.GetAsString(0));
            ASSERT_EQ(11.1f, row_view.GetFloatUnsafe(1));
            ASSERT_EQ(22.2, row_view.GetDoubleUnsafe(2));
            ASSERT_EQ(30, row_view.GetInt16Unsafe(3));
            ASSERT_EQ(40, row_view.GetInt32Unsafe(4));
            ASSERT_EQ(50L, row_view.GetInt64Unsafe(5));
            ASSERT_EQ(1587647804000L, row_view.GetTimestampUnsafe(6));
        }
    }
}

TEST_F(SQLCaseTest, ExtractYamlSQLCase) {
    std::string fesql_dir = fesql::sqlcase::FindFesqlDirPath();
    std::string case_path = fesql_dir + "/cases/yaml/demo.yaml";
    std::vector<SQLCase> cases;

    ASSERT_TRUE(
        fesql::sqlcase::SQLCase::CreateSQLCasesFromYaml(case_path, cases));
    ASSERT_EQ(4, cases.size());
    {
        SQLCase& sql_case = cases[0];
        ASSERT_EQ(sql_case.id(), 1);
        ASSERT_EQ("batch", sql_case.mode());
        ASSERT_EQ("SELECT所有列", sql_case.desc());
        ASSERT_EQ(sql_case.inputs()[0].name_, "t1");
        ASSERT_EQ(sql_case.db(), "test");
        ASSERT_EQ(
            sql_case.inputs()[0].schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.inputs()[0].index_, "index1:col1|col2:col5");
        ASSERT_EQ(sql_case.inputs()[0].data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22\n0, 3, "
                  "55, 3.3, "
                  "33.3, 1, 333\n0, 4, 55, 4.4, 44.4, 2, 4444\n0, 5, 55, 5.5, "
                  "55.5, 3, "
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "aaaaaa");
        ASSERT_EQ(
            sql_case.output().schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.output().order_, "col1");
        ASSERT_EQ(sql_case.output().data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22\n0, 3, "
                  "55, 3.3, "
                  "33.3, 1, 333\n0, 4, 55, 4.4, 44.4, 2, 4444\n0, 5, 55, 5.5, "
                  "55.5, 3, "
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "aaaaaa");
    }
    {
        SQLCase& sql_case = cases[1];
        ASSERT_EQ(sql_case.id(), 2);
        ASSERT_EQ("batch", sql_case.mode());
        ASSERT_EQ("SELECT所有列使用resource输入", sql_case.desc());
        ASSERT_EQ(sql_case.inputs()[0].name_, "t1");
        ASSERT_EQ(sql_case.db(), "test");
        ASSERT_EQ(
            sql_case.inputs()[0].schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.inputs()[0].index_, "index2:col2:col5");
        ASSERT_EQ(sql_case.inputs()[0].data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22\n1, 3, "
                  "55, 3.3, "
                  "33.3, 1, 333\n1, 4, 55, 4.4, 44.4, 2, 4444\n2, 5, 55, 5.5, "
                  "55.5, 3, "
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "aaaaaa");
        ASSERT_EQ(
            sql_case.output().schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.output().order_, "");
        ASSERT_EQ(sql_case.output().data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22\n0, 3, "
                  "55, 3.3, "
                  "33.3, 1, 333\n0, 4, 55, 4.4, 44.4, 2, 4444\n0, 5, 55, 5.5, "
                  "55.5, 3, "
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "aaaaaa");
    }

    {
        SQLCase& sql_case = cases[2];
        ASSERT_EQ(sql_case.id(), 3);
        ASSERT_EQ("SELECT UDF", sql_case.desc());
        ASSERT_EQ("request", sql_case.mode());
        ASSERT_EQ(sql_case.inputs()[0].name_, "t1");
        ASSERT_EQ(sql_case.db(), "test");
        ASSERT_EQ(sql_case.sql_str(),
                  "%%fun\n"
                  "def test(a:i32,b:i32):i32\n"
                  "    c=a+b\n"
                  "    d=c+1\n"
                  "    return d\n"
                  "end\n"
                  "%%sql\n"
                  "SELECT col0, test(col1,col1), col2 , col6 FROM t1 limit 2;");
        ASSERT_EQ(
            sql_case.inputs()[0].schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.inputs()[0].index_,
                  "index1:col1|col2:col5, index2:col1:col5");
        ASSERT_EQ(sql_case.inputs()[0].data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22");
        ASSERT_EQ(sql_case.output().schema_,
                  "col0:string, col1:int32, col2:int16, col6:string");
        ASSERT_EQ(sql_case.output().order_, "");
        ASSERT_EQ(sql_case.output().data_, "0, 3, 5, 1\n0, 4, 5, 22");
    }

    {
        SQLCase& sql_case = cases[3];
        ASSERT_EQ(sql_case.id(), 4);
        ASSERT_EQ("简单INSERT", sql_case.desc());
        ASSERT_EQ(sql_case.db(), "test");
        ASSERT_EQ(
            sql_case.create_str(),
            "create table t1 (\n  col0 string not null,\n  col1 int not "
            "null,\n  col2 smallint not null,\n  col3 float not null,\n  col4 "
            "double not null,\n  col5 bigint not null,\n  col6 string not "
            "null,\n  index(name=index1, key=(col2), ts=col5)\n);");
        ASSERT_EQ(sql_case.insert_str(),
                  "insert into t1 values(\"hello\", 1, 2, 3.3f, 4.4, 5L, "
                  "\"world\");");
        ASSERT_EQ(sql_case.output().schema_,
                  "col0:string, col1:int32, col2:int16, col3:float, "
                  "col4:double, col5:int64, col6:string");
        ASSERT_EQ(sql_case.output().order_, "col1");
        ASSERT_EQ(sql_case.output().data_, "hello, 1, 2, 3.3, 4.4, 5, world");
    }
}
}  // namespace sqlcase
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
