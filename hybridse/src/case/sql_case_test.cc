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

#include "case/sql_case.h"
#include <vector>
#include "boost/filesystem/operations.hpp"
#include "gtest/gtest.h"
#include "yaml-cpp/yaml.h"
namespace hybridse {
namespace sqlcase {

class SqlCaseTest : public ::testing::Test {};

TEST_F(SqlCaseTest, ExtractTableDefTest) {
    type::TableDef table;
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->mutable_schema()->set_base_type(::hybridse::type::kVarchar);
        column->set_name("col0");
        column->set_is_not_null(false);
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->mutable_schema()->set_base_type(::hybridse::type::kInt32);
        column->set_name("col1");
        column->set_is_not_null(false);
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt16);
        column->mutable_schema()->set_base_type(::hybridse::type::kInt16);
        column->set_name("col2");
        column->set_is_not_null(false);
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kFloat);
        column->mutable_schema()->set_base_type(::hybridse::type::kFloat);
        column->set_name("col3");
        column->set_is_not_null(false);
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->mutable_schema()->set_base_type(::hybridse::type::kDouble);
        column->set_name("col4");
        column->set_is_not_null(false);
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->mutable_schema()->set_base_type(::hybridse::type::kInt64);
        column->set_name("col5");
        column->set_is_not_null(false);
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->mutable_schema()->set_base_type(::hybridse::type::kVarchar);
        column->set_name("col6");
        column->set_is_not_null(false);
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
        ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
        const std::string index_str = "index1:col1:col5";
        ASSERT_TRUE(SqlCase::ExtractIndex(index_str, output_table));
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
        ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
        const std::string index_str = "index1:col1|col2:col5";
        ASSERT_TRUE(SqlCase::ExtractIndex(index_str, output_table));
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
        ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
        const std::string index_str = "index1:col1:col5, index2:col1|col2:col5";
        ASSERT_TRUE(SqlCase::ExtractIndex(index_str, output_table));
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(exp_table.DebugString(), output_table.DebugString());
    }

    type::TableDef table2;
    table2.set_name("t1");
    table2.set_catalog("db");
    {
        ::hybridse::type::ColumnDef* column = table2.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->mutable_schema()->set_base_type(::hybridse::type::kVarchar);
        column->set_name("col0");
        column->set_is_not_null(false);
    }
    {
        ::hybridse::type::ColumnDef* column = table2.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->mutable_schema()->set_base_type(::hybridse::type::kInt32);
        column->set_name("col1");
        column->set_is_not_null(false);
    }
    {
        ::hybridse::type::ColumnDef* column = table2.add_columns();
        column->set_type(::hybridse::type::kTimestamp);
        column->mutable_schema()->set_base_type(::hybridse::type::kTimestamp);
        column->set_name("coltime");
        column->set_is_not_null(false);
    }

    // column with timestamp
    {
        const std::string schema_str =
            "col0:string, col1:int32, coltime:timestamp";
        type::TableDef output_table;
        output_table.set_name(table2.name());
        output_table.set_catalog(table2.catalog());
        ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
        ASSERT_EQ(table2.DebugString(), output_table.DebugString());
    }

    // Invalid Type
    {
        const std::string schema_str = "col0:str, col1:long";
        type::TableDef output_table;
        ASSERT_FALSE(SqlCase::ExtractSchema(schema_str, output_table));
    }

    {
        const std::string schema_str = "col0:integer, col1:int32";
        type::TableDef output_table;
        ASSERT_FALSE(SqlCase::ExtractSchema(schema_str, output_table));
    }

    // InValid Column
    {
        const std::string schema_str = "col0:str, col1:int32,,";
        type::TableDef output_table;
        ASSERT_FALSE(SqlCase::ExtractSchema(schema_str, output_table));
    }
}

TEST_F(SqlCaseTest, ExtractDataTest) {
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
        ASSERT_TRUE(SqlCase::ExtractSchema(data_schema_str, output_table));
        std::vector<hybridse::codec::Row> rows;
        ASSERT_TRUE(
            SqlCase::ExtractRows(output_table.columns(), data_str, rows));
        ASSERT_EQ(5u, rows.size());
        hybridse::codec::RowView row_view(output_table.columns());

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
TEST_F(SqlCaseTest, ExtractInsertSqlTest) {
    const std::string schema_str =
        "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
        "col5:int64, col6:string, col7:timestamp";
    type::TableDef output_table;
    ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
    {
        std::string row_str =
            "0, 1, 5, 1.1, 11.1, 1, 1, 1587647803000\n1, 10, 50, 10.1, 110.1, "
            "11, "
            "111, 1587647804000\n";
        std::string create_sql;
        ASSERT_TRUE(SqlCase::BuildInsertSqlFromData(output_table, row_str,
                                                    &create_sql));
        ASSERT_EQ(
            "Insert into  values\n('0', 1, 5, 1.1, 11.1, 1, '1', "
            "1587647803000),\n('1', 10, 50, 10.1, 110.1, 11, '111', "
            "1587647804000);",
            create_sql);
    }
    {
        std::vector<std::vector<std::string>> rows;
        rows.push_back(std::vector<std::string>{"0", "1", "5", "1.1", "11.1",
                                                "1", "1", "1587647803000"});
        rows.push_back(std::vector<std::string>{
            "1", "10", "50", "10.1", "110.1", "11", "111", "1587647804000"});
        std::string create_sql;
        ASSERT_TRUE(
            SqlCase::BuildInsertSqlFromRows(output_table, rows, &create_sql));
        ASSERT_EQ(
            "Insert into  values\n('0', 1, 5, 1.1, 11.1, 1, '1', "
            "1587647803000),\n('1', 10, 50, 10.1, 110.1, 11, '111', "
            "1587647804000);",
            create_sql);
    }
}
TEST_F(SqlCaseTest, ExtractRowTest) {
    const std::string schema_str =
        "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
        "col5:int64, col6:string, col7:timestamp, col8:date";

    std::string row_str =
        "0, 1, 5, 1.1, 11.1, 1, 1, 1587647803000, 2020-05-28\n";
    type::TableDef output_table;
    ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
    std::vector<hybridse::codec::Row> rows;
    int8_t* row_ptr = nullptr;
    int32_t row_size = 0;
    ASSERT_TRUE(SqlCase::ExtractRow(output_table.columns(), row_str, &row_ptr,
                                    &row_size));
    hybridse::codec::RowView row_view(output_table.columns());

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
        int32_t year;
        int32_t month;
        int32_t day;
        ASSERT_EQ(0, row_view.GetDate(8, &year, &month, &day));
        ASSERT_EQ(2020, year);
        ASSERT_EQ(5, month);
        ASSERT_EQ(28, day);
    }
}

TEST_F(SqlCaseTest, ExtractRowFromStringListTest) {
    const std::vector<std::string> columns = {
        "col0:string", "col1:int32",     "col2:int16",
        "col3:float",  "col4:double",    "col5:int64",
        "col6:string", "col7:timestamp", "col8:date"};

    std::vector<std::string> str_list = {
        "0", "1", "5", "1.1", "11.1", "1", "1", "1587647803000", "2020-05-28"};
    type::TableDef output_table;
    ASSERT_TRUE(SqlCase::ExtractSchema(columns, output_table));
    std::vector<hybridse::codec::Row> rows;
    int8_t* row_ptr = nullptr;
    int32_t row_size = 0;
    ASSERT_TRUE(SqlCase::ExtractRow(output_table.columns(), str_list, &row_ptr,
                                    &row_size));
    hybridse::codec::RowView row_view(output_table.columns());

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
        int32_t year;
        int32_t month;
        int32_t day;
        ASSERT_EQ(0, row_view.GetDate(8, &year, &month, &day));
        ASSERT_EQ(2020, year);
        ASSERT_EQ(5, month);
        ASSERT_EQ(28, day);
    }
}
TEST_F(SqlCaseTest, ExtractColumnsTest) {
    const std::vector<std::string> columns = {"col0 string", "col1 int32"};
    type::TableDef output_table;
    ASSERT_TRUE(SqlCase::ExtractSchema(columns, output_table));
    ASSERT_EQ(2, output_table.columns_size());
    ASSERT_EQ("col0", output_table.columns(0).name());
    ASSERT_EQ(hybridse::type::kVarchar, output_table.columns(0).type());
    ASSERT_EQ("col1", output_table.columns(1).name());
    ASSERT_EQ(hybridse::type::kInt32, output_table.columns(1).type());
}
TEST_F(SqlCaseTest, ExtractRowWithNullTest) {
    const std::string schema_str = "col0:string, col1:int32";
    std::string row_str = "0, NULL\n";
    type::TableDef output_table;
    ASSERT_TRUE(SqlCase::ExtractSchema(schema_str, output_table));
    std::vector<hybridse::codec::Row> rows;
    int8_t* row_ptr = nullptr;
    int32_t row_size = 0;
    ASSERT_TRUE(SqlCase::ExtractRow(output_table.columns(), row_str, &row_ptr,
                                    &row_size));
    hybridse::codec::RowView row_view(output_table.columns());

    {
        row_view.Reset(row_ptr);
        ASSERT_EQ("0", row_view.GetAsString(0));
        ASSERT_TRUE(row_view.IsNULL(1));
    }
}
TEST_F(SqlCaseTest, ExtractSqlCase) {
    SqlCase sql_case;

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
        SqlCase::TableInfo table_data = {.name_ = "",
                                         .schema_ = schema,
                                         .index_ = index,
                                         .data_ = data,
                                         .order_ = order};
        sql_case.AddInput(table_data);
    }

    {
        SqlCase::ExpectInfo table_data;
        table_data.schema_ =
            "f0:string, f1:float, f2:double, f3:int16, f4:int32, f5:int64, "
            "f6:timestamp";
        table_data.data_ =
            "A, 1.1, 2.2, 3, 4, 5, 1587647803000\n"
            "BB, 11.1, 22.2, 30, 40, 50, 1587647804000";
        sql_case.set_expect(table_data);
    }

    // Check Data Schema
    {
        type::TableDef output_table;
        ASSERT_TRUE(sql_case.ExtractInputTableDef(output_table));
        type::TableDef table;
        table.set_name("t1");
        table.set_catalog("db");
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kVarchar);
            column->mutable_schema()->set_base_type(::hybridse::type::kVarchar);
            column->set_name("col0");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->mutable_schema()->set_base_type(::hybridse::type::kInt32);
            column->set_name("col1");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kInt16);
            column->mutable_schema()->set_base_type(::hybridse::type::kInt16);
            column->set_name("col2");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kFloat);
            column->mutable_schema()->set_base_type(::hybridse::type::kFloat);
            column->set_name("col3");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kDouble);
            column->mutable_schema()->set_base_type(::hybridse::type::kDouble);
            column->set_name("col4");
            column->set_is_not_null(false);
        }

        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kInt64);
            column->mutable_schema()->set_base_type(::hybridse::type::kInt64);
            column->set_name("col5");
            column->set_is_not_null(false);
        }

        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kVarchar);
            column->mutable_schema()->set_base_type(::hybridse::type::kVarchar);
            column->set_name("col6");
            column->set_is_not_null(false);
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
                SqlCase::BuildCreateSqlFromSchema(output_table, &create_sql));
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
                ") options(partitionnum=1, replicanum=1);",
                create_sql);
        }
    }

    // Check Data
    {
        type::TableDef output_table;
        ASSERT_TRUE(sql_case.ExtractInputTableDef(output_table));
        std::vector<hybridse::codec::Row> rows;
        ASSERT_TRUE(sql_case.ExtractInputData(rows, 0, output_table.columns()));
        ASSERT_EQ(5u, rows.size());
        hybridse::codec::RowView row_view(output_table.columns());

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

    // Check Insert SQL List
    {
        std::string create_sql;
        std::vector<std::string> sql_list;
        sql_case.BuildInsertSqlListFromInput(0, &sql_list);
        ASSERT_EQ(5u, sql_list.size());
        ASSERT_EQ("Insert into  values\n('0', 1, 5, 1.1, 11.1, 1, '1');",
                  sql_list[0]);
        ASSERT_EQ("Insert into  values\n('0', 2, 5, 2.2, 22.2, 2, '22');",
                  sql_list[1]);
        ASSERT_EQ("Insert into  values\n('0', 3, 55, 3.3, 33.3, 1, '333');",
                  sql_list[2]);
        ASSERT_EQ("Insert into  values\n('0', 4, 55, 4.4, 44.4, 2, '4444');",
                  sql_list[3]);
        ASSERT_EQ(
            "Insert into  values\n('0', 5, 55, 5.5, 55.5, 3, "
            "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a');",
            sql_list[4]);
    }

    // Check Data Schema
    {
        type::TableDef output_table;
        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
        type::TableDef table;
        table.set_name("t1");
        table.set_catalog("db");
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kVarchar);
            column->mutable_schema()->set_base_type(::hybridse::type::kVarchar);
            column->set_name("f0");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kFloat);
            column->mutable_schema()->set_base_type(::hybridse::type::kFloat);
            column->set_name("f1");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kDouble);
            column->mutable_schema()->set_base_type(::hybridse::type::kDouble);
            column->set_name("f2");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kInt16);
            column->mutable_schema()->set_base_type(::hybridse::type::kInt16);
            column->set_name("f3");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->mutable_schema()->set_base_type(::hybridse::type::kInt32);
            column->set_name("f4");
            column->set_is_not_null(false);
        }
        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kInt64);
            column->mutable_schema()->set_base_type(::hybridse::type::kInt64);
            column->set_name("f5");
            column->set_is_not_null(false);
        }

        {
            ::hybridse::type::ColumnDef* column = table.add_columns();
            column->set_type(::hybridse::type::kTimestamp);
            column->mutable_schema()->set_base_type(::hybridse::type::kTimestamp);
            column->set_name("f6");
            column->set_is_not_null(false);
        }
        output_table.set_name(table.name());
        output_table.set_catalog(table.catalog());
        ASSERT_EQ(table.DebugString(), output_table.DebugString());
    }

    // Check Data
    {
        type::TableDef output_table;
        std::vector<hybridse::codec::Row> rows;
        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
        ASSERT_EQ(2u, rows.size());
        sql_case.ExtractOutputSchema(output_table);
        hybridse::codec::RowView row_view(output_table.columns());

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

TEST_F(SqlCaseTest, ExtractYamlSqlCase) {
    std::string hybridse_dir = hybridse::sqlcase::FindSqlCaseBaseDirPath();
    std::string case_path = "cases/yaml/demo.yaml";
    std::vector<SqlCase> cases;

    ASSERT_TRUE(hybridse::sqlcase::SqlCase::CreateSqlCasesFromYaml(
        hybridse_dir, case_path, cases));
    ASSERT_EQ(5, cases.size());
    {
        SqlCase& sql_case = cases[0];
        ASSERT_EQ(sql_case.id(), "1");
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
            sql_case.expect().schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.expect().order_, "col1");
        ASSERT_EQ(sql_case.expect().data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22\n0, 3, "
                  "55, 3.3, "
                  "33.3, 1, 333\n0, 4, 55, 4.4, 44.4, 2, 4444\n0, 5, 55, 5.5, "
                  "55.5, 3, "
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "aaaaaa");
    }
    {
        SqlCase& sql_case = cases[1];
        ASSERT_EQ(sql_case.id(), "2");
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
            sql_case.expect().schema_,
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string");
        ASSERT_EQ(sql_case.expect().order_, "");
        ASSERT_EQ(sql_case.expect().data_,
                  "0, 1, 5, 1.1, 11.1, 1, 1\n0, 2, 5, 2.2, 22.2, 2, 22\n0, 3, "
                  "55, 3.3, "
                  "33.3, 1, 333\n0, 4, 55, 4.4, 44.4, 2, 4444\n0, 5, 55, 5.5, "
                  "55.5, 3, "
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "aaaaaa");
    }

    {
        SqlCase& sql_case = cases[2];
        ASSERT_EQ(sql_case.id(), "3");
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
        ASSERT_EQ(sql_case.expect().schema_,
                  "col0:string, col1:int32, col2:int16, col6:string");
        ASSERT_EQ(sql_case.expect().order_, "");
        ASSERT_EQ(sql_case.expect().data_, "0, 3, 5, 1\n0, 4, 5, 22");
    }

    {
        SqlCase& sql_case = cases[3];
        ASSERT_EQ(sql_case.id(), "4");
        ASSERT_EQ("简单INSERT", sql_case.desc());
        ASSERT_EQ(sql_case.db(), "test");
        ASSERT_EQ(2, sql_case.inputs().size());
        ASSERT_EQ(sql_case.inputs()[0].create_,
                  "create table t1 (\n"
                  "col0 string not null,\n"
                  "col1 int not null,\n"
                  "col2 smallint not null,\n"
                  "col3 float not null,\n"
                  "col4 double not null,\n"
                  "col5 bigint not null,\n"
                  "col6 string not null,\n"
                  "index(name=index1, key=(col2), ts=col5)\n"
                  ");");
        ASSERT_EQ(sql_case.inputs()[1].create_,
                  "create table t2 (\n"
                  "c1 string not null,\n"
                  "c2 bigint not null,\n"
                  "index(name=index2, key=(c1), ts=c2)\n"
                  ");");
        ASSERT_EQ(sql_case.inputs()[0].insert_,
                  "insert into t1 values\n(\"hello\", 1, 2, 3.3f, 4.4, 5L, "
                  "\"world\");");

        ASSERT_EQ(sql_case.inputs()[1].insert_,
                  "insert into t2 values"
                  "\n(\"hello\", 1),"
                  "\n(\"world\", 2);");
        ASSERT_EQ(sql_case.expect().schema_,
                  "col0:string, col1:int32, col2:int16, col3:float, "
                  "col4:double, col5:int64, col6:string");
        ASSERT_EQ(sql_case.expect().order_, "col1");
        ASSERT_EQ(sql_case.expect().data_, "hello, 1, 2, 3.3, 4.4, 5, world");
    }
    // 简单INSERT with inserts
    {
        SqlCase& sql_case = cases[4];
        ASSERT_EQ(sql_case.id(), "5");
        ASSERT_EQ("简单INSERT with inserts", sql_case.desc());
        ASSERT_EQ(sql_case.db(), "test");
        ASSERT_EQ(2, sql_case.inputs().size());
        ASSERT_EQ(sql_case.inputs()[0].create_,
                  "create table t1 (\n"
                  "col0 string not null,\n"
                  "col1 int not null,\n"
                  "col2 smallint not null,\n"
                  "col3 float not null,\n"
                  "col4 double not null,\n"
                  "col5 bigint not null,\n"
                  "col6 string not null,\n"
                  "index(name=index1, key=(col2), ts=col5)\n"
                  ");");
        ASSERT_EQ(sql_case.inputs()[1].create_,
                  "create table t2 (\n"
                  "c1 string not null,\n"
                  "c2 bigint not null,\n"
                  "index(name=index2, key=(c1), ts=c2)\n"
                  ");");
        ASSERT_EQ(sql_case.inputs()[0].inserts_,
                  std::vector<std::string>(
                      {"insert into t1 values\n"
                       "(\"hello\", 1, 2, 3.3f, 4.4, 5L, \"world\");\n",
                       "insert into t1 values (\"happy\", 10, 20, 30.3f, 40.4, "
                       "50L, \"newyear\");"}));
        ASSERT_EQ(sql_case.expect().schema_,
                  "col0:string, col1:int32, col2:int16, col3:float, "
                  "col4:double, col5:int64, col6:string");
        ASSERT_EQ(sql_case.expect().order_, "col1");
        ASSERT_EQ(sql_case.expect().data_, "hello, 1, 2, 3.3, 4.4, 5, world");
    }
}

TEST_F(SqlCaseTest, ExtractYamlSqlCase2) {
    std::string hybridse_dir = hybridse::sqlcase::FindSqlCaseBaseDirPath();
    std::string case_path = "cases/yaml/rtidb_demo.yaml";
    std::vector<SqlCase> cases;

    ASSERT_TRUE(hybridse::sqlcase::SqlCase::CreateSqlCasesFromYaml(
        hybridse_dir, case_path, cases));
    ASSERT_EQ(4, cases.size());
    {
        SqlCase& sql_case = cases[0];
        ASSERT_EQ(sql_case.id(), "0");
        ASSERT_EQ("正常拼接", sql_case.desc());
        ASSERT_EQ(2u, sql_case.inputs().size());
        ASSERT_EQ(sql_case.db(), "test_zw");
        {
            auto input = sql_case.inputs()[0];
            ASSERT_EQ(input.name_, "");
            ASSERT_EQ(input.schema_, "");
            std::vector<std::string> columns = {"c1 string", "c2 int",
                                                "c3 bigint", "c4 timestamp"};
            ASSERT_EQ(input.columns_, columns);
            std::vector<std::string> indexs = {"index1:c1:c4"};
            ASSERT_EQ(input.index_, "");
            ASSERT_EQ(input.indexs_, indexs);
            std::vector<std::vector<std::string>> rows = {
                {"aa", "2", "3", "1590738989000L"},
                {"bb", "21", "31", "1590738990000L"},
                {"cc", "41", "51", "1590738991000L"}};
            ASSERT_EQ(sql_case.inputs()[0].rows_, rows);
        }
        {
            auto input = sql_case.inputs()[1];
            ASSERT_EQ(input.name_, "");
            ASSERT_EQ(input.schema_, "");
            std::vector<std::string> columns = {"col1 string", "col2 int",
                                                "c3 bigint", "c4 timestamp"};
            ASSERT_EQ(input.columns_, columns);
            std::vector<std::string> indexs = {"index1:col1:c4"};
            ASSERT_EQ(input.index_, "");
            ASSERT_EQ(input.indexs_, indexs);
            std::vector<std::vector<std::string>> rows = {
                {"aa", "2", "13", "1590738989000L"},
                {"bb", "21", "131", "1590738990000L"},
                {"cc", "41", "151", "1590738992000L"},
            };
            ASSERT_EQ(input.rows_, rows);
        }
        std::vector<std::string> expect_columns = {"c1 string", "c2 int",
                                                   "c3 bigint"};
        std::vector<std::vector<std::string>> expect_rows = {
            {"aa", "2", "13"}, {"cc", "41", "151"}, {"bb", "21", "131"}};

        ASSERT_EQ(sql_case.expect().schema_, "");
        ASSERT_EQ(sql_case.expect().data_, "");
        ASSERT_EQ(sql_case.expect().order_, "c1");
        ASSERT_EQ(sql_case.expect().count_, 3);
        ASSERT_EQ(sql_case.expect().columns_, expect_columns);
        ASSERT_EQ(sql_case.expect().rows_, expect_rows);
        ASSERT_TRUE(sql_case.expect().success_);
    }

    {
        SqlCase& sql_case = cases[1];
        ASSERT_EQ(sql_case.id(), "1");
        ASSERT_EQ("普通select", sql_case.desc());
        ASSERT_EQ(1u, sql_case.inputs().size());
        ASSERT_EQ(sql_case.db(), "test_zw");
        {
            auto input = sql_case.inputs()[0];
            ASSERT_EQ(input.name_, "");
            ASSERT_EQ(input.schema_, "");
            std::vector<std::string> columns = {"c1 string", "c2 int",
                                                "c3 bigint", "c4 timestamp"};
            ASSERT_EQ(input.columns_, columns);
            std::vector<std::string> indexs = {"index1:c1:c4"};
            ASSERT_EQ(input.index_, "");
            ASSERT_EQ(input.indexs_, indexs);
            std::vector<std::vector<std::string>> rows = {
                {"aa", "null", "3", "1590738989000L"}};
            ASSERT_EQ(sql_case.inputs()[0].rows_, rows);
        }
        std::vector<std::string> expect_columns = {"c1 string", "c2 int"};
        std::vector<std::vector<std::string>> expect_rows = {{"aa", "null"}};

        ASSERT_EQ(sql_case.expect().schema_, "");
        ASSERT_EQ(sql_case.expect().data_, "");
        ASSERT_EQ(sql_case.expect().count_, -1);
        ASSERT_EQ(sql_case.expect().columns_, expect_columns);
        ASSERT_EQ(sql_case.expect().rows_, expect_rows);
        ASSERT_TRUE(sql_case.expect().success_);
    }

    {
        SqlCase& sql_case = cases[2];
        ASSERT_EQ(sql_case.id(), "2");
        ASSERT_EQ("普通select,Sucess false", sql_case.desc());
        ASSERT_EQ(1u, sql_case.inputs().size());
        ASSERT_EQ(sql_case.db(), "test_zw");
        {
            auto input = sql_case.inputs()[0];
            ASSERT_EQ(input.name_, "");
            ASSERT_EQ(input.schema_, "");
            std::vector<std::string> columns = {"c1 string", "c2 int",
                                                "c3 bigint", "c4 timestamp"};
            ASSERT_EQ(input.columns_, columns);
            std::vector<std::string> indexs = {"index1:c1:c4"};
            ASSERT_EQ(input.index_, "");
            ASSERT_EQ(input.indexs_, indexs);
            std::vector<std::vector<std::string>> rows = {
                {"aa", "null", "3", "1590738989000L"}};
            ASSERT_EQ(sql_case.inputs()[0].rows_, rows);
        }
        std::vector<std::string> expect_columns = {"c1 string", "c2 int"};
        std::vector<std::vector<std::string>> expect_rows = {{"aa", "null"}};

        ASSERT_EQ(sql_case.expect().schema_, "");
        ASSERT_EQ(sql_case.expect().data_, "");
        ASSERT_EQ(sql_case.expect().count_, -1);
        ASSERT_TRUE(sql_case.expect().columns_.empty());
        ASSERT_TRUE(sql_case.expect().rows_.empty());
        ASSERT_FALSE(sql_case.expect().success_);
        ASSERT_EQ(1000, sql_case.expect().code_);
        ASSERT_EQ("unknow error", sql_case.expect().msg_);
    }
    {
        SqlCase& sql_case = cases[3];
        ASSERT_EQ(sql_case.id(), "3");
        ASSERT_EQ("普通select with placeholders", sql_case.desc());
        ASSERT_EQ(1u, sql_case.inputs().size());
        ASSERT_EQ(sql_case.db(), "test_zw");
        {
            auto input = sql_case.inputs()[0];
            ASSERT_EQ(input.name_, "");
            ASSERT_EQ(input.schema_, "");
            std::vector<std::string> columns = {"c1 string", "c2 int",
                                                "c3 bigint", "c4 timestamp"};
            ASSERT_EQ(input.columns_, columns);
            std::vector<std::string> indexs = {"index1:c1:c4"};
            ASSERT_EQ(input.index_, "");
            ASSERT_EQ(input.indexs_, indexs);
            std::vector<std::vector<std::string>> rows = {
                {"aa", "null", "3", "1590738989000L"}};
            ASSERT_EQ(sql_case.inputs()[0].rows_, rows);
        }
        std::vector<std::string> expect_paramters_columns = {"1 string", "2 int", "3 double"};
        std::vector<std::vector<std::string>> expect_parameters_rows = {{"aa", "1", "3.1"}};
        ASSERT_EQ(sql_case.parameters().columns_, expect_paramters_columns);
        ASSERT_EQ(sql_case.parameters().rows_, expect_parameters_rows);
        ASSERT_EQ(sql_case.parameters().schema_, "");
        ASSERT_EQ(sql_case.parameters().data_, "");

        std::vector<std::string> expect_columns = {"c1 string", "c2 int"};
        std::vector<std::vector<std::string>> expect_rows = {{"aa", "null"}};
        ASSERT_EQ(sql_case.expect().schema_, "");
        ASSERT_EQ(sql_case.expect().data_, "");
        ASSERT_EQ(sql_case.expect().count_, -1);
        ASSERT_EQ(sql_case.expect().columns_, expect_columns);
        ASSERT_EQ(sql_case.expect().rows_, expect_rows);
        ASSERT_TRUE(sql_case.expect().success_);
    }
}

TEST_F(SqlCaseTest, ExtractYamlWithDebugSqlCase) {
    std::string hybridse_dir = hybridse::sqlcase::FindSqlCaseBaseDirPath();
    std::string case_path = "cases/yaml/rtidb_demo_debug.yaml";
    std::vector<SqlCase> cases;

    ASSERT_TRUE(hybridse::sqlcase::SqlCase::CreateSqlCasesFromYaml(
        hybridse_dir, case_path, cases));
    ASSERT_EQ(1, cases.size());
    {
        SqlCase& sql_case = cases[0];
        ASSERT_EQ(sql_case.id(), "1");
        ASSERT_EQ("普通select", sql_case.desc());
        ASSERT_EQ(1u, sql_case.inputs().size());
        ASSERT_EQ(sql_case.db(), "test_zw");
    }
}

TEST_F(SqlCaseTest, InitCasesTest) {
    std::string case_path = "cases/yaml/demo.yaml";
    {
        std::vector<SqlCase> cases = InitCases(case_path);
        ASSERT_EQ(5u, cases.size());
    }
    {
        std::vector<SqlCase> cases = InitCases(case_path, std::vector<std::string>({"request"}));
        ASSERT_EQ(4u, cases.size());
    }
}

// dataProvider size = 1
TEST_F(SqlCaseTest, DataProviderSize1Test) {
    std::string case_path = "cases/yaml/demo_data_provider_sz1.yaml";
    auto cases = InitCases(case_path);
    ASSERT_EQ(5u, cases.size());
    auto& case2 = cases.at(1);
    // second case is MOD
    EXPECT_STREQ("select t1.c2 MOD t2.c2 as b2 from t1 last join t2 ORDER BY t2.c7 on t1.id=t2.id;",
                 case2.sql_str().c_str());

    ASSERT_EQ(1u, case2.expect().rows_.size());
    ASSERT_EQ(1u, case2.expect().rows_.at(0).size());
    EXPECT_STREQ("1", case2.expect().rows_.at(0).at(0).c_str());
}

// dataProvider size > 1
TEST_F(SqlCaseTest, DataProviderSize2Test) {
    std::string case_path = "cases/yaml/demo_data_provider_sz2.yaml";
    auto cases = InitCases(case_path);
    ASSERT_EQ(6u, cases.size());

    auto& case2 = cases.at(1);
    EXPECT_STREQ("select 1 NOT IN (1, 10) as col1 from t1;", case2.sql_str().c_str());
    ASSERT_EQ(1u, case2.expect().rows_.size());
    ASSERT_EQ(1u, case2.expect().rows_.at(0).size());
    EXPECT_STREQ("false", case2.expect().rows_.at(0).at(0).c_str());

    auto& case5 = cases.at(4);
    EXPECT_STREQ("select NULL IN (1, 10) as col1 from t1;", case5.sql_str().c_str());
    ASSERT_EQ(1u, case5.expect().rows_.size());
    ASSERT_EQ(1u, case5.expect().rows_.at(0).size());
    // NOTE: we parse NULL as 'null'
    EXPECT_STRCASEEQ("NULL", case5.expect().rows_.at(0).at(0).c_str());
}

// dataProvider is two dimension sequence
TEST_F(SqlCaseTest, DataProviderSize2SeqTest) {
    std::string case_path = "cases/yaml/demo_data_provider_sz2_sequence.yaml";
    auto cases = InitCases(case_path);
    ASSERT_EQ(6u, cases.size());

    auto& case2 = cases.at(1);
    EXPECT_STREQ("select 1 NOT IN (1, 10) as col1 from t1;", case2.sql_str().c_str());
    ASSERT_EQ(1u, case2.expect().rows_.size());
    ASSERT_EQ(1u, case2.expect().rows_.at(0).size());
    EXPECT_STREQ("false", case2.expect().rows_.at(0).at(0).c_str());

    auto& case5 = cases.at(4);
    EXPECT_STREQ("select NULL IN (1, 10) as col1 from t1;", case5.sql_str().c_str());
    ASSERT_EQ(1u, case5.expect().rows_.size());
    ASSERT_EQ(1u, case5.expect().rows_.at(0).size());
    EXPECT_STREQ("wrong_answer", case5.expect().rows_.at(0).at(0).c_str());
}

// dataProvider is 3 dimension map mixed of map and sequence
TEST_F(SqlCaseTest, DataProviderSize3MixedTest) {
    std::string case_path = "cases/yaml/demo_data_provider_sz3_mixed.yaml";
    auto cases = InitCases(case_path);
    ASSERT_EQ(8u, cases.size());

    auto& case2 = cases.at(1);
    EXPECT_STREQ("select 1 IN (1, 10) as col1 from t1;", case2.sql_str().c_str());
    ASSERT_EQ(1u, case2.expect().rows_.size());
    ASSERT_EQ(1u, case2.expect().rows_.at(0).size());
    EXPECT_STREQ("true", case2.expect().rows_.at(0).at(0).c_str());

    auto& case5 = cases.at(4);
    EXPECT_STREQ("select 2 IN (1, 9) as col1 from t1;", case5.sql_str().c_str());
    ASSERT_EQ(1u, case5.expect().rows_.size());
    ASSERT_EQ(2u, case5.expect().rows_.at(0).size());
    EXPECT_STREQ("hello", case5.expect().rows_.at(0).at(0).c_str());
}

TEST_F(SqlCaseTest, ExpectProviderDefaultTest) {
    std::string case_path = "cases/yaml/demo_expect_provider_sz2_default.yaml";
    auto cases = InitCases(case_path);
    ASSERT_EQ(6u, cases.size());

    auto& case0 = cases.at(0);
    ASSERT_EQ(1u, case0.expect().rows_.size());
    ASSERT_EQ(1u, case0.expect().rows_.at(0).size());
    EXPECT_STREQ("true", case0.expect().rows_.at(0).at(0).c_str());

    auto& case3 = cases.at(2);
    ASSERT_EQ(1u, case3.expect().rows_.size());
    ASSERT_EQ(2u, case3.expect().rows_.at(0).size());
    EXPECT_STREQ("false", case3.expect().rows_.at(0).at(0).c_str());
}

TEST_F(SqlCaseTest, EmptyExpectProviderTest) {
    std::string case_path = "cases/yaml/demo_empty_expect_provider.yaml";
    auto cases = InitCases(case_path);
    ASSERT_EQ(10u, cases.size());

    auto& case3 = cases.at(2);
    EXPECT_STREQ("select t1.c2 MOD t2.c2 as b2 from t1 last join t2 ORDER BY a on t1.id=t2.id;",
                 case3.sql_str().c_str());
    EXPECT_EQ(false, case3.expect().success_);
}

TEST_F(SqlCaseTest, BuildCreateSpSqlFromInputTest) {
    {
        SqlCase::TableInfo input;
        input.columns_ = {"c1 string", "c2 int", "c3 bigint", "c4 timestamp"};
        SqlCase sql_case;
        sql_case.inputs_.push_back(input);
        sql_case.sp_name_ = "sp";
        std::string sql = " select c1, c2, c3, c4 from t1   ";
        std::string sp_sql = "";
        auto s = sql_case.BuildCreateSpSql(sql, {}, 0);
        ASSERT_TRUE(s.ok()) << s.status();
        ASSERT_EQ(R"s(CREATE PROCEDURE sp (
c1 string,
c2 int,
c3 bigint,
c4 timestamp)
BEGIN
select c1, c2, c3, c4 from t1;
END;)s",
                  s.value());
    }

    // create procedure with common idx
    {
        SqlCase::TableInfo input;
        input.columns_ = {"c1 string", "c2 int", "c3 bigint", "c4 timestamp"};
        SqlCase sql_case;
        sql_case.sp_name_ = "sp1";
        sql_case.inputs_.push_back(input);
        std::string sql = "select c1, c2, c3, c4 from t1;";
        std::string sp_sql = "";
        auto s = sql_case.BuildCreateSpSql(sql, {0, 1, 3}, 0);
        ASSERT_TRUE(s.ok()) << s.status();
        ASSERT_EQ(R"s(CREATE PROCEDURE sp1 (
const c1 string,
const c2 int,
c3 bigint,
const c4 timestamp)
BEGIN
select c1, c2, c3, c4 from t1;
END;)s",
                  s.value());
    }
}
}  // namespace sqlcase
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
