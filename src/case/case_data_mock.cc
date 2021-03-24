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
#include "case/case_data_mock.h"
namespace hybridse {
namespace sqlcase {
using hybridse::codec::Row;

bool CaseDataMock::LoadResource(const std::string& resource_path,
                                type::TableDef& table_def,  // NOLINT
                                std::vector<Row>& rows) {   // NOLINT
    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            hybridse::sqlcase::FindSQLCaseBaseDirPath(), resource_path, table_def,
            rows)) {
        return false;
    }
    return true;
}
void CaseDataMock::BuildOnePkTableData(type::TableDef& table_def,  // NOLINT
                                       std::vector<Row>& buffer,   // NOLINT
                                       int64_t data_size) {
    ::hybridse::sqlcase::Repeater<std::string> col0(
        std::vector<std::string>({"hello"}));
    IntRepeater<int32_t> col1;
    col1.Range(1, 100, 1);
    IntRepeater<int16_t> col2;
    col2.Range(1u, 100u, 2);
    RealRepeater<float> col3;
    col3.Range(1.0, 100.0, 3.0f);
    RealRepeater<double> col4;
    col4.Range(100.0, 10000.0, 10.0);
    IntRepeater<int64_t> col5;
    col5.Range(1576571615000 - 100000000, 1576571615000, 1000);
    Repeater<std::string> col6({"astring", "bstring", "cstring", "dstring",
                                "estring", "fstring", "gstring", "hstring"});

    CaseSchemaMock::BuildTableDef(table_def);
    for (int i = 0; i < data_size; ++i) {
        std::string str1 = col0.GetValue();
        std::string str2 = col6.GetValue();
        codec::RowBuilder builder(table_def.columns());
        uint32_t total_size = builder.CalTotalLength(str1.size() + str2.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str1.c_str(), str1.size());
        builder.AppendInt32(col1.GetValue());
        builder.AppendInt16(col2.GetValue());
        builder.AppendFloat(col3.GetValue());
        builder.AppendDouble(col4.GetValue());
        builder.AppendInt64(col5.GetValue());
        builder.AppendString(str2.c_str(), str2.size());
        buffer.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
}

void CaseDataMock::BuildTableAndData(type::TableDef& table_def,  // NOLINT
                                     std::vector<Row>& buffer,   // NOLINT
                                     int64_t data_size) {
    CaseSchemaMock::BuildTableDef(table_def);
    for (int i = 0; i < data_size; ++i) {
        std::string str1 = "hello";
        std::string str2 = "astring";
        codec::RowBuilder builder(table_def.columns());
        uint32_t total_size = builder.CalTotalLength(str1.size() + str2.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str1.c_str(), str1.size());
        builder.AppendInt32(1);
        builder.AppendInt16(2);
        builder.AppendFloat(3.0);
        builder.AppendDouble(4.0);
        builder.AppendInt64(1576571615000 - i);
        builder.AppendString(str2.c_str(), str2.size());
        buffer.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
}
void CaseSchemaMock::BuildTableDef(
    ::hybridse::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->set_name("col1");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt16);
        column->set_name("col2");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kFloat);
        column->set_name("col3");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->set_name("col4");
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("col5");
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("col6");
    }
}
}  // namespace sqlcase
}  // namespace hybridse
