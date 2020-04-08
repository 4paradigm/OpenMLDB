/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_bm_case.cc
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/
#include "bm/udf_bm_case.h"
#include <memory>
#include <string>
#include <vector>
#include "bm/base_bm.h"
#include "codec/row_codec.h"
#include "codegen/ir_base_builder.h"
#include "codegen/window_ir_builder.h"
#include "gtest/gtest.h"
#include "udf/udf.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace bm {
using base::Slice;
using codec::ColumnImpl;
using vm::MemTableHandler;
static void BuildData(type::TableDef& table_def,    // NOLINT
                      vm::MemTableHandler& window,  // NOLINT
                      int64_t data_size);
static void BuildTableDef(::fesql::type::TableDef& table) {  // NOLINT
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
}

static void DeleteData(vm::MemTableHandler& window);  // NOLINT

static void DeleteData(vm::MemTableHandler& window) {  // NOLINT
    auto iter = window.GetIterator();
    while (iter->Valid()) {
        delete iter->GetValue().buf();
        iter->Next();
    }
}
static void BuildData(type::TableDef& table_def,    // NOLINT
                      vm::MemTableHandler& window,  // NOLINT
                      int64_t data_size) {
    ::fesql::bm::Repeater<std::string> col0(
        std::vector<std::string>({"hello"}));
    ::fesql::bm::IntRepeater<int32_t> col1;
    col1.Range(1, 100, 1);
    ::fesql::bm::IntRepeater<int16_t> col2;
    col2.Range(1u, 100u, 2);
    ::fesql::bm::RealRepeater<float> col3;
    col3.Range(1.0, 100.0, 3.0f);
    ::fesql::bm::RealRepeater<double> col4;
    col4.Range(100.0, 10000.0, 10.0);
    ::fesql::bm::IntRepeater<int64_t> col5;
    col5.Range(1576571615000 - 100000000, 1576571615000, 1000);
    ::fesql::bm::Repeater<std::string> col6({"astring", "bstring", "cstring",
                                             "dstring", "estring", "fstring",
                                             "gstring", "hstring"});

    BuildTableDef(table_def);
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
        window.AddRow(Slice(ptr, total_size));
    }
}

int32_t RunCopyTable(MemTableHandler window, type::TableDef table_def);
void CopyMemTable(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunCopyTable(window, table_def));
            }
            DeleteData(window);
            break;
        }
        case TEST: {
            if (data_size == RunCopyTable(window, table_def)) {
                DeleteData(window);
            } else {
                DeleteData(window);
                FAIL();
            }
        }
    }
}

int64_t RunCopyArrayList(MemTableHandler window, type::TableDef table_def);
void CopyArrayList(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunCopyArrayList(window, table_def));
            }
            DeleteData(window);
            break;
        }
        case TEST: {
            if (data_size == RunCopyArrayList(window, table_def)) {
                DeleteData(window);
            } else {
                DeleteData(window);
                FAIL();
            }
        }
    }
}
int64_t RunCopyArrayList(MemTableHandler window, type::TableDef table_def) {
    std::vector<Slice> window_table;
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        window_table.push_back(from_iter->GetValue());
        from_iter->Next();
    }
    return window_table.size();
}

int32_t RunCopyTable(MemTableHandler window, type::TableDef table_def) {
    auto window_table = std::shared_ptr<vm::MemTableHandler>(
        new MemTableHandler(&table_def.columns()));
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        window_table->AddRow(from_iter->GetKey(), from_iter->GetValue());
        from_iter->Next();
    }
    return window_table->GetCount();
}

void SumArrayListCol(benchmark::State* state, MODE mode, int64_t data_size,
            const std::string& col_name) {
    vm::MemTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    std::vector<Slice> buffer;
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        buffer.push_back(from_iter->GetValue());
        from_iter->Next();
    }
    codec::ArrayListV<Slice> list_table(&buffer);

    codegen::MemoryWindowDecodeIRBuilder builder(table_def.columns(), nullptr);
    uint32_t offset;
    node::DataType type;
    uint32_t col_size;
    ASSERT_TRUE(builder.GetColOffsetType(col_name, &offset, &type));
    ASSERT_TRUE(codegen::GetLLVMColumnSize(type, &col_size));
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(col_size));
    ::fesql::codec::ListRef list_ref;

    list_ref.list = buf;
    int8_t* col = reinterpret_cast<int8_t*>(&list_ref);
    type::Type storage_type;
    ASSERT_TRUE(codegen::DataType2SchemaType(type, &storage_type));
    ASSERT_EQ(0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&list_table),
                                            offset, storage_type, buf));
    {
        switch (mode) {
            case BENCHMARK: {
                switch (type) {
                    case node::kInt32: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<int32_t>(col));
                        }
                        break;
                    }
                    case node::kInt64: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<int64_t>(col));
                        }
                        break;
                    }
                    case node::kDouble: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<double>(col));
                        }
                        break;
                    }
                    case node::kFloat: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<float>(col));
                        }
                        break;
                    }
                    default: {
                        FAIL();
                    }
                }
            }
            case TEST: {
                switch (type) {
                    case node::kInt32: {
                        if (fesql::udf::v1::sum_list<int32_t>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    case node::kInt64: {
                        if (fesql::udf::v1::sum_list<int64_t>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    case node::kDouble: {
                        if (fesql::udf::v1::sum_list<double>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    case node::kFloat: {
                        if (fesql::udf::v1::sum_list<float>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    default: {
                        FAIL();
                    }
                }
            }
        }
    }
    DeleteData(window);
}

void SumMemTableCol(benchmark::State* state, MODE mode, int64_t data_size,
            const std::string& col_name) {
    vm::MemTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    codegen::MemoryWindowDecodeIRBuilder builder(table_def.columns(), nullptr);
    uint32_t offset;
    node::DataType type;
    uint32_t col_size;
    ASSERT_TRUE(builder.GetColOffsetType(col_name, &offset, &type));
    ASSERT_TRUE(codegen::GetLLVMColumnSize(type, &col_size));
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(col_size));
    ::fesql::codec::ListRef list_ref;

    list_ref.list = buf;
    int8_t* col = reinterpret_cast<int8_t*>(&list_ref);
    type::Type storage_type;
    ASSERT_TRUE(codegen::DataType2SchemaType(type, &storage_type));
    ASSERT_EQ(0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window),
                                            offset, storage_type, buf));
    {
        switch (mode) {
            case BENCHMARK: {
                switch (type) {
                    case node::kInt32: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<int32_t>(col));
                        }
                        break;
                    }
                    case node::kInt64: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<int64_t>(col));
                        }
                        break;
                    }
                    case node::kDouble: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<double>(col));
                        }
                        break;
                    }
                    case node::kFloat: {
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(
                                fesql::udf::v1::sum_list<float>(col));
                        }
                        break;
                    }
                    default: {
                        FAIL();
                    }
                }
            }
            case TEST: {
                switch (type) {
                    case node::kInt32: {
                        if (fesql::udf::v1::sum_list<int32_t>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    case node::kInt64: {
                        if (fesql::udf::v1::sum_list<int64_t>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    case node::kDouble: {
                        if (fesql::udf::v1::sum_list<double>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    case node::kFloat: {
                        if (fesql::udf::v1::sum_list<float>(col) <= 0) {
                            DeleteData(window);
                            FAIL();
                        }
                        break;
                    }
                    default: {
                        FAIL();
                    }
                }
            }
        }
    }
    DeleteData(window);
}

}  // namespace bm
}  // namespace fesql
