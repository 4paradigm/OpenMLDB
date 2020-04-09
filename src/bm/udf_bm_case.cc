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
using vm::MemSegmentHandler;
using vm::MemTableHandler;
static void BuildData(type::TableDef& table_def,      // NOLINT
                      vm::MemSegmentHandler& window,  // NOLINT
                      int64_t data_size);

static void DeleteData(vm::MemSegmentHandler& window);        // NOLINT
static void DeleteData(vm::MemTableHandler& window);          // NOLINT
int64_t RunCopyArrayList(MemSegmentHandler& window,           // NOLINT
                         type::TableDef& table_def);          // NOLINT
int32_t RunCopyTable(vm::TableHandler* table,                 // NOLINT
                     const vm::Schema* schema);               // NOLINT
int32_t RunCopySegment(MemSegmentHandler& segment,            // NOLINT
                       type::TableDef& table_def);            // NOLINT
int32_t TableHanlderIterate(vm::TableHandler* table_hander);  // NOLINT
const int64_t PartitionHandlerIterate(vm::PartitionHandler* partition_handler,
                                      const std::string& key);
static void DeleteData(vm::MemSegmentHandler& window) {  // NOLINT
    auto iter = window.GetIterator();
    while (iter->Valid()) {
        delete iter->GetValue().buf();
        iter->Next();
    }
}
static void DeleteData(vm::MemTableHandler& window) {  // NOLINT
    auto iter = window.GetIterator();
    while (iter->Valid()) {
        delete iter->GetValue().buf();
        iter->Next();
    }
}

static void BuildData(type::TableDef& table_def,   // NOLINT
                      std::vector<Slice>& buffer,  // NOLINT
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
        buffer.push_back(Slice(ptr, total_size));
    }
}
static void BuildData(type::TableDef& table_def,       // NOLINT
                      vm::MemSegmentHandler& segment,  // NOLINT
                      int64_t data_size) {
    std::vector<Slice> buffer;
    BuildData(table_def, buffer, data_size);
    uint64_t ts = 1;
    for (auto row : buffer) {
        segment.AddRow(ts, row);
    }
}
static void BuildData(type::TableDef& table_def,   // NOLINT
                      vm::MemTableHandler& table,  // NOLINT
                      int64_t data_size) {
    std::vector<Slice> buffer;
    BuildData(table_def, buffer, data_size);
    for (auto row : buffer) {
        table.AddRow(row);
    }
}

void CopyArrayList(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemSegmentHandler window;
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
void CopyMemTable(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemSegmentHandler table;
    type::TableDef table_def;
    BuildData(table_def, table, data_size);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunCopyTable(&table, &(table_def.columns())));
            }
            DeleteData(table);
            break;
        }
        case TEST: {
            if (data_size == RunCopyTable(&table, &(table_def.columns()))) {
                DeleteData(table);
            } else {
                DeleteData(table);
                FAIL();
            }
        }
    }
}
void TabletFullIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    auto catalog = Data_WindowCase1(data_size);
    auto table_hanlder = catalog->GetTable("db", "t1");
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    TableHanlderIterate(table_hanlder.get()));
            }
            break;
        }
        case TEST: {
            if (data_size != TableHanlderIterate(table_hanlder.get())) {
                FAIL();
            }
        }
    }
}

void TabletWindowIterate(benchmark::State* state, MODE mode,
                         int64_t data_size) {
    auto catalog = Data_WindowCase1(data_size);
    auto table_hanlder = catalog->GetTable("db", "t1");
    auto partition_handler =
        table_hanlder->GetPartition(table_hanlder, "index1");
    if (!partition_handler) {
        FAIL();
    }
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    PartitionHandlerIterate(partition_handler.get(), "hello"));
            }
            break;
        }
        case TEST: {
            if (data_size !=
                PartitionHandlerIterate(partition_handler.get(), "hello")) {
                FAIL();
            }
        }
    }
}

const int64_t PartitionHandlerIterate(vm::PartitionHandler* partition_handler,
                                      const std::string& key) {
    int64_t cnt = 0;
    auto p_iter = partition_handler->GetWindowIterator();
    p_iter->Seek(key);
    auto iter = p_iter->GetValue();
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        cnt++;
    }
    return cnt;
}

void MemTableIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTableHandler table_hanlder;
    type::TableDef table_def;
    BuildData(table_def, table_hanlder, data_size);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(TableHanlderIterate(&table_hanlder));
            }
            break;
        }
        case TEST: {
            if (data_size != TableHanlderIterate(&table_hanlder)) {
                FAIL();
            }
        }
    }
}

void MemSegmentIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemSegmentHandler table_hanlder;
    type::TableDef table_def;
    BuildData(table_def, table_hanlder, data_size);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(TableHanlderIterate(&table_hanlder));
            }
            break;
        }
        case TEST: {
            if (data_size != TableHanlderIterate(&table_hanlder)) {
                FAIL();
            }
        }
    }
}
void CopyMemSegment(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemSegmentHandler table;
    type::TableDef table_def;
    BuildData(table_def, table, data_size);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunCopySegment(table, table_def));
            }
            DeleteData(table);
            break;
        }
        case TEST: {
            if (data_size == RunCopySegment(table, table_def)) {
                DeleteData(table);
            } else {
                DeleteData(table);
                FAIL();
            }
        }
    }
}

int64_t RunCopyArrayList(MemSegmentHandler& window,    // NOLINT
                         type::TableDef& table_def) {  // NOLINT
    std::vector<Slice> window_table;
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        window_table.push_back(from_iter->GetValue());
        from_iter->Next();
    }
    return window_table.size();
}
int32_t RunCopyTable(vm::TableHandler* table_hander,  // NOLINT
                     const vm::Schema* schema) {      // NOLINT
    auto window_table =
        std::shared_ptr<vm::MemTableHandler>(new MemTableHandler(schema));
    auto from_iter = table_hander->GetIterator();
    while (from_iter->Valid()) {
        window_table->AddRow(from_iter->GetValue());
        from_iter->Next();
    }
    return window_table->GetCount();
}
int32_t RunCopySegment(MemSegmentHandler& window,    // NOLINT
                       type::TableDef& table_def) {  // NOLINT
    auto window_table = std::shared_ptr<vm::MemSegmentHandler>(
        new MemSegmentHandler(&table_def.columns()));
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        window_table->AddRow(from_iter->GetKey(), from_iter->GetValue());
        from_iter->Next();
    }
    return window_table->GetCount();
}

int32_t TableHanlderIterate(vm::TableHandler* table_hander) {  // NOLINT
    int64_t cnt = 0;
    auto from_iter = table_hander->GetIterator();
    while (from_iter->Valid()) {
        from_iter->Next();
        cnt++;
    }
    return cnt;
}

void SumArrayListCol(benchmark::State* state, MODE mode, int64_t data_size,
                     const std::string& col_name) {
    vm::MemSegmentHandler window;
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
    ASSERT_EQ(0,
              ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&list_table),
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
