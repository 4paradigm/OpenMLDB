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
#include "codec/fe_row_codec.h"
#include "codec/type_codec.h"
#include "codegen/ir_base_builder.h"
#include "codegen/window_ir_builder.h"
#include "gtest/gtest.h"
#include "udf/udf.h"
#include "udf/udf_test.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace bm {
using codec::ColumnImpl;
using codec::Row;
using vm::MemTableHandler;
using vm::MemTimeTableHandler;
static void BuildData(type::TableDef& table_def,        // NOLINT
                      vm::MemTimeTableHandler& window,  // NOLINT
                      int64_t data_size);

int64_t RunCopyToArrayList(MemTimeTableHandler& window,           // NOLINT
                           type::TableDef& table_def);            // NOLINT
int32_t RunCopyToTable(vm::TableHandler* table,                   // NOLINT
                       const vm::Schema* schema);                 // NOLINT
int32_t RunCopyToTimeTable(MemTimeTableHandler& segment,          // NOLINT
                           type::TableDef& table_def);            // NOLINT
int32_t TableHanlderIterate(vm::TableHandler* table_hander);      // NOLINT
int32_t TableHanlderIterateTest(vm::TableHandler* table_hander);  // NOLINT
const int64_t PartitionHandlerIterate(vm::PartitionHandler* partition_handler,
                                      const std::string& key);
const int64_t PartitionHandlerIterateTest(
    vm::PartitionHandler* partition_handler, const std::string& key);

static void BuildData(type::TableDef& table_def,         // NOLINT
                      vm::MemTimeTableHandler& segment,  // NOLINT
                      int64_t data_size) {
    std::vector<Row> buffer;
    BuildOnePkTableData(table_def, buffer, data_size);
    uint64_t ts = 1;
    for (auto row : buffer) {
        segment.AddRow(ts, row);
    }
}
static void BuildData(type::TableDef& table_def,   // NOLINT
                      vm::MemTableHandler& table,  // NOLINT
                      int64_t data_size) {
    std::vector<Row> buffer;
    BuildOnePkTableData(table_def, buffer, data_size);
    for (auto row : buffer) {
        table.AddRow(row);
    }
}

void CopyArrayList(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTimeTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunCopyToArrayList(window, table_def));
            }
            break;
        }
        case TEST: {
            if (data_size != RunCopyToArrayList(window, table_def)) {
                FAIL();
            }
        }
    }
}
void CopyMemTable(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTimeTableHandler table;
    type::TableDef table_def;
    BuildData(table_def, table, data_size);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunCopyToTable(&table, &(table_def.columns())));
            }
            break;
        }
        case TEST: {
            if (data_size != RunCopyToTable(&table, &(table_def.columns()))) {
                FAIL();
            }
        }
    }
}
void TabletFullIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    auto catalog = BuildOnePkTableStorage(data_size);
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
            if (data_size != TableHanlderIterateTest(table_hanlder.get())) {
                FAIL();
            }
        }
    }
}

void TabletWindowIterate(benchmark::State* state, MODE mode,
                         int64_t data_size) {
    auto catalog = BuildOnePkTableStorage(data_size);
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
                PartitionHandlerIterateTest(partition_handler.get(), "hello")) {
                FAIL();
            }
        }
    }
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
            if (data_size != TableHanlderIterateTest(&table_hanlder)) {
                FAIL();
            }
        }
    }
}

void MemSegmentIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTimeTableHandler table_hanlder;
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
            if (data_size != TableHanlderIterateTest(&table_hanlder)) {
                FAIL();
            }
        }
    }
}
void CopyMemSegment(benchmark::State* state, MODE mode, int64_t data_size) {
    vm::MemTimeTableHandler table;
    type::TableDef table_def;
    BuildData(table_def, table, data_size);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunCopyToTimeTable(table, table_def));
            }
            break;
        }
        case TEST: {
            if (data_size != RunCopyToTimeTable(table, table_def)) {
                FAIL();
            }
        }
    }
}

int64_t RunCopyToArrayList(MemTimeTableHandler& window,  // NOLINT
                           type::TableDef& table_def) {  // NOLINT
    std::vector<Row> window_table;
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        window_table.push_back(from_iter->GetValue());
        from_iter->Next();
    }
    return window_table.size();
}
int32_t RunCopyToTable(vm::TableHandler* table,     // NOLINT
                       const vm::Schema* schema) {  // NOLINT
    auto window_table =
        std::shared_ptr<vm::MemTableHandler>(new MemTableHandler(schema));
    auto from_iter = table->GetIterator();
    while (from_iter->Valid()) {
        window_table->AddRow(from_iter->GetValue());
        from_iter->Next();
    }
    return window_table->GetCount();
}
int32_t RunCopyToTimeTable(MemTimeTableHandler& segment,  // NOLINT
                           type::TableDef& table_def) {   // NOLINT
    auto window_table = std::shared_ptr<vm::MemTimeTableHandler>(
        new MemTimeTableHandler(&table_def.columns()));
    auto from_iter = segment.GetIterator();
    while (from_iter->Valid()) {
        window_table->AddRow(from_iter->GetKey(), from_iter->GetValue());
        from_iter->Next();
    }
    return window_table->GetCount();
}

int32_t TableHanlderIterateTest(vm::TableHandler* table_hander) {  // NOLINT
    int64_t cnt = 0;
    auto from_iter = table_hander->GetIterator();
    while (from_iter->Valid()) {
        from_iter->Next();
        cnt++;
    }
    return cnt;
}
int32_t TableHanlderIterate(vm::TableHandler* table_hander) {  // NOLINT
    auto from_iter = table_hander->GetIterator();
    while (from_iter->Valid()) {
        from_iter->Next();
    }
    return 0;
}

const int64_t PartitionHandlerIterate(vm::PartitionHandler* partition_handler,
                                      const std::string& key) {
    auto p_iter = partition_handler->GetWindowIterator();
    p_iter->Seek(key);
    auto iter = p_iter->GetValue();
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
    }
    return 0;
}
const int64_t PartitionHandlerIterateTest(
    vm::PartitionHandler* partition_handler, const std::string& key) {
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

template <typename V>
auto CreateSumFunc() {
    return udf::UDFFunctionBuilder("sum")
        .args<codec::ListRef<V>>()
        .template returns<V>()
        .build();
}

void SumArrayListCol(benchmark::State* state, MODE mode, int64_t data_size,
                     const std::string& col_name) {
    vm::MemTimeTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    std::vector<Row> buffer;
    auto from_iter = window.GetIterator();
    while (from_iter->Valid()) {
        buffer.push_back(from_iter->GetValue());
        from_iter->Next();
    }
    codec::ArrayListV<Row> list_table(&buffer);

    codegen::MemoryWindowDecodeIRBuilder builder(table_def.columns(), nullptr);
    codec::ColInfo info;
    node::TypeNode type;
    uint32_t col_size;
    ASSERT_TRUE(builder.ResolveFieldInfo(col_name, 0, &info, &type));
    ASSERT_TRUE(codegen::GetLLVMColumnSize(&type, &col_size));
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(col_size));
    type::Type storage_type;
    ASSERT_TRUE(codegen::DataType2SchemaType(type, &storage_type));
    ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                     reinterpret_cast<int8_t*>(&list_table), 0, info.idx,
                     info.offset, storage_type, buf));

    {
        switch (mode) {
            case BENCHMARK: {
                switch (type.base_) {
                    case node::kInt32: {
                        auto sum = CreateSumFunc<int32_t>();
                        ::fesql::codec::ListRef<int32_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::fesql::codec::ListRef<int64_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::fesql::codec::ListRef<double> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::fesql::codec::ListRef<float> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    default: {
                        FAIL();
                    }
                }
            }
            case TEST: {
                switch (type.base_) {
                    case node::kInt32: {
                        auto sum = CreateSumFunc<int32_t>();
                        ::fesql::codec::ListRef<int32_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::fesql::codec::ListRef<int64_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::fesql::codec::ListRef<double> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::fesql::codec::ListRef<float> list_ref({buf});
                        if (sum(list_ref) <= 0) {
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
}
void SumMemTableCol(benchmark::State* state, MODE mode, int64_t data_size,
                    const std::string& col_name) {
    vm::MemTableHandler window;
    type::TableDef table_def;
    BuildData(table_def, window, data_size);

    codegen::MemoryWindowDecodeIRBuilder builder(table_def.columns(), nullptr);
    codec::ColInfo info;
    node::TypeNode type;
    uint32_t col_size;
    ASSERT_TRUE(builder.ResolveFieldInfo(col_name, 0, &info, &type));
    ASSERT_TRUE(codegen::GetLLVMColumnSize(&type, &col_size));
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(col_size));
    type::Type storage_type;
    ASSERT_TRUE(codegen::DataType2SchemaType(type, &storage_type));
    ASSERT_EQ(0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window),
                                            0, info.idx, info.offset,
                                            storage_type, buf));
    {
        switch (mode) {
            case BENCHMARK: {
                switch (type.base_) {
                    case node::kInt32: {
                        auto sum = CreateSumFunc<int32_t>();
                        ::fesql::codec::ListRef<int32_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::fesql::codec::ListRef<int64_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::fesql::codec::ListRef<double> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::fesql::codec::ListRef<float> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    default: {
                        FAIL();
                    }
                }
            }
            case TEST: {
                switch (type.base_) {
                    case node::kInt32: {
                        auto sum = CreateSumFunc<int32_t>();
                        ::fesql::codec::ListRef<int32_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::fesql::codec::ListRef<int64_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::fesql::codec::ListRef<double> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::fesql::codec::ListRef<float> list_ref({buf});
                        if (sum(list_ref) <= 0) {
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
}

bool CTimeDays(int data_size) {
    for (int i = 0; i < data_size; i++) {
        udf::v1::dayofmonth(1590115420000L + ((i)) * 86400000);
    }
    return true;
}

void CTimeDay(benchmark::State* state, MODE mode, const int32_t data_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(CTimeDays(data_size));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(22, udf::v1::dayofmonth(1590115420000L));
            break;
        }
    }
}
void CTimeMonth(benchmark::State* state, MODE mode, const int32_t data_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(CTimeDays(data_size));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(05, udf::v1::month(1590115420000L));
            break;
        }
    }
}
void CTimeYear(benchmark::State* state, MODE mode, const int32_t data_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(CTimeDays(data_size));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(2020, udf::v1::year(1590115420000L));
            break;
        }
    }
}
int32_t RunByteMemPoolAlloc1000(size_t request_size) {
    for (int i = 0; i < 1000; i++) {
        fesql::udf::ThreadLocalMemoryPoolAlloc(request_size);
    }
    fesql::udf::ThreadLocalMemoryPoolReset();
    return 1;
}
int32_t RunNewFree1000(size_t request_size) {
    fesql::base::ByteMemoryPool pool;
    std::vector<char*> chucks;
    for (int i = 0; i < 1000; i++) {
        chucks.push_back(new char[request_size]);
    }
    for (auto chuck : chucks) {
        delete[] chuck;
    }
    return 1;
}
void NewFree1000(benchmark::State* state, MODE mode, size_t request_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunNewFree1000(request_size));
            }
            break;
        }
        case TEST: {
            RunByteMemPoolAlloc1000(request_size);
        }
    }
}
void ByteMemPoolAlloc1000(benchmark::State* state, MODE mode,
                          size_t request_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunByteMemPoolAlloc1000(request_size));
            }
            break;
        }
        case TEST: {
            RunByteMemPoolAlloc1000(request_size);
        }
    }
}
}  // namespace bm
}  // namespace fesql
