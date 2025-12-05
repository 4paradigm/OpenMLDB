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

#include "benchmark/udf_bm_case.h"
#include <memory>
#include <string>
#include <vector>
#include "case/case_data_mock.h"
#include "codec/fe_row_codec.h"
#include "codec/type_codec.h"
#include "codegen/ir_base_builder.h"
#include "codegen/window_ir_builder.h"
#include "gtest/gtest.h"
#include "udf/udf.h"
#include "udf/udf_test.h"
#include "vm/jit_runtime.h"
#include "vm/mem_catalog.h"
namespace hybridse {
namespace bm {
using codec::ColumnImpl;
using codec::Row;
using sqlcase::CaseDataMock;
using vm::MemTableHandler;
using vm::MemTimeTableHandler;
static void BuildData(type::TableDef& table_def,        // NOLINT
                      vm::MemTimeTableHandler& window,  // NOLINT
                      int64_t data_size);

int64_t RunCopyToArrayList(MemTimeTableHandler& window,   // NOLINT
                           type::TableDef& table_def);    // NOLINT
int32_t RunCopyToTable(vm::TableHandler* table,           // NOLINT
                       const vm::Schema* schema);         // NOLINT
int32_t RunCopyToTimeTable(MemTimeTableHandler& segment,  // NOLINT
                           type::TableDef& table_def);    // NOLINT
static void BuildData(type::TableDef& table_def,          // NOLINT
                      vm::MemTimeTableHandler& segment,   // NOLINT
                      int64_t data_size) {
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);
    uint64_t ts = 1;
    for (auto row : buffer) {
        segment.AddRow(ts, row);
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

template <typename V>
auto CreateSumFunc() {
    return udf::UdfFunctionBuilder("sum")
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
    codec::ListRef<Row> list_table_ref;
    list_table_ref.list = reinterpret_cast<int8_t*>(&list_table);

    vm::SchemasContext schemas_context;
    schemas_context.BuildTrivial(table_def.catalog(), {&table_def});
    size_t schema_idx;
    size_t col_idx;
    ASSERT_TRUE(
        schemas_context
            .ResolveColumnIndexByName("", col_name, &schema_idx, &col_idx)
            .isOK());
    const codec::ColInfo* info =
        schemas_context.GetRowFormat(schema_idx)->GetColumnInfo(col_idx);

    codegen::MemoryWindowDecodeIRBuilder builder(&schemas_context, nullptr);
    node::NodeManager nm;
    auto rs = codegen::ColumnSchema2Type(info->schema, &nm);
    ASSERT_TRUE(rs.ok());
    auto* type = rs.value();

    uint32_t col_size;
    ASSERT_TRUE(codegen::GetLlvmColumnSize(&type, &col_size));

    int8_t* buf = reinterpret_cast<int8_t*>(alloca(col_size));

    ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                     reinterpret_cast<int8_t*>(&list_table_ref), 0, info->idx,
                     info->offset, info->type(), buf));

    {
        switch (mode) {
            case BENCHMARK: {
                switch (type.base_) {
                    case node::kInt32: {
                        auto sum = CreateSumFunc<int32_t>();
                        ::hybridse::codec::ListRef<int32_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::hybridse::codec::ListRef<int64_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::hybridse::codec::ListRef<double> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::hybridse::codec::ListRef<float> list_ref({buf});
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
                        ::hybridse::codec::ListRef<int32_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::hybridse::codec::ListRef<int64_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::hybridse::codec::ListRef<double> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::hybridse::codec::ListRef<float> list_ref({buf});
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

void DoSumTableCol(vm::TableHandler* window, benchmark::State* state, MODE mode,
                   int64_t data_size, const std::string& col_name) {
    vm::SchemasContext schemas_context;
    schemas_context.BuildTrivial(window->GetDatabase(), {window->GetSchema()});
    codegen::MemoryWindowDecodeIRBuilder builder(&schemas_context, nullptr);

    size_t schema_idx;
    size_t col_idx;
    ASSERT_TRUE(
        schemas_context
            .ResolveColumnIndexByName("", col_name, &schema_idx, &col_idx)
            .isOK());

    const codec::ColInfo* info =
        schemas_context.GetRowFormat(schema_idx)->GetColumnInfo(col_idx);

    node::TypeNode type;
    ASSERT_TRUE(codegen::SchemaType2DataType(info->type, &type));

    uint32_t col_size;
    ASSERT_TRUE(codegen::GetLlvmColumnSize(&type, &col_size));

    int8_t* buf = reinterpret_cast<int8_t*>(alloca(col_size));
    codec::ListRef<> window_ref;
    window_ref.list = reinterpret_cast<int8_t*>(window);
    ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                     reinterpret_cast<int8_t*>(&window_ref), 0, info->idx,
                     info->offset, info->type, buf));
    {
        switch (mode) {
            case BENCHMARK: {
                switch (type.base_) {
                    case node::kInt32: {
                        auto sum = CreateSumFunc<int32_t>();
                        ::hybridse::codec::ListRef<int32_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::hybridse::codec::ListRef<int64_t> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::hybridse::codec::ListRef<double> list_ref({buf});
                        for (auto _ : *state) {
                            benchmark::DoNotOptimize(sum(list_ref));
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::hybridse::codec::ListRef<float> list_ref({buf});
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
                        ::hybridse::codec::ListRef<int32_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kInt64: {
                        auto sum = CreateSumFunc<int64_t>();
                        ::hybridse::codec::ListRef<int64_t> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kDouble: {
                        auto sum = CreateSumFunc<double>();
                        ::hybridse::codec::ListRef<double> list_ref({buf});
                        if (sum(list_ref) <= 0) {
                            FAIL();
                        }
                        break;
                    }
                    case node::kFloat: {
                        auto sum = CreateSumFunc<float>();
                        ::hybridse::codec::ListRef<float> list_ref({buf});
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
    type::TableDef table_def;
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);
    vm::MemTableHandler window(&table_def.columns());
    for (int i = 0; i < data_size - 1; ++i) {
        window.AddRow(buffer[i]);
    }
    DoSumTableCol(&window, state, mode, data_size, col_name);
}

void SumRequestUnionTableCol(benchmark::State* state, MODE mode,
                             int64_t data_size, const std::string& col_name) {
    type::TableDef table_def;
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);
    auto window = std::make_shared<vm::MemTableHandler>(&table_def.columns());
    for (int i = 0; i < data_size - 1; ++i) {
        window->AddRow(buffer[i]);
    }
    auto request_union = std::make_shared<vm::RequestUnionTableHandler>(
        1, buffer[data_size - 1], window);
    DoSumTableCol(request_union.get(), state, mode, data_size, col_name);
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
    std::vector<int8_t*> chucks;
    for (int i = 0; i < 1000; i++) {
        chucks.push_back(
            hybridse::vm::JitRuntime::get()->AllocManaged(request_size));
    }
    hybridse::vm::JitRuntime::get()->ReleaseRunStep();
    return 1;
}
int32_t RunNewFree1000(size_t request_size) {
    openmldb::base::ByteMemoryPool pool;
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
void TimestampFormat(benchmark::State* state, MODE mode) {
    codec::Timestamp timestamp(1590115420000L);
    const std::string format = "%Y-%m-%d %H:%M:%S";
    codec::StringRef str_format(format);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                codec::StringRef str;
                udf::v1::date_format(&timestamp, &str_format, &str);
            }
            break;
        }
        case TEST: {
            codec::StringRef str;
            udf::v1::date_format(&timestamp, &str_format, &str);
            ASSERT_EQ(codec::StringRef("2020-05-22 10:43:40"), str);
            break;
        }
    }
}
void TimestampToString(benchmark::State* state, MODE mode) {
    codec::Timestamp timestamp(1590115420000L);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                codec::StringRef str;
                udf::v1::timestamp_to_string(&timestamp, &str);
            }
            break;
        }
        case TEST: {
            codec::StringRef str;
            udf::v1::timestamp_to_string(&timestamp, &str);
            ASSERT_EQ(codec::StringRef("2020-05-22 10:43:40"), str);
            break;
        }
    }
}

void DateFormat(benchmark::State* state, MODE mode) {
    codec::Date date(2020, 05, 22);
    const std::string format = "%Y-%m-%d";
    codec::StringRef str_format(format);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                codec::StringRef str;
                udf::v1::date_format(&date, &str_format, &str);
            }
            break;
        }
        case TEST: {
            codec::StringRef str;
            udf::v1::date_format(&date, &str_format, &str);
            ASSERT_EQ(codec::StringRef("2020-05-22"), str);
            break;
        }
    }
}
void DateToString(benchmark::State* state, MODE mode) {
    codec::Date date(2020, 05, 22);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                codec::StringRef str;
                udf::v1::date_to_string(&date, &str);
            }
            break;
        }
        case TEST: {
            codec::StringRef str;
            udf::v1::date_to_string(&date, &str);
            ASSERT_EQ(codec::StringRef("2020-05-22"), str);
            break;
        }
    }
}
int64_t RunHistoryWindowBuffer(const vm::WindowRange& window_range,
                               uint64_t data_size,
                               const bool exclude_current_time) {  // NOLINT
    auto window = std::make_shared<vm::HistoryWindow>(window_range);
    window->set_exclude_current_time(exclude_current_time);
    Row row;
    uint64_t ts = 1L;
    while (ts < data_size) {
        window->BufferData(ts, row);
        ts++;
    }
    return window->GetCount();
}
void HistoryWindowBuffer(benchmark::State* state, MODE mode,
                         int64_t data_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunHistoryWindowBuffer(
                    vm::WindowRange(vm::Window::kFrameRowsRange, -100, 0, 0, 0),
                    data_size, false));
            }
            break;
        }
        case TEST: {
        }
    }
}
void HistoryWindowBufferExcludeCurrentTime(benchmark::State* state, MODE mode,
                                           int64_t data_size) {
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunHistoryWindowBuffer(
                    vm::WindowRange(vm::Window::kFrameRowsRange, -100, 0, 0, 0),
                    data_size, true));
            }
            break;
        }
        case TEST: {
        }
    }
}

void RequestUnionWindow(benchmark::State* state, MODE mode, int64_t data_size) {
    Row request;
    Row row;
    auto table = std::make_shared<MemTimeTableHandler>();
    for (uint64_t key = 0; key < data_size; key++) {
        table->AddRow(key, row);
    }
    uint64_t current_key = data_size + 1;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    vm::RequestUnionRunner::RequestUnionWindow(
                        request,
                        std::vector<std::shared_ptr<vm::TableHandler>>({table}),
                        current_key,
                        vm::WindowRange(vm::Window::kFrameRowsRange, -100, 0, 0,
                                        0),
                        true, false));
            }
            break;
        }
        case TEST: {
        }
    }
}
void RequestUnionWindowExcludeCurrentTime(benchmark::State* state, MODE mode,
                                          int64_t data_size) {
    Row request;
    Row row;
    auto table = std::make_shared<MemTimeTableHandler>();
    for (uint64_t key = 0; key < data_size; key++) {
        table->AddRow(key, row);
    }
    uint64_t current_key = data_size + 1;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    vm::RequestUnionRunner::RequestUnionWindow(
                        request,
                        std::vector<std::shared_ptr<vm::TableHandler>>({table}),
                        current_key,
                        vm::WindowRange(vm::Window::kFrameRowsRange, -100, 0, 0,
                                        0),
                        true, true));
            }
            break;
        }
        case TEST: {
        }
    }
}
}  // namespace bm
}  // namespace hybridse
