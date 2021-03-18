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

#include "bm/storage_bm_case.h"
#include <memory>
#include <vector>
#include "codec/fe_row_codec.h"
#include "gtest/gtest.h"
#include "storage/list.h"

namespace hybridse {
namespace bm {

using codec::Row;
using codec::RowView;
using ::hybridse::base::DefaultComparator;
using storage::ArrayList;
using hybridse::sqlcase::CaseDataMock;
using hybridse::sqlcase::CaseSchemaMock;
DefaultComparator cmp;

int64_t RunIterate(storage::BaseList<uint64_t, int64_t>* list);
int64_t RunIterateTest(storage::BaseList<uint64_t, int64_t>* list);
const int64_t PartitionHandlerIterate(vm::PartitionHandler* partition_handler,
                                      const std::string& key);
const int64_t PartitionHandlerIterateTest(
    vm::PartitionHandler* partition_handler, const std::string& key);
static void BuildData(type::TableDef& table_def,   // NOLINT
                      vm::MemTableHandler& table,  // NOLINT
                      int64_t data_size) {
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);
    for (auto row : buffer) {
        table.AddRow(row);
    }
}
static void BuildData(type::TableDef& table_def,         // NOLINT
                      vm::MemTimeTableHandler& segment,  // NOLINT
                      int64_t data_size) {
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);
    uint64_t ts = 1;
    for (auto row : buffer) {
        segment.AddRow(ts, row);
    }
}
void ArrayListIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    type::TableDef table_def;
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);

    RowView row_view(table_def.columns());
    ArrayList<uint64_t, int64_t, DefaultComparator> list(cmp);
    for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
        row_view.Reset(iter->buf());
        int64_t key;
        row_view.GetInt64(5, &key);
        int64_t addr = reinterpret_cast<int64_t>(iter->buf());
        list.Insert(static_cast<uint64_t>(key), addr);
    }

    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(RunIterate(&list));
            }
            break;
        }
        case TEST: {
            int64_t cnt = RunIterateTest(&list);
            ASSERT_EQ(cnt, data_size);
        }
    }
}

int64_t RunIterateTest(storage::BaseList<uint64_t, int64_t>* list) {
    int64_t cnt = 0;
    auto iter = list->NewIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        cnt++;
    }
    return cnt;
}
int64_t RunIterate(storage::BaseList<uint64_t, int64_t>* list) {
    auto iter = list->NewIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
    }
    return 0;
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

void RequestUnionTableIterate(benchmark::State* state, MODE mode,
                              int64_t data_size) {
    auto table_handler = std::make_shared<vm::MemTimeTableHandler>();
    type::TableDef table_def;
    BuildData(table_def, *table_handler.get(), data_size);
    codec::Row request_row = table_handler->GetBackRow().second;
    table_handler->PopBackRow();

    auto request_union = std::make_shared<vm::RequestUnionTableHandler>(
        0, request_row, table_handler);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    TableHanlderIterate(request_union.get()));
            }
            break;
        }
        case TEST: {
            if (data_size != TableHanlderIterateTest(request_union.get())) {
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
void TabletFullIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    auto catalog = vm::BuildOnePkTableStorage(data_size);
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
void TabletWindowIterate(benchmark::State* state, MODE mode,
                         int64_t data_size) {
    auto catalog = vm::BuildOnePkTableStorage(data_size);
    auto table_hanlder = catalog->GetTable("db", "t1");
    auto partition_handler = table_hanlder->GetPartition("index1");
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
}  // namespace bm
}  // namespace hybridse
