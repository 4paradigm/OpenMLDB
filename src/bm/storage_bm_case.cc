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
#include <string>
#include <vector>
#include "base/mem_pool.h"
#include "bm/base_bm.h"
#include "codec/fe_row_codec.h"
#include "codegen/ir_base_builder.h"
#include "codegen/window_ir_builder.h"
#include "gtest/gtest.h"
#include "storage/list.h"

namespace fesql {
namespace bm {

using codec::Row;
using codec::RowView;
using ::fesql::base::DefaultComparator;
using storage::ArrayList;
DefaultComparator cmp;

int64_t RunIterate(storage::BaseList<uint64_t, int64_t>* list);
int64_t RunIterateTest(storage::BaseList<uint64_t, int64_t>* list);
void ArrayListIterate(benchmark::State* state, MODE mode, int64_t data_size) {
    type::TableDef table_def;
    std::vector<Row> buffer;
    BuildOnePkTableData(table_def, buffer, data_size);

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
}  // namespace bm
}  // namespace fesql
