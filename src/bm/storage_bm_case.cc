/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_bm_case.cc
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/
#include "bm/storage_bm_case.h"
#include <memory>
#include <string>
#include <vector>
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
