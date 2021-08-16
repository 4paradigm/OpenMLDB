/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <memory>
#include <string>
#include <vector>
#include "codegen/fn_let_ir_builder_test.h"

namespace hybridse {
namespace codegen {

using hybridse::codec::ArrayListV;
using hybridse::codec::Row;

class AggregateIRBuilderTest : public ::testing::Test {
 public:
    AggregateIRBuilderTest() {}
    ~AggregateIRBuilderTest() {}
    node::NodeManager manager;
};

TEST_F(AggregateIRBuilderTest, TestMixedMultipleAgg) {
    std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as col1_sum, "
        "avg(col1) OVER w1 as col1_avg, "
        "count(col1) OVER w1 as col1_count, "
        "min(col1) OVER w1 as col1_min, "
        "max(col1) OVER w1 as col1_max, "

        "sum(col3) OVER w1 as col3_sum, "
        "avg(col3) OVER w1 as col3_avg, "
        "count(col3) OVER w1 as col3_count, "
        "min(col3) OVER w1 as col3_min, "
        "max(col3) OVER w1 as col3_max, "

        "sum(col2) OVER w1 as col2_sum, "
        "avg(col2) OVER w1 as col2_avg, "
        "count(col2) OVER w1 as col2_count, "
        "min(col2) OVER w1 as col2_min, "
        "max(col2) OVER w1 as col2_max, "

        "sum(col4) OVER w1 as col4_sum, "
        "avg(col4) OVER w1 as col4_avg, "
        "count(col4) OVER w1 as col4_count, "
        "min(col4) OVER w1 as col4_min, "
        "max(col4) OVER w1 as col4_max, "

        "sum(col5) OVER w1 as col5_sum, "
        "avg(col5) OVER w1 as col5_avg, "
        "count(col5) OVER w1 as col5_count, "
        "min(col5) OVER w1 as col5_min, "
        "max(col5) OVER w1 as col5_max "

        "FROM t1 WINDOW "
        "w1 AS "
        "(PARTITION BY COL2 ORDER BY `TS` ROWS_RANGE BETWEEN 3 PRECEDING AND "
        "CURRENT ROW) limit 10;";

    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);
    int8_t* output = NULL;
    int8_t* row_ptr = reinterpret_cast<int8_t*>(&window[window.size() - 1]);
    codec::ListRef<Row> window_ref;
    window_ref.list = ptr;
    int8_t* window_ptr = reinterpret_cast<int8_t*>(&window_ref);
    codec::Schema schema;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptr, window_ptr, &schema,
                      &output);

    codec::RowView view(schema);
    view.Reset(output, view.GetSize(output));
    ASSERT_EQ(view.GetInt32Unsafe(0), 1 + 11 + 111 + 1111 + 11111);
    ASSERT_FLOAT_EQ(view.GetDoubleUnsafe(1),
                    (1 + 11 + 111 + 1111 + 11111) / 5.0);
    ASSERT_EQ(view.GetInt64Unsafe(2), 5);
    ASSERT_EQ(view.GetInt32Unsafe(3), 1);
    ASSERT_EQ(view.GetInt32Unsafe(4), 11111);

    ASSERT_FLOAT_EQ(view.GetFloatUnsafe(5),
                    3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f);
    ASSERT_FLOAT_EQ(view.GetDoubleUnsafe(6),
                    (0.0 + 3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f) / 5.0);
    ASSERT_EQ(view.GetInt64Unsafe(7), 5);
    ASSERT_FLOAT_EQ(view.GetFloatUnsafe(8), 3.1f);
    ASSERT_FLOAT_EQ(view.GetFloatUnsafe(9), 33333.1f);

    ASSERT_EQ(view.GetInt16Unsafe(10), 2 + 22 + 222 + 2222 + 22222);
    ASSERT_DOUBLE_EQ(view.GetDoubleUnsafe(11),
                     (2 + 22 + 222 + 2222 + 22222) / 5.0);
    ASSERT_EQ(view.GetInt64Unsafe(12), 5);
    ASSERT_EQ(view.GetInt16Unsafe(13), 2);
    ASSERT_EQ(view.GetInt16Unsafe(14), 22222);

    ASSERT_DOUBLE_EQ(view.GetDoubleUnsafe(15),
                     4.1 + 44.1 + 444.1 + 4444.1 + 44444.1);
    ASSERT_DOUBLE_EQ(view.GetDoubleUnsafe(16),
                     (4.1 + 44.1 + 444.1 + 4444.1 + 44444.1) / 5.0);
    ASSERT_EQ(view.GetInt64Unsafe(17), 5);
    ASSERT_DOUBLE_EQ(view.GetDoubleUnsafe(18), 4.1);
    ASSERT_DOUBLE_EQ(view.GetDoubleUnsafe(19), 44444.1);

    ASSERT_EQ(view.GetInt64Unsafe(20), 5L + 55L + 555L + 5555L + 55555L);
    ASSERT_DOUBLE_EQ(view.GetDoubleUnsafe(21),
                     (5L + 55L + 555L + 5555L + 55555L) / 5.0);
    ASSERT_EQ(view.GetInt64Unsafe(22), 5);
    ASSERT_EQ(view.GetInt64Unsafe(23), 5L);
    ASSERT_EQ(view.GetInt64Unsafe(24), 55555L);

    free(ptr);
}

}  // namespace codegen
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
