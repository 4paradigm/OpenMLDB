/*
 * row_test.cc
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


#include <string>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "gtest/gtest.h"

namespace fesql {
namespace codec {

class RowTest : public ::testing::Test {};

TEST_F(RowTest, NewRowTest) {
    const std::string schema1 =
        "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
        "col5:int64, col6:string";
    const std::string schema2 =
        "str0:string, str1:string, col3:float, col4:double, col2:int16, "
        "col1:int32, col5:int64";
    const std::string schema3 =
        "c3:float, c4:double, col2:int16, "
        "str2:string";

    const std::string data1 =
        "2, 5, 55, 5.500000, 55.500000, 3, "
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const std::string data2 = "2, EEEEE, 5.500000, 550.500000, 550, 5, 3";
    const std::string data3 = "5.500000, 55.500000, 3, EEEEE";

    type::TableDef table1;
    ASSERT_TRUE(fesql::sqlcase::SQLCase::ExtractTableDef(schema1, "", table1));
    type::TableDef table2;
    ASSERT_TRUE(fesql::sqlcase::SQLCase::ExtractTableDef(schema2, "", table2));
    type::TableDef table3;
    ASSERT_TRUE(fesql::sqlcase::SQLCase::ExtractTableDef(schema3, "", table3));
    {
        int8_t* ptr1;
        int32_t ptr_size1;
        ASSERT_TRUE(fesql::sqlcase::SQLCase::ExtractRow(table1.columns(), data1,
                                                        &ptr1, &ptr_size1));
        Row row1(base::RefCountedSlice::Create(ptr1, ptr_size1));
        int8_t* ptr2;
        int32_t ptr_size2;
        ASSERT_TRUE(fesql::sqlcase::SQLCase::ExtractRow(table2.columns(), data2,
                                                        &ptr2, &ptr_size2));
        Row row2(base::RefCountedSlice::Create(ptr2, ptr_size2));

        int8_t* ptr3;
        int32_t ptr_size3;
        ASSERT_TRUE(fesql::sqlcase::SQLCase::ExtractRow(table3.columns(), data3,
                                                        &ptr3, &ptr_size3));
        Row row3(base::RefCountedSlice::Create(ptr3, ptr_size3));

        Row row12(1, row1, 1, row2);
        ASSERT_EQ(2, row12.GetRowPtrCnt());
        {
            RowView row_view1(table1.columns());
            row_view1.Reset(row12.buf(0));

            RowView row_view2(table2.columns());
            row_view2.Reset(row12.buf(1));
            ASSERT_EQ(data1, row_view1.GetRowString());
            ASSERT_EQ(data2, row_view2.GetRowString());
        }

        Row row123(2, row12, 1, row3);
        ASSERT_EQ(3, row123.GetRowPtrCnt());
        {
            RowView row_view1(table1.columns());
            row_view1.Reset(row123.buf(0));

            RowView row_view2(table2.columns());
            row_view2.Reset(row123.buf(1));

            RowView row_view3(table3.columns());
            row_view3.Reset(row123.buf(2));
            ASSERT_EQ(data1, row_view1.GetRowString());
            ASSERT_EQ(data2, row_view2.GetRowString());
            ASSERT_EQ(data3, row_view3.GetRowString());
        }
    }
}

TEST_F(RowTest, JoinRowTest) {
    Row l, r;
    Row join(2, l, 2, r);
    ASSERT_EQ(join.buf(0), nullptr);
    ASSERT_EQ(join.buf(1), nullptr);
    ASSERT_EQ(join.buf(2), nullptr);
    ASSERT_EQ(join.buf(3), nullptr);
}

}  // namespace codec
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
