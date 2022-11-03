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

#include "base/texttable.h"
#include "gtest/gtest.h"
namespace hybridse {
namespace base {
class TextTableTest : public ::testing::Test {
 public:
    TextTableTest() {}
    ~TextTableTest() {}
};

TEST_F(TextTableTest, TextTableFormatTest) {
    std::ostringstream oss;
    base::TextTable t('-', '|', '+');
    t.add("Field");
    t.add("Type");
    t.add("NULL");
    t.end_of_row();

    t.add("column1");
    t.add("kInt32");
    t.add("No");
    t.end_of_row();

    t.add("column2");
    t.add("kInt64");
    t.add("YES");
    t.end_of_row();

    t.add("ts");
    t.add("kTimestamp");
    t.add("YES");
    t.end_of_row();
    oss << t;
    ASSERT_EQ(
        "+---------+------------+------+\n"
        "| Field   | Type       | NULL |\n"
        "+---------+------------+------+\n"
        "| column1 | kInt32     | No   |\n"
        "| column2 | kInt64     | YES  |\n"
        "| ts      | kTimestamp | YES  |\n"
        "+---------+------------+------+\n",
        oss.str());
}

TEST_F(TextTableTest, TextTableMultipleLinesTest) {
    std::ostringstream oss;
    base::TextTable t('-', '|', '+', true);
    t.add("Field");
    t.add("Type");
    t.add("NULL");
    t.end_of_row();

    t.add("column1");
    t.add("kInt32");
    t.add("No\nYes");
    t.end_of_row();

    t.add("column2");
    t.add("kInt64\nKint32");
    t.add("YES");
    t.end_of_row();

    t.add("ts\ncolumn3");
    t.add("kTimestamp");
    t.add("YES");
    t.end_of_row();
    oss << t;
    ASSERT_EQ(
        "+---------+------------+------+\n"
        "| Field   | Type       | NULL |\n"
        "+---------+------------+------+\n"
        "| column1 | kInt32     | No   |\n"
        "|         |            | Yes  |\n"
        "+---------+------------+------+\n"
        "| column2 | kInt64     | YES  |\n"
        "|         | Kint32     |      |\n"
        "+---------+------------+------+\n"
        "| ts      | kTimestamp | YES  |\n"
        "| column3 |            |      |\n"
        "+---------+------------+------+\n",
        oss.str());
}

}  // namespace base
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
