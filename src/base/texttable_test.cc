/*
 * texttable_test.cc
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * texttable_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/12
 *--------------------------------------------------------------------------
 **/
#include "base/texttable.h"
#include "gtest/gtest.h"
namespace fesql {
namespace base {
class TextTableTest : public ::testing::Test {
 public:
    TextTableTest() {}
    ~TextTableTest() {}
};

TEST_F(TextTableTest, TextTableFormatTest) {
    base::TextTable t('-', '|', '+');
    t.add("Field");
    t.add("Type");
    t.add("NULL");
    t.endOfRow();

    t.add("column1");
    t.add("kInt32");
    t.add("No");
    t.endOfRow();

    t.add("column2");
    t.add("kInt64");
    t.add("YES");
    t.endOfRow();

    t.add("ts");
    t.add("kTimestamp");
    t.add("YES");
    t.endOfRow();
    std::cout << t << std::endl;
}
}  // namespace base
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
