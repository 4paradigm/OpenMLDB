/*
 * parser/sql_test.h
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
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
#include "parser/node.h"
#include "parser/parser.h"
#include "gtest/gtest.h"
#include <strstream>

namespace fedb {
namespace sql {

struct StringPair {
    StringPair(const std::string &sqlstr, const std::string &exp) : sqlstr_(sqlstr), exp_(exp) {}
    std::string sqlstr_;
    std::string exp_;
};

class SqlTest : public ::testing::TestWithParam<std::string> {

public:
    SqlTest() {}

    ~SqlTest() {}
};
INSTANTIATE_TEST_CASE_P(StringReturn, SqlTest, testing::Values(
    "SELECT COL1 FROM t1;",
    "SELECT COL1 as c1 FROM t1;",
    "SELECT COL1 c1 FROM t1;",
    "SELECT t1.COL1 FROM t1;",
    "SELECT t1.COL1 as c1 FROM t1;",
    "SELECT t1.COL1 c1 FROM t1;"

));

TEST_P(SqlTest, Parser_Select_Expr_List) {
    std::string sqlstr = GetParam();
    SQLNodeList *list = new SQLNodeList();
    int ret = FeSqlParse(sqlstr.c_str(), list);

    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    std::strstream out;
    list->Print(out);
    std::cout << out.str() << std::endl;
}

} // namespace of sql
} // namespace of fedb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



