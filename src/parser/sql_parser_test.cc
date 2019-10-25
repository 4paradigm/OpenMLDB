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

namespace fesql {
namespace parser {

// TODO: add ut: 检查SQL的语法树节点预期 2019.10.23
class SqlParserTest : public ::testing::TestWithParam<std::string> {

public:
    SqlParserTest() {}

    ~SqlParserTest() {}
};
INSTANTIATE_TEST_CASE_P(StringReturn, SqlParserTest, testing::Values(
    "SELECT COL1 FROM t1;",
    "SELECT COL1 as c1 FROM t1;",
    "SELECT COL1 c1 FROM t1;",
    "SELECT t1.COL1 FROM t1;",
    "SELECT t1.COL1 as c1 FROM t1;",
    "SELECT t1.COL1 c1 FROM t1;",
    "SELECT * FROM t1;",
    "SELECT COUNT(*) FROM t1;",
    "SELECT COUNT(COL1) FROM t1;",
    "SELECT TRIM(COL1) FROM t1;",
    "SELECT trim(COL1) as trim_col1 FROM t1;",
    "SELECT MIN(COL1) FROM t1;",
    "SELECT min(COL1) FROM t1;",
    "SELECT MAX(COL1) FROM t1;",
    "SELECT max(COL1) as max_col1 FROM t1;",
    "SELECT SUM(COL1) FROM t1;",
    "SELECT sum(COL1) as sum_col1 FROM t1;",
    "SELECT COL1, COL2, TS, AVG(AMT) OVER w, SUM(AMT) OVER w FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY TS ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);",

    "SELECT COL1, trim(COL2), TS, AVG(AMT) OVER w, SUM(AMT) OVER w FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY TS ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING);"

));

TEST_P(SqlParserTest, Parser_Select_Expr_List) {
    std::string sqlstr = GetParam();
    SQLNodeList *list = new SQLNodeList();
    std::cout << sqlstr << std::endl;
    int ret = FeSqlParse(sqlstr.c_str(), list);

    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    std::cout << *list << std::endl;
}

} // namespace of parser
} // namespace of fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



