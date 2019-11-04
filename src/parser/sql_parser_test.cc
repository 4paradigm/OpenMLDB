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
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "parser/parser.h"
#include "gtest/gtest.h"
#include <strstream>

namespace fesql {
namespace parser {
using fesql::node::NodeManager;
using fesql::node::NodePointVector;
using fesql::node::SQLNode;
// TODO: add ut: 检查SQL的语法树节点预期 2019.10.23
class SqlParserTest : public ::testing::TestWithParam<std::string> {

public:
    SqlParserTest() {
        manager_ = new NodeManager();
        parser_ = new FeSQLParser();
    }

    ~SqlParserTest() {
        delete parser_;
        delete manager_;
    }
protected:
    NodeManager *manager_;
    FeSQLParser *parser_;
};
INSTANTIATE_TEST_CASE_P(StringReturn, SqlParserTest, testing::Values(
    "SELECT COL1 FROM t1;",
    "SELECT COL1 as c1 FROM t1;",
    "SELECT COL1 c1 FROM t1;",
    "SELECT t1.COL1 FROM t1;",
    "SELECT t1.COL1 as c1 FROM t1;",
    "SELECT t1.COL1 c1 FROM t1;",
    "SELECT t1.COL1 c1 FROM t1 limit 10;",
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

INSTANTIATE_TEST_CASE_P(UDFParse, SqlParserTest, testing::Values(
    // simple udf
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend",
    // newlines test
    "%%fun\n\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n",
    "%%fun\n\ndef test(x:i32,y:i32):i32\n\n\n    c=x+y\n    return c\n\n\n\n\nend\n",
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n\n",
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n\n    return c\nend",
    // multi def fun
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend",
    // multi def fun with newlines
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n",
    "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return d\nend"

));

INSTANTIATE_TEST_CASE_P(SQLAndUDFParse, SqlParserTest, testing::Values(
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n%%sql\nSELECT COUNT(COL1) FROM t1;",
    "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n"
        "%%sql\nSELECT COL1, trim(COL2), TS, AVG(AMT) OVER w, SUM(AMT) OVER w FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY TS ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING);"

));

TEST_P(SqlParserTest, Parser_Select_Expr_List) {
    std::string sqlstr = GetParam();
    std::cout << sqlstr << std::endl;

    NodePointVector trees;
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_);

    ASSERT_EQ(0, ret);
//    ASSERT_EQ(1, trees.size());
    std::cout << trees.front() << std::endl;
}

} // namespace of parser
} // namespace of fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



