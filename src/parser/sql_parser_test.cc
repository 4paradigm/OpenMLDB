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
#include <utility>
#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "parser/parser.h"

namespace fesql {
namespace parser {
using fesql::node::NodeManager;
using fesql::node::NodePointVector;
using fesql::node::SQLNode;

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

INSTANTIATE_TEST_CASE_P(
    SqlParse, SqlParserTest,
    testing::Values(
        "SELECT COL1 FROM t1;", "SELECT COL1 as c1 FROM t1;",
        "SELECT COL1 c1 FROM t1;", "SELECT t1.COL1 FROM t1;",
        "SELECT t1.COL1 as c1 FROM t1;", "SELECT t1.COL1 c1 FROM t1;",
        "SELECT t1.COL1 c1 FROM t1 limit 10;", "SELECT * FROM t1;",
        "SELECT COUNT(*) FROM t1;", "SELECT COUNT(COL1) FROM t1;",
        "SELECT TRIM(COL1) FROM t1;", "SELECT trim(COL1) as trim_col1 FROM t1;",
        "SELECT MIN(COL1) FROM t1;", "SELECT min(COL1) FROM t1;",
        "SELECT MAX(COL1) FROM t1;", "SELECT max(COL1) as max_col1 FROM t1;",
        "SELECT SUM(COL1) FROM t1;", "SELECT sum(COL1) as sum_col1 FROM t1;",
        "SELECT COL1, COL2, `TS`, AVG(AMT) OVER w, SUM(AMT) OVER w FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN UNBOUNDED PRECEDING AND "
        "UNBOUNDED FOLLOWING);",
        "SELECT COL1, trim(COL2), `TS`, AVG(AMT) OVER w, SUM(AMT) OVER w FROM "
        "t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT COL1, SUM(AMT) OVER w as w_amt_sum FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT COL1 + COL2 as col12 FROM t1;",
        "SELECT COL1 - COL2 as col12 FROM t1;",
        "SELECT COL1 * COL2 as col12 FROM t1;",
        "SELECT COL1 / COL2 as col12 FROM t1;",
        "SELECT COL1 % COL2 as col12 FROM t1;",
        "SELECT COL1 = COL2 as col12 FROM t1;",
        "SELECT COL1 == COL2 as col12 FROM t1;",
        "SELECT COL1 < COL2 as col12 FROM t1;",
        "SELECT COL1 > COL2 as col12 FROM t1;",
        "SELECT COL1 <= COL2 as col12 FROM t1;",
        "SELECT COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 && COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 and COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 || COL1 != COL2 as col12 FROM t1;",
        "SELECT COL1 >= COL2 or COL1 != COL2 as col12 FROM t1;",
        "SELECT !(COL1 >= COL2 or COL1 != COL2) as col12 FROM t1;",

        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col15 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "SELECT COUNT(*) FROM t1;"));

INSTANTIATE_TEST_CASE_P(
    SqlDistinctParse, SqlParserTest,
    testing::Values(
        "SELECT distinct COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20;",
        "SELECT DISTINCT sum(COL1) as col1sum, * FROM t1 group by COL1, COL2;",
        "SELECT DISTINCT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col15 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "SELECT DISTINCT COUNT(*) FROM t1;",
        "SELECT distinct COL1 FROM t1 where COL1+COL2;",
        "SELECT DISTINCT COL1 FROM t1 where COL1 > 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlWhereParse, SqlParserTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL1+COL2;",
        "SELECT COL1 FROM t1 where COL1;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 where COL1 > 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlLikeParse, SqlParserTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL like \"%abc\";",
        "SELECT COL1 FROM t1 where COL1 like '%123';",
        "SELECT COL1 FROM t1 where COL not like \"%abc\";",
        "SELECT COL1 FROM t1 where COL1 not like '%123';",
        "SELECT COL1 FROM t1 where COL1 not like 10;",
        "SELECT COL1 FROM t1 where COL1 like 10;"
        ));

INSTANTIATE_TEST_CASE_P(
    SqlInParse, SqlParserTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL in (1, 2, 3, 4, 5);",
        "SELECT COL1 FROM t1 where COL1 in (\"abc\", \"xyz\", \"test\");",
        "SELECT COL1 FROM t1 where COL1 not in (1,2,3,4,5);"));

INSTANTIATE_TEST_CASE_P(
    SqlGroupParse, SqlParserTest,
    testing::Values(
        "SELECT distinct sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10;",
        "SELECT sum(COL1) as col1sum, * FROM t1 group by COL1, COL2;",
        "SELECT COL1 FROM t1 group by COL1+COL2;",
        "SELECT COL1 FROM t1 group by COL1;",
        "SELECT COL1 FROM t1 group by COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 group by COL1, COL2;",
        "SELECT COL1 FROM t1 group by COL1;"));

INSTANTIATE_TEST_CASE_P(
    SqlHavingParse, SqlParserTest,
    testing::Values(
        "SELECT COL1 FROM t1 having COL1+COL2;",
        "SELECT COL1 FROM t1 having COL1;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlOrderParse, SqlParserTest,
    testing::Values("SELECT COL1 FROM t1 order by COL1 + COL2 - COL3;",
                    "SELECT COL1 FROM t1 order by COL1, COL2, COL3;",
                    "SELECT COL1 FROM t1 order by COL1, COL2;",
                    "SELECT COL1 FROM t1 order by COL1;"));
INSTANTIATE_TEST_CASE_P(
    SqlWhereGroupHavingOrderParse, SqlParserTest,
    testing::Values(
        "SELECT sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10;",
        "SELECT sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1 limit 10;",
        "SELECT sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 limit 10;",
        "SELECT sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0;",
        "SELECT sum(COL1) as col1sum, * FROM t1 group by COL1, COL2 having "
        "sum(COL1) > 0;",
        "SELECT sum(COL1) as col1sum, * FROM t1 group by COL1, COL2 having "
        "col1sum > 0;"));

INSTANTIATE_TEST_CASE_P(
    SqlJoinParse, SqlParserTest,
    testing::Values("SELECT * FROM t1 full join t2 on t1.col1 = t2.col2;",
                    "SELECT * FROM t1 left join t2 on t1.col1 = t2.col2;",
                    "SELECT * FROM t1 right join t2 on t1.col1 = t2.col2;",
                    "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col2;"));

INSTANTIATE_TEST_CASE_P(
    SqlUnionParse, SqlParserTest,
    testing::Values(
        "SELECT * FROM t1 UNION SELECT * FROM t2;",
        "SELECT * FROM t1 UNION DISTINCT SELECT * FROM t2;",
        "SELECT * FROM t1 UNION ALL SELECT * FROM t2;",
        "SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION SELECT * FROM t3;",
        "SELECT * FROM t1 left join t2 on t1.col1 = t2.col2 UNION ALL SELECT * "
        "FROM t3 UNION SELECT * FROM t4;",
        "SELECT sum(COL1) as col1sum, * FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10 UNION ALL "
        "SELECT sum(COL1) as col1sum, * FROM t1 group by COL1, COL2 having "
        "sum(COL1) > 0;",
        "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col2 UNION "
        "SELECT * FROM t3 inner join t4 on t3.col1 = t4.col2 UNION "
        "SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;"));

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryParse, SqlParserTest,
    testing::Values(
        "SELECT * FROM t1 WHERE COL1 > (select avg(COL1) from t1) limit 10;",
        "select * from (select * from t1 where col1>0);",
        "SELECT LastName,FirstName, Title, Salary FROM Employees AS T1 "
        "WHERE Salary >=(SELECT Avg(Salary) "
        "FROM Employees WHERE T1.Title = Employees.Title) Order by Title;",
        "select * from \n"
        "    (select * from stu where grade = 7) s\n"
        "left join \n"
        "    (select * from sco where subject = \"math\") t\n"
        "on s.id = t.stu_id\n"
        "union\n"
        "select distinct * from \n"
        "    (select distinct * from stu where grade = 7) s\n"
        "right join \n"
        "    (select distinct * from sco where subject = \"math\") t\n"
        "on s.id = t.stu_id;",
        "SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;",
        "select * from \n"
        "    (select * from stu where grade = 7) s\n"
        "left join \n"
        "    (select * from sco where subject = \"math\") t\n"
        "on s.id = t.stu_id\n"
        "union\n"
        "select * from \n"
        "    (select * from stu where grade = 7) s\n"
        "right join \n"
        "    (select * from sco where subject = \"math\") t\n"
        "on s.id = t.stu_id;",
        "SELECT * FROM t5 inner join t6 on t5.col1 = t6.col2;"));


INSTANTIATE_TEST_CASE_P(
    UDFParse, SqlParserTest,
    testing::Values(
        // simple udf
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend",
        // newlines test
        "%%fun\n\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n",
        "%%fun\n\ndef test(x:i32,y:i32):i32\n\n\n    c=x+y\n    return "
        "c\n\n\n\n\nend\n",
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n\n",
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n\n    return c\nend",
        // multi def fun
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\ndef "
        "test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend",
        // multi def fun with newlines
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n\ndef "
        "test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n",
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend",
        // 变量赋值
        "%%fun\ndef test(x:i32,y:i32):i32\n    result=1\n    c=x+y\n"
        "    result=c\n    return result\nend",
        "%%fun\ndef test(x:i32,y:i32):i32\n\tresult=1\n\tc=x+y\n"
        "\tresult=c\n\treturn result\nend",
        "%%fun\ndef test(x:i32,y:i32):i32\n    result = 1\n\tc = x + y\n"
        "\tresult=c\n\treturn result\nend",
        "%%fun\ndef test(x:i32,y:i32):i32\n\tresult=1\n\tc = x+y\n"
        "\tresult=c\n\treturn result\nend",
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    if x > 1\n"
        "    \tc=x+y\n"
        "    elif y >1\n"
        "    \tc=x-y\n"
        "    else\n"
        "    \tc=x*y\n"
        "    return c\n"
        "end",
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    if x > 1\n"
        "    \tc=x+y\n"
        "    elif y >1\n"
        "    \tif x-y >0\n"
        "    \t\tc=x-y\n"
        "    \telif x-y <0\n"
        "    \t\tc = y-x\n"
        "    \telse\n"
        "    \t\tc = 9999\n"
        "    else\n"
        "    \tif x < -100\n"
        "    \t\tc = x+100\n"
        "    \telif y < -100\n"
        "    \t\tc = y+100\n"
        "    \telse\n"
        "    \t\tc=x*y\n"
        "    return c\n"
        "end",
        // call function in udf
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    c = test_at(x,y)\n"
        "    return c\n"
        "end"));

INSTANTIATE_TEST_CASE_P(
    ListOpParse, SqlParserTest,
    testing::Values("%%fun\n"
                    "def test(col:list<float>):float\n"
                    "\tsum = col[0]\n"
                    "\treturn sum\n"
                    "end",
                    "%%fun\n"
                    "def test(col:list<float>, pos:i32):float\n"
                    "\tsum = col[pos]\n"
                    "\treturn sum\n"
                    "end",
                    "%%fun\n"
                    "def test(col:list<float>, pos:i32):float\n"
                    "\tsum = col[pos]\n"
                    "\treturn sum\n"
                    "end"));

INSTANTIATE_TEST_CASE_P(IfElseParse, SqlParserTest,
                        testing::Values("%%fun\n"
                                        "def test(x:i32,y:i32):i32\n"
                                        "    if x > 1\n"
                                        "    \tc=x+y\n"
                                        "    elif y >1\n"
                                        "    \tc=x-y\n"
                                        "    else\n"
                                        "    \tc=x*y\n"
                                        "    return c\n"
                                        "end",
                                        "%%fun\n"
                                        "def test(x:i32,y:i32):i32\n"
                                        "    if x > 1\n"
                                        "    \tc=x+y\n"
                                        "    elif y >1\n"
                                        "    \tif x-y >0\n"
                                        "    \t\tc=x-y\n"
                                        "    \telif x-y <0\n"
                                        "    \t\tc = y-x\n"
                                        "    \telse\n"
                                        "    \t\tc = 9999\n"
                                        "    else\n"
                                        "    \tif x < -100\n"
                                        "    \t\tc = x+100\n"
                                        "    \telif y < -100\n"
                                        "    \t\tc = y+100\n"
                                        "    \telse\n"
                                        "    \t\tc=x*y\n"
                                        "    return c\n"
                                        "end"));

INSTANTIATE_TEST_CASE_P(LoopParse, SqlParserTest,
                        testing::Values("%%fun\n"
                                        "def test_sum(col:list<i32>):i32\n"
                                        "\tres = 0\n"
                                        "\tfor x in col\n"
                                        "\t\tres = res + x\n"
                                        "\treturn res\n"
                                        "end",
                                        "%%fun\n"
                                        "def test(l:list<i32>, a:i32):i32\n"
                                        "    sum=0\n"
                                        "    for x in l\n"
                                        "        if x > a\n"
                                        "            sum = sum + x\n"
                                        "    return sum\n"
                                        "end"));

INSTANTIATE_TEST_CASE_P(
    SQLCreate, SqlParserTest,
    testing::Values("create table test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL\n"
                    ");",
                    "create table IF NOT EXISTS test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL\n"
                    ");",
                    "create table test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL,\n"
                    "    index(key=(column4))\n"
                    ");",
                    "create table test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL,\n"
                    "    index(key=(column4, column3))\n"
                    ");",
                    "create table test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL,\n"
                    "    index(key=(column4, column3), ts=column5)\n"
                    ");",
                    "create table test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL,\n"
                    "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
                    ");",
                    "create table test(\n"
                    "    column1 int NOT NULL,\n"
                    "    column2 timestamp NOT NULL,\n"
                    "    column3 int NOT NULL,\n"
                    "    column4 string NOT NULL,\n"
                    "    column5 int NOT NULL,\n"
                    "    index(key=(column4, column3), version=(column5, 3), "
                    "ts=column2, ttl=60d)\n"
                    ");"));

INSTANTIATE_TEST_CASE_P(
    SQLInsert, SqlParserTest,
    testing::Values("insert into t1 values(1, 2, 3.1, \"string\");",
                    "insert into t1 (col1, col2, col3, col4) values(1, 2, 3.1, "
                    "\"string\");"));

INSTANTIATE_TEST_CASE_P(SQLCmdParserTest, SqlParserTest,
                        testing::Values("CREATE DATABASE db1;",
                                        "CREATE TABLE \"schema.sql\";",
                                        "CREATE GROUP group1;", "DESC t1;",
                                        "SHOW TABLES;", "SHOW DATABASES;"));

INSTANTIATE_TEST_CASE_P(
    SQLAndUDFParse, SqlParserTest,
    testing::Values(
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return "
        "c\nend\n%%sql\nSELECT COUNT(COL1) FROM t1;",
        "%%fun\ndef test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\ndef "
        "test(x:i32,y:i32):i32\n    c=x+y\n    return c\nend\n"
        "%%sql\nSELECT COL1, trim(COL2), `TS`, AVG(AMT) OVER w, SUM(AMT) OVER "
        "w FROM t \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);"));

TEST_P(SqlParserTest, Parser_Select_Expr_List) {
    std::string sqlstr = GetParam();
    std::cout << sqlstr << std::endl;

    NodePointVector trees;
    base::Status status;
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_, status);

    if (0 != status.code) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(0, ret);
    //    ASSERT_EQ(1, trees.size());
    std::cout << *(trees.front()) << std::endl;
}

TEST_F(SqlParserTest, Assign_Op_Test) {
    std::string sqlstr =
        "%%fun\n"
        "def test(a:int, b:int):int\n"
        "\tres= 0\n"
        "\tres += a\n"
        "\tres -= b\n"
        "\tres *= a\n"
        "\tres /= b\n"
        "\treturn res\n"
        "end";
    std::cout << sqlstr << std::endl;

    NodePointVector trees;
    base::Status status;
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_, status);

    if (0 != status.code) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, trees.size());
    std::cout << *(trees.front()) << std::endl;
    ASSERT_EQ(fesql::node::kFnDef, trees[0]->GetType());
    node::FnNodeFnDef *fn_def = dynamic_cast<node::FnNodeFnDef *>(trees[0]);
    ASSERT_EQ(6u, fn_def->block_->children.size());

    {
        ASSERT_EQ(fesql::node::kFnAssignStmt,
                  fn_def->block_->children[0]->GetType());
        fesql::node::FnAssignNode *assign =
            dynamic_cast<fesql::node::FnAssignNode *>(
                fn_def->block_->children[0]);
        ASSERT_EQ(fesql::node::kExprPrimary,
                  assign->expression_->GetExprType());
    }
    {
        ASSERT_EQ(fesql::node::kFnAssignStmt,
                  fn_def->block_->children[1]->GetType());
        fesql::node::FnAssignNode *assign =
            dynamic_cast<fesql::node::FnAssignNode *>(
                fn_def->block_->children[1]);
        ASSERT_EQ(fesql::node::kExprBinary, assign->expression_->GetExprType());
        const fesql::node::BinaryExpr *expr =
            dynamic_cast<const fesql::node::BinaryExpr *>(assign->expression_);
        ASSERT_EQ(fesql::node::kFnOpAdd, expr->GetOp());
    }
    {
        ASSERT_EQ(fesql::node::kFnAssignStmt,
                  fn_def->block_->children[2]->GetType());
        fesql::node::FnAssignNode *assign =
            dynamic_cast<fesql::node::FnAssignNode *>(
                fn_def->block_->children[2]);
        ASSERT_EQ(fesql::node::kExprBinary, assign->expression_->GetExprType());
        const fesql::node::BinaryExpr *expr =
            dynamic_cast<const fesql::node::BinaryExpr *>(assign->expression_);
        ASSERT_EQ(fesql::node::kFnOpMinus, expr->GetOp());
    }
    {
        ASSERT_EQ(fesql::node::kFnAssignStmt,
                  fn_def->block_->children[3]->GetType());
        fesql::node::FnAssignNode *assign =
            dynamic_cast<fesql::node::FnAssignNode *>(
                fn_def->block_->children[3]);
        ASSERT_EQ(fesql::node::kExprBinary, assign->expression_->GetExprType());
        const fesql::node::BinaryExpr *expr =
            dynamic_cast<const fesql::node::BinaryExpr *>(assign->expression_);
        ASSERT_EQ(fesql::node::kFnOpMulti, expr->GetOp());
    }
    {
        ASSERT_EQ(fesql::node::kFnAssignStmt,
                  fn_def->block_->children[4]->GetType());
        fesql::node::FnAssignNode *assign =
            dynamic_cast<fesql::node::FnAssignNode *>(
                fn_def->block_->children[4]);
        ASSERT_EQ(fesql::node::kExprBinary, assign->expression_->GetExprType());
        const fesql::node::BinaryExpr *expr =
            dynamic_cast<const fesql::node::BinaryExpr *>(assign->expression_);
        ASSERT_EQ(fesql::node::kFnOpFDiv, expr->GetOp());
    }
}
TEST_F(SqlParserTest, Parser_Insert_ALL_Stmt) {
    const std::string sqlstr =
        "insert into t1 values(1, 2.3, 3.1, \"string\");";
    NodePointVector trees;
    base::Status status;
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_, status);

    if (0 != ret) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, trees.size());
    std::cout << *(trees.front()) << std::endl;
    ASSERT_EQ(node::kInsertStmt, trees[0]->GetType());
    node::InsertStmt *insert_stmt = dynamic_cast<node::InsertStmt *>(trees[0]);

    ASSERT_EQ(true, insert_stmt->is_all_);
    ASSERT_EQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[0])->GetInt(), 1);
    ASSERT_EQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[1])->GetDouble(),
        2.3);
    ASSERT_EQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[2])->GetDouble(),
        3.1);
    ASSERT_STREQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[3])->GetStr(),
        "string");
}

TEST_F(SqlParserTest, Parser_Insert_Stmt) {
    const std::string sqlstr =
        "insert into t1(col1, c2, column3, item4) values(1, 2.3, 3.1, "
        "\"string\");";
    NodePointVector trees;
    base::Status status;
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_, status);

    if (0 != ret) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, trees.size());
    std::cout << *(trees.front()) << std::endl;
    ASSERT_EQ(node::kInsertStmt, trees[0]->GetType());
    node::InsertStmt *insert_stmt = dynamic_cast<node::InsertStmt *>(trees[0]);

    ASSERT_EQ(false, insert_stmt->is_all_);
    ASSERT_EQ(std::vector<std::string>({"col1", "c2", "column3", "item4"}),
              insert_stmt->columns_);
    ASSERT_EQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[0])->GetInt(), 1);
    ASSERT_EQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[1])->GetDouble(),
        2.3);
    ASSERT_EQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[2])->GetDouble(),
        3.1);
    ASSERT_STREQ(
        dynamic_cast<node::ConstNode *>(insert_stmt->values_[3])->GetStr(),
        "string");
    //
    //
    //    ASSERT_EQ(false, insert_stmt->is_all_);
    //    ASSERT_EQ(std::vector<std::string>({"col1", "col2", "col3"}),
    //              insert_stmt->columns_);
    //
    //    ASSERT_EQ(dynamic_cast<ConstNode*>(insert_stmt->values_[0])->GetInt(),
    //    1);
    //    ASSERT_EQ(dynamic_cast<ConstNode*>(insert_stmt->values_[1])->GetFloat(),
    //    2.3f);
    //    ASSERT_EQ(dynamic_cast<ConstNode*>(insert_stmt->values_[2])->GetDouble(),
    //    2.3);
}

TEST_F(SqlParserTest, Parser_Create_Stmt) {
    const std::string sqlstr =
        "create table IF NOT EXISTS test(\n"
        "    column1 int NOT NULL,\n"
        "    column2 timestamp NOT NULL,\n"
        "    column3 int NOT NULL,\n"
        "    column4 string NOT NULL,\n"
        "    column5 int,\n"
        "    index(key=(column4, column3), version=(column5, 3), "
        "ts=column2, "
        "ttl=60d)\n"
        ");";
    NodePointVector trees;
    base::Status status;
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_, status);

    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, trees.size());
    std::cout << *(trees.front()) << std::endl;

    ASSERT_EQ(node::kCreateStmt, trees.front()->GetType());
    node::CreateStmt *createStmt = (node::CreateStmt *)trees.front();

    ASSERT_EQ("test", createStmt->GetTableName());
    ASSERT_EQ(true, createStmt->GetOpIfNotExist());

    ASSERT_EQ(6u, createStmt->GetColumnDefList().size());

    ASSERT_EQ("column1",
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[0]))
                  ->GetColumnName());
    ASSERT_EQ(node::kInt32,
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[0]))
                  ->GetColumnType());
    ASSERT_EQ(true, ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[0]))
                        ->GetIsNotNull());

    ASSERT_EQ("column2",
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[1]))
                  ->GetColumnName());
    ASSERT_EQ(node::kTimestamp,
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[1]))
                  ->GetColumnType());
    ASSERT_EQ(true, ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[1]))
                        ->GetIsNotNull());

    ASSERT_EQ("column3",
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[2]))
                  ->GetColumnName());
    ASSERT_EQ(node::kInt32,
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[2]))
                  ->GetColumnType());
    ASSERT_EQ(true, ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[2]))
                        ->GetIsNotNull());

    ASSERT_EQ("column4",
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[3]))
                  ->GetColumnName());
    ASSERT_EQ(node::kVarchar,
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[3]))
                  ->GetColumnType());
    ASSERT_EQ(true, ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[3]))
                        ->GetIsNotNull());

    ASSERT_EQ("column5",
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[4]))
                  ->GetColumnName());
    ASSERT_EQ(node::kInt32,
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[4]))
                  ->GetColumnType());
    ASSERT_EQ(false,
              ((node::ColumnDefNode *)(createStmt->GetColumnDefList()[4]))
                  ->GetIsNotNull());

    ASSERT_EQ(node::kColumnIndex,
              (createStmt->GetColumnDefList()[5])->GetType());
    node::ColumnIndexNode *index_node =
        (node::ColumnIndexNode *)(createStmt->GetColumnDefList()[5]);
    std::vector<std::string> key;
    key.push_back("column4");
    key.push_back("column3");
    ASSERT_EQ(key, index_node->GetKey());

    ASSERT_EQ("column2", index_node->GetTs());
    ASSERT_EQ("column5", index_node->GetVersion());
    ASSERT_EQ(3, index_node->GetVersionCount());
    ASSERT_EQ(60 * 86400000L, index_node->GetTTL());
}

class SqlParserErrorTest : public ::testing::TestWithParam<
                               std::pair<common::StatusCode, std::string>> {
 public:
    SqlParserErrorTest() {
        manager_ = new NodeManager();
        parser_ = new FeSQLParser();
    }

    ~SqlParserErrorTest() {
        delete parser_;
        delete manager_;
    }

 protected:
    NodeManager *manager_;
    FeSQLParser *parser_;
};

// TODO(chenjing): line and column check
TEST_P(SqlParserErrorTest, ParserErrorStatusTest) {
    auto param = GetParam();
    std::cout << param.second << std::endl;

    NodePointVector trees;
    base::Status status;
    int ret = parser_->parse(param.second.c_str(), trees, manager_, status);
    ASSERT_EQ(1, ret);
    ASSERT_EQ(0u, trees.size());
    ASSERT_EQ(param.first, status.code);
    std::cout << status.msg << std::endl;
}

INSTANTIATE_TEST_CASE_P(
    SQLErrorParse, SqlParserErrorTest,
    testing::Values(std::make_pair(common::kSQLError, "SELECT SUM(*) FROM t1;"),
                    std::make_pair(common::kSQLError, "SELECT t1;")));

INSTANTIATE_TEST_CASE_P(
    UDFErrorParse, SqlParserErrorTest,
    testing::Values(
        std::make_pair(common::kSQLError,
                       "%%fun\ndefine test(x:i32,y:i32):i32\n    c=x+y\n    "
                       "return c\nend"),
        std::make_pair(common::kSQLError,
                       "%%fun\ndef 123test(x:i32,y:i32):i32\n    c=x+y\n    "
                       "return c\nend"),
        std::make_pair(common::kSQLError,
                       "%%fun\ndef test(x:i32,y:i32):i32\n    c=x)(y\n    "
                       "return c\nend"),
        std::make_pair(common::kSQLError, "SELECT t1;")));

}  // namespace parser
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
