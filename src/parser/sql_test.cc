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

class SqlTest : public ::testing::Test {

public:
    SqlTest() {}

    ~SqlTest() {}
};

TEST_F(SqlTest, Parser_Simple_Sql) {

    const char *sqlstr = "SELECT t1.COL1 AS c1, t2.COL2 AS c2 FROM t1;";
    SQLNodeList *list = new SQLNodeList();
    int ret = FeSqlParse(sqlstr, list);


    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    std::strstream out;
    list->Print(out);
    ASSERT_STREQ("[\n"
                     "\tkSelectStmt\n"
                     "\t\tselect_list: \n"
                     "\t\t\t[\n"
                     "\t\t\t\tkResTarget\n"
                     "\t\t\t\t\tval: \n"
                     "\t\t\t\t\t\tkColumn\n"
                     "\t\t\t\t\t\t\tcolumn_ref: {relation_name: t2, column_name: COL2}\n"
                     "\t\t\t\t\tname: \n"
                     "\t\t\t\t\t\tc2\n"
                     "\t\t\t\tkResTarget\n"
                     "\t\t\t\t\tval: \n"
                     "\t\t\t\t\t\tkColumn\n"
                     "\t\t\t\t\t\t\tcolumn_ref: {relation_name: t1, column_name: COL1}\n"
                     "\t\t\t\t\tname: \n"
                     "\t\t\t\t\t\tc1\n"
                     "\t\t\t]\n"
                     "\t\ttableref_list_: \n"
                     "\t\t\t[\n"
                     "\t\t\t\tkTable\n"
                     "\t\t\t\t\ttable: t1, alias: \n"
                     "\t\t\t]\n"
                     "\t\twhere_clause_: NULL\n"
                     "\t\tgroup_clause_: NULL\n"
                     "\t\thaving_clause_: NULL\n"
                     "\t\torder_clause_: NULL\n"
                     "\n"
                     "]"
    , out.str());
    std::cout << out.str() << std::endl;

}

} // namespace of sql
} // namespace of fedb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



