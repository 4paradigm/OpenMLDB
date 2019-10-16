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

namespace fedb {
namespace sql {

class SqlTest : public ::testing::Test {

public:
    SqlTest() {}

    ~SqlTest() {}
};


TEST_F(SqlTest, Parser_Simple_Sql) {

    using namespace fedb::sql;
    const char *sqlstr = "SELECT COL1 AS c1, COL2 AS c2 FROM t1;";
    SQLNodeList *list = new SQLNodeList();
    int ret = FeSqlParse(sqlstr, list);

    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    SQLNode *node = list->head_->node_;
    ASSERT_EQ(kSelectStmt, node->type_);
    std::cout << *node << std::endl;

}

} // namespace of base
} // namespace of fedb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



