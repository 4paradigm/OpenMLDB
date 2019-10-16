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
#include "gtest/gtest.h"

namespace fedb {
namespace sql {

class SqlNodeTest : public ::testing::Test {

public:
    SqlNodeTest() {}

    ~SqlNodeTest() {}
};

TEST_F(SqlNodeTest, MakeNode) {
    using namespace std;
    SQLNode * node = fedb::sql::MakeNode(kTable);
    TableNode* columnnode = (TableNode*)node;

}

TEST_F(SqlNodeTest, MakeColumnRefNodeTest) {
    using namespace std;
    SQLNode * node = MakeColumnRefNode("col", "t");
    ColumnRefNode * columnnode = (ColumnRefNode*)node;
    ASSERT_EQ(kColumn, columnnode->type_);
    ASSERT_EQ("t", columnnode->GetRelationName());
    ASSERT_EQ("col", columnnode->GetColumnName());

}

} // namespace of base
} // namespace of fedb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



