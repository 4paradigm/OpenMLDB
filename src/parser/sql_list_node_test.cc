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
#include <strstream>

namespace fedb {
namespace sql {

class SqlListNodeTest : public ::testing::Test {

public:
    SqlListNodeTest() {}

    ~SqlListNodeTest() {}
};

TEST_F(SqlListNodeTest, PushFrontTest) {
    SQLNodeList *pList = new SQLNodeList();

    ASSERT_EQ(0, pList->Size());

    pList->PushFront(new ConstNode(1));
    pList->PushFront(new ConstNode(2));
    pList->PushFront(new ConstNode(3));

    ASSERT_EQ(3, pList->Size());
    std::strstream out;
    pList->Print(out);
    std::cout << out.str() << std::endl;
    ASSERT_STREQ("[\n"
                     "\tkInt\n"
                     "\t\tvalue: 3\n"
                     "\tkInt\n"
                     "\t\tvalue: 2\n"
                     "\tkInt\n"
                     "\t\tvalue: 1\n"
                     "]",
                 out.str());
}

TEST_F(SqlListNodeTest, AppendNodeListTest) {
    SQLNodeList *pList = new SQLNodeList();
    pList->PushFront(new ConstNode(1));
    ASSERT_EQ(1, pList->Size());

    SQLNodeList *pList2 = new SQLNodeList();
    pList2->PushFront(new ConstNode(2));
    pList2->PushFront(new ConstNode(3));
    ASSERT_EQ(2, pList2->Size());

    pList->AppendNodeList(pList2);
    std::strstream out;
    pList->Print(out);
    std::cout << out.str() << std::endl;
    ASSERT_STREQ("[\n"
                     "\tkInt\n"
                     "\t\tvalue: 1\n"
                     "\tkInt\n"
                     "\t\tvalue: 3\n"
                     "\tkInt\n"
                     "\t\tvalue: 2\n"
                     "]",
                 out.str());

}

} // namespace of base
} // namespace of fedb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



