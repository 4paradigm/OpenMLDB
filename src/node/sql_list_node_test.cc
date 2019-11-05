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
#include "gtest/gtest.h"
#include "sql_node.h"
#include "node_manager.h"

namespace fesql {
namespace node {

class SqlListNodeTest : public ::testing::Test {

public:
    SqlListNodeTest() {
        manager_ = new NodeManager();
    }

    ~SqlListNodeTest() {
        delete manager_;
    }
protected:
    NodeManager * manager_;
};

TEST_F(SqlListNodeTest, PushFrontTest) {
    SQLNodeList *pList = manager_->MakeNodeList();

    ASSERT_EQ(0, pList->GetSize());

    pList->PushFront(manager_->MakeLinkedNode(manager_->MakeConstNode(1)));
    pList->PushFront(manager_->MakeLinkedNode(manager_->MakeConstNode(2)));
    pList->PushFront(manager_->MakeLinkedNode(manager_->MakeConstNode(3)));

    ASSERT_EQ(3, pList->GetSize());
    std::cout << *pList << std::endl;
}

TEST_F(SqlListNodeTest, AppendNodeListTest) {
    SQLNodeList *pList = manager_->MakeNodeList();
    pList->PushFront(manager_->MakeLinkedNode(manager_->MakeConstNode(1)));
    ASSERT_EQ(1, pList->GetSize());

    SQLNodeList *pList2 = manager_->MakeNodeList();
    pList2->PushFront(manager_->MakeLinkedNode(manager_->MakeConstNode(2)));
    pList2->PushFront(manager_->MakeLinkedNode(manager_->MakeConstNode(3)));
    ASSERT_EQ(2, pList2->GetSize());

    pList->AppendNodeList(pList2);
    std::cout << *pList << std::endl;


}

} // namespace of base
} // namespace of fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



