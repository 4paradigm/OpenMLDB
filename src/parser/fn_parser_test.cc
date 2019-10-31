/*
 * discrete_test.cc
 * Copyright (C) 2019 wangtaize <wangtaize@wangtaizedeMacBook-Pro-2.local>
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
#include <iostream>
#include "parser.h"
#include "gtest/gtest.h"

namespace fesql {
namespace parser {

class FnASTParserTest : public ::testing::Test {

public:
    FnASTParserTest() {
        manager_ = new NodeManager();
        parser_ = new FeSQLParser();
    }
    ~FnASTParserTest() {
        delete manager_;
        delete parser_;
    }
protected:
    NodeManager *manager_;
    FeSQLParser *parser_;
};

TEST_F(FnASTParserTest, test) {
    NodePointVector trees;
    const std::string test = "def test(x:i32,y:i32):i32\n    c=x+y\n    return c";
    int ret = parser_->parse(sqlstr.c_str(), trees, manager_);

    std::cout << trees.front() << std::endl;
    node::FnNode * node = trees.front();
    ASSERT_EQ(0, ret);
    ASSERT_EQ(3, node->children.size());

    ASSERT_EQ(::fesql::node::kFnDef, node->children[0]->type);
    ASSERT_EQ(0, node->children[0]->indent);

    ASSERT_EQ(::fesql::node::kFnAssignStmt, node->children[1]->type);
    ASSERT_EQ(4, node->children[1]->indent);

    ASSERT_EQ(::fesql::node::kFnReturnStmt, node->children[2]->type);
    ASSERT_EQ(4, node->children[2]->indent);

}

} // namespace of parser
} // namespace of fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



