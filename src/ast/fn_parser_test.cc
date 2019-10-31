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
#include "ast/fn_parser.h"
#include "gtest/gtest.h"

namespace fesql {
namespace node {

class FnASTParserTest : public ::testing::Test {

public:
    FnASTParserTest() {}
    ~FnASTParserTest() {}
};

TEST_F(FnASTParserTest, test) {
    yyscan_t scan;
    int ret0 = fnlex_init(&scan);
    const char* test ="def test(a:i32,b:i32):i32\n    c=a+b\n    return c";
    fn_scan_string(test, scan);
    ::fesql::node::FnNode node;
    int ret = fnparse(scan, &node);

    ASSERT_EQ(0, ret);
    ASSERT_EQ(3, node.children.size());

    ASSERT_EQ(::fesql::node::kFnDef, node.children[0]->type);
    ASSERT_EQ(0, node.children[0]->indent);

    ASSERT_EQ(::fesql::node::kFnAssignStmt, node.children[1]->type);
    ASSERT_EQ(4, node.children[1]->indent);

    ASSERT_EQ(::fesql::node::kFnReturnStmt, node.children[2]->type);
    ASSERT_EQ(4, node.children[2]->indent);

}

} // namespace of ast
} // namespace of fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



