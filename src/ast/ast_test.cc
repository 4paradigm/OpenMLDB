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
#include "ast/parser.h"
#include "gtest/gtest.h"

namespace cxx {
namespace ast {

class ASTTest : public ::testing::Test {

public:
    ASTTest() {}
    ~ASTTest() {}
};

TEST_F(ASTTest, test) {
    yyscan_t scan;
    int ret0 = yylex_init(&scan);
    const char* test ="a=discrete(xxxx)\nb=discrete(xxxx)";
    yy_scan_string(test, scan);
    ::cxx::ast::TreeNode node;
    int ret = yyparse(scan, &node);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, node.children.size());
    ::cxx::ast::TreeNode* tn = node.children[0];
    ASSERT_EQ(2, tn->children.size());
    ::cxx::ast::VarNode* vn = (::cxx::ast::VarNode*)tn->children[0];
    std::string expect = "a";
    std::string target(vn->name);
    ASSERT_EQ(2, vn->type);
    ASSERT_EQ(expect, target);
    ::cxx::ast::FnCallNode* fn = (::cxx::ast::FnCallNode*)tn->children[1];
    std::string fn_name_expect("discrete");
    std::string target_fn_name(fn->name);
    ASSERT_EQ(fn_name_expect, target_fn_name);
    ASSERT_EQ(fn->children.size(), 1);
}

} // namespace of ast
} // namespace of cxx

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



