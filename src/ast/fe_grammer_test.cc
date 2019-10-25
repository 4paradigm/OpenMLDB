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
#include "ast/fe_grammer.h"
#include "gtest/gtest.h"
#include "tao/pegtl/contrib/parse_tree.hpp"
#include "tao/pegtl/contrib/parse_tree_to_dot.hpp"




namespace fesql {
namespace ast {

class FeGrammerTest : public ::testing::Test {

public:
    FeGrammerTest() {}
    ~FeGrammerTest() {}
};


TEST_F(FeGrammerTest, test) {

    std::string fn ="def test(a:i32):";
    ::tao::pegtl::memory_input<> in(fn.c_str(), fn.size(), fn);
    const auto root = ::tao::pegtl::parse_tree::parse<::fesql::ast::grammer>(in);
    ::tao::pegtl::parse_tree::print_dot( std::cout, *root );
}

} // namespace of ast
} // namespace of fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



