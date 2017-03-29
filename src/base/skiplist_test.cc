//
// skip_list_test.cc
// Copyright 2017 elasticlog <elasticlog01@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "base/skiplist.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class NodeTest : public ::testing::Test {

public:
    NodeTest(){}
    ~NodeTest() {}
};



TEST_F(NodeTest, SetNext) {
    Node<uint32_t> node(1, 2);
    Node<uint32_t> node2(3, 2);
    node.SetNext(1, &node2);
    const Node<uint32_t>* node_ptr = node.GetNext(1);
    const uint32_t result = node_ptr->GetData();
    ASSERT_EQ(3, result);
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
