/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest/gtest.h"
#include "schema/index_util.h"

namespace openmldb {
namespace schema {

class IndexTest : public ::testing::Test {
 public:
    IndexTest() {}
    ~IndexTest() {}
};

TEST_F(IndexTest, CheckUnique) {
    PBIndex indexs;
    auto index = indexs.Add();
    index->set_index_name("index1");
    index->add_col_name("col1");
    index = indexs.Add();
    index->set_index_name("index2");
    index->add_col_name("col2");
    ASSERT_TRUE(IndexUtil::CheckUnique(indexs).OK());
    index = indexs.Add();
    index->set_index_name("index3");
    index->add_col_name("col2");
    ASSERT_FALSE(IndexUtil::CheckUnique(indexs).OK());
}

}  // namespace schema
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
