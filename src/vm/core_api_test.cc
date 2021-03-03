/*
 * core_api_test.cc
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "vm/core_api.h"
#include "gtest/gtest.h"

namespace fesql {
namespace vm {

class CoreAPITest : public ::testing::Test {
 public:
    CoreAPITest() {}
    ~CoreAPITest() {}
};

TEST_F(CoreAPITest, test_create_new_row) {
    Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt16);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::fesql::type::kBool);
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);

    fesql::codec::Row rowPtr = CoreAPI::NewRow(size);
    int8_t* buf = rowPtr.buf(0);
    builder.SetBuffer(buf, size);

    ASSERT_TRUE(builder.AppendNULL());
    ASSERT_TRUE(builder.AppendBool(false));
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
