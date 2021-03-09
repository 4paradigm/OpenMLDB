/*
 * Copyright 2021 4Paradigm
 *
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

#include "request_row.h"
#include "base_impl.h"
#include "codec/fe_row_codec.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "vm/catalog.h"

namespace fesql {
namespace sdk {
class RequestRowTest : public ::testing::Test {};

TEST_F(RequestRowTest, normal_test) {
    vm::Schema schema;
    {
        ::fesql::type::ColumnDef* column = schema.Add();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = schema.Add();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = schema.Add();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col2");
    }
    SchemaImpl schema_impl(schema);
    RequestRow rr(&schema_impl);
    ASSERT_TRUE(rr.Init(5));
    ASSERT_TRUE(rr.AppendInt32(32));
    ASSERT_TRUE(rr.AppendString("hello"));
    ASSERT_TRUE(rr.AppendInt64(64));
    ASSERT_TRUE(rr.Build());

    codec::RowView rv(schema);
    bool ok = rv.Reset(reinterpret_cast<const int8_t*>(rr.GetRow().c_str()),
                       rr.GetRow().size());
    ASSERT_TRUE(ok);
    int32_t i32 = 0;
    rv.GetInt32(0, &i32);
    ASSERT_EQ(32, i32);
}

}  // namespace sdk
}  // namespace fesql

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
