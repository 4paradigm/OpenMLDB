/*
 * sql_request_row_test.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "sdk/sql_request_row.h"

#include "codec/fe_row_codec.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "sdk/base_impl.h"
#include "vm/catalog.h"

namespace rtidb {
namespace sdk {

class SQLRequestRowTest : public ::testing::Test {};

TEST_F(SQLRequestRowTest, str_null) {
    ::fesql::vm::Schema schema;
    {
        ::fesql::type::ColumnDef* column = schema.Add();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    ::fesql::sdk::SchemaImpl* schema_impl =
        new ::fesql::sdk::SchemaImpl(schema);
    std::shared_ptr<::fesql::sdk::Schema> schema_shared(schema_impl);
    SQLRequestRow rr(schema_shared);
    rr.Init(0);
    ASSERT_TRUE(rr.AppendNULL());
    ASSERT_TRUE(rr.Build());
}

TEST_F(SQLRequestRowTest, not_null) {
    ::fesql::vm::Schema schema;
    {
        ::fesql::type::ColumnDef* column = schema.Add();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
        column->set_is_not_null(true);
    }
    ::fesql::sdk::SchemaImpl* schema_impl =
        new ::fesql::sdk::SchemaImpl(schema);
    std::shared_ptr<::fesql::sdk::Schema> schema_shared(schema_impl);
    SQLRequestRow rr(schema_shared);
    rr.Init(0);
    ASSERT_FALSE(rr.AppendNULL());
    ASSERT_FALSE(rr.Build());
}

TEST_F(SQLRequestRowTest, invalid_str_len) {
    ::fesql::vm::Schema schema;
    {
        ::fesql::type::ColumnDef* column = schema.Add();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    ::fesql::sdk::SchemaImpl* schema_impl =
        new ::fesql::sdk::SchemaImpl(schema);
    std::shared_ptr<::fesql::sdk::Schema> schema_shared(schema_impl);
    SQLRequestRow rr(schema_shared);
    rr.Init(1);
    std::string hello = "hello";
    ASSERT_FALSE(rr.AppendString(hello));
    ASSERT_FALSE(rr.Build());
}

TEST_F(SQLRequestRowTest, normal_test) {
    ::fesql::vm::Schema schema;
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

    ::fesql::sdk::SchemaImpl* schema_impl =
        new ::fesql::sdk::SchemaImpl(schema);
    std::shared_ptr<::fesql::sdk::Schema> schema_shared(schema_impl);
    SQLRequestRow rr(schema_shared);
    ASSERT_TRUE(rr.Init(5));
    ASSERT_TRUE(rr.AppendInt32(32));
    ASSERT_TRUE(rr.AppendString("hello"));
    ASSERT_TRUE(rr.AppendInt64(64));
    ASSERT_TRUE(rr.Build());
    ::fesql::codec::RowView rv(schema);
    bool ok = rv.Reset(reinterpret_cast<const int8_t*>(rr.GetRow().c_str()),
                       rr.GetRow().size());
    ASSERT_TRUE(ok);
    int32_t i32 = 0;
    rv.GetInt32(0, &i32);
    ASSERT_EQ(32, i32);
}

}  // namespace sdk
}  // namespace rtidb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
