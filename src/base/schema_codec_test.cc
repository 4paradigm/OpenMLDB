/*
 * schema_codec_test.cc
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

#include "base/schema_codec.h"

#include "gtest/gtest.h"

namespace fesql {
namespace base {


class SchemaCodecTest : public ::testing::TestWithParam<std::pair<vm::Schema, uint32_t>> {
 public:
    SchemaCodecTest() {}
    ~SchemaCodecTest() {}

};

std::vector<std::pair<vm::Schema, uint32_t>> GenTestInput() {
    std::vector<std::pair<vm::Schema, uint32_t>> inputs;
    vm::Schema empty;
    inputs.push_back(std::make_pair(empty, 2u));
    {
        // varchar
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kVarchar);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }

    {
        // int16 
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kInt16);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }
    {
        // int32 
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kInt32);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }
    {
        // int64
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kInt64);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }
    {
        // float
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kFloat);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }
    {
        // double
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kDouble);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }
    {
        // date
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kDate);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }
    {
        // timestamp
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kTimestamp);
            column->set_name("col0");
        }
        inputs.push_back(std::make_pair(schema, 8));
    }

    {
        vm::Schema schema;
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kTimestamp);
            column->set_name("col0");
        }
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kVarchar);
            column->set_name("col20");
        }
        {
            ::fesql::type::ColumnDef* column = schema.Add();
            column->set_type(::fesql::type::kInt32);
            column->set_name("col21");
        }
        inputs.push_back(std::make_pair(schema, 22));
    }
    return inputs;
}

void CompareSchema(const vm::Schema& left, 
        const vm::Schema& right) {
    ASSERT_EQ(left.size(), right.size());
    for (int32_t i = 0; i < left.size(); i++) {
        const type::ColumnDef& left_column = left.Get(i);
        const type::ColumnDef& right_column = right.Get(i);
        ASSERT_EQ(left_column.name(), right_column.name());
        ASSERT_TRUE(left_column.type() == right_column.type());
    }
}

TEST_P(SchemaCodecTest, test_normal) {
    auto pair = GetParam();
    std::string buffer;
    bool ok = SchemaCodec::Encode(pair.first, &buffer);
    ASSERT_TRUE(ok);
    ASSERT_EQ(buffer.size(), pair.second);
    vm::Schema schema;
    ok = SchemaCodec::Decode(buffer, &schema);
    ASSERT_TRUE(ok);
    CompareSchema(pair.first, schema);
}

INSTANTIATE_TEST_CASE_P(SchemaCodecTestPrefix,
                         SchemaCodecTest, 
                         testing::ValuesIn(GenTestInput()));

}  // namespace base
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

