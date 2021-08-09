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

#include <utility>
#include "codec/fe_schema_codec.h"
#include "gtest/gtest.h"

namespace hybridse {
namespace codec {

class SchemaCodecTest
    : public ::testing::TestWithParam<std::pair<vm::Schema, uint32_t>> {
 public:
    SchemaCodecTest() {}
    ~SchemaCodecTest() {}
};

std::vector<std::pair<vm::Schema, uint32_t>> GenTestInput() {
    std::vector<std::pair<vm::Schema, uint32_t>> inputs;
    vm::Schema empty;
    inputs.push_back(std::make_pair(empty, 0));
    {
        // varchar
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kVarchar);
            column->set_name("col0");
            column->set_is_constant(true);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }

    {
        // int16
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kInt16);
            column->set_name("col0");
            column->set_is_constant(true);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }
    {
        // int32
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("col0");
            column->set_is_constant(true);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }
    {
        // int64
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kInt64);
            column->set_name("col0");
            column->set_is_constant(false);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }
    {
        // float
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kFloat);
            column->set_name("col0");
            column->set_is_constant(false);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }
    {
        // double
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kDouble);
            column->set_name("col0");
            column->set_is_constant(false);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }
    {
        // date
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kDate);
            column->set_name("col0");
            column->set_is_constant(false);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }
    {
        // timestamp
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kTimestamp);
            column->set_name("col0");
            column->set_is_constant(false);
        }
        inputs.push_back(std::make_pair(schema, 9));
    }

    {
        vm::Schema schema;
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kTimestamp);
            column->set_name("col0");
            column->set_is_constant(true);
        }
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kVarchar);
            column->set_name("col20");
            column->set_is_constant(false);
        }
        {
            ::hybridse::type::ColumnDef* column = schema.Add();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("col21");
            column->set_is_constant(false);
        }
        inputs.push_back(std::make_pair(schema, 25));
    }
    return inputs;
}

void CompareSchema(const vm::Schema& left, const vm::Schema& right) {
    ASSERT_EQ(left.size(), right.size());
    for (int32_t i = 0; i < left.size(); i++) {
        const type::ColumnDef& left_column = left.Get(i);
        const type::ColumnDef& right_column = right.Get(i);
        ASSERT_EQ(left_column.name(), right_column.name());
        ASSERT_TRUE(left_column.type() == right_column.type());
        ASSERT_TRUE(left_column.is_constant() == right_column.is_constant());
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

INSTANTIATE_TEST_SUITE_P(SchemaCodecTestPrefix, SchemaCodecTest,
                        testing::ValuesIn(GenTestInput()));

}  // namespace codec
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
