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

#include <string>
#include <vector>

#include "base/glog_wrapper.h"
#include "base/strings.h"
#include "codec/codec.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"

namespace openmldb {
namespace codec {

struct TestArgs {
    Schema schema;
    ProjectList plist;
    void* row_ptr;
    uint32_t row_size;
    void* out_ptr;
    uint32_t out_size;
    Schema output_schema;
    TestArgs() : schema(), plist(), row_ptr(NULL), row_size(0), out_ptr(NULL), out_size(0), output_schema() {}
    ~TestArgs() {}
};

class ProjectCodecTest : public ::testing::TestWithParam<TestArgs*> {
 public:
    ProjectCodecTest() {}
    ~ProjectCodecTest() {}
};

std::vector<TestArgs*> GenCommonCase() {
    std::vector<TestArgs*> args;
    {
        TestArgs* testargs = new TestArgs();

        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);

        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);

        common::ColumnDesc* column3 = testargs->output_schema.Add();
        column3->set_name("col2");
        column3->set_data_type(type::kInt);

        RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(0);
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr), input_row_size);
        int16_t c1 = 1;
        (void)input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        (void)input_rb.AppendInt32(c2);

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(0);
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        (void)output_rb.AppendInt32(c2);
        uint32_t* idx = testargs->plist.Add();
        *idx = 1;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }

    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        RowBuilder input_rb(testargs->schema);
        std::string hello = "hello";
        uint32_t input_row_size = input_rb.CalTotalLength(hello.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr), input_row_size);
        int16_t c1 = 1;
        (void)input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        (void)input_rb.AppendInt32(c2);
        int64_t c3 = 64;
        (void)input_rb.AppendInt64(c3);
        (void)input_rb.AppendString(hello.c_str(), hello.size());

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(hello.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        (void)output_rb.AppendString(hello.c_str(), hello.size());
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        common::ColumnDesc* column6 = testargs->output_schema.Add();
        column6->set_name("col3");
        column6->set_data_type(type::kBigInt);

        RowBuilder input_rb(testargs->schema);
        std::string hello = "hello";
        uint32_t input_row_size = input_rb.CalTotalLength(hello.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr), input_row_size);
        int16_t c1 = 1;
        (void)input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        (void)input_rb.AppendInt32(c2);
        int64_t c3 = 64;
        (void)input_rb.AppendInt64(c3);
        (void)input_rb.AppendString(hello.c_str(), hello.size());

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(hello.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        (void)output_rb.AppendString(hello.c_str(), hello.size());
        (void)output_rb.AppendInt64(c3);
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    // add null
    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        common::ColumnDesc* column6 = testargs->output_schema.Add();
        column6->set_name("col3");
        column6->set_data_type(type::kBigInt);

        RowBuilder input_rb(testargs->schema);
        std::string hello = "hello";
        uint32_t input_row_size = input_rb.CalTotalLength(hello.size());
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr), input_row_size);
        int16_t c1 = 1;
        (void)input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        (void)input_rb.AppendInt32(c2);
        (void)input_rb.AppendNULL();
        (void)input_rb.AppendString(hello.c_str(), hello.size());

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(hello.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        (void)output_rb.AppendString(hello.c_str(), hello.size());
        (void)output_rb.AppendNULL();
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    // add null str
    {
        TestArgs* testargs = new TestArgs();
        common::ColumnDesc* column1 = testargs->schema.Add();
        column1->set_name("col1");
        column1->set_data_type(type::kSmallInt);
        common::ColumnDesc* column2 = testargs->schema.Add();
        column2->set_name("col2");
        column2->set_data_type(type::kInt);
        common::ColumnDesc* column3 = testargs->schema.Add();
        column3->set_name("col3");
        column3->set_data_type(type::kBigInt);
        common::ColumnDesc* column4 = testargs->schema.Add();
        column4->set_name("col4");
        column4->set_data_type(type::kVarchar);

        common::ColumnDesc* column5 = testargs->output_schema.Add();
        column5->set_name("col4");
        column5->set_data_type(type::kVarchar);

        common::ColumnDesc* column6 = testargs->output_schema.Add();
        column6->set_name("col3");
        column6->set_data_type(type::kBigInt);

        RowBuilder input_rb(testargs->schema);
        uint32_t input_row_size = input_rb.CalTotalLength(0);
        void* input_ptr = ::malloc(input_row_size);
        input_rb.SetBuffer(reinterpret_cast<int8_t*>(input_ptr), input_row_size);
        int16_t c1 = 1;
        (void)input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        (void)input_rb.AppendInt32(c2);
        int64_t c3 = 64;
        (void)input_rb.AppendInt64(c3);
        (void)input_rb.AppendNULL();

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(0);
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        (void)output_rb.AppendNULL();
        (void)output_rb.AppendInt64(c3);
        uint32_t* idx = testargs->plist.Add();
        *idx = 3;
        uint32_t* idx2 = testargs->plist.Add();
        *idx2 = 2;
        testargs->row_ptr = input_ptr;
        testargs->row_size = input_row_size;
        testargs->out_ptr = output_ptr;
        testargs->out_size = output_row_size;
        args.push_back(testargs);
    }
    return args;
}

void CompareRow(RowView* left, RowView* right, const Schema& schema) {
    for (int32_t i = 0; i < schema.size(); i++) {
        uint32_t idx = (uint32_t)i;
        const common::ColumnDesc& column = schema.Get(i);
        ASSERT_EQ(left->IsNULL(idx), right->IsNULL(i));
        if (left->IsNULL(idx)) continue;
        int32_t ret = 0;
        switch (column.data_type()) {
            case ::openmldb::type::kBool: {
                bool left_val = false;
                bool right_val = false;
                ret = left->GetBool(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetBool(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }

            case ::openmldb::type::kSmallInt: {
                int16_t left_val = 0;
                int16_t right_val = 0;
                ret = left->GetInt16(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt16(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }

            case ::openmldb::type::kInt: {
                int32_t left_val = 0;
                int32_t right_val = 0;
                ret = left->GetInt32(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt32(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kTimestamp:
            case ::openmldb::type::kBigInt: {
                int64_t left_val = 0;
                int64_t right_val = 0;
                ret = left->GetInt64(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetInt64(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kFloat: {
                float left_val = 0;
                float right_val = 0;
                ret = left->GetFloat(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetFloat(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kDouble: {
                double left_val = 0;
                double right_val = 0;
                ret = left->GetDouble(idx, &left_val);
                ASSERT_EQ(0, ret);
                ret = right->GetDouble(idx, &right_val);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_val, right_val);
                break;
            }
            case ::openmldb::type::kVarchar: {
                char* left_val = NULL;
                uint32_t left_size = 0;
                char* right_val = NULL;
                uint32_t right_size = 0;
                ret = left->GetString(idx, &left_val, &left_size);
                ASSERT_EQ(0, ret);
                ret = right->GetString(idx, &right_val, &right_size);
                ASSERT_EQ(0, ret);
                ASSERT_EQ(left_size, right_size);
                std::string left_str(left_val, left_size);
                std::string right_str(right_val, right_size);
                ASSERT_EQ(left_str, right_str);
                break;
            }
            default: {
                PDLOG(WARNING, "not supported type");
            }
        }
    }
}

TEST_P(ProjectCodecTest, common_case) {
    auto args = GetParam();
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema;
    vers_schema.insert(std::make_pair(1, std::make_shared<Schema>(args->schema)));
    RowProject rp(vers_schema, args->plist);
    ASSERT_TRUE(rp.Init());
    int8_t* output = NULL;
    uint32_t output_size = 0;
    ASSERT_TRUE(rp.Project(reinterpret_cast<int8_t*>(args->row_ptr), args->row_size, &output, &output_size));
    ASSERT_EQ(output_size, args->out_size);
    RowView left(args->output_schema);
    left.Reset(output, output_size);
    RowView right(args->output_schema);
    right.Reset(reinterpret_cast<int8_t*>(args->out_ptr), args->out_size);
    CompareRow(&left, &right, args->output_schema);
}

INSTANTIATE_TEST_SUITE_P(ProjectCodecTestPrefix, ProjectCodecTest, testing::ValuesIn(GenCommonCase()));

}  // namespace codec
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
