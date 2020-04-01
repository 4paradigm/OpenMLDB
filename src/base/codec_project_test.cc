/*
 * project_codec_test.cc
 */

#include <string>
#include <vector>
#include "base/codec.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "base/strings.h"


namespace rtidb {
namespace base {

using Schema = ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>;
using ProjectList = ::google::protobuf::RepeatedPtrField<uint32_t>;

struct TestArgs {
    Schema schema;
    ProjectList plist;
    void* row_ptr;
    uint32_t row_size;
    void* out_ptr;
    uint32_t out_size;
    Schema output_schema;
    TestArgs():schema(),plist(),row_ptr(NULL), row_size(0),
    out_ptr(NULL), out_size(0), output_schema() {}
    ~TestArgs() {}
};

class ProjectCodecTest
    : public ::testing::TestWithParam<TestArgs*> {
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
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(0);
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        output_rb.AppendInt32(c2);
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
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);
        int64_t c3 = 64;
        input_rb.AppendInt64(c3);
        input_rb.AppendString(hello.c_str(), hello.size());

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(hello.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        output_rb.AppendString(hello.c_str(), hello.size());
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
        input_rb.AppendInt16(c1);
        int32_t c2 = 2;
        input_rb.AppendInt32(c2);
        int64_t c3 = 64;
        input_rb.AppendInt64(c3);
        input_rb.AppendString(hello.c_str(), hello.size());

        RowBuilder output_rb(testargs->output_schema);
        uint32_t output_row_size = output_rb.CalTotalLength(hello.size());
        void* output_ptr = ::malloc(output_row_size);
        output_rb.SetBuffer(reinterpret_cast<int8_t*>(output_ptr), output_row_size);
        output_rb.AppendString(hello.c_str(), hello.size());
        output_rb.AppendInt64(c3);
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

TEST_P(ProjectCodecTest, common_case) {
    auto args = GetParam();
    RowProject rp(args->schema, args->plist);
    ASSERT_TRUE(rp.Init());
    int8_t* output = NULL;
    uint32_t output_size = 0;
    ASSERT_TRUE(rp.Project(reinterpret_cast<int8_t*>(args->row_ptr), args->row_size, &output, &output_size));
    std::string left = DebugCharArray(reinterpret_cast<char*>(output), output_size);
    std::string right = DebugCharArray(reinterpret_cast<char*>(args->out_ptr), args->out_size);
    ASSERT_EQ(output_size, args->out_size);
    ASSERT_EQ(0, left.compare(right));
}

INSTANTIATE_TEST_CASE_P(ProjectCodecTestPrefix, ProjectCodecTest,
                       testing::ValuesIn(GenCommonCase()));

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
