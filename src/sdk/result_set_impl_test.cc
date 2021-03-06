/*
 * Copyright (C) 4Paradigm
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

#include "sdk/result_set_impl.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "brpc/controller.h"
#include "codec/fe_row_codec.h"
#include "codec/fe_schema_codec.h"
#include "gtest/gtest.h"
#include "proto/fe_tablet.pb.h"

namespace fesql {
namespace sdk {
struct TestArgs {
    tablet::QueryResponse* response;
    brpc::Controller* ctrl;
    std::vector<std::string> rows;
};

class ResultSetImplTest : public ::testing::TestWithParam<TestArgs*> {
 public:
    ResultSetImplTest() {}
    ~ResultSetImplTest() {}
};

std::vector<TestArgs*> GetTestCase() {
    std::vector<TestArgs*> case_set;
    // empty
    {
        TestArgs* args = new TestArgs();
        args->response = new tablet::QueryResponse();
        args->ctrl = new brpc::Controller();
        case_set.push_back(args);
    }
    {
        TestArgs* args = new TestArgs();
        args->response = new tablet::QueryResponse();
        args->ctrl = new brpc::Controller();

        vm::Schema schema;
        type::ColumnDef* col1 = schema.Add();
        col1->set_type(type::kVarchar);
        std::string schema_raw;
        codec::SchemaCodec::Encode(schema, &schema_raw);
        args->response->set_schema(schema_raw);
        codec::RowBuilder rb(schema);
        uint32_t size = rb.CalTotalLength(5);
        std::string row;
        row.resize(size);
        char* row_buf = &(row[0]);
        rb.SetBuffer(reinterpret_cast<int8_t*>(row_buf), size);
        std::string hello = "hello";
        rb.AppendString(hello.c_str(), 5);
        butil::IOBuf& buf = args->ctrl->response_attachment();
        buf.append(row);
        args->response->set_count(1);
        args->response->set_byte_size(buf.size());
        args->rows.push_back(buf.to_string());
        case_set.push_back(args);
    }

    {
        TestArgs* args = new TestArgs();
        args->response = new tablet::QueryResponse();
        args->ctrl = new brpc::Controller();
        vm::Schema schema;

        {
            type::ColumnDef* col1 = schema.Add();
            col1->set_type(type::kInt32);
            col1->set_name("col1");
        }
        {
            type::ColumnDef* col1 = schema.Add();
            col1->set_type(type::kFloat);
            col1->set_name("col2");
        }
        {
            type::ColumnDef* col1 = schema.Add();
            col1->set_type(type::kDouble);
            col1->set_name("col3");
        }
        std::string schema_raw;
        codec::SchemaCodec::Encode(schema, &schema_raw);
        args->response->set_schema(schema_raw);

        codec::RowBuilder rb(schema);
        uint32_t size = rb.CalTotalLength(0);
        std::string row;
        row.resize(size);
        char* row_buf = &(row[0]);
        rb.SetBuffer(reinterpret_cast<int8_t*>(row_buf), size);
        rb.AppendInt32(1);
        rb.AppendFloat(1.2f);
        rb.AppendDouble(2.1);

        butil::IOBuf& buf = args->ctrl->response_attachment();
        buf.append(row);
        args->response->set_count(1);
        args->response->set_byte_size(buf.size());
        args->rows.push_back(buf.to_string());
        case_set.push_back(args);
    }
    // multi record
    {
        TestArgs* args = new TestArgs();
        args->response = new tablet::QueryResponse();
        args->ctrl = new brpc::Controller();
        vm::Schema schema;

        {
            type::ColumnDef* col1 = schema.Add();
            col1->set_type(type::kInt32);
            col1->set_name("col1");
        }
        {
            type::ColumnDef* col1 = schema.Add();
            col1->set_type(type::kFloat);
            col1->set_name("col2");
        }
        {
            type::ColumnDef* col1 = schema.Add();
            col1->set_type(type::kDouble);
            col1->set_name("col3");
        }
        std::string schema_raw;
        codec::SchemaCodec::Encode(schema, &schema_raw);
        args->response->set_schema(schema_raw);

        codec::RowBuilder rb(schema);
        uint32_t size = rb.CalTotalLength(0);
        std::string row;
        row.resize(size);
        char* row_buf = &(row[0]);
        rb.SetBuffer(reinterpret_cast<int8_t*>(row_buf), size);
        rb.AppendInt32(1);
        rb.AppendFloat(1.2f);
        rb.AppendDouble(2.1);

        butil::IOBuf& buf = args->ctrl->response_attachment();
        buf.append(row);
        buf.append(row);
        args->response->set_count(2);
        args->response->set_byte_size(buf.size() * 2);
        args->rows.push_back(row);
        args->rows.push_back(row);
        case_set.push_back(args);
    }
    return case_set;
}

TEST_P(ResultSetImplTest, test_normal) {
    auto args = GetParam();
    std::unique_ptr<tablet::QueryResponse> resp_ptr(args->response);
    std::unique_ptr<brpc::Controller> cntrl(args->ctrl);
    ResultSetImpl rs(std::move(resp_ptr), std::move(cntrl));
    rs.Init();
    vm::Schema schema;
    bool ok = codec::SchemaCodec::Decode(args->response->schema(), &schema);
    ASSERT_TRUE(ok);
    codec::RowView row_view(schema);
    for (uint32_t i = 0; i < args->response->count(); i++) {
        const std::string& row = args->rows[i];
        ASSERT_TRUE(rs.Next());
        row_view.Reset(reinterpret_cast<const int8_t*>(row.c_str()),
                       row.size());
        for (int32_t j = 0; j < schema.size(); j++) {
            const type::ColumnDef& column = schema.Get(j);
            switch (column.type()) {
                case type::kBool: {
                    bool left = false;
                    bool right = true;
                    row_view.GetBool(j, &left);
                    ASSERT_TRUE(rs.GetBool(j, &right));
                    ASSERT_EQ(left, right);
                    break;
                }
                case type::kInt16: {
                    int16_t left = 0;
                    int16_t right = 1;
                    row_view.GetInt16(j, &left);
                    ASSERT_TRUE(rs.GetInt16(j, &right));
                    ASSERT_EQ(left, right);
                    break;
                }
                case type::kInt32: {
                    int32_t left = 0;
                    int32_t right = 1;
                    row_view.GetInt32(j, &left);
                    ASSERT_TRUE(rs.GetInt32(j, &right));
                    ASSERT_EQ(left, right);
                    break;
                }
                case type::kFloat: {
                    float left = 0.1f;
                    float right = 1.1f;
                    row_view.GetFloat(j, &left);
                    ASSERT_TRUE(rs.GetFloat(j, &right));
                    ASSERT_EQ(left, right);
                    break;
                }
                case type::kDouble: {
                    double left = 1.1;
                    double right = 2.1;
                    row_view.GetDouble(j, &left);
                    ASSERT_TRUE(rs.GetDouble(j, &right));
                    ASSERT_EQ(left, right);
                    break;
                }
                case type::kVarchar: {
                    const char* left_ptr = NULL;
                    std::string right;
                    uint32_t left_size = 0;
                    row_view.GetString(j, &left_ptr, &left_size);
                    ASSERT_TRUE(rs.GetString(j, &right));
                    ASSERT_EQ(left_size, right.size());
                    ASSERT_EQ(std::string(left_ptr), right);
                    break;
                }
                default: {
                    ASSERT_TRUE(false);
                }
            }
        }
    }
    delete args;
}
INSTANTIATE_TEST_CASE_P(ResultSetImplTestPrefix, ResultSetImplTest,
                        testing::ValuesIn(GetTestCase()));

}  // namespace sdk
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
