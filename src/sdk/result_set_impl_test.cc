/*
 * result_set_impl_test.cc
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

#include "sdk/result_set_impl.h"

#include "gtest/gtest.h"
#include "proto/tablet.pb.h"
#include "codec/row_codec.h"

namespace fesql {
namespace sdk {

class ResultSetImplTest : public ::testing::TestWithParam<tablet::QueryResponse> {
 public:
    ResultSetImplTest() {}
    ~ResultSetImplTest() {}
};


std::vector<tablet::QueryResponse> GetTestCase() {
    std::vector<tablet::QueryResponse> responses;
    // empty
    {
        responses.push_back(tablet::QueryResponse());
    }
    {
        tablet::QueryResponse response;
        type::ColumnDef* col1 = response.mutable_schema()->Add();
        col1->set_type(type::kVarchar);
        codec::RowBuilder rb(response.schema());
        uint32_t size = rb.CalTotalLength(5);
        std::string* row_data = response.add_result_set();
        row_data->resize(size);
        char* row_buf = &(row_data->at(0));
        rb.SetBuffer(reinterpret_cast<int8_t*>(row_buf), size);
        std::string hello = "hello";
        rb.AppendString(hello.c_str(), 5);
        responses.push_back(response);
    }
    {
        tablet::QueryResponse response;

        {
            type::ColumnDef* col1 = response.mutable_schema()->Add();
            col1->set_type(type::kInt32);
            col1->set_name("col1");
        }
        {
            type::ColumnDef* col1 = response.mutable_schema()->Add();
            col1->set_type(type::kFloat);
            col1->set_name("col2");
        }
        {
            type::ColumnDef* col1 = response.mutable_schema()->Add();
            col1->set_type(type::kDouble);
            col1->set_name("col3");
        }

        codec::RowBuilder rb(response.schema());
        uint32_t size = rb.CalTotalLength(0);
        std::string* row_data = response.add_result_set();
        row_data->resize(size);

        char* row_buf = &(row_data->at(0));
        rb.SetBuffer(reinterpret_cast<int8_t*>(row_buf), size);
        rb.AppendInt32(1);
        rb.AppendFloat(1.2f);
        rb.AppendDouble(2.1);
        responses.push_back(response);
    }
    return responses;
}

TEST_P(ResultSetImplTest, test_normal) {
    auto response = GetParam();
    std::unique_ptr<tablet::QueryResponse> resp_ptr(new tablet::QueryResponse());
    resp_ptr->CopyFrom(response);
    ResultSetImpl rs(std::move(resp_ptr));
    rs.Init();
    codec::RowView row_view(response.schema());
    for (int32_t i = 0; i < response.result_set_size(); i++) {
        const std::string& row = response.result_set(i);
        ASSERT_TRUE(rs.Next());
        row_view.Reset(reinterpret_cast<const int8_t*>(row.c_str()), row.size());
        for (int32_t j = 0;  j < response.schema().size(); j++) {
            const type::ColumnDef& column = response.schema().Get(j);
            switch(column.type()) {
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
                    char* left_ptr = NULL;
                    char* right_ptr = NULL;
                    uint32_t left_size = 0;
                    uint32_t right_size = 1;
                    row_view.GetString(j, &left_ptr, &left_size);
                    ASSERT_TRUE(rs.GetString(j, &right_ptr, &right_size));
                    ASSERT_EQ(left_size, right_size);
                    ASSERT_EQ(left_ptr, right_ptr);
                    break;
                }
                default:{}
            }
        }
    }

}
INSTANTIATE_TEST_CASE_P(ResultSetImplTestPrefix,
                         ResultSetImplTest, 
                         testing::ValuesIn(GetTestCase()));

}  // namespace sdk
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

