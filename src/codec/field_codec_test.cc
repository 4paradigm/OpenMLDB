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

#include <vector>
#include "codec/field_codec.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace codec {

class SingleColumnCodecTest : public ::testing::Test {
 public:
    SingleColumnCodecTest() {}
    ~SingleColumnCodecTest() {}
};

TEST_F(SingleColumnCodecTest, TestEncodec) {
    std::vector<std::string> vec;
    {//  encode part
     {std::string val_1 = "";
    val_1.resize(1);
    char* buf = const_cast<char*>(val_1.data());
    ::rtidb::codec::Convert(true, buf);
    vec.push_back(val_1);
}
{
    std::string val_2 = "";
    val_2.resize(1);
    char* buf = const_cast<char*>(val_2.data());
    ::rtidb::codec::Convert(false, buf);
    vec.push_back(val_2);
}
{
    int16_t int16_val = 33;
    std::string val_3 = "";
    val_3.resize(2);
    char* buf = const_cast<char*>(val_3.data());
    ::rtidb::codec::Convert(int16_val, buf);
    vec.push_back(val_3);
}
{
    int32_t int32_val = 44;
    std::string val_4 = "";
    val_4.resize(4);
    char* buf = const_cast<char*>(val_4.data());
    ::rtidb::codec::Convert(int32_val, buf);
    vec.push_back(val_4);
}
{
    int64_t int64_val = 55;
    std::string val_5 = "";
    val_5.resize(8);
    char* buf = const_cast<char*>(val_5.data());
    ::rtidb::codec::Convert(int64_val, buf);
    vec.push_back(val_5);
}
{
    float float_val = 3.3;
    std::string val_6 = "";
    val_6.resize(4);
    char* buf = const_cast<char*>(val_6.data());
    ::rtidb::codec::Convert(float_val, buf);
    vec.push_back(val_6);
}
{
    double double_val = 4.4;
    std::string val_7 = "";
    val_7.resize(8);
    char* buf = const_cast<char*>(val_7.data());
    ::rtidb::codec::Convert(double_val, buf);
    vec.push_back(val_7);
}
}  // namespace base
{
    // decode part
    bool v1 = false;
    ::rtidb::codec::GetBool(vec[0].data(), &v1);
    ASSERT_EQ(v1, true);

    bool v2 = true;
    ::rtidb::codec::GetBool(vec[1].data(), &v2);
    ASSERT_EQ(v2, false);

    int16_t v3 = 0;
    ::rtidb::codec::GetInt16(vec[2].data(), &v3);
    ASSERT_EQ(v3, 33);

    int32_t v4 = 0;
    ::rtidb::codec::GetInt32(vec[3].data(), &v4);
    ASSERT_EQ(v4, 44);

    int64_t v5 = 0;
    ::rtidb::codec::GetInt64(vec[4].data(), &v5);
    ASSERT_EQ(v5, 55);

    float v6 = 0.0;
    ::rtidb::codec::GetFloat(vec[5].data(), &v6);
    ASSERT_EQ(v6, 3.3f);

    double v7 = 0.0;
    ::rtidb::codec::GetDouble(vec[6].data(), &v7);
    ASSERT_EQ(v7, 4.4);
}
}  // namespace rtidb

TEST_F(SingleColumnCodecTest, TestBatchEncodec) {
    std::vector<std::string> vec;
    {// encode part
     {std::string val_1 = "";
    ::rtidb::codec::Convert("true", ::rtidb::type::kBool, &val_1);
    vec.push_back(val_1);
}
{
    std::string val_2 = "";
    ::rtidb::codec::Convert("false", ::rtidb::type::kBool, &val_2);
    vec.push_back(val_2);
}
{
    std::string val_3 = "";
    ::rtidb::codec::Convert("33", ::rtidb::type::kSmallInt, &val_3);
    vec.push_back(val_3);
}
{
    std::string val_4 = "";
    ::rtidb::codec::Convert("44", ::rtidb::type::kInt, &val_4);
    vec.push_back(val_4);
}
{
    std::string val_5 = "";
    ::rtidb::codec::Convert("55", ::rtidb::type::kBigInt, &val_5);
    vec.push_back(val_5);
}
{
    std::string val_6 = "";
    ::rtidb::codec::Convert("3.3", ::rtidb::type::kFloat, &val_6);
    vec.push_back(val_6);
}
{
    std::string val_7 = "";
    ::rtidb::codec::Convert("4.4", ::rtidb::type::kDouble, &val_7);
    vec.push_back(val_7);
}
}
{
    // decode part
    bool v1 = false;
    ::rtidb::codec::GetBool(vec[0].data(), &v1);
    ASSERT_EQ(v1, true);

    bool v2 = true;
    ::rtidb::codec::GetBool(vec[1].data(), &v2);
    ASSERT_EQ(v2, false);

    int16_t v3 = 0;
    ::rtidb::codec::GetInt16(vec[2].data(), &v3);
    ASSERT_EQ(v3, 33);

    int32_t v4 = 0;
    ::rtidb::codec::GetInt32(vec[3].data(), &v4);
    ASSERT_EQ(v4, 44);

    int64_t v5 = 0;
    ::rtidb::codec::GetInt64(vec[4].data(), &v5);
    ASSERT_EQ(v5, 55);

    float v6 = 0.0;
    ::rtidb::codec::GetFloat(vec[5].data(), &v6);
    ASSERT_EQ(v6, 3.3f);

    double v7 = 0.0;
    ::rtidb::codec::GetDouble(vec[6].data(), &v7);
    ASSERT_EQ(v7, 4.4);
}
}

}  // namespace codec
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
