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


#include <iostream>
#include "base/strings.h"
#include "codec/flat_array.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace codec {

class FlatArrayTest : public ::testing::Test {
 public:
    FlatArrayTest() {}
    ~FlatArrayTest() {}
};

TEST_F(FlatArrayTest, Decode) {
    std::string buffer;
    FlatArrayCodec codec(&buffer, 5);
    bool ok = codec.Append(1.2f);
    ASSERT_TRUE(ok);
    std::string big_col1(100, 'a');
    ok = codec.Append(big_col1);
    ASSERT_TRUE(ok);
    std::string big_col2(127, 'b');
    ok = codec.Append(big_col2);
    ASSERT_TRUE(ok);
    std::string big_col3(128, 'c');
    ok = codec.Append(big_col3);
    ASSERT_TRUE(ok);
    std::string big_col4(32767, 'e');
    ok = codec.Append(big_col4);
    ASSERT_TRUE(ok);
    codec.Build();

    FlatArrayIterator it(buffer.c_str(), buffer.size(), 5);
    float value = 0;
    ok = it.GetFloat(&value);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1.2f, value);
    std::string value2;
    it.Next();
    ASSERT_TRUE(it.Valid());
    ok = it.GetString(&value2);
    ASSERT_TRUE(ok);
    ASSERT_EQ(big_col1, value2);
    it.Next();
    ASSERT_TRUE(it.Valid());
    value2.clear();
    ok = it.GetString(&value2);
    ASSERT_TRUE(ok);
    ASSERT_EQ(big_col2, value2);
    it.Next();
    ASSERT_TRUE(it.Valid());
    value2.clear();
    ok = it.GetString(&value2);
    ASSERT_TRUE(ok);
    ASSERT_EQ(big_col3, value2);
    it.Next();
    ASSERT_TRUE(it.Valid());
    value2.clear();
    ok = it.GetString(&value2);
    ASSERT_TRUE(ok);
    ASSERT_EQ(big_col4, value2);
    it.Next();
    ASSERT_FALSE(it.Valid());
}

TEST_F(FlatArrayTest, Encode) {
    std::string buffer;
    FlatArrayCodec codec(&buffer, 2);
    bool ok = codec.Append(1.2f);
    ASSERT_TRUE(ok);
    ok = codec.Append(1.0f);
    ASSERT_TRUE(ok);
    codec.Build();
    std::cout << buffer.size() << std::endl;
    std::cout << ::rtidb::base::DebugString(buffer) << std::endl;
    ASSERT_EQ((int32_t)buffer.size(), 13);
    FlatArrayIterator it(buffer.c_str(), buffer.size(), 2);
    ASSERT_EQ(kFloat, it.GetType());
    ASSERT_TRUE(it.Valid());
    ASSERT_EQ(2, it.Size());
    float value = 0;
    ok = it.GetFloat(&value);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1.2f, value);
    std::cout << value << std::endl;
    it.Next();
    ASSERT_TRUE(it.Valid());
    ok = it.GetFloat(&value);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1.0f, value);
    it.Next();
    ASSERT_FALSE(it.Valid());
    std::string buffer2;
    FlatArrayCodec codec2(&buffer2, 2);
    codec2.Append("wtz");
    codec2.Append(1.0f);
    codec2.Build();
    std::cout << ::rtidb::base::DebugString(buffer2) << std::endl;
}

TEST_F(FlatArrayTest, Encode1) {
    std::string buffer;
    FlatArrayCodec codec(&buffer, 2);
    bool ok = codec.Append("test");
    ASSERT_TRUE(ok);
    double v = 1.0;
    ok = codec.Append(v);
    ASSERT_TRUE(ok);
    codec.Build();
    std::cout << ::rtidb::base::DebugString(buffer) << std::endl;
}

TEST_F(FlatArrayTest, TimestampEncode) {
    std::string buffer;
    FlatArrayCodec codec(&buffer, 3);
    bool ok = codec.Append("test");
    ASSERT_TRUE(ok);
    double v = 1.0;
    ok = codec.Append(v);
    ASSERT_TRUE(ok);
    ok = codec.AppendTimestamp(11111);
    ASSERT_TRUE(ok);
    codec.Build();
    std::cout << ::rtidb::base::DebugString(buffer) << std::endl;
}

TEST_F(FlatArrayTest, DateEncode) {
    std::string buffer;
    FlatArrayCodec codec(&buffer, 6);
    bool ok = codec.Append("test");
    ASSERT_TRUE(ok);
    double v = 1.0;
    ok = codec.Append(v);
    ASSERT_TRUE(ok);
    ok = codec.AppendDate(11111);
    ASSERT_TRUE(ok);
    ok = codec.Append(true);
    ASSERT_TRUE(ok);
    uint16_t value = 10;
    ok = codec.Append(value);
    ASSERT_TRUE(ok);
    int16_t value2 = -10;
    ok = codec.Append(value2);
    ASSERT_TRUE(ok);
    codec.Build();
    std::cout << ::rtidb::base::DebugString(buffer) << std::endl;
}

TEST_F(FlatArrayTest, EncodeNullEmpty) {
    std::string buffer;
    FlatArrayCodec codec(&buffer, 3);
    bool ok = codec.Append(1.2f);
    ASSERT_TRUE(ok);
    ok = codec.AppendNull();
    ASSERT_TRUE(ok);
    std::string empty_str;
    ok = codec.Append(empty_str);
    ASSERT_TRUE(ok);
    codec.Build();
    std::cout << ::rtidb::base::DebugString(buffer) << std::endl;
    ASSERT_EQ((int64_t)(buffer.size()), 11);
    FlatArrayIterator it(buffer.c_str(), buffer.size(), 3);
    ASSERT_EQ(kFloat, it.GetType());
    ASSERT_TRUE(it.Valid());
    ASSERT_FALSE(it.IsNULL());
    ASSERT_EQ(3, it.Size());
    float value = 0;
    ASSERT_TRUE(it.GetFloat(&value));
    ASSERT_EQ(1.2f, value);
    std::cout << value << std::endl;
    it.Next();
    ASSERT_TRUE(it.Valid());
    ASSERT_TRUE(it.IsNULL());
    it.Next();
    ASSERT_TRUE(it.Valid());
    ASSERT_FALSE(it.IsNULL());
    std::string str_value;
    ASSERT_TRUE(it.GetString(&str_value));
    ASSERT_STREQ(str_value.c_str(), EMPTY_STRING.c_str());
    it.Next();
    ASSERT_FALSE(it.Valid());
}

}  // namespace codec
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
