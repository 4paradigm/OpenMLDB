//
// flat_array_test.cc
// Copyright 2017 4paradigm.com 


#include "base/flat_array.h"
#include "gtest/gtest.h"
#include "base/strings.h"
#include <iostream>

namespace rtidb {
namespace base {

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
    std::string big_col1(100,'a'); 
    ok = codec.Append(big_col1);
    ASSERT_TRUE(ok);
    std::string big_col2(127,'b'); 
    ok = codec.Append(big_col2);
    ASSERT_TRUE(ok);
    std::string big_col3(128,'c'); 
    ok = codec.Append(big_col3);
    ASSERT_TRUE(ok);
    std::string big_col4(32767,'e'); 
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
    ASSERT_TRUE(buffer.size() == 13);
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



}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
