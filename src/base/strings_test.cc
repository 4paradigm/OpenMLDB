//
// skip_list_test.cc
// Copyright 2017 4paradigm.com 

#include "base/strings.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class StringsTest : public ::testing::Test {

public:
    StringsTest(){}
    ~StringsTest() {}
};


TEST_F(StringsTest, FormatToString) {

    std::string result = FormatToString(98, 4);
    ASSERT_EQ("0098", result);
    result = FormatToString(10298, 4);
    ASSERT_EQ("0298", result);
    result = FormatToString(10298, 1);
    ASSERT_EQ("8", result);

}

TEST_F(StringsTest, ReadableTime) {

    std::string result = HumanReadableTime(60000);
    ASSERT_EQ("1m", result);
    result = HumanReadableTime(600);
    ASSERT_EQ("600ms", result);
    result = HumanReadableTime(6000 + 5);
    ASSERT_EQ("6s", result);
    result = HumanReadableTime(60000 * 5 + 100);
    ASSERT_EQ("5m", result);
    result = HumanReadableTime(60000 * 60 * 5 + 100);
    ASSERT_EQ("5h", result);
    result = HumanReadableTime(60000 * 60 * 24 * 5 + 100);
    ASSERT_EQ("5d", result);
}

TEST_F(StringsTest, StrToUINT64) {
    std::string str = "112233";
    uint64_t value = 0;
    ASSERT_TRUE(StrToUINT64(str.c_str(), str.length(), value));
    ASSERT_EQ(112233, value);

    str = "00112200394";
    ASSERT_TRUE(StrToUINT64(str.c_str(), str.length(), value));
    ASSERT_EQ(112200394, value);

    str = "0";
    ASSERT_TRUE(StrToUINT64(str.c_str(), str.length(), value));
    ASSERT_EQ(0, value);

    str = "-1";
    ASSERT_FALSE(StrToUINT64(str.c_str(), str.length(), value));

    str = "0011a2200394";
    ASSERT_FALSE(StrToUINT64(str.c_str(), str.length(), value));
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
