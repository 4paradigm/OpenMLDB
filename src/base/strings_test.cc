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

TEST_F(StringsTest, Split) {
    std::string test1 = "xxxx,xxxxx";
    std::vector<std::string> result;
    SplitString(test1, ",", result);
    ASSERT_EQ(2, result.size());
    ASSERT_EQ("xxxx", result[0]);
    ASSERT_EQ("xxxxx", result[1]);
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

TEST_F(StringsTest, getNowTimeInSecond) {
    ASSERT_EQ(1573620180, ParseTimeToSecond("20191113124300", "%Y%m%d%H%M%S"));
    ASSERT_EQ(1582952399, ParseTimeToSecond("20200229125959", "%Y%m%d%H%M%S"));
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
