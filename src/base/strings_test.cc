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
    ASSERT_EQ("1.000m", result);
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
