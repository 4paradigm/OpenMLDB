/*
 * memcomparable_format_test.cc
 */


#include "base/memcomparable_format.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class MemComFormatTest : public ::testing::Test {

public:
    MemComFormatTest(){}
    ~MemComFormatTest() {}
};

TEST_F(MemComFormatTest, TestInteger) {
    int16_t small_int = 10;
    char* to = new char[sizeof(int16_t)];
    MakeSortKeyInteger(&small_int, sizeof(small_int), false, to);
    int16_t dst;
    UnpackInteger(to, sizeof(int16_t), false, &dst);
    ASSERT_EQ(dst, 10);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
