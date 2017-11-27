//
// slice_test.cc
// Copyright 2017 4paradigm.com 

#include "gtest/gtest.h"
#include "base/slice.h"

namespace rtidb {
namespace base {

class SliceTest : public ::testing::Test {

public:
    SliceTest(){}
    ~SliceTest() {}
};




TEST_F(SliceTest, Compare) {
    Slice a("test1");
    Slice b("test1");
    ASSERT_EQ(0, a.compare(b));
    ASSERT_TRUE(a==b);

    ASSERT_EQ(sizeof(a), 16);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
