//
// slice_test.cc
// Copyright 2017 4paradigm.com

#include "base/fe_slice.h"
#include "gtest/gtest.h"

namespace fesql {
namespace base {

class SliceTest : public ::testing::Test {
 public:
    SliceTest() {}
    ~SliceTest() {}
};

TEST_F(SliceTest, Compare) {
    Slice a("test1");
    Slice b("test1");
    ASSERT_EQ(0, a.compare(b));
    ASSERT_TRUE(a == b);
    ASSERT_EQ(sizeof(a), 16u);
}

TEST_F(SliceTest, ref_cnt_slice) {
    auto buf = reinterpret_cast<int8_t*>(malloc(1024));
    strcpy(reinterpret_cast<char*>(buf), "hello world");  // NOLINT

    RefCountedSlice ref;
    {
        auto slice = RefCountedSlice::CreateManaged(buf, 1024);
        ref = slice;
    }
    ASSERT_EQ(0, strcmp(reinterpret_cast<char*>(ref.buf()), "hello world"));
}

}  // namespace base
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
