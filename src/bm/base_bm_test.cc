/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * base_bm_test.cc
 *
 * Author: chenjing
 * Date: 2019/12/24
 *--------------------------------------------------------------------------
 **/
#include "bm/base_bm.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
namespace fesql {
namespace bm {

class BMBaseTest : public ::testing::Test {
 public:
    BMBaseTest() {}
    ~BMBaseTest() {}
};

TEST_F(BMBaseTest, Int16_Repeater) {
    {
        IntRepeater<int16_t> repeater;
        repeater.Range(0, 100, 1);
        ASSERT_EQ(repeater.GetValue(), 0);
        ASSERT_EQ(repeater.GetValue(), 1);
        ASSERT_EQ(repeater.GetValue(), 2);
    }
    {
        IntRepeater<int16_t> repeater;
        repeater.Range(0, 100, 20);
        ASSERT_EQ(repeater.GetValue(), 0);
        ASSERT_EQ(repeater.GetValue(), 20);
        ASSERT_EQ(repeater.GetValue(), 40);
    }
}

}  // namespace bm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
