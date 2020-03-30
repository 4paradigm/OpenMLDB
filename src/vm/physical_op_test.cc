/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_op_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "gtest/gtest.h"
namespace fesql {
namespace vm {
class PhysicalOpTest : public ::testing::Test {
 public:
    PhysicalOpTest() {}
    ~PhysicalOpTest() {}
};
TEST_F(PhysicalOpTest, test) {
}
}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
