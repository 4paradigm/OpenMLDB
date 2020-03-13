/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_op_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "vm/physical_op.h"
#include "node/node_manager.h"
#include "plan/planner.h"
#include "parser/parser.h"
#include <storage/table.h>
#include <stack>
#include "base/status.h"
#include "gtest/gtest.h"
#include "vm/test_base.h"
namespace fesql {
namespace vm {
class PhysicalOpTest : public ::testing::Test {
 public:
    PhysicalOpTest() {}
    ~PhysicalOpTest() {}
};

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
