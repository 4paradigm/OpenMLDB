/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * date_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/5/22
 *--------------------------------------------------------------------------
 **/
#include "codegen/date_ir_builder.h"
#include <memory>
#include <utility>
#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
ExitOnError ExitOnErr;
namespace fesql {
namespace codegen {
class DateIRBuilderTest : public ::testing::Test {
 public:
    DateIRBuilderTest() {}
    ~DateIRBuilderTest() {}
};

TEST_F(DateIRBuilderTest, DateOp) {
    codec::Date t1(2020, 05, 27);
    codec::Date t2(2020, 05, 27);
    codec::Date t3(2020, 05, 28);
    codec::Date t4(2020, 05, 26);

    ASSERT_EQ(t1, t2);
    ASSERT_TRUE(t1 >= t2);
    ASSERT_TRUE(t1 < t3);
    ASSERT_TRUE(t1 <= t3);
    ASSERT_TRUE(t1 >= t4);
    ASSERT_TRUE(t1 > t4);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
