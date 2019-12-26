/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * fesql_case_test.cc
 *
 * Author: chenjing
 * Date: 2019/12/25
 *--------------------------------------------------------------------------
 **/
#include "bm/fesql_client_bm_case.h"
#include "gtest/gtest.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
namespace fesql {
namespace bm {
class FeSQL_CASE_Test : public ::testing::Test {
 public:
    FeSQL_CASE_Test() {}
    ~FeSQL_CASE_Test() {}
};

TEST_F(FeSQL_CASE_Test, SIMPLE_QUERY_CASE1_TEST) {
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 10);
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 99);
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 100);
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 101);
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 1000);
}

TEST_F(FeSQL_CASE_Test, SIMPLE_QUERY_CASE1_BATCH_TEST) {
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 10);
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 99);
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 100);
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 101);
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 1000);
}

TEST_F(FeSQL_CASE_Test, WINDOW_CASE1_QUERY_TEST) {
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 10);
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 99);
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 100);
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 101);
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 1000);
}

TEST_F(FeSQL_CASE_Test, WINDOW_CASE1_QUERY_BATCH_TEST) {
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 10);
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 99);
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 100);
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 101);
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 1000);
}
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    InitLLVM X(argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
