/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * dbms_sdk_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/7
 *--------------------------------------------------------------------------
 **/
#include "sdk/dbms_sdk.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace fesql {
namespace sdk {
class DBMSSdkTest : public ::testing::Test {
 public:
    DBMSSdkTest() {}

    ~DBMSSdkTest() {}
};

 TEST_F(DBMSSdkTest, CreateTableTest) {
  ::fesql::sdk::DBMSSdk *dbms_sdk = nullptr;
  ASSERT_TRUE(nullptr == dbms_sdk);

}
}  // namespace sdk
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
