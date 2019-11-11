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

// TEST_F(DBMSSdkTest, CreateTableTest) {
//  ::fesql::sdk::DBMSSdk *dbms_sdk = nullptr;
//  dbms_sdk = ::fesql::sdk::CreateDBMSSdk("127.0.0.1");
//  ASSERT_TRUE(nullptr != dbms_sdk);
//
//  const std::string sql =
//      "create table IF NOT EXISTS test(\n"
//      "    column1 int NOT NULL,\n"
//      "    column2 timestamp NOT NULL,\n"
//      "    column3 int NOT NULL,\n"
//      "    column4 string NOT NULL,\n"
//      "    column5 int NOT NULL\n"
//      ");";
//  ::fesql::sdk::Status status;
//  dbms_sdk->CreateTable(sql, status);
//
//  ASSERT_EQ(0, status.code);
//}
}  // namespace sdk
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
