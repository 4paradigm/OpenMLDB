/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * dbms_sdk_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/7
 *--------------------------------------------------------------------------
 **/
#include "sdk/dbms_sdk.h"
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace fesql {
namespace sdk {
using fesql::base::Status;

class DBMSSdkTest : public ::testing::Test {
 public:
    DBMSSdkTest() {}
    ~DBMSSdkTest() {}
};

void StartDBMSSever(brpc::Server &server, const std::string &endpoint) {
    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    brpc::ServerOptions options;
    options.num_threads = 2;
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }

    if (server.Start(endpoint.c_str(), &options) != 0) {
        LOG(WARNING) << "Fail to start dbms server";
        exit(1);
    }
    LOG(INFO) << "start dbms on port " << endpoint;
}

TEST_F(DBMSSdkTest, DatabasesAPITest) {
    brpc::Server server;
    int port = 9444;
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    StartDBMSSever(server, endpoint);
    ::fesql::sdk::DBMSSdk *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    {
        // get databases is empty
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetDatabases(names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0, names.size());
    }

    // create database db1
    {
        Status status;
        DatabaseDef db;
        db.name = "db_1";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db2
    {
        Status status;
        DatabaseDef db;
        db.name = "db_2";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db3
    {
        Status status;
        DatabaseDef db;
        db.name = "db_3";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // get databases
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetDatabases(names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3, names.size());
    }

    {
        // use databases db2
        DatabaseDef db;
        db.name = "db_2";
        Status status;
        dbms_sdk->EnterDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // use databases db_not_exist
        DatabaseDef db;
        db.name = "db_not_exist";
        Status status;
        dbms_sdk->EnterDatabase(db, status);
        ASSERT_TRUE(0 != static_cast<int>(status.code));
    }
}
TEST_F(DBMSSdkTest, GroupAPITest) {
    brpc::Server server;
    int port = 9445;
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    StartDBMSSever(server, endpoint);
    ::fesql::sdk::DBMSSdk *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    // create group g1
    {
        Status status;
        GroupDef group;
        group.name = "g1";
        dbms_sdk->CreateGroup(group, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
}

TEST_F(DBMSSdkTest, TableAPITest) {
    brpc::Server server;
    int port = 9446;
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    StartDBMSSever(server, endpoint);
    ::fesql::sdk::DBMSSdk *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    // create database db1
    {
        Status status;
        DatabaseDef db;
        db.name = "db_1";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db2
    {
        Status status;
        DatabaseDef db;
        db.name = "db_2";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // use databases db1
        DatabaseDef db;
        db.name = "db_1";
        Status status;
        dbms_sdk->EnterDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }



    {
        // create table test1
        DatabaseDef db;
        std::string sql =
            "create table test1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
            ");";
        db.name = "db_1";
        Status status;
        dbms_sdk->ExecuteScript(sql, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // create table test2
        DatabaseDef db;
        std::string sql =
            "create table IF NOT EXISTS test2(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL\n"
            ");";

        db.name = "db_1";
        Status status;
        dbms_sdk->ExecuteScript(sql, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // create table test3
        DatabaseDef db;
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4))\n"
            ");";

        db.name = "db_1";
        Status status;
        dbms_sdk->ExecuteScript(sql, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show tables
        DatabaseDef db;
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetTables(names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3, names.size());
    }
    {
        // use databases db2
        DatabaseDef db;
        db.name = "db_2";
        Status status;
        dbms_sdk->EnterDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show tables empty
        DatabaseDef db;
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetTables(names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0, names.size());
    }
}

TEST_F(DBMSSdkTest, ExecuteScriptAPITest) {
    brpc::Server server;
    int port = 9447;
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    StartDBMSSever(server, endpoint);
    ::fesql::sdk::DBMSSdk *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    // create database db1
    {
        Status status;
        DatabaseDef db;
        db.name = "db_1";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // use databases db1
        DatabaseDef db;
        db.name = "db_1";
        Status status;
        dbms_sdk->EnterDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        DatabaseDef db;
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4))\n"
            ");";

        db.name = "db_1";
        Status status;
        dbms_sdk->ExecuteScript(sql, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show tables
        DatabaseDef db;
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetTables(names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(1, names.size());
    }
}

}  // namespace sdk
}  // namespace fesql
int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
