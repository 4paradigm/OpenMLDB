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
#include "tablet/tablet_server_impl.h"

namespace fesql {
namespace sdk {

class DBMSSdkTest : public ::testing::Test {
 public:
    DBMSSdkTest() {}
    ~DBMSSdkTest() {}
};

TEST_F(DBMSSdkTest, DatabasesAPITest) {
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8200;
    int port = 9444;

    tablet::TabletServerImpl *tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    ::fesql::sdk::DBMSSdk *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    {
        // get databases is empty
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetDatabases(names, status);
        std::cout << status.msg << std::endl;
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0u, names.size());
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
        ASSERT_EQ(3u, names.size());
    }

    {
        // use databases db2
        DatabaseDef db;
        db.name = "db_2";
        Status status;
        dbms_sdk->IsExistDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // use databases db_not_exist
        DatabaseDef db;
        db.name = "db_not_exist";
        Status status;
        ASSERT_EQ(false, dbms_sdk->IsExistDatabase(db, status));
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    delete tablet;
    delete dbms;
    delete dbms_sdk;
}
TEST_F(DBMSSdkTest, GroupAPITest) {
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8201;
    int port = 9445;

    tablet::TabletServerImpl *tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
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
    delete dbms;
    delete dbms_sdk;
    delete tablet;
}

TEST_F(DBMSSdkTest, TableAPITest) {
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8203;
    int port = 9446;

    tablet::TabletServerImpl *tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
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
        ::fesql::sdk::ExecuteResult result;
        db.name = "db_2";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table test1
        DatabaseDef db;
        std::string sql =
            "create table test1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int,\n"
            "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
            ");";
        db.name = "db_1";
        Status status;
        ::fesql::sdk::ExecuteResult result;
        ::fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        std::cout << status.msg << std::endl;
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // desc test1
    {
        DatabaseDef db;
        db.name = "db_1";
        Status status;
        std::unique_ptr<::fesql::sdk::Schema> rs =
            dbms_sdk->GetSchema(db, "test1", status);
        std::cout << status.msg << std::endl;
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_NE(rs, 0);
        ASSERT_EQ(5, rs.get()->GetColumnCnt());
        ASSERT_EQ("column1", rs.get()->GetColumnName(0));
        ASSERT_EQ("column2", rs.get()->GetColumnName(1));
        ASSERT_EQ("column3", rs.get()->GetColumnName(2));
        ASSERT_EQ("column4", rs.get()->GetColumnName(3));
        ASSERT_EQ("column5", rs.get()->GetColumnName(4));

        ASSERT_EQ(sdk::kTypeInt32, rs.get()->GetColumnType(0));
        ASSERT_EQ(sdk::kTypeTimestamp, rs.get()->GetColumnType(1));
        ASSERT_EQ(sdk::kTypeInt32, rs.get()->GetColumnType(2));
        ASSERT_EQ(sdk::kTypeString, rs.get()->GetColumnType(3));
        ASSERT_EQ(sdk::kTypeInt32, rs.get()->GetColumnType(4));

        ASSERT_EQ(true, rs.get()->IsColumnNotNull(0));
        ASSERT_EQ(true, rs.get()->IsColumnNotNull(1));
        ASSERT_EQ(false, rs.get()->IsColumnNotNull(2));
        ASSERT_EQ(true, rs.get()->IsColumnNotNull(3));
        ASSERT_EQ(false, rs.get()->IsColumnNotNull(4));
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
        ::fesql::sdk::ExecuteResult result;
        ::fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
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
        ::fesql::sdk::ExecuteResult result;
        ::fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show db_1 tables
        DatabaseDef db;
        db.name = "db_1";
        std::vector<std::string> names;
        Status status;
        ::fesql::sdk::ExecuteRequst request;

        dbms_sdk->GetTables(db, names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3u, names.size());
    }

    {
        // use databases db2
        DatabaseDef db;
        db.name = "db_2";
        Status status;
        dbms_sdk->IsExistDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show tables empty
        DatabaseDef db;
        db.name = "db_2";
        std::vector<std::string> names;
        Status status;
        dbms_sdk->GetTables(db, names, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0u, names.size());
    }
    delete dbms;
    delete dbms_sdk;
    delete tablet;
}

TEST_F(DBMSSdkTest, ExecuteScriptAPITest) {
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8204;
    int port = 9447;

    tablet::TabletServerImpl *tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
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
        // exist databases db1
        DatabaseDef db;
        db.name = "db_1";
        Status status;
        ASSERT_EQ(true, dbms_sdk->IsExistDatabase(db, status));
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
        fesql::sdk::ExecuteResult result;
        fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        DatabaseDef db;
        std::string sql =
            "create table test4(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    index(key=(column4))\n"
            ");";

        db.name = "db_1";
        Status status;
        fesql::sdk::ExecuteResult result;
        fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    delete dbms;
    delete dbms_sdk;
    delete tablet;
}

}  // namespace sdk
}  // namespace fesql
int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
