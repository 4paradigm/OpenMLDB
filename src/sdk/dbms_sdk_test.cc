/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * dbms_sdk_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/7
 *--------------------------------------------------------------------------
 **/

#include "sdk/dbms_sdk.h"
#include <unistd.h>
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "tablet/tablet_server_impl.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"


DECLARE_string(dbms_endpoint);
DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_bool(enable_keep_alive);

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT


namespace fesql {
namespace sdk {
class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};
class DBMSSdkTest : public ::testing::Test {
 public:
    DBMSSdkTest()
        : dbms_server_(), tablet_server_(), tablet_(NULL), dbms_(NULL) {}
    ~DBMSSdkTest() {}
    void SetUp() {
        brpc::ServerOptions options;
        tablet_ = new tablet::TabletServerImpl();
        tablet_->Init();
        tablet_server_.AddService(tablet_, brpc::SERVER_DOESNT_OWN_SERVICE);
        tablet_server_.Start(tablet_port, &options);
        dbms_ = new ::fesql::dbms::DBMSServerImpl();
        dbms_server_.AddService(dbms_, brpc::SERVER_DOESNT_OWN_SERVICE);
        dbms_server_.Start(dbms_port, &options);
        {
            std::string tablet_endpoint =
                "127.0.0.1:" + std::to_string(tablet_port);
            MockClosure closure;
            dbms::KeepAliveRequest request;
            request.set_endpoint(tablet_endpoint);
            dbms::KeepAliveResponse response;
            dbms_->KeepAlive(NULL, &request, &response, &closure);
        }
    }

    void TearDown() {
        dbms_server_.Stop(10);
        tablet_server_.Stop(10);
        delete tablet_;
        delete dbms_;
    }

 public:
    brpc::Server dbms_server_;
    brpc::Server tablet_server_;
    int tablet_port = 7212;
    int dbms_port = 7211;
    tablet::TabletServerImpl *tablet_;
    dbms::DBMSServerImpl *dbms_;
};

TEST_F(DBMSSdkTest, DatabasesAPITest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::fesql::sdk::DBMSSdk> dbms_sdk =
        ::fesql::sdk::CreateDBMSSdk(endpoint);
    {
        Status status;
        std::vector<std::string> names = dbms_sdk->GetDatabases(&status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0u, names.size());
    }

    // create database db1
    {
        Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    // create database db2
    {
        Status status;
        std::string name = "db_2";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db3
    {
        Status status;
        std::string name = "db_3";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // get databases
        Status status;
        std::vector<std::string> names = dbms_sdk->GetDatabases(&status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3u, names.size());
    }
}

TEST_F(DBMSSdkTest, TableAPITest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::fesql::sdk::DBMSSdk> dbms_sdk =
        ::fesql::sdk::CreateDBMSSdk(endpoint);
    // create database db1
    {
        Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db2
    {
        Status status;
        std::string name = "db_2";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // create table test1
        std::string sql =
            "create table test1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int,\n"
            "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
            ");";
        std::string name = "db_1";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table test2
        std::string sql =
            "create table IF NOT EXISTS test2(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column1), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table test3
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show db_1 tables
        std::string name = "db_1";
        Status status;
        std::shared_ptr<TableSet> tablet_set =
            dbms_sdk->GetTables(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3u, tablet_set->Size());
    }
    {
        // show tables empty
        std::string name = "db_2";
        Status status;
        std::shared_ptr<TableSet> ts = dbms_sdk->GetTables(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0u, ts->Size());
    }
}

TEST_F(DBMSSdkTest, ExecuteSQLTest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::fesql::sdk::DBMSSdk> dbms_sdk =
        ::fesql::sdk::CreateDBMSSdk(endpoint);
    std::string name = "db_2";
    {

        Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {

        Status status;
        // create table db1
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 bigint NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=column4, ts=column2)\n"
            ");";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    
    {

        Status status;
        // insert
        std::string sql = "insert into test3 values(1, 4000, 2, \"hello\", 3);";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {

        Status status;
        std::string sql = "select column1, column2, column3, column4, column5 from test3 limit 1;";
        std::shared_ptr<ResultSet> rs = dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(5, schema.GetColumnCnt());
            ASSERT_EQ("column1", schema.GetColumnName(0));
            ASSERT_EQ("column2", schema.GetColumnName(1));
            ASSERT_EQ("column3", schema.GetColumnName(2));
            ASSERT_EQ("column4", schema.GetColumnName(3));
            ASSERT_EQ("column5", schema.GetColumnName(4));

            ASSERT_EQ(kTypeInt32, schema.GetColumnType(0));
            ASSERT_EQ(kTypeInt64, schema.GetColumnType(1));
            ASSERT_EQ(kTypeInt32, schema.GetColumnType(2));
            ASSERT_EQ(kTypeString, schema.GetColumnType(3));
            ASSERT_EQ(kTypeInt32, schema.GetColumnType(4));

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(1, &val));
                ASSERT_EQ(val, 4000);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(2, &val));
                ASSERT_EQ(val, 2);
            }
           
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 3);
            }

            {
                char* val = NULL;
                uint32_t size = 0;
                ASSERT_TRUE(rs->GetString(3, &val, &size));
                ASSERT_EQ(size, 5);
                std::string str(val, 5);
                ASSERT_EQ(str, "hello");
            }
        }else{
            ASSERT_FALSE(true);
        }

    }

}

TEST_F(DBMSSdkTest, ExecuteScriptAPITest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::fesql::sdk::DBMSSdk> dbms_sdk =
        ::fesql::sdk::CreateDBMSSdk(endpoint);

    {
        Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table test4(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    index(key=(column1), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
}


}  // namespace sdk
}  // namespace fesql
int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    InitLLVM X(argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();

    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_enable_keep_alive = false;
    return RUN_ALL_TESTS();
}
