/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * dbms_server_impl_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/11
 *--------------------------------------------------------------------------
 **/
#include "dbms/dbms_server_impl.h"
#include "brpc/server.h"
#include "gtest/gtest.h"
#include "node/node_enum.h"
#include "tablet/tablet_server_impl.h"
#include "gflags/gflags.h"

DECLARE_string(dbms_endpoint);
DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_bool(enable_keep_alive);

namespace fesql {
namespace dbms {
using fesql::base::Status;

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class DBMSServerImplTest : public ::testing::Test {
 public:
    DBMSServerImplTest() {}
    ~DBMSServerImplTest() {}
    void SetUp() {
        tablet_endpoint = "127.0.0.1:8120";
        tablet_ = new tablet::TabletServerImpl();
        tablet_->Init();
        brpc::ServerOptions options;
        server_.AddService(tablet_, brpc::SERVER_DOESNT_OWN_SERVICE);
        int32_t port = 8120;
        server_.Start(port, &options);
        dbms_ = new ::fesql::dbms::DBMSServerImpl();
        {
            MockClosure closure;
            dbms::KeepAliveRequest request;
            request.set_endpoint(tablet_endpoint);
            dbms::KeepAliveResponse response;
            dbms_->KeepAlive(NULL, &request, &response, &closure);
        }
    }
    void TearDown() {
        server_.Stop(10);
        delete dbms_;
        delete tablet_;
    }
 public:
    std::string tablet_endpoint;
    brpc::Server server_;
    ::fesql::dbms::DBMSServerImpl* dbms_;
    tablet::TabletServerImpl* tablet_;
};

TEST_F(DBMSServerImplTest, CreateTableTest) {
    MockClosure closure;
    // null db
    {
        ::fesql::dbms::AddTableRequest request;
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
        }
        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kNoDatabase, response.status().code());
    }
    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // null db
    {
        ::fesql::dbms::AddTableRequest request;
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kNoDatabase, response.status().code());
    }

    // create table
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt32);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create table
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test2");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt32);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create table with index
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test3");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
        }

        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt64);
        }

        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column3");
            column->set_type(fesql::type::kTimestamp);
        }

        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column4");
            column->set_type(fesql::type::kVarchar);
        }

        {
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->add_first_keys("column2");
            index->set_second_key("column3");
            index->add_ttl(86400000);
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // empty table name
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kBadRequest, response.status().code());
        ASSERT_EQ("table name is empty", response.status().msg());
    }

    // table name exist
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kTableExists, response.status().code());
        ASSERT_EQ("table already exists", response.status().msg());
    }

    {
        ::fesql::dbms::GetTablesRequest request;
        request.set_db_name("db_test");
        ::fesql::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(3, response.tables_size());
        ASSERT_EQ("test1", response.tables(0).name());
        ASSERT_EQ("test2", response.tables(1).name());
        ASSERT_EQ("test3", response.tables(2).name());
    }

    // show tables with empty items
    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test1");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    {
        ::fesql::dbms::GetTablesRequest request;
        request.set_db_name("db_test1");
        ::fesql::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(0, response.tables_size());
    }
}

TEST_F(DBMSServerImplTest, GetDatabasesAndTablesTest) {

    MockClosure closure;
    // show database
    {
        ::fesql::dbms::GetDatabasesRequest request;
        ::fesql::dbms::GetDatabasesResponse response;
        dbms_->GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(0, response.names_size());
    }
    // create database 1
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test1");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }
    // show database
    {
        ::fesql::dbms::GetDatabasesRequest request;
        ::fesql::dbms::GetDatabasesResponse response;
        dbms_->GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(1, response.names_size());
        ASSERT_EQ("db_test1", response.names(0));
    }
    // create database 2
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test2");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create database 3
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test3");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // show database
    {
        ::fesql::dbms::GetDatabasesRequest request;
        ::fesql::dbms::GetDatabasesResponse response;
        dbms_->GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(3, response.names_size());
        ASSERT_EQ("db_test1", response.names(0));
        ASSERT_EQ("db_test2", response.names(1));
        ASSERT_EQ("db_test3", response.names(2));
    }

    // show tables out of database
    {
        ::fesql::dbms::GetTablesRequest request;
        ::fesql::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kNoDatabase, response.status().code());
    }

    // create table 1
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test1");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt32);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create table 2
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test1");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test2");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt64);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create table 3
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test1");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test3");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt32);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create database 2 : table 1
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test2");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kInt32);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt32);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create database 2: table 4
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test2");
        ::fesql::type::TableDef* table = request.mutable_table();
        table->set_name("test4");
        {
            ::fesql::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(fesql::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(fesql::type::kInt32);
            ::fesql::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    {
        ::fesql::dbms::GetTablesRequest request;
        request.set_db_name("db_test1");
        ::fesql::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(3, response.tables_size());
        ASSERT_EQ("test1", response.tables(0).name());
        ASSERT_EQ("test2", response.tables(1).name());
        ASSERT_EQ("test3", response.tables(2).name());
    }

    {
        ::fesql::dbms::GetTablesRequest request;
        request.set_db_name("db_test2");
        ::fesql::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(2, response.tables_size());
        ASSERT_EQ("test1", response.tables(0).name());
        ASSERT_EQ("test4", response.tables(1).name());
    }

    {
        ::fesql::dbms::GetTablesRequest request;
        request.set_db_name("db_test3");
        ::fesql::dbms::GetTablesResponse response;
        MockClosure closure;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(0, response.tables_size());
    }
}

TEST_F(DBMSServerImplTest, GetTableTest) {
    MockClosure closure;
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    ::fesql::type::TableDef table;
    table.set_name("test");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("column1");
        column->set_type(fesql::type::kInt32);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("column2");
        column->set_type(fesql::type::kInt64);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("column3");
        column->set_type(fesql::type::kTimestamp);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("column4");
        column->set_type(fesql::type::kVarchar);
    }

    {
        ::fesql::type::IndexDef* index = table.add_indexes();
        index->set_name("index1");
        index->add_first_keys("column1");
        index->add_first_keys("column2");
        index->set_second_key("column3");
        index->add_ttl(86400000);
    }

    // create table with index
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        *(request.mutable_table()) = table;
        ::fesql::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // show table test
    {
        ::fesql::dbms::GetSchemaRequest request;
        request.set_db_name("db_test");
        ::fesql::dbms::GetSchemaResponse response;
        request.set_name("test");
        MockClosure closure;
        dbms_->GetSchema(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(table.DebugString(), response.table().DebugString());
    }
}

}  // namespace dbms
}  // namespace fesql
int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    FLAGS_enable_keep_alive = false;
    return RUN_ALL_TESTS();
}
