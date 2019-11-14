/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * dbms_server_impl_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/11
 *--------------------------------------------------------------------------
 **/
#include "dbms/dbms_server_impl.h"
#include "gtest/gtest.h"
#include "node/node_enum.h"
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
};

TEST_F(DBMSServerImplTest, CreateTableTest) {
    ::fesql::dbms::DBMSServerImpl dbms_server;

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
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kNoDatabase, response.status().code());
    }

    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
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
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
            column->set_type(fesql::type::kString);
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
            column->set_type(fesql::type::kString);
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
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // empty table name
    {
        ::fesql::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
        dbms_server.AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kTableExists, response.status().code());
        ASSERT_EQ("table already exists", response.status().msg());
    }

    {
        ::fesql::dbms::GetItemsRequest request;
        request.set_db_name("db_test");
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(3, response.items_size());
        ASSERT_EQ("test1", response.items(0));
        ASSERT_EQ("test2", response.items(1));
        ASSERT_EQ("test3", response.items(2));
    }

    // show tables with empty items
    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test1");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    {
        ::fesql::dbms::GetItemsRequest request;
        request.set_db_name("db_test1");
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(0, response.items_size());
    }
}

TEST_F(DBMSServerImplTest, GetDatabasesAndTablesTest) {
    ::fesql::dbms::DBMSServerImpl dbms_server;
    // show database
    {
        ::fesql::dbms::GetItemsRequest request;
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(0, response.items_size());
    }
    // create database 1
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test1");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }
    // show database
    {
        ::fesql::dbms::GetItemsRequest request;
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(1, response.items_size());
        ASSERT_EQ("db_test1", response.items(0));
    }
    // create database 2
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test2");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create database 3
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test3");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // show database
    {
        ::fesql::dbms::GetItemsRequest request;
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(3, response.items_size());
        ASSERT_EQ("db_test1", response.items(0));
        ASSERT_EQ("db_test2", response.items(1));
        ASSERT_EQ("db_test3", response.items(2));
    }

    // show tables out of database
    {
        ::fesql::dbms::GetItemsRequest request;
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetTables(NULL, &request, &response, &closure);
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
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
            column->set_type(fesql::type::kString);
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
            column->set_type(fesql::type::kString);
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
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
            column->set_type(fesql::type::kString);
        }

        ::fesql::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }


    {
        ::fesql::dbms::GetItemsRequest request;
        request.set_db_name("db_test1");
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(3, response.items_size());
        ASSERT_EQ("test1", response.items(0));
        ASSERT_EQ("test2", response.items(1));
        ASSERT_EQ("test3", response.items(2));
    }

    {
        ::fesql::dbms::GetItemsRequest request;
        request.set_db_name("db_test2");
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(2, response.items_size());
        ASSERT_EQ("test1", response.items(0));
        ASSERT_EQ("test4", response.items(1));
    }

    {
        ::fesql::dbms::GetItemsRequest request;
        request.set_db_name("db_test3");
        ::fesql::dbms::GetItemsResponse response;
        MockClosure closure;
        dbms_server.GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(0, response.items_size());
    }
}
TEST_F(DBMSServerImplTest, GetTableTest) {
    ::fesql::dbms::DBMSServerImpl dbms_server;
    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
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
        column->set_type(fesql::type::kString);
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
        MockClosure closure;
        dbms_server.AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // show table test
    {
        ::fesql::dbms::GetSchemaRequest request;
        request.set_db_name("db_test");
        ::fesql::dbms::GetSchemaResponse response;
        request.set_name("test");
        MockClosure closure;
        dbms_server.GetSchema(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(table.DebugString(), response.table().DebugString());
    }
}

TEST_F(DBMSServerImplTest, DataBaseTest) {
    ::fesql::dbms::DBMSServerImpl dbms_server;
    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create database empty name
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kBadRequest, response.status().code());
        ASSERT_EQ("database name is empty", response.status().msg());
    }

    // create database already exist
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kNameExists, response.status().code());
        ASSERT_EQ("database name exists", response.status().msg());
    }

    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test2");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // create database
    {
        ::fesql::dbms::AddDatabaseRequest request;
        ::fesql::dbms::AddDatabaseResponse response;
        request.set_name("db_test3");
        MockClosure closure;
        dbms_server.AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
    }

    // exist database
    {
        ::fesql::dbms::IsExistRequest request;
        ::fesql::dbms::IsExistResponse response;
        request.set_name("db_test");
        MockClosure closure;
        dbms_server.IsExistDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(true, response.exist());
    }

    // not exist database
    {
        ::fesql::dbms::IsExistRequest request;
        ::fesql::dbms::IsExistResponse response;
        request.set_name("db_test_not_exist");
        MockClosure closure;
        dbms_server.IsExistDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(fesql::common::kOk, response.status().code());
        ASSERT_EQ(false, response.exist());
    }
}

}  // namespace dbms
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}