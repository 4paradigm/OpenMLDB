/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dbms/dbms_server_impl.h"
#include "brpc/server.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "node/node_enum.h"
#include "tablet/tablet_server_impl.h"

DECLARE_string(dbms_endpoint);
DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_bool(enable_keep_alive);

namespace hybridse {
namespace dbms {
using hybridse::base::Status;

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
        tablet_endpoint = "127.0.0.1:7121";
        tablet_ = new tablet::TabletServerImpl();
        tablet_->Init();
        brpc::ServerOptions options;
        server_.AddService(tablet_, brpc::SERVER_DOESNT_OWN_SERVICE);
        int32_t port = 7121;
        server_.Start(port, &options);
        dbms_ = new ::hybridse::dbms::DBMSServerImpl();
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
    ::hybridse::dbms::DBMSServerImpl* dbms_;
    tablet::TabletServerImpl* tablet_;
};

TEST_F(DBMSServerImplTest, CreateTableTest) {
    MockClosure closure;
    // null db
    {
        ::hybridse::dbms::AddTableRequest request;
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
        }
        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kNoDatabase, response.status().code());
    }
    // create database
    {
        ::hybridse::dbms::AddDatabaseRequest request;
        ::hybridse::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // null db
    {
        ::hybridse::dbms::AddTableRequest request;
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kNoDatabase, response.status().code());
    }

    // create table
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create table
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test2");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create table with index
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test3");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
        }

        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
        }

        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column3");
            column->set_type(hybridse::type::kTimestamp);
        }

        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column4");
            column->set_type(hybridse::type::kVarchar);
        }

        {
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->add_first_keys("column2");
            index->set_second_key("column3");
            index->add_ttl(86400000);
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // empty table name
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::hybridse::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kBadRequest, response.status().code());
        ASSERT_EQ("table name is empty", response.status().msg());
    }

    // table name exist
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
        }

        ::hybridse::dbms::AddTableResponse response;
        MockClosure closure;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kTableExists, response.status().code());
        ASSERT_EQ("table already exists", response.status().msg());
    }

    {
        ::hybridse::dbms::GetTablesRequest request;
        request.set_db_name("db_test");
        ::hybridse::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(3, response.tables_size());
        ASSERT_EQ("test1", response.tables(0).name());
        ASSERT_EQ("test2", response.tables(1).name());
        ASSERT_EQ("test3", response.tables(2).name());
    }

    // show tables with empty items
    // create database
    {
        ::hybridse::dbms::AddDatabaseRequest request;
        ::hybridse::dbms::AddDatabaseResponse response;
        request.set_name("db_test1");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    {
        ::hybridse::dbms::GetTablesRequest request;
        request.set_db_name("db_test1");
        ::hybridse::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(0, response.tables_size());
    }
}

TEST_F(DBMSServerImplTest, GetDatabasesAndTablesTest) {
    MockClosure closure;
    // show database
    {
        ::hybridse::dbms::GetDatabasesRequest request;
        ::hybridse::dbms::GetDatabasesResponse response;
        dbms_->GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(0, response.names_size());
    }
    // create database 1
    {
        ::hybridse::dbms::AddDatabaseRequest request;
        ::hybridse::dbms::AddDatabaseResponse response;
        request.set_name("db_test1");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }
    // show database
    {
        ::hybridse::dbms::GetDatabasesRequest request;
        ::hybridse::dbms::GetDatabasesResponse response;
        dbms_->GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(1, response.names_size());
        ASSERT_EQ("db_test1", response.names(0));
    }
    // create database 2
    {
        ::hybridse::dbms::AddDatabaseRequest request;
        ::hybridse::dbms::AddDatabaseResponse response;
        request.set_name("db_test2");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create database 3
    {
        ::hybridse::dbms::AddDatabaseRequest request;
        ::hybridse::dbms::AddDatabaseResponse response;
        request.set_name("db_test3");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // show database
    {
        ::hybridse::dbms::GetDatabasesRequest request;
        ::hybridse::dbms::GetDatabasesResponse response;
        dbms_->GetDatabases(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(3, response.names_size());
        ASSERT_EQ("db_test1", response.names(0));
        ASSERT_EQ("db_test2", response.names(1));
        ASSERT_EQ("db_test3", response.names(2));
    }

    // show tables out of database
    {
        ::hybridse::dbms::GetTablesRequest request;
        ::hybridse::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kNoDatabase, response.status().code());
    }

    // create table 1
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test1");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create table 2
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test1");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test2");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create table 3
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test1");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test3");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create database 2 : table 1
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test2");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test1");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kInt32);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // create database 2: table 4
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test2");
        ::hybridse::type::TableDef* table = request.mutable_table();
        table->set_name("test4");
        {
            ::hybridse::type::ColumnDef* column = table->add_columns();
            column->set_name("column1");
            column->set_type(hybridse::type::kVarchar);
            column = table->add_columns();
            column->set_name("column2");
            column->set_type(hybridse::type::kInt64);
            ::hybridse::type::IndexDef* index = table->add_indexes();
            index->set_name("index1");
            index->add_first_keys("column1");
            index->set_second_key("column2");
        }

        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    {
        ::hybridse::dbms::GetTablesRequest request;
        request.set_db_name("db_test1");
        ::hybridse::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(3, response.tables_size());
        ASSERT_EQ("test1", response.tables(0).name());
        ASSERT_EQ("test2", response.tables(1).name());
        ASSERT_EQ("test3", response.tables(2).name());
    }

    {
        ::hybridse::dbms::GetTablesRequest request;
        request.set_db_name("db_test2");
        ::hybridse::dbms::GetTablesResponse response;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(2, response.tables_size());
        ASSERT_EQ("test1", response.tables(0).name());
        ASSERT_EQ("test4", response.tables(1).name());
    }

    {
        ::hybridse::dbms::GetTablesRequest request;
        request.set_db_name("db_test3");
        ::hybridse::dbms::GetTablesResponse response;
        MockClosure closure;
        dbms_->GetTables(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(0, response.tables_size());
    }
}

TEST_F(DBMSServerImplTest, GetTableTest) {
    MockClosure closure;
    {
        ::hybridse::dbms::AddDatabaseRequest request;
        ::hybridse::dbms::AddDatabaseResponse response;
        request.set_name("db_test");
        dbms_->AddDatabase(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    ::hybridse::type::TableDef table;
    table.set_name("test");
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_name("column1");
        column->set_type(hybridse::type::kInt32);
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_name("column2");
        column->set_type(hybridse::type::kInt64);
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_name("column3");
        column->set_type(hybridse::type::kTimestamp);
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_name("column4");
        column->set_type(hybridse::type::kVarchar);
    }

    {
        ::hybridse::type::IndexDef* index = table.add_indexes();
        index->set_name("index1");
        index->add_first_keys("column1");
        index->add_first_keys("column2");
        index->set_second_key("column3");
        index->add_ttl(86400000);
    }

    // create table with index
    {
        ::hybridse::dbms::AddTableRequest request;
        request.set_db_name("db_test");
        *(request.mutable_table()) = table;
        ::hybridse::dbms::AddTableResponse response;
        dbms_->AddTable(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
    }

    // show table test
    {
        ::hybridse::dbms::GetSchemaRequest request;
        request.set_db_name("db_test");
        ::hybridse::dbms::GetSchemaResponse response;
        request.set_name("test");
        MockClosure closure;
        dbms_->GetSchema(NULL, &request, &response, &closure);
        ASSERT_EQ(hybridse::common::kOk, response.status().code());
        ASSERT_EQ(table.DebugString(), response.table().DebugString());
    }
}

}  // namespace dbms
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_enable_keep_alive = false;
    return RUN_ALL_TESTS();
}
