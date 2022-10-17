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

#include "sdk/openmldb_api.h"

#include <unistd.h>

#include <ctime>
#include <iostream>
#include <string>
#include <vector>

#include "case/sql_case.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/result_set.h"

namespace openmldb {
namespace sdk {
::openmldb::sdk::MiniCluster* mc_;
OpenmldbHandler* handler;

class OpenmldbApiTest : public ::testing::Test {
 public:
    OpenmldbApiTest() {
        std::time_t t = std::time(0);
        db_ = "cxx_api_db" + std::to_string(t);
        std::string sql = "create database " + db_ + ";";
        EXPECT_TRUE(execute(*handler, sql));
        LOG(INFO) << "create db " << db_ << " succeed";
    }
    ~OpenmldbApiTest() {
        std::string sql = "drop database " + db_ + ";";
        EXPECT_TRUE(execute(*handler, sql)) << handler->get_status()->msg;
    }

 protected:
    std::string db_;
};

TEST_F(OpenmldbApiTest, SimpleApiTest) {
    ASSERT_TRUE(execute(*handler, "SET @@execute_mode='online';"));

    auto sql = "use " + db_ + ";";
    ASSERT_TRUE(execute(*handler, sql));
    LOG(INFO) << "use db succeed";
    std::string table = "test_table";
    sql = "create table " + table +
          "("
          "col1 string, col2 bigint,"
          "index(key=col1, ts=col2));";
    ASSERT_TRUE(execute(*handler, sql));
    LOG(INFO) << "create table test_table succeed";

    sql = "insert test_table values(\"hello\", 1)";
    ASSERT_TRUE(execute(*handler, sql));
    sql = "insert test_table values(\"Hi~\", 2)";
    ASSERT_TRUE(execute(*handler, sql));

    sql = "select * from test_table;";
    ASSERT_TRUE(execute(*handler, sql));
    auto res = get_resultset();
    ASSERT_EQ(2u, res->Size());
    print_resultset(res);

    sql = "select * from test_table where col1 = ? ;";
    ParameterRow para(handler);
    para << "Hi~";
    ASSERT_TRUE(execute_parameterized(*handler, db_, sql, para));
    res = get_resultset();
    ASSERT_TRUE(res->Next());
    ASSERT_EQ("Hi~, 2", res->GetRowString());
    ASSERT_FALSE(res->Next());
    print_resultset(res);

    sql =
        "select col1, sum(col2) over w as w_col2_sum from test_table "
        "window w as (partition by test_table.col1 order by test_table.col2 "
        "rows between 2 preceding and current row);";
    RequestRow req(handler, db_, sql);
    req << "Hi~" << 3l;
    ASSERT_TRUE(execute_request(req));
    res = get_resultset();
    ASSERT_TRUE(res->Next());
    ASSERT_EQ("Hi~, 5", res->GetRowString());
    ASSERT_FALSE(res->Next());
    print_resultset(res);

    ASSERT_TRUE(execute(*handler, "drop table " + table));
}

// test execute() execute_parameterized() execute_request
TEST_F(OpenmldbApiTest, ComplexApiTest) {
    // test execute() and execute_parameterized()
    LOG(INFO) << "test execute() and execute_parameterized()";
    {
        // table name
        std::string table_name = "trans";

        // creat table
        std::string sql = "create table " + table_name +
                          "(c_sk_seq string,\n"
                          "                   cust_no string,\n"
                          "                   pay_cust_name string,\n"
                          "                   pay_card_no string,\n"
                          "                   payee_card_no string,\n"
                          "                   card_type string,\n"
                          "                   merch_id string,\n"
                          "                   txn_datetime string,\n"
                          "                   txn_amt double,\n"
                          "                   txn_curr string,\n"
                          "                   card_balance double,\n"
                          "                   day_openbuy double,\n"
                          "                   credit double,\n"
                          "                   remainning_credit double,\n"
                          "                   indi_openbuy double,\n"
                          "                   lgn_ip string,\n"
                          "                   IEMI string,\n"
                          "                   client_mac string,\n"
                          "                   chnl_type int32,\n"
                          "                   cust_idt int32,\n"
                          "                   cust_idt_no string,\n"
                          "                   province string,\n"
                          "                   city string,\n"
                          "                   latitudeandlongitude string,\n"
                          "                   txn_time int64,\n"
                          "                   index(key=pay_card_no, ts=txn_time),\n"
                          "                   index(key=merch_id, ts=txn_time));";
        ASSERT_TRUE(execute(*handler, db_, sql));
        LOG(INFO) << "create table " << table_name << "succeed";

        // insert data into table
        int64_t ts = 1594800959827;
        {
            char buffer[4096];
            sprintf(buffer,  // NOLINT
                    "insert into trans "
                    "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                    "payee_card_no0','card_type0','mc_%d','2020-"
                    "10-20 "
                    "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                    ",'client_mac0',10,20,'cust_idt_no0','"
                    "province0',"
                    "'city0', 'longitude', %s);",
                    0, 0, std::to_string(ts++).c_str());  // NOLINT
            std::string insert_sql = std::string(buffer, strlen(buffer));
            ASSERT_TRUE(execute(*handler, db_, insert_sql));
        }
        {
            char buffer[4096];
            sprintf(buffer,  // NOLINT
                    "insert into trans "
                    "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                    "payee_card_no0','card_type0','mc_%d','2020-"
                    "10-20 "
                    "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                    ",'client_mac0',10,20,'cust_idt_no0','"
                    "province0',"
                    "'city0', 'longitude', %s);",
                    0, 0, std::to_string(ts++).c_str());  // NOLINT
            std::string insert_sql = std::string(buffer, strlen(buffer));
            ASSERT_TRUE(execute(*handler, db_, insert_sql));
        }
        {
            char buffer[4096];
            sprintf(buffer,  // NOLINT
                    "insert into trans "
                    "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                    "payee_card_no0','card_type0','mc_%d','2020-"
                    "10-20 "
                    "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                    ",'client_mac0',10,20,'cust_idt_no0','"
                    "province0',"
                    "'city0', 'longitude', %s);",
                    0, 0, std::to_string(ts++).c_str());  // NOLINT
            std::string insert_sql = std::string(buffer, strlen(buffer));
            ASSERT_TRUE(execute(*handler, db_, insert_sql));
        }

        std::string select_all = "select * from " + table_name + ";";
        ASSERT_TRUE(execute(*handler, db_, select_all));
        auto res = get_resultset();
        ASSERT_EQ(3u, res->Size());
        print_resultset(res);
        LOG(INFO) << "test execute() succeed";

        std::string sql_para = "select * from " + table_name + " where merch_id = ? and txn_time < ?;";
        ParameterRow para(handler);
        LOG(INFO) << "condition  txn_time = 1594800959828";
        para << "mc_0" << 1594800959828;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_TRUE(res->Next());
        ASSERT_EQ(
            "c_sk_seq0, cust_no0, pay_cust_name0, card_0, payee_card_no0, "
            "card_type0, mc_0, 2020-10-20 10:23:50, 1.000000, txn_curr, 2.000000, "
            "3.000000, 4.000000, 5.000000, 6.000000, lgn_ip0, iemi0, client_mac0, 10, "
            "20, cust_idt_no0, province0, city0, longitude, 1594800959827",
            res->GetRowString());
        ASSERT_FALSE(res->Next());
        print_resultset(res);

        para.reset();
        LOG(INFO) << "condition  txn_time = 1594800959830";
        para << "mc_0" << 1594800959830;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(3u, res->Size());
        print_resultset(res);
        LOG(INFO) << "test parameter_execute() succeed";
        ASSERT_TRUE(execute(*handler, db_, "drop table " + table_name));
    }

    //  test execute_request()
    LOG(INFO) << "test execute_request()";
    {
        std::string table_name = "trans1";
        std::string create_table = "create table " + table_name +
                                   "("
                                   "col1 string, col2 bigint,"
                                   "index(key=col1, ts=col2));";
        ASSERT_TRUE(execute(*handler, db_, create_table));
        LOG(INFO) << "create table test_table succeed";

        std::string insert = "insert into " + table_name + " values('hello', 1590);";
        ASSERT_TRUE(execute(*handler, db_, insert));
        LOG(INFO) << "insert 1row into test_table succeed";

        std::string sql_req = "select sum(col2)  over w as sum_col2 from " + table_name +
                              " window w as (partition by " + table_name + ".col1 order by " + table_name +
                              ".col2 rows between 3 preceding and current row);";
        RequestRow req(handler, db_, sql_req);
        req << "hello" << (int64_t)2000;
        ASSERT_TRUE(execute_request(req));
        auto res = get_resultset();
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("3590", res->GetRowString());
        ASSERT_FALSE(res->Next());
        print_resultset(res);
        LOG(INFO) << "execute_request() succeed";
        ASSERT_TRUE(execute(*handler, db_, "drop table " + table_name));
    }
}

// test types of ParameterRow and RequestRow
// users should set the data inserted into ParameterRow and RequestRow to the appropriate type or perform forced type
// conversion, which can not only ensure that the data type is clearly determined, but also avoid errors caused by
// implicit type conversion.
TEST_F(OpenmldbApiTest, TypesOfParameterRowAndRequestRowTest) {
    LOG(INFO) << "test types of ParameterRow and RequestRow";
    std::string table_name = "paratypestest";
    // creat table
    std::string sql = "create table " + table_name +
                      "(test_bool bool,\n"
                      "                   test_int16 smallint, \n"
                      "                   test_int32 int, \n"
                      "                   test_int64 bigint, \n"
                      "                   test_float float, \n"
                      "                   test_double double, \n"
                      "                   test_string string, \n"
                      "                   test_date date, \n"
                      "                   test_timestamp TimeStamp);";
    ASSERT_TRUE(execute(*handler, db_, sql));
    LOG(INFO) << "create table succeed";

    // insert data
    {
        std::string insert_sql = "insert into " + table_name +
                                 " values(true, 32760, 2147483640, 922337203685477580, "
                                 "3.14, 6.88, 'the first row', '2020-1-1', 1594800959827)";
        ASSERT_TRUE(execute(*handler, db_, insert_sql));
        insert_sql = "insert into " + table_name +
                     " values(true, 32761, 2147483641, 922337203685477581, "
                     "3.14563, 6.885247821, 'the second row', '2020-1-2', 1594800959828)";
        ASSERT_TRUE(execute(*handler, db_, insert_sql));
        insert_sql = "insert into " + table_name +
                     " values(true, 32762, 2147483642, 922337203685477582, "
                     "2.14, 7.899, 'the third row', '2020-1-3', 1594800959829)";
        ASSERT_TRUE(execute(*handler, db_, insert_sql));
        insert_sql = "insert into " + table_name +
                     " values(false, 32763, 2147483643, 922337203685477583, "
                     "4.86, 5.733, 'the forth row', '2020-1-4', 15948009598296)";
        ASSERT_TRUE(execute(*handler, db_, insert_sql));
        LOG(INFO) << "insert rows succeed";
    }
    ASSERT_TRUE(execute(*handler, db_, "select * from " + table_name + ";"));
    auto res = get_resultset();
    ASSERT_EQ(4u, res->Size());
    print_resultset(res);

    // test all parameter types of ParameterRow
    // bool
    {
        std::string sql_para = "select * from " + table_name + " where test_bool = ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << false;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(1u, res->Size());
        print_resultset(res);
    }
    // int16
    {
        std::string sql_para = "select * from " + table_name + " where test_int16 >= ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << 32762;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(2u, res->Size());
        print_resultset(res);
    }
    // int32
    {
        std::string sql_para = "select * from " + table_name + " where test_int32 < ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << 2147483642;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(2u, res->Size());
        print_resultset(res);
    }
    // int64
    {
        std::string sql_para = "select * from " + table_name + " where test_int64 = ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << 922337203685477583;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_TRUE(res->Next());
        ASSERT_EQ(
            "false, 32763, 2147483643, 922337203685477583, "
            "4.860000, 5.733000, the forth row, 2020-01-04, 15948009598296",
            res->GetRowString());
        ASSERT_FALSE(res->Next());
        print_resultset(res);
    }
    // float
    {
        std::string sql_para = "select * from " + table_name + " where test_float >= ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << 3.14563f;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(2u, res->Size());
        print_resultset(res);
    }
    // double
    {
        std::string sql_para = "select * from " + table_name + " where test_double >= ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << 6.88;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(3u, res->Size());
        print_resultset(res);
    }
    // string
    {
        std::string sql_para = "select * from " + table_name + " where test_string = ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << "the first row";
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_TRUE(res->Next());
        ASSERT_EQ(
            "true, 32760, 2147483640, 922337203685477580, "
            "3.140000, 6.880000, the first row, 2020-01-01, 1594800959827",
            res->GetRowString());
        ASSERT_FALSE(res->Next());
        print_resultset(res);
        print_resultset(get_resultset());
    }
    // date
    {
        Date date(2020, 1, 2);
        std::string sql_para = "select * from " + table_name + " where test_date >= ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << date;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_EQ(3u, res->Size());
        print_resultset(res);
    }
    // TimeStamp
    {
        TimeStamp ts(1594800959828);
        std::string sql_para = "select * from " + table_name + " where test_timestamp = ?;";
        ParameterRow para(handler);
        LOG(INFO) << sql_para << std::endl;
        para << ts;
        ASSERT_TRUE(execute_parameterized(*handler, db_, sql_para, para));
        res = get_resultset();
        ASSERT_TRUE(res->Next());
        ASSERT_EQ(
            "true, 32761, 2147483641, 922337203685477581, "
            "3.145630, 6.885248, the second row, 2020-01-02, 1594800959828",
            res->GetRowString());
        ASSERT_FALSE(res->Next());
        print_resultset(res);
    }
    ASSERT_TRUE(execute(*handler, db_, "drop table " + table_name));

    table_name = "reqtypestest";
    sql = "create table " + table_name +
          "(c1 string,\n"
          "                           c2 smallint,\n"
          "                           c3 int,\n"
          "                           c4 bigint,\n"
          "                           c5 float,\n"
          "                           c6 double,\n"
          "                           c7 TimeStamp,\n"
          "                           c8 date,\n"
          "                           index(key=c1, ts=c7));";
    ASSERT_TRUE(execute(*handler, db_, sql));
    LOG(INFO) << "create table succeed";

    // insert data
    {
        std::string insert_sql =
            "insert into " + table_name + " values(\"aa\",13,23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        ASSERT_TRUE(execute(*handler, db_, insert_sql));
        insert_sql = "insert into " + table_name + " values(\"bb\",14,24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
        ASSERT_TRUE(execute(*handler, db_, insert_sql));
        LOG(INFO) << "insert rows succeed";
    }
    // test parameter types of RequestRow
    {
        std::string sql_req = "select c1, c2, c3, sum(c4) over w1 as w1_c4_sum from " + table_name +
                              " window w1 as "
                              "(partition by " +
                              table_name + ".c1 order by " + table_name +
                              ".c7 rows between 2 preceding and current row);";
        RequestRow req(handler, db_, sql_req);
        TimeStamp timestamp(1590738994000);
        Date date(2020, 5, 5);
        // need to check short, disable cpplint
        req << "bb" << (short)14 << 24 << 35l << 1.5f << 2.5 << timestamp << date;  // NOLINT
        ASSERT_TRUE(execute_request(req));
        res = get_resultset();
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("bb, 14, 24, 69", res->GetRowString());
        ASSERT_FALSE(res->Next());
        print_resultset(res);
        LOG(INFO) << "execute_request() succeed";
    }

    ASSERT_TRUE(execute(*handler, db_, "drop table " + table_name));
}

}  // namespace sdk
}  // namespace openmldb

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(nullptr));
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_zk_session_timeout = 100000;
    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::sdk::mc_ = &mc;
    FLAGS_enable_distsql = true;
    int ok = ::openmldb::sdk::mc_->SetUp(3);
    sleep(5);
    ::openmldb::sdk::handler =
        new OpenmldbHandler(::openmldb::sdk::mc_->GetZkCluster(), ::openmldb::sdk::mc_->GetZkPath());

    ok = RUN_ALL_TESTS();
    delete ::openmldb::sdk::handler;
    ::openmldb::sdk::mc_->Close();
    return ok;
}
