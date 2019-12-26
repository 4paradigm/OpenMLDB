/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * fesql_case.cc
 *
 * Author: chenjing
 * Date: 2019/12/25
 *--------------------------------------------------------------------------
 **/

#include "bm/fesql_client_bm_case.h"
#include <memory>
#include <string>
#include "glog/logging.h"
#include "gtest/gtest.h"
namespace fesql {
namespace bm {

const std::string host = "127.0.0.1";    // NOLINT
const static size_t dbms_port = 6603;    // NOLINT
const static size_t tablet_port = 7703;  // NOLINT

static bool feql_dbms_sdk_init(::fesql::sdk::DBMSSdk **dbms_sdk) {
    DLOG(INFO) << "Connect to Tablet dbms sdk... ";
    const std::string endpoint = host + ":" + std::to_string(dbms_port);
    *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    if (nullptr == *dbms_sdk) {
        LOG(WARNING) << "Fail to create dbms sdk";
        return false;
    }
    return true;
}

static bool fesql_server_init(brpc::Server &tablet_server,  // NOLINT
                              brpc::Server &dbms_server,    // NOLINT
                              ::fesql::tablet::TabletServerImpl *tablet,
                              ::fesql::dbms::DBMSServerImpl *dbms) {
    DLOG(INFO) << ("Start FeSQL tablet server...");
    if (!tablet->Init()) {
        LOG(WARNING) << "Fail to start FeSQL server";
    }

    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        return false;
    }
    tablet_server.Start(tablet_port, &options);

    DLOG(INFO) << ("Start FeSQL dbms server...");
    if (dbms_server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        return false;
        exit(1);
    }
    dbms_server.Start(dbms_port, &options);
    dbms->SetTabletEndpoint(host + ":" + std::to_string(tablet_port));
    return true;
}

static bool init_db(::fesql::sdk::DBMSSdk *dbms_sdk, std::string db_name) {
    DLOG(INFO) << ("Creating database 'test'...");
    // create database
    fesql::sdk::Status status;
    ::fesql::sdk::DatabaseDef db;
    db.name = db_name;
    dbms_sdk->CreateDatabase(db, status);
    if (0 != status.code) {
        return false;
    }
    return true;
}

static bool init_tbl(::fesql::sdk::DBMSSdk *dbms_sdk,
                     const std::string &db_name,
                     const std::string &schema_sql) {
    DLOG(INFO) << ("Creating table 'tbl' in database 'test'...\n");
    // create table db1
    ::fesql::sdk::DatabaseDef db;
    db.name = db_name;
    fesql::sdk::Status status;
    fesql::sdk::ExecuteResult result;
    fesql::sdk::ExecuteRequst request;
    request.database = db;
    request.sql = schema_sql;
    dbms_sdk->ExecuteScript(request, result, status);
    if (0 != status.code) {
        LOG(WARNING)
            << ("Could not create 'tbl' table in the 'test' database!\n");
        return false;
    }
    return true;
}
static bool repeated_insert_tbl(::fesql::sdk::TabletSdk *tablet_sdk,
                                const std::string &db_name,
                                const std::string &insert_sql,
                                int32_t record_size) {
    DLOG(INFO) << ("Running inserts ...\n");
    int32_t fail = 0;
    for (int i = 0; i < record_size; ++i) {
        ::fesql::sdk::Status status;
        tablet_sdk->SyncInsert(db_name, insert_sql, status);
        if (0 != status.code) {
            fail++;
            LOG(WARNING)
                << ("Could not insert 'tbl' table in the 'test' "
                    "database!\n");
        }
    }
    DLOG(INFO) << "Insert tbl, fail cnt: " << fail;
    return true;
}

void SIMPLE_CASE1_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode,
                        int64_t record_size) {  // NOLINT
    bool failure_flag = false;
    std::string db_name = "test";
    std::string schema_sql =
        "create table tbl (\n"
        "        col_i32 int,\n"
        "        col_i16 int,\n"
        "        col_i64 bigint,\n"
        "        col_f float,\n"
        "        col_d double,\n"
        "        col_str64 string,\n"
        "        col_str255 string,\n"
        "       index(key=(col_str64), ts=col_i64, ttl=60d)"
        "    );";

    const char *schema_insert_sql =
        "insert into tbl values(1,1,1,1,1,\"key1\", \"string1\");";

    std::string select_sql =
        "select col_str64, col_i64, col_i32, col_i16, col_f, col_d, col_str255 "
        "from tbl limit " +
        std::to_string(record_size) + ";";

    brpc::Server tablet_server;
    brpc::Server dbms_server;
    ::fesql::tablet::TabletServerImpl table_server_impl;
    ::fesql::dbms::DBMSServerImpl dbms_server_impl;
    ::fesql::sdk::DBMSSdk *dbms_sdk = nullptr;

    if (!fesql_server_init(tablet_server, dbms_server, &table_server_impl,
                           &dbms_server_impl)) {
        LOG(WARNING) << "Fail to init server";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }

    if (!feql_dbms_sdk_init(&dbms_sdk)) {
        LOG(WARNING) << "Fail to create to dbms sdk";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }

    std::unique_ptr<::fesql::sdk::TabletSdk> sdk =
        ::fesql::sdk::CreateTabletSdk(host + ":" + std::to_string(tablet_port));
    if (!sdk) {
        LOG(WARNING) << "Fail to create to tablet sdk";
        failure_flag = true;
        goto failure;
    }

    if (false == init_db(dbms_sdk, db_name)) {
        LOG(WARNING) << "Fail to create db";
        failure_flag = true;
        goto failure;
    }
    if (false == init_tbl(dbms_sdk, db_name, schema_sql)) {
        LOG(WARNING) << "Fail to create table";
        failure_flag = true;
        goto failure;
    }
    {
        ::fesql::sdk::Status insert_status;
        int32_t fail = 0;
        for (int i = 0; i < record_size; ++i) {
            sdk->SyncInsert("test", schema_insert_sql, insert_status);
            if (0 != insert_status.code) {
                fail += 1;
            }
        }
        LOG(INFO) << "Insert Total cnt: " << record_size
                  << ", fail cnt: " << fail;
    }

    switch (mode) {
        case BENCHMARK: {
            {
                LOG(INFO) << "Running query ...\n" << select_sql;
                int32_t fail = 0;
                int32_t total_cnt = 0;
                for (auto _ : *state_ptr) {
                    total_cnt++;

                    ::fesql::sdk::Query query;
                    sdk::Status query_status;
                    query.db = "test";
                    query.sql = select_sql;
                    query.is_batch_mode = is_batch_mode;
                    benchmark::DoNotOptimize(
                        sdk->SyncQuery(query, query_status));
                    if (0 != query_status.code) {
                        fail++;
                    }
                }
                LOG(INFO) << "Total cnt: " << total_cnt
                          << ", fail cnt: " << fail;
            }
            break;
        }
        case TEST: {
            ::fesql::sdk::Query query;
            sdk::Status query_status;
            query.db = "test";
            query.sql = select_sql;
            query.is_batch_mode = is_batch_mode;
            std::unique_ptr<::fesql::sdk::ResultSet> rs =
                sdk->SyncQuery(query, query_status);
            ASSERT_TRUE(0 != rs);  // NOLINT
            ASSERT_EQ(0, query_status.code);
            ASSERT_EQ(record_size, rs->GetRowCnt());
        }
    }
failure:
    if (nullptr != dbms_sdk) {
        delete dbms_sdk;
    }
    if (TEST == mode) {
        ASSERT_FALSE(failure_flag);
    }
}
void WINDOW_CASE1_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t record_size) {
    bool failure_flag = false;
    std::string db_name = "test";
    std::string schema_sql =
        "create table tbl (\n"
        "        col_i32 int,\n"
        "        col_i16 int,\n"
        "        col_i64 bigint,\n"
        "        col_f float,\n"
        "        col_d double,\n"
        "        col_str64 string,\n"
        "        col_str255 string,\n"
        "       index(key=(col_str64), ts=col_i64, ttl=60d)"
        "    );";

    std::string select_sql =
        "SELECT "
        "sum(col_i32) OVER w1 as sum_col_i32, \n"
        "sum(col_f) OVER w1 as sum_col_f \n"
        "FROM tbl\n"
        "window w1 as (PARTITION BY col_str64 \n"
        "                  ORDER BY col_i64\n"
        "                  ROWS BETWEEN 86400000 PRECEDING AND CURRENT ROW) "
        "limit " +
        std::to_string(record_size) + ";";

    brpc::Server tablet_server;
    brpc::Server dbms_server;
    ::fesql::tablet::TabletServerImpl table_server_impl;
    ::fesql::dbms::DBMSServerImpl dbms_server_impl;
    ::fesql::sdk::DBMSSdk *dbms_sdk = nullptr;

    if (!fesql_server_init(tablet_server, dbms_server, &table_server_impl,
                           &dbms_server_impl)) {
        LOG(WARNING) << "Fail to init server";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }

    if (!feql_dbms_sdk_init(&dbms_sdk)) {
        LOG(WARNING) << "Fail to create to dbms sdk";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }

    std::unique_ptr<::fesql::sdk::TabletSdk> sdk =
        ::fesql::sdk::CreateTabletSdk(host + ":" + std::to_string(tablet_port));
    if (!sdk) {
        LOG(WARNING) << "Fail to create to tablet sdk";
        failure_flag = true;
        goto failure;
    }

    if (false == init_db(dbms_sdk, db_name)) {
        failure_flag = true;
        goto failure;
    }
    if (false == init_tbl(dbms_sdk, db_name, schema_sql)) {
        failure_flag = true;
        goto failure;
    }

    {
        IntRepeater<int32_t> col_i32;
        col_i32.Range(0, 100, 1);
        IntRepeater<int16_t> col_i16;
        col_i16.Range(0u, 100u, 1u);
        IntRepeater<int64_t> col_i64;
        col_i64.Range(1576571615000 - record_size * 1000, 1576571615000, 1000);
        RealRepeater<float> col_f;
        col_f.Range(0, 1000, 2.0f);
        RealRepeater<double> col_d;
        col_d.Range(0, 10000, 10.0);
        ::fesql::bm::Repeater<std::string> col_str64(
            {"astring", "bstring", "cstring", "dstring", "estring", "fstring",
             "gstring", "hstring", "istring", "jstring"});
        ::fesql::bm::Repeater<std::string> col_str255(
            {"aaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbb", "ccccccccccccccccccc",
             "ddddddddddddddddd"});
        int32_t fail = 0;
        DLOG(INFO) << "Running insert ...\n" << select_sql;
        for (int i = 0; i < record_size; ++i) {
            std::ostringstream oss;
            oss << "insert into tbl values (" << col_i32.GetValue() << ", "
                << col_i16.GetValue() << ", " << col_i64.GetValue() << ", "
                << col_f.GetValue() << ", " << col_d.GetValue() << ", "
                << "\"" << col_str64.GetValue() << "\", "
                << "\"" << col_str255.GetValue() << "\""
                << ");";
            //            LOG(INFO) << oss.str();
            ::fesql::sdk::Status insert_status;
            int32_t fail = 0;
            sdk->SyncInsert("test", oss.str().c_str(), insert_status);
            if (0 != insert_status.code) {
                fail += 1;
            }
        }
        DLOG(INFO) << "Insert cnt: " << record_size << ", fail cnt: " << fail;
    }

    switch (mode) {
        case BENCHMARK: {
            DLOG(INFO) << "Running query ...\n" << select_sql;
            int32_t fail = 0;
            int32_t total_cnt = 0;
            for (auto _ : *state_ptr) {
                total_cnt++;

                ::fesql::sdk::Query query;
                sdk::Status query_status;
                query.db = "test";
                query.sql = select_sql;
                query.is_batch_mode = is_batch_mode;

                benchmark::DoNotOptimize(sdk->SyncQuery(query, query_status));
                if (0 != query_status.code) {
                    fail++;
                }
            }
            DLOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
            break;
        }
        case TEST: {
            ::fesql::sdk::Query query;
            sdk::Status query_status;
            query.db = "test";
            query.sql = select_sql;
            query.is_batch_mode = is_batch_mode;
            std::unique_ptr<::fesql::sdk::ResultSet> rs =
                sdk->SyncQuery(query, query_status);
            ASSERT_TRUE(0 != rs);  // NOLINT
            ASSERT_EQ(0, query_status.code);
            ASSERT_EQ(record_size, rs->GetRowCnt());
        }
    }

failure:
    if (nullptr != dbms_sdk) {
        delete dbms_sdk;
    }
    if (TEST == mode) {
        ASSERT_FALSE(failure_flag);
    }
}

}  // namespace bm
}  // namespace fesql
