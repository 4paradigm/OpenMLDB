/* Compile with:
 *
 * cc multi_threaded_inserts.c -lmysqlclient -pthread -o mti
 */

#include <stdio.h>
#include <stdlib.h>
#include "benchmark/benchmark.h"
#include "bm/base_bm.h"
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mysql/mysql.h"
#include "sdk/dbms_sdk.h"
#include "sdk/tablet_sdk.h"
#include "tablet/tablet_server_impl.h"
namespace fesql {
namespace bm {

std::string host = "127.0.0.1";
const static size_t dbms_port = 6603;
const static size_t tablet_port = 7703;

static bool feql_dbms_sdk_init(::fesql::sdk::DBMSSdk **dbms_sdk) {
    LOG(INFO) << "Connect to Tablet dbms sdk... ";
    const std::string endpoint = host + ":" + std::to_string(dbms_port);
    *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    if (nullptr == *dbms_sdk) {
        LOG(WARNING) << "Fail to create dbms sdk";
        return false;
    }
    return true;
}
static bool fesql_server_init(brpc::Server &tablet_server,
                              brpc::Server &dbms_server,
                              ::fesql::tablet::TabletServerImpl *tablet,
                              ::fesql::dbms::DBMSServerImpl *dbms) {
    LOG(INFO) << ("Start FeSQL tablet server...");
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

    LOG(INFO) << ("Start FeSQL dbms server...");
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
    LOG(INFO) << ("Creating database 'test'...");
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

static bool init_tbl(::fesql::sdk::DBMSSdk *dbms_sdk, std::string &db_name,
                     std::string &schema_sql) {
    LOG(INFO) << ("Creating table 'tbl' in database 'test'...\n");
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
static bool repeated_insert_tbl(MYSQL &conn, const char *insert_sql,
                                int32_t record_size) {
    LOG(INFO) << ("Running inserts ...\n");
    int32_t fail = 0;
    for (int i = 0; i < record_size; ++i) {
        if (mysql_query(&conn, insert_sql)) {
            fail++;
            LOG(WARNING)
                << ("Could not insert 'tbl' table in the 'test' "
                    "database!\n");
        }
    }
    LOG(INFO) << "Insert tbl, fail cnt: " << fail;
    return true;
}

static bool insert_tbl(MYSQL &conn, std::vector<std::string> sqls) {
    LOG(INFO) << ("Running inserts ...\n");
    int32_t fail = 0;
    for (std::string sql : sqls) {
        if (mysql_query(&conn, sql.c_str())) {
            fail++;
            LOG(WARNING)
                << ("Could not insert 'tbl' table in the 'test' "
                    "database!\n");
        }
    }
    LOG(INFO) << "Insert tbl, fail cnt: " << fail;
    return true;
}

static void BM_SIMPLE_QUERY(benchmark::State &state) {  // NOLINT
    int64_t record_size = state.range(0);
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
        return;
    }

    if (!feql_dbms_sdk_init(&dbms_sdk)) {
        LOG(WARNING) << "Fail to create to dbms sdk";
        return;
    }

    std::unique_ptr<::fesql::sdk::TabletSdk> sdk =
        ::fesql::sdk::CreateTabletSdk(host + ":" + std::to_string(tablet_port));
    if (!sdk) {
        LOG(WARNING) << "Fail to create to tablet sdk";
        goto failure;
    }

    if (false == init_db(dbms_sdk, db_name)) {
        goto failure;
    }
    if (false == init_tbl(dbms_sdk, db_name, schema_sql)) {
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

    {
        LOG(INFO) << "Running query ...\n" << select_sql;
        int32_t fail = 0;
        int32_t total_cnt = 0;
        for (auto _ : state) {
            total_cnt++;

            ::fesql::sdk::Query query;
            sdk::Status query_status;
            query.db = "test";
            query.sql = select_sql;
            std::unique_ptr<::fesql::sdk::ResultSet> rs =
                sdk->SyncQuery(query, query_status);
            if (!sdk->SyncQuery(query, query_status)) {
                fail++;
            }
        }
        LOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
    }

failure:
    if (nullptr != dbms_sdk) {
        delete dbms_sdk;
    }
}

static void BM_WINDOW_CASE1_QUERY(benchmark::State &state) {  // NOLINT
    int64_t record_size = state.range(0);
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
        "limit " + std::to_string(record_size) + ";";

    brpc::Server tablet_server;
    brpc::Server dbms_server;
    ::fesql::tablet::TabletServerImpl table_server_impl;
    ::fesql::dbms::DBMSServerImpl dbms_server_impl;
    ::fesql::sdk::DBMSSdk *dbms_sdk = nullptr;

    if (!fesql_server_init(tablet_server, dbms_server, &table_server_impl,
                           &dbms_server_impl)) {
        LOG(WARNING) << "Fail to init server";
        return;
    }

    if (!feql_dbms_sdk_init(&dbms_sdk)) {
        LOG(WARNING) << "Fail to create to dbms sdk";
        return;
    }

    std::unique_ptr<::fesql::sdk::TabletSdk> sdk =
        ::fesql::sdk::CreateTabletSdk(host + ":" + std::to_string(tablet_port));
    if (!sdk) {
        LOG(WARNING) << "Fail to create to tablet sdk";
        goto failure;
    }

    if (false == init_db(dbms_sdk, db_name)) {
        goto failure;
    }
    if (false == init_tbl(dbms_sdk, db_name, schema_sql)) {
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
        LOG(INFO) << "Running insert ...\n" << select_sql;
        for (int i = 0; i < record_size; ++i) {
            std::ostringstream oss;
            oss << "insert into tbl values (" << col_i32.GetValue() << ", "
                << col_i16.GetValue() << ", " << col_i64.GetValue() << ", "
                << col_f.GetValue() << ", " << col_d.GetValue() << ", "
                << "\"" << col_str64.GetValue() << "\", "
                << "\"" << col_str255.GetValue() << "\""
                << ");";
            LOG(INFO) << oss.str();
            ::fesql::sdk::Status insert_status;
            int32_t fail = 0;
            sdk->SyncInsert("test", oss.str().c_str(), insert_status);
            if (0 != insert_status.code) {
                fail += 1;
            }
        }
        LOG(INFO) << "Insert cnt: " << record_size << ", fail cnt: " << fail;
        std::cout << "Insert cnt: " << record_size << ", fail cnt: " << fail << std::endl;
    }

    {
        LOG(INFO) << "Running query ...\n" << select_sql;
        int32_t fail = 0;
        int32_t total_cnt = 0;
        for (auto _ : state) {
            total_cnt++;

            ::fesql::sdk::Query query;
            sdk::Status query_status;
            query.db = "test";
            query.sql = select_sql;
            std::unique_ptr<::fesql::sdk::ResultSet> rs =
                sdk->SyncQuery(query, query_status);
            if (!sdk->SyncQuery(query, query_status)) {
                fail++;
            }
        }
        LOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
        std::cout << "Total cnt: " << total_cnt << ", fail cnt: " << fail << std::endl;
    }

failure:
    if (nullptr != dbms_sdk) {
        delete dbms_sdk;
    }
}

// BENCHMARK(BM_SIMPLE_INSERT);
// BENCHMARK(BM_INSERT_WITH_INDEX);
BENCHMARK(BM_WINDOW_CASE1_QUERY)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_SIMPLE_QUERY)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);
// BENCHMARK(BM_INSERT_SINGLE_THREAD);
}  // namespace bm
};  // namespace fesql

BENCHMARK_MAIN();
