/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * memsql_client_bm.cc
 *
 * Author: chenjing
 * Date: 2019/12/24
 *--------------------------------------------------------------------------
 **/

#include <stdio.h>
#include <stdlib.h>
#include "benchmark/benchmark.h"
#include "bm/base_bm.h"
#include "glog/logging.h"
#include "mysql/mysql.h"

namespace fesql {
namespace bm {
// const static std::string host = "172.17.0.2";  // NOLINT
const static std::string host = "127.0.0.1";  // NOLINT
const static std::string user = "root";       // NOLINT
const static std::string passwd = "";         // NOLINT
const static size_t port = 3306;              // NOLINT
static bool init(MYSQL &conn) {               // NOLINT
    mysql_init(&conn);
    mysql_options(&conn, MYSQL_DEFAULT_AUTH, "mysql_native_password");

    DLOG(INFO) << ("Connecting to MemSQL...");
    if (mysql_real_connect(&conn, host.c_str(), user.c_str(), passwd.c_str(),
                           NULL, port, NULL, 0) != &conn) {
        printf("Could not connect to the MemSQL database!\n");
        return false;
    }
    return true;
}

static bool init_db(MYSQL &conn, const char *db_sql) {  // NOLINT
    DLOG(INFO) << ("Creating database 'test'...");
    if (mysql_query(&conn, db_sql)) {
        LOG(WARNING) << ("Could not create 'test' database!");
        mysql_close(&conn);
        return false;
    }
    return true;
}

static bool use_db(MYSQL &conn, const char *use_sql) {  // NOLINT
    DLOG(INFO) << ("use database 'test'...");
    if (mysql_query(&conn, use_sql)) {
        LOG(WARNING) << ("Could not use 'test' database!");
        mysql_close(&conn);
        return false;
    }
    return true;
}
static bool init_tbl(MYSQL &conn, const char *schema_sql) {  // NOLINT
    DLOG(INFO) << ("Creating table 'tbl' in database 'test'...\n");
    if (mysql_query(&conn, schema_sql)) {
        LOG(WARNING)
            << ("Could not create 'tbl' table in the 'test' database!\n");
        mysql_close(&conn);
        return false;
    }
    return true;
}

static bool create_index(MYSQL &conn, const char *index_sql) {  // NOLINT
    DLOG(INFO) << ("Creating table 'tbl' index in database 'test'...\n");
    if (mysql_query(&conn, index_sql)) {
        LOG(WARNING)
            << ("Could not create 'tbl' index in the 'test' database!\n");
        mysql_close(&conn);
        return false;
    }
    return true;
}
static bool repeated_insert_tbl(MYSQL &conn, const char *insert_sql,  // NOLINT
                                int32_t record_size) {
    DLOG(INFO) << ("Running inserts ...\n");
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

static bool delete_tbl(MYSQL &conn, const char *delete_tbl) {  // NOLINT
    if (mysql_query(&conn, delete_tbl)) {
        LOG(WARNING) << ("Could not delete tbl 'test'!\n");
        mysql_close(&conn);
        return false;
    }

    return true;
}

static bool drop_tbl(MYSQL &conn, const char *drop_tbl) {  // NOLINT
    if (mysql_query(&conn, drop_tbl)) {
        LOG(WARNING) << ("Could not drop table tbl 'test'!\n");
        mysql_close(&conn);
        return false;
    }

    return true;
}

static bool drop_db(MYSQL &conn, const char *drop_db) {  // NOLINT
    if (mysql_query(&conn, drop_db)) {
        LOG(WARNING) << ("Could not drop the testing database 'test'!\n");
        mysql_close(&conn);
        return false;
    }
    return true;
}
static void BM_SIMPLE_INSERT(benchmark::State &state) {  // NOLINT
    const char *db_sql = "create database test";
    const char *use_sql = "use test";
    const char *schema_sql =
        "create table tbl (\n"
        "        col_i32 int,\n"
        "        col_i16 smallint,\n"
        "        col_i64 bigint\n"
        "        col_f float,\n"
        "        col_d double,\n"
        "        col_str64 VARCHAR(64),\n"
        "        col_str255 VARCHAR(255)\n"
        "    );";

    const char *schema_insert_sql =
        "insert into tbl values(1,1,1,1,1,\"key1\", \"string1\");";
    const char *delete_sql = "delete from tbl";
    const char *drop_tbl_sql = "drop table tbl";
    const char *drop_db_sql = "drop database test";

    MYSQL conn;
    if (!init(conn)) goto failure;
    if (!use_db(conn, use_sql)) goto failure;
    if (!init_tbl(conn, schema_sql)) goto failure;

    {
        LOG(INFO) << ("Running insert ...\n");
        int32_t fail = 0;
        int32_t total_cnt = 0;
        for (auto _ : state) {
            total_cnt++;
            if (mysql_query(&conn, schema_insert_sql)) {
                fail++;
            }
        }
        LOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
    }

    if (!delete_tbl(conn, delete_sql)) goto failure;
    if (!drop_tbl(conn, drop_tbl_sql)) goto failure;
//    if (!drop_db(conn, drop_db_sql)) goto failure;
    mysql_close(&conn);

failure:
    mysql_close(&conn);
}

static void BM_INSERT_WITH_INDEX(benchmark::State &state) {  // NOLINT
    const char *db_sql = "create database test";
    const char *use_sql = "use test";
    const char *schema_sql =
        "create table tbl (\n"
        "        col_i32 int,\n"
        "        col_i16 int,\n"
        "        col_i64 bigint,\n"
        "        col_f float,\n"
        "        col_d double,\n"
        "        col_str64 VARCHAR(64),\n"
        "        col_str255 VARCHAR(255)\n"
        "    );";

    const char *schema_insert_sql =
        "insert into tbl values(1,1,1,1,1,\"key1\", \"string1\");";

    const char *delete_sql = "delete from tbl";
    const char *drop_tbl_sql = "drop table tbl";
    const char *drop_db_sql = "drop database test";

    MYSQL conn;
    if (!init(conn)) goto failure;
    if (!use_db(conn, use_sql)) goto failure;
    if (!init_tbl(conn, schema_sql)) goto failure;

    {
        const char *index_sql =
            "CREATE INDEX col_str64_index ON tbl (col_str64);";
        if (!create_index(conn, index_sql)) goto failure;
    }

    {
        const char *index_sql = "CREATE INDEX col_i64_index ON tbl (col_i64);";
        if (!create_index(conn, index_sql)) goto failure;
    }

    {
        LOG(INFO) << ("Running insert ...\n");
        int32_t fail = 0;
        int32_t total_cnt = 0;
        for (auto _ : state) {
            total_cnt++;
            if (mysql_query(&conn, schema_insert_sql)) {
                fail++;
            }
        }
        LOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
    }

    if (!delete_tbl(conn, delete_sql)) goto failure;
    if (!drop_tbl(conn, drop_tbl_sql)) goto failure;
//    if (!drop_db(conn, drop_db_sql)) goto failure;
    mysql_close(&conn);

failure:
    mysql_close(&conn);
}

static void BM_SIMPLE_QUERY(benchmark::State &state) {  // NOLINT
    const char *db_sql = "create database test";
    const char *use_sql = "use test";
    const char *schema_sql =
        "create table tbl (\n"
        "        col_i32 int,\n"
        "        col_i16 smallint,\n"
        "        col_i64 bigint,\n"
        "        col_f float,\n"
        "        col_d double,\n"
        "        col_str64 VARCHAR(64),\n"
        "        col_str255 VARCHAR(255)\n"
        "    );";

    const char *schema_insert_sql =
        "insert into tbl values(1,1,1,1,1,\"key1\", \"string1\");";

    const char *select_sql =
        "select col_str64, col_i64, col_i32, col_i16, col_f, col_d, col_str255 "
        "from tbl;";
    const char *delete_sql = "delete from tbl";
    const char *drop_tbl_sql = "drop table tbl";
    const char *drop_db_sql = "drop database test";

    int64_t record_size = state.range(0);
    MYSQL conn;

    if (!init(conn)) goto failure;
    if (!use_db(conn, use_sql)) goto failure;
    if (!init_tbl(conn, schema_sql)) goto failure;
    if (!repeated_insert_tbl(conn, schema_insert_sql, record_size))
        goto failure;

    {
        LOG(INFO) << ("Running query ...\n");
        int32_t fail = 0;
        int32_t total_cnt = 0;
        for (auto _ : state) {
            total_cnt++;

            if (mysql_query(&conn, select_sql)) {
                fail++;
            } else {
                MYSQL_RES *result = mysql_store_result(&conn);
                mysql_free_result(result);
            }
        }
        LOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
    }

    if (!delete_tbl(conn, delete_sql)) goto failure;
    if (!drop_tbl(conn, drop_tbl_sql)) goto failure;
//    if (!drop_db(conn, drop_db_sql)) goto failure;
    mysql_close(&conn);

failure:
    mysql_close(&conn);
}

static void BM_WINDOW_CASE1_QUERY(benchmark::State &state) {  // NOLINT
    const char *db_sql = "create database test";
    const char *use_sql = "use test";
    const char *schema_sql =
        "create table tbl (\n"
        "        col_i32 int,\n"
        "        col_i16 smallint,\n"
        "        col_i64 bigint,\n"
        "        col_f float,\n"
        "        col_d double,\n"
        "        col_str64 VARCHAR(64),\n"
        "        col_str255 VARCHAR(255)\n"
        "    );";

    const char *select_sql =
        "SELECT "
        "SUM(col_i32) OVER w1 as sum_col_i32, \n"
        "SUM(col_f) OVER w1 as sum_col_f \n"
        "FROM tbl\n"
        "window w1 as (PARTITION BY col_str64 \n"
        "                  ORDER BY col_i64\n"
        "                  ROWS BETWEEN 86400000 PRECEDING AND CURRENT ROW);";
    const char *delete_sql = "delete from tbl";
    const char *drop_tbl_sql = "drop table tbl";
    const char *drop_db_sql = "drop database test";

    int64_t record_size = state.range(0);
    MYSQL conn;

    if (!init(conn)) goto failure;
    if (!use_db(conn, use_sql)) goto failure;
    if (!init_tbl(conn, schema_sql)) goto failure;
    {
        const char *index_sql =
            "CREATE INDEX col_str64_index ON tbl (col_str64);";
        if (!create_index(conn, index_sql)) goto failure;
    }

    {
        const char *index_sql = "CREATE INDEX col_i64_index ON tbl (col_i64);";
        if (!create_index(conn, index_sql)) goto failure;
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
        for (int i = 0; i < record_size; ++i) {
            std::ostringstream oss;
            oss << "insert into tbl values (" << col_i32.GetValue() << ", "
                << col_i16.GetValue() << ", " << col_i64.GetValue() << ", "
                << col_f.GetValue() << ", " << col_d.GetValue() << ", "
                << "\"" << col_str64.GetValue() << "\", "
                << "\"" << col_str255.GetValue() << "\""
                << ");";
            LOG(INFO) << oss.str();
            if (!repeated_insert_tbl(conn, oss.str().c_str(), 1)) {
                fail += 1;
            }
        }
        LOG(INFO) << "Insert cnt: " << record_size << ", fail cnt: " << fail;
    }
    {
        LOG(INFO) << "Running query ...\n" << select_sql;
        int32_t fail = 0;
        int32_t total_cnt = 0;
        for (auto _ : state) {
            total_cnt++;

            if (mysql_query(&conn, select_sql)) {
                fail++;
            } else {
                MYSQL_RES *result = mysql_store_result(&conn);
                mysql_free_result(result);
            }
        }
        LOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;
    }

    if (!delete_tbl(conn, delete_sql)) goto failure;
    if (!drop_tbl(conn, drop_tbl_sql)) goto failure;
//    if (!drop_db(conn, drop_db_sql)) goto failure;
    mysql_close(&conn);

failure:
    mysql_close(&conn);
}
BENCHMARK(BM_SIMPLE_QUERY)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_WINDOW_CASE1_QUERY)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);
}  // namespace bm
};  // namespace fesql

BENCHMARK_MAIN();
