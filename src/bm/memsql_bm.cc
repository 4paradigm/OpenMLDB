/* Compile with:
 *
 * cc multi_threaded_inserts.c -lmysqlclient -pthread -o mti
 */

#include <stdio.h>
#include <stdlib.h>
#include "benchmark/benchmark.h"
#include "mysql/mysql.h"

namespace fesql {
namespace bm {

const static char *host = "172.17.0.2";
const static char *user = "root";
const static char *passwd = "";
const static size_t port = 3306;

int32_t insert(MYSQL *conn_ptr) {
    return mysql_query(conn_ptr,
                       "insert into tbl values (null), (null), (null),"
                       "(null), (null), (null), (null), (null)");
}



static void BM_SIMPLE_QUERY(benchmark::State &state) {  // NOLINT
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

    MYSQL conn;
    mysql_init(&conn);
    mysql_options(&conn, MYSQL_DEFAULT_AUTH, "mysql_native_password");

    printf("Connecting to MemSQL...\n");
    if (mysql_real_connect(&conn, host, user, passwd, NULL, port, NULL, 0) !=
        &conn) {
        printf("Could not connect to the MemSQL database!\n");
        return;
    }

//    printf("Creating database 'test'...\n");
//    if (mysql_query(&conn, "create database test") ||
//        mysql_query(&conn, "use test")) {
//        printf("Could not create 'test' database!\n");
//        mysql_close(&conn);
//        return;
//    }
    if (mysql_query(&conn, "use test")) {
        printf("Could not use 'test' database!\n");
        mysql_close(&conn);
        return;
    }

    //    printf("Creating table 'tbl' in database 'test'...\n");
    //    if (mysql_query(&conn, schema_sql)) {
    //        printf("Could not create 'tbl' table in the 'test' database!\n");
    //        mysql_close(&conn);
    //        return;
    //    }
    //
    //    printf("Running inserts ...\n");
    //    int32_t fail = 0;
    //    for (int i = 0; i < 100000; ++i) {
    //        if (mysql_query(&conn, schema_insert_sql)) {
    //            fail++;
    //            printf("Could not insert 'tbl' table in the 'test'
    //            database!\n");
    //        }
    //    }

    //    printf("Insert tbl, fail cnt: %d\n", fail);
    printf("Running query ...\n");
    int32_t fail = 0;
    int32_t total_cnt = 0;
    for (auto _ : state) {
        total_cnt ++;
        if (mysql_query(&conn, select_sql)) {
            fail ++;
        }
    }
    printf("Total cnt: %d, fail cnt: %d\n", total_cnt, fail);
    //
    //    if (mysql_query(&conn, delete_sql)) {
    //        printf("Could not delete tbl 'test'!\n");
    //        mysql_close(&conn);
    //        return;
    //    }
    //
    //    if (mysql_query(&conn, "drop database test")) {
    //        printf("Could not drop the testing database 'test'!\n");
    //        mysql_close(&conn);
    //        return;
    //    }

    mysql_close(&conn);
}

BENCHMARK(BM_SIMPLE_QUERY);
// BENCHMARK(BM_INSERT_SINGLE_THREAD);
}  // namespace bm
};  // namespace fesql

BENCHMARK_MAIN();
