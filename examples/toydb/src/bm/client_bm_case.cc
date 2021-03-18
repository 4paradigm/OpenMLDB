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

#include "bm/hybridse_client_bm_case.h"
#include <memory>
#include <string>
#include <vector>
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "sdk/dbms_sdk.h"
#include "sdk/tablet_sdk.h"
#include "case/case_data_mock.h"
#include "tablet/tablet_server_impl.h"
DECLARE_string(dbms_endpoint);
DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_bool(enable_keep_alive);

namespace hybridse {
namespace bm {
using hybridse::sqlcase::Repeater;
using hybridse::sqlcase::IntRepeater;
using hybridse::sqlcase::RealRepeater;
class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};
const std::string host = "127.0.0.1";    // NOLINT
const static size_t dbms_port = 6603;    // NOLINT
const static size_t tablet_port = 7703;  // NOLINT

static std::shared_ptr<hybridse::sdk::DBMSSdk> feql_dbms_sdk_init() {
    DLOG(INFO) << "Connect to Tablet dbms sdk... ";
    const std::string endpoint = host + ":" + std::to_string(dbms_port);
    return hybridse::sdk::CreateDBMSSdk(endpoint);
}

static bool FeSqlServerInit(brpc::Server &tablet_server,  // NOLINT
                            brpc::Server &dbms_server,    // NOLINT
                            ::hybridse::tablet::TabletServerImpl *tablet,
                            ::hybridse::dbms::DBMSServerImpl *dbms) {
    FLAGS_enable_keep_alive = false;
    DLOG(INFO) << ("Start HybridSE tablet server...");
    if (!tablet->Init()) {
        LOG(WARNING) << "Fail to start HybridSE server";
    }

    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        return false;
    }
    tablet_server.Start(tablet_port, &options);

    DLOG(INFO) << ("Start HybridSE dbms server...");
    if (dbms_server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        return false;
        exit(1);
    }
    {
        std::string tablet_endpoint =
            "127.0.0.1:" + std::to_string(tablet_port);
        MockClosure closure;
        dbms::KeepAliveRequest request;
        request.set_endpoint(tablet_endpoint);
        dbms::KeepAliveResponse response;
        dbms->KeepAlive(NULL, &request, &response, &closure);
    }
    dbms_server.Start(dbms_port, &options);
    return true;
}

static bool InitDB(std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk,
                   std::string db_name) {
    LOG(INFO) << "Creating database " << db_name;
    // create database
    hybridse::sdk::Status status;
    dbms_sdk->CreateDatabase(db_name, &status);
    if (0 != status.code) {
        LOG(WARNING) << "create database faled " << db_name << " with error "
                     << status.msg;
        return false;
    }
    return true;
}

static bool InitTBL(std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk,
                    const std::string &db_name, const std::string &schema_sql) {
    DLOG(INFO) << ("Creating table 'tbl' in database 'test'...\n");
    // create table db1
    hybridse::sdk::Status status;
    dbms_sdk->ExecuteQuery(db_name, schema_sql, &status);
    if (0 != status.code) {
        LOG(WARNING)
            << ("Could not create 'tbl' table in the 'test' database!\n");
        return false;
    }
    return true;
}

static void SIMPLE_CASE_QUERY(benchmark::State *state_ptr, MODE mode,
                              bool is_batch_mode, std::string select_sql,
                              int64_t group_size, int64_t window_max_size) {
    int64_t record_size = group_size * window_max_size;
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

    brpc::Server tablet_server;
    brpc::Server dbms_server;
    ::hybridse::tablet::TabletServerImpl table_server_impl;
    ::hybridse::dbms::DBMSServerImpl dbms_server_impl;

    if (!FeSqlServerInit(tablet_server, dbms_server, &table_server_impl,
                         &dbms_server_impl)) {
        LOG(WARNING) << "Fail to init server";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk = feql_dbms_sdk_init();
    if (!dbms_sdk) {
        LOG(WARNING) << "Fail to create to dbms sdk";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }

    std::shared_ptr<::hybridse::sdk::TabletSdk> sdk =
        ::hybridse::sdk::CreateTabletSdk(host + ":" + std::to_string(tablet_port));
    if (!sdk) {
        LOG(WARNING) << "Fail to create to tablet sdk";
        failure_flag = true;
        goto failure;
    }

    if (false == InitDB(dbms_sdk, db_name)) {
        LOG(WARNING) << "Fail to create db";
        failure_flag = true;
        goto failure;
    }
    if (false == InitTBL(dbms_sdk, db_name, schema_sql)) {
        LOG(WARNING) << "Fail to create table";
        failure_flag = true;
        goto failure;
    }
    {
        ::hybridse::sdk::Status insert_status;
        int32_t fail = 0;
        for (int i = 0; i < record_size; ++i) {
            sdk->Insert("test", schema_insert_sql, &insert_status);
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

                    sdk::Status query_status;
                    const std::string db = "test";
                    const std::string sql = select_sql;
                    benchmark::DoNotOptimize(
                        sdk->Query(db, sql, &query_status));
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
            sdk::Status query_status;
            const std::string db = "test";
            const std::string sql = select_sql;
            std::shared_ptr<::hybridse::sdk::ResultSet> rs =
                sdk->Query(db, sql, &query_status);
            ASSERT_TRUE(0 != rs);  // NOLINT
            ASSERT_EQ(0, query_status.code);
            ASSERT_EQ(record_size, rs->Size());
        }
    }
failure:
    if (TEST == mode) {
        ASSERT_FALSE(failure_flag);
    }
}
static void WINDOW_CASE_QUERY(benchmark::State *state_ptr, MODE mode,
                              bool is_batch_mode, std::string select_sql,
                              int64_t group_size, int64_t max_window_size) {
    int64_t record_size = group_size * max_window_size;
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

    brpc::Server tablet_server;
    brpc::Server dbms_server;
    ::hybridse::tablet::TabletServerImpl table_server_impl;
    ::hybridse::dbms::DBMSServerImpl dbms_server_impl;

    if (!FeSqlServerInit(tablet_server, dbms_server, &table_server_impl,
                         &dbms_server_impl)) {
        LOG(WARNING) << "Fail to init server";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk = feql_dbms_sdk_init();
    if (!dbms_sdk) {
        LOG(WARNING) << "Fail to create to dbms sdk";
        if (TEST == mode) {
            FAIL();
        }
        return;
    }

    std::shared_ptr<::hybridse::sdk::TabletSdk> sdk =
        ::hybridse::sdk::CreateTabletSdk(host + ":" + std::to_string(tablet_port));
    if (!sdk) {
        LOG(WARNING) << "Fail to create to tablet sdk";
        failure_flag = true;
        goto failure;
    }

    if (false == InitDB(dbms_sdk, db_name)) {
        failure_flag = true;
        goto failure;
    }
    if (false == InitTBL(dbms_sdk, db_name, schema_sql)) {
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
        std::vector<std::string> groups;
        {
            for (int i = 0; i < group_size; ++i) {
                groups.push_back("group" + std::to_string(i));
            }
        }
        Repeater<std::string> col_str64(groups);
        Repeater<std::string> col_str255(
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
            ::hybridse::sdk::Status insert_status;
            int32_t fail = 0;
            sdk->Insert("test", oss.str().c_str(), &insert_status);
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

                sdk::Status query_status;
                const std::string db = "test";
                const std::string sql = select_sql;
                benchmark::DoNotOptimize(sdk->Query(db, sql, &query_status));
                if (0 != query_status.code) {
                    fail++;
                }
            }
            DLOG(INFO) << "Total cnt: " << total_cnt << ", fail cnt: " << fail;

            break;
        }
        case TEST: {
            sdk::Status query_status;
            const std::string db = "test";
            const std::string sql = select_sql;
            std::shared_ptr<::hybridse::sdk::ResultSet> rs =
                sdk->Query(db, sql, &query_status);
            ASSERT_TRUE(0 != rs);  // NOLINT
            ASSERT_EQ(0, query_status.code);
            ASSERT_EQ(record_size, rs->Size());
        }
    }

failure:
    if (TEST == mode) {
        ASSERT_FALSE(failure_flag);
    }
}
void SIMPLE_CASE1_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t window_max_size) {  // NOLINT
    int64_t record_size = group_size * window_max_size;
    std::string select_sql =
        "select col_str64, col_i64, col_i32, col_i16, col_f, col_d, col_str255 "
        "from tbl limit " +
        std::to_string(record_size) + ";";
    if (BENCHMARK == mode) {
        std::string query_type = "select 5 cols";
        std::string label = query_type + "/group " +
                            std::to_string(group_size) + "/max window size " +
                            std::to_string(window_max_size);
        state_ptr->SetLabel(label);
    }
    SIMPLE_CASE_QUERY(state_ptr, mode, is_batch_mode, select_sql, group_size,
                      window_max_size);
}

void WINDOW_CASE0_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t window_max_size) {
    int64_t record_size = group_size * window_max_size;
    std::string select_sql =
        "SELECT "
        "sum(col_i32) OVER w1 as sum_col_i32 \n"
        "FROM tbl\n"
        "window w1 as (PARTITION BY col_str64 \n"
        "                  ORDER BY col_i64\n"
        "                  ROWS_RANGE BETWEEN 86400000 PRECEDING AND CURRENT "
        "ROW) "
        "limit " +
        std::to_string(record_size) + ";";
    if (BENCHMARK == mode) {
        std::string query_type = "sum_col_i32";
        std::string label = query_type + "/group " +
                            std::to_string(group_size) + "/max window size " +
                            std::to_string(window_max_size);
        state_ptr->SetLabel(label);
    }
    WINDOW_CASE_QUERY(state_ptr, mode, is_batch_mode, select_sql, group_size,
                      window_max_size);
}
void GROUPBY_CASE0_QUERY(benchmark::State *state_ptr, MODE mode,
                         bool is_batch_mode, int64_t group_size,
                         int64_t window_max_size) {
    std::string select_sql =
        "SELECT "
        "sum(col_i32) \n"
        "FROM tbl group by col_str64 ;";

    if (BENCHMARK == mode) {
        std::string query_type = "sum 1 cols";
        std::string label = query_type + "/group " +
                            std::to_string(group_size) + "/max window size " +
                            std::to_string(window_max_size);
        state_ptr->SetLabel(label);
    }
    WINDOW_CASE_QUERY(state_ptr, mode, is_batch_mode, select_sql, group_size,
                      window_max_size);
}

void WINDOW_CASE1_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t window_max_size) {
    int64_t record_size = group_size * window_max_size;
    std::string select_sql =
        "SELECT "
        "sum(col_i32) OVER w1 as sum_col_i32, \n"
        "sum(col_f) OVER w1 as sum_col_f \n"
        "FROM tbl\n"
        "window w1 as (PARTITION BY col_str64 \n"
        "                  ORDER BY col_i64\n"
        "                  ROWS_RANGE BETWEEN 86400000 PRECEDING AND CURRENT "
        "ROW) "
        "limit " +
        std::to_string(record_size) + ";";
    if (BENCHMARK == mode) {
        std::string query_type = "sum 2 cols";
        std::string label = query_type + "/group " +
                            std::to_string(group_size) + "/max window size " +
                            std::to_string(window_max_size);
        state_ptr->SetLabel(label);
    }
    WINDOW_CASE_QUERY(state_ptr, mode, is_batch_mode, select_sql, group_size,
                      window_max_size);
}

void WINDOW_CASE2_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t window_max_size) {
    int64_t record_size = group_size * window_max_size;
    std::string select_sql =
        "SELECT "
        "sum(col_i32) OVER w1 as sum_col_i32, \n"
        "sum(col_i16) OVER w1 as sum_col_i16, \n"
        "sum(col_f) OVER w1 as sum_col_f, \n"
        "sum(col_d) OVER w1 as sum_col_d \n"
        "FROM tbl\n"
        "window w1 as (PARTITION BY col_str64 \n"
        "                  ORDER BY col_i64\n"
        "                  ROWS_RANGE BETWEEN 86400000 PRECEDING AND CURRENT "
        "ROW) "
        "limit " +
        std::to_string(record_size) + ";";
    if (BENCHMARK == mode) {
        std::string query_type = "sum 4 cols";
        std::string label =
            query_type + "/group " + std::to_string(state_ptr->range(0)) +
            "/max window size " + std::to_string(state_ptr->range(1));
        state_ptr->SetLabel(label);
    }

    WINDOW_CASE_QUERY(state_ptr, mode, is_batch_mode, select_sql, group_size,
                      window_max_size);
}

void WINDOW_CASE3_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t window_max_size) {
    int64_t record_size = group_size * window_max_size;
    std::string select_sql =
        "SELECT "
        "max(col_i32) OVER w1 as max_col_i32 \n"
        "FROM tbl\n"
        "window w1 as (PARTITION BY col_str64 \n"
        "                  ORDER BY col_i64\n"
        "                  ROWS_RANGE BETWEEN 86400000 PRECEDING AND CURRENT "
        "ROW) "
        "limit " +
        std::to_string(record_size) + ";";
    if (BENCHMARK == mode) {
        std::string query_type = "max_col_i32";
        std::string label = query_type + "/group " +
                            std::to_string(group_size) + "/max window size " +
                            std::to_string(window_max_size);
        state_ptr->SetLabel(label);
    }
    WINDOW_CASE_QUERY(state_ptr, mode, is_batch_mode, select_sql, group_size,
                      window_max_size);
}

}  // namespace bm
}  // namespace hybridse
