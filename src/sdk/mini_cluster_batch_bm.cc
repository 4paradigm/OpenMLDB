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

#include <gflags/gflags.h>
#include <stdio.h>

#include "absl/strings/str_replace.h"
#include "benchmark/benchmark.h"
#include "codec/fe_row_codec.h"
#include "schema/schema_adapter.h"
#include "sdk/base.h"
#include "sdk/mini_cluster.h"
#include "sdk/mini_cluster_bm.h"
#include "sdk/sql_router.h"
#include "sdk/table_reader.h"
#include "test/base_test.h"
#include "vm/catalog.h"

DECLARE_bool(enable_distsql);
DECLARE_bool(enable_localtablet);

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> PBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;

::openmldb::sdk::MiniCluster* mc;

static void BM_SimpleQueryFunction(benchmark::State& state) {  // NOLINT
    ::openmldb::nameserver::TableInfo table_info;
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    auto ns_client = mc->GetNsClient();
    std::string error;
    bool ok = ns_client->CreateDatabase(db, error);
    table_info.set_name(name);
    table_info.set_db(db);
    table_info.set_partition_num(1);
    PBSchema* schema = table_info.mutable_column_desc();
    auto col1 = schema->Add();
    col1->set_name("col1");
    col1->set_data_type(::openmldb::type::kVarchar);
    auto col2 = schema->Add();
    col2->set_name("col2");
    col2->set_data_type(::openmldb::type::kBigInt);
    auto col3 = schema->Add();
    col3->set_name("col3");
    col3->set_data_type(::openmldb::type::kBigInt);
    auto col4 = schema->Add();
    col4->set_name("col4");
    col4->set_data_type(::openmldb::type::kBigInt);
    auto col5 = schema->Add();
    col5->set_name("col5");
    col5->set_data_type(::openmldb::type::kBigInt);

    RtiDBIndex* index = table_info.mutable_column_key();
    auto key1 = index->Add();
    key1->set_index_name("index0");
    key1->add_col_name("col1");
    key1->set_ts_name("col2");
    ok = ns_client->CreateTable(table_info, false, error);

    ::hybridse::vm::Schema fe_schema;
    ::openmldb::schema::SchemaAdapter::ConvertSchema(table_info.column_desc(), &fe_schema);
    ::hybridse::codec::RowBuilder rb(fe_schema);
    std::string pk = "pk1";
    uint64_t ts = 1589780888000l;
    uint32_t size = rb.CalTotalLength(pk.size());
    std::string value;
    value.resize(size);
    rb.SetBuffer(reinterpret_cast<int8_t*>(&(value[0])), size);
    rb.AppendString(pk.c_str(), pk.size());
    rb.AppendInt64(ts);
    rb.AppendInt64(ts);
    rb.AppendInt64(ts);
    rb.AppendInt64(ts);
    ::openmldb::sdk::ClusterOptions option;
    option.zk_cluster = mc->GetZkCluster();
    option.zk_path = mc->GetZkPath();
    ::openmldb::sdk::ClusterSDK sdk(option);
    sdk.Init();
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablet;
    ok = sdk.GetTablet(db, name, &tablet);
    if (!ok || tablet.size() <= 0) return;
    uint32_t tid = sdk.GetTableId(db, name);
    {
        for (int32_t i = 0; i < 1000; i++) {
            tablet[0]->GetClient()->Put(tid, 0, pk, ts + i, value);
        }
    }
    std::string sql = "select col1, col2 + 1, col3, col4, col5 from " + name + " ;";
    ::hybridse::sdk::Status status;
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) return;
    for (auto _ : state) {
        benchmark::DoNotOptimize(router->ExecuteSQL(db, sql, &status));
        if (hybridse::sqlcase::SqlCase::IsDebug()) {
            state.SkipWithError("benchmark case debug");
            break;
        }
    }
}

static bool Async3Times(const std::string& db, const std::string& table, const std::string& key, int64_t st, int64_t et,
                        std::shared_ptr<openmldb::sdk::TableReader> reader, hybridse::sdk::Status* status) {
    ::openmldb::sdk::ScanOption so;
    std::vector<std::shared_ptr<::openmldb::sdk::ScanFuture>> tasks;
    for (int i = 0; i < 3; i++) {
        auto f = reader->AsyncScan(db, table, key, st, et, so, 100, status);
        if (!f) {
            LOG(WARNING) << "null future ";
            return false;
        }
        tasks.push_back(f);
    }
    for (int i = 0; i < 3; i++) {
        tasks.at(i)->GetResultSet(status);
    }
    return true;
}
static void BM_SimpleTableReaderAsyncMulti(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string ddl =
        "create table t1"
        "("
        "col1 string, col2 bigint,"
        "index(key=col1, ts=col2));";
    router->ExecuteDDL(db, ddl, &status);
    router->RefreshCatalog();
    int64_t st = 361;
    int64_t et = 1;
    int records = state.range(0);
    std::string key = "k1";
    for (int32_t i = 1; i < records + 361; i++) {
        std::string row = "insert into t1 values('" + key + "', " + std::to_string(i) + "L);";
        router->ExecuteInsert(db, row, &status);
    }
    auto reader = router->GetTableReader();

    ::openmldb::sdk::ScanOption so;
    auto rs = reader->Scan(db, "t1", key, st, et, so, &status);
    if (!rs || rs->Size() < 300) {
        LOG(WARNING) << "result count is mismatch";
        return;
    }
    for (auto _ : state) {
        benchmark::DoNotOptimize(Async3Times(db, "t1", key, st, et, reader, &status));
    }
}

static void BM_SimpleTableReaderAsync(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string ddl =
        "create table t1"
        "("
        "col1 string, col2 bigint,"
        "index(key=col1, ts=col2));";
    router->ExecuteDDL(db, ddl, &status);
    router->RefreshCatalog();
    int64_t st = 361;
    int64_t et = 1;
    int records = state.range(0);
    std::string key = "k1";
    for (int32_t i = 1; i < records + 361; i++) {
        std::string row = "insert into t1 values('" + key + "', " + std::to_string(i) + "L);";
        router->ExecuteInsert(db, row, &status);
    }
    auto reader = router->GetTableReader();

    ::openmldb::sdk::ScanOption so;
    auto rs = reader->Scan(db, "t1", key, st, et, so, &status);
    if (!rs || rs->Size() < 300) {
        LOG(WARNING) << "result count is mismatch";
        return;
    }
    for (auto _ : state) {
        benchmark::DoNotOptimize(reader->AsyncScan(db, "t1", key, st, et, so, 10, &status)->GetResultSet(&status));
    }
}

static void BM_SimpleTableReaderSync(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string ddl =
        "create table t1"
        "("
        "col1 string, col2 bigint,"
        "index(key=col1, ts=col2));";
    router->ExecuteDDL(db, ddl, &status);
    router->RefreshCatalog();
    int64_t st = 361;
    int64_t et = 1;
    int records = state.range(0);
    std::string key = "k1";
    for (int32_t i = 1; i < records + 361; i++) {
        std::string row = "insert into t1 values('" + key + "', " + std::to_string(i) + "L);";
        router->ExecuteInsert(db, row, &status);
    }
    auto reader = router->GetTableReader();

    ::openmldb::sdk::ScanOption so;
    auto rs = reader->Scan(db, "t1", key, st, et, so, &status);
    if (!rs || rs->Size() < 300) {
        LOG(WARNING) << "result count is mismatch";
        return;
    }
    for (auto _ : state) {
        benchmark::DoNotOptimize(reader->Scan(db, "t1", key, st, et, so, &status));
    }
}

static void GenerateInsertSQLSample(uint32_t size, std::string name, std::vector<std::string>* sample) {
    uint64_t time = 1589780888000l;
    for (uint64_t i = 0; i < size; ++i) {
        std::string insert_sql = "insert into " + name + " values('hello'," + std::to_string(time + i) + "," +
                                 std::to_string(i) + "," + std::to_string(2.7 + i) + "," + std::to_string(3.14 + i) +
                                 ");";
        sample->push_back(insert_sql);
    }
}

static void BM_SimpleInsertFunction(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string create = "create table " + name +
                         "(col1 string, col2 bigint, col3 int, col4 float, "
                         "col5 double, index(key=col1, ts=col2));";
    router->ExecuteDDL(db, create, &status);
    if (status.msg != "ok") {
        std::cout << "fail to create table" << std::endl;
        return;
    }
    sleep(2);
    router->RefreshCatalog();
    std::vector<std::string> sample;
    GenerateInsertSQLSample(state.range(0), name, &sample);
    for (auto _ : state) {
        for (uint64_t i = 0; i < sample.size(); ++i) {
            benchmark::DoNotOptimize(router->ExecuteInsert(db, sample[i], &status));
            if (hybridse::sqlcase::SqlCase::IsDebug()) {
                state.SkipWithError("benchmark case debug");
                break;
            }
        }
    }
}

static void BM_InsertPlaceHolderFunction(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string create = "create table " + name +
                         "(col1 string, col2 bigint, col3 int, col4 float, "
                         "col5 double, index(key=col1, ts=col2));";
    router->ExecuteDDL(db, create, &status);
    if (status.msg != "ok") {
        std::cout << "fail to create table" << std::endl;
        return;
    }
    sleep(2);
    router->RefreshCatalog();
    uint64_t time = 1589780888000l;
    for (auto _ : state) {
        std::string insert = "insert into " + name + " values(?, ?, ?, ?, ?);";
        for (int i = 0; i < state.range(0); ++i) {
            std::shared_ptr<::openmldb::sdk::SQLInsertRow> row = router->GetInsertRow(db, insert, &status);
            if (row != nullptr) {
                row->Init(5);
                row->AppendString("hello");
                row->AppendInt64(i + time);
                row->AppendInt32(i);
                row->AppendFloat(3.14 + i);
                row->AppendDouble(2.7 + i);
                benchmark::DoNotOptimize(router->ExecuteInsert(db, insert, row, &status));
            } else {
                std::cout << "get insert row failed" << std::endl;
            }
            if (hybridse::sqlcase::SqlCase::IsDebug()) {
                state.SkipWithError("benchmark case debug");
                break;
            }
        }
    }
}

static void BM_InsertPlaceHolderBatchFunction(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string create = "create table " + name +
                         "(col1 string, col2 bigint, col3 int, col4 float, "
                         "col5 double, index(key=col1, ts=col2));";
    router->ExecuteDDL(db, create, &status);
    if (status.msg != "ok") {
        std::cout << "fail to create table" << std::endl;
        return;
    }
    sleep(2);
    router->RefreshCatalog();
    uint64_t time = 1589780888000l;
    for (auto _ : state) {
        std::string insert = "insert into " + name + " values(?, ?, ?, ?, ?);";
        std::shared_ptr<::openmldb::sdk::SQLInsertRows> rows = router->GetInsertRows(db, insert, &status);
        if (rows != nullptr) {
            for (int i = 0; i < state.range(0); ++i) {
                std::shared_ptr<::openmldb::sdk::SQLInsertRow> row = rows->NewRow();
                row->Init(5);
                row->AppendString("hello");
                row->AppendInt64(i + time);
                row->AppendInt32(i);
                row->AppendFloat(3.14 + i);
                row->AppendDouble(2.7 + i);
            }
            benchmark::DoNotOptimize(router->ExecuteInsert(db, insert, rows, &status));
        } else {
            std::cout << "get insert row failed" << std::endl;
        }
        if (hybridse::sqlcase::SqlCase::IsDebug()) {
            state.SkipWithError("benchmark case debug");
            break;
        }
    }
}

static void BM_SimpleRowWindow(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    if (hybridse::sqlcase::SqlCase::IsDebug()) {
        sql_opt.enable_debug = true;
    } else {
        sql_opt.enable_debug = false;
    }
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string create = "create table " + name +
                         "(id int, c1 string, c2 string, c3 string, c4 string, "
                         "c6 double, c7 timestamp, "
                         "index(key=(c1), ts=c7, ttl=3650d)"
                         ") options(partitionnum=8);";
    router->ExecuteDDL(db, create, &status);
    if (status.msg != "ok") {
        std::cout << "fail to create table" << std::endl;
        return;
    }
    sleep(2);
    router->RefreshCatalog();
    std::vector<std::string> sample;
    std::string base_sql = "insert into " + name;
    int window_size = state.range(0);
    int id = 1;
    int64_t ts = 1590738991000;
    for (int i = 0; i < window_size; i++) {
        sample.push_back(base_sql + " values(" + std::to_string(id++) + ", 'a', 'aa', 'aaa', 'aaaa', " +
                         std::to_string(i) + ", " + std::to_string(ts - i * 1000) + ");");
        sample.push_back(base_sql + " values(" + std::to_string(id++) + ", 'b', 'bb', 'bbb', 'bbbb', " +
                         std::to_string(i) + ", " + std::to_string(ts - i * 1000) + ");");
        sample.push_back(base_sql + " values(" + std::to_string(id++) + ", 'c', 'cc', 'ccc', 'cccc', " +
                         std::to_string(i) + ", " + std::to_string(ts - i * 1000) + ");");
    }
    for (const auto& sql : sample) {
        router->ExecuteInsert(db, sql, &status);
    }
    std::string preceding = std::to_string(window_size - 1);
    char sql[1000];
    int size = snprintf(sql, sizeof(sql),
                        "SELECT id, c1, c2, c3, c4, c6, c7 "
                        ", min(c6) OVER w1 as w1_c6_min, count(id) OVER w1 as w1_cnt "
                        "FROM %s WINDOW "
                        "w1 AS (PARTITION BY c1 ORDER BY c7 ROWS BETWEEN %s PRECEDING AND CURRENT ROW);",
                        name.c_str(), preceding.c_str());
    std::string exe_sql(sql, size);
    auto request_row = router->GetRequestRow(db, exe_sql, &status);
    request_row->Init(10);
    request_row->AppendInt32(id);
    request_row->AppendString("a");
    request_row->AppendString("aa");
    request_row->AppendString("aaa");
    request_row->AppendString("aaaa");
    request_row->AppendDouble(1.0);
    request_row->AppendTimestamp(ts + 1000);
    request_row->Build();
    for (int i = 0; i < 10; i++) {
        router->ExecuteSQLRequest(db, exe_sql, request_row, &status);
    }
    LOG(INFO) << "------------WARMUP FINISHED ------------\n\n";
    if (hybridse::sqlcase::SqlCase::IsDebug() || hybridse::sqlcase::SqlCase::IS_PERF()) {
        for (auto _ : state) {
            router->ExecuteSQLRequest(db, exe_sql, request_row, &status);
            state.SkipWithError("benchmark case debug");
            break;
        }
    } else {
        for (auto _ : state) {
            benchmark::DoNotOptimize(router->ExecuteSQLRequest(db, exe_sql, request_row, &status));
        }
    }
}
static void BM_SimpleRow4Window(benchmark::State& state) {  // NOLINT
    ::openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    if (hybridse::sqlcase::SqlCase::IsDebug()) {
        sql_opt.enable_debug = true;
    } else {
        sql_opt.enable_debug = false;
    }
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    std::string create = "create table " + name +
                         "(id int, c1 string, c2 string, c3 string, c4 string, "
                         "c6 double, c7 timestamp, "
                         "index(key=(c1), ts=c7, ttl=3650d), "
                         "index(key=(c2), ts=c7, ttl=3650d), "
                         "index(key=(c3), ts=c7, ttl=3650d), "
                         "index(key=(c4), ts=c7, ttl=3650d) "
                         ") options(partitionnum=8);";
    router->ExecuteDDL(db, create, &status);
    if (status.msg != "ok") {
        std::cout << "fail to create table" << std::endl;
        return;
    }
    sleep(2);
    router->RefreshCatalog();
    std::vector<std::string> sample;
    std::string base_sql = "insert into " + name;
    int window_size = state.range(0);
    int id = 1;
    int64_t ts = 1590738991000;
    for (int i = 0; i < window_size; i++) {
        sample.push_back(base_sql + " values(" + std::to_string(id++) + ", 'a', 'aa', 'aaa', 'aaaa', " +
                         std::to_string(i) + ", " + std::to_string(ts - i * 1000) + ");");
        sample.push_back(base_sql + " values(" + std::to_string(id++) + ", 'b', 'bb', 'bbb', 'bbbb', " +
                         std::to_string(i) + ", " + std::to_string(ts - i * 1000) + ");");
        sample.push_back(base_sql + " values(" + std::to_string(id++) + ", 'c', 'cc', 'ccc', 'cccc', " +
                         std::to_string(i) + ", " + std::to_string(ts - i * 1000) + ");");
    }
    for (const auto& sql : sample) {
        router->ExecuteInsert(db, sql, &status);
    }
    char sql[1000];
    std::string preceding = std::to_string(window_size - 1);
    int size = snprintf(sql, sizeof(sql),
                        "SELECT id, c1, c2, c3, c4, c6, c7 "
                        ", min(c6) OVER w1 as w1_c6_min, count(id) OVER w1 as w1_cnt "
                        ", min(c6) OVER w2 as w2_c6_min, count(id) OVER w2 as w2_cnt "
                        ", min(c6) OVER w3 as w3_c6_min, count(id) OVER w3 as w3_cnt "
                        ", min(c6) OVER w4 as w4_c6_min, count(id) OVER w4 as w4_cnt "
                        "FROM %s WINDOW "
                        "  w1 AS (PARTITION BY c1 ORDER BY c7 ROWS BETWEEN %s PRECEDING AND CURRENT ROW)"
                        ", w2 AS (PARTITION BY c2 ORDER BY c7 ROWS BETWEEN %s PRECEDING AND CURRENT ROW)"
                        ", w3 AS (PARTITION BY c3 ORDER BY c7 ROWS BETWEEN %s PRECEDING AND CURRENT ROW)"
                        ", w4 AS (PARTITION BY c4 ORDER BY c7 ROWS BETWEEN %s PRECEDING AND CURRENT ROW)"
                        ";",
                        name.c_str(), preceding.c_str(), preceding.c_str(), preceding.c_str(), preceding.c_str());
    std::string exe_sql(sql, size);
    auto request_row = router->GetRequestRow(db, exe_sql, &status);
    request_row->Init(10);
    request_row->AppendInt32(id);
    request_row->AppendString("a");
    request_row->AppendString("aa");
    request_row->AppendString("aaa");
    request_row->AppendString("aaaa");
    request_row->AppendDouble(1.0);
    request_row->AppendTimestamp(ts + 1000);
    request_row->Build();
    for (int i = 0; i < 10; i++) {
        router->ExecuteSQLParameterized(db, exe_sql, request_row, &status);
    }
    LOG(INFO) << "------------WARMUP FINISHED ------------\n\n";
    if (hybridse::sqlcase::SqlCase::IsDebug() || hybridse::sqlcase::SqlCase::IS_PERF()) {
        for (auto _ : state) {
            router->ExecuteSQLParameterized(db, exe_sql, request_row, &status);
            state.SkipWithError("benchmark case debug");
            break;
        }
    } else {
        for (auto _ : state) {
            benchmark::DoNotOptimize(router->ExecuteSQLParameterized(db, exe_sql, request_row, &status));
        }
    }
}

static void SimpleLastJoinNCaseData(hybridse::sqlcase::SqlCase& sql_case, int32_t window_size) {  // NOLINT
    sql_case.db_ = hybridse::sqlcase::SqlCase::GenRand("db");
    // table {0}
    {
        hybridse::sqlcase::SqlCase::TableInfo input;
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        input.columns_ = {"id int", "c1 string", "c2 string", "c3 string", "c4 string", "c6 double", "c7 timestamp"};
        input.indexs_ = {"index1:c1:c7"};
        sql_case.inputs_.push_back(input);
    }

    // table {1}
    {
        hybridse::sqlcase::SqlCase::TableInfo input;
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        input.columns_ = {"rid int", "x1 string", "x2 string", "x3 string", "x4 string", "x6 double", "x7 timestamp"};
        input.indexs_ = {"index1:x1:x7"};
        int id = 0;
        int64_t ts = 1590738991000;
        for (int i = 1; i < window_size; i++) {
            ts -= 1000;
            // prepare row {id, c1, c2, c3, c4, c5, c6, c7};
            input.rows_.push_back(
                {std::to_string(id++), "a", "aa", "aaa", "aaaa", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "b", "bb", "bbb", "bbbb", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "c", "cc", "ccc", "cccc", std::to_string(i), std::to_string(ts)});
        }
        sql_case.inputs_.push_back(input);
    }
    // table {2}
    {
        hybridse::sqlcase::SqlCase::TableInfo input = sql_case.inputs_[1];
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        input.indexs_ = {"index2:x2:x7"};
        sql_case.inputs_.push_back(input);
    }
    // table {3}
    {
        hybridse::sqlcase::SqlCase::TableInfo input = sql_case.inputs_[1];
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        input.indexs_ = {"index3:x3:x7"};
        sql_case.inputs_.push_back(input);
    }
    // table {4}
    {
        hybridse::sqlcase::SqlCase::TableInfo input = sql_case.inputs_[1];
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        input.indexs_ = {"index4:x4:x7"};
        sql_case.inputs_.push_back(input);
    }

    // request table {0}
    {
        hybridse::sqlcase::SqlCase::TableInfo request;
        request.columns_ = {"id int", "c1 string", "c2 string", "c3 string", "c4 string", "c6 double", "c7 timestamp"};
        request.indexs_ = {"index1:c1:c7"};
        request.rows_.push_back(
            {std::to_string(0), "a", "bb", "ccc", "aaaa", "1.0", std::to_string(1590738991000 + 1000)});
        sql_case.batch_request_ = request;
    }
}

static void SimpleWindowOutputLastJoinNCaseData(hybridse::sqlcase::SqlCase& sql_case, int32_t window_size) {  // NOLINT
    sql_case.db_ = hybridse::sqlcase::SqlCase::GenRand("db");
    // table {0}
    {
        int id = 0;
        hybridse::sqlcase::SqlCase::TableInfo input;
        input.columns_ = {"id int", "c1 string", "c2 string", "c3 string", "c4 string", "c6 double", "c7 timestamp"};
        input.indexs_ = {"index1:c1:c7"};
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        int64_t ts = 1590738991000;
        for (int i = 1; i < window_size; i++) {
            ts -= 1000;
            // prepare row {id, c1, c2, c3, c4, c5, c6, c7};
            input.rows_.push_back(
                {std::to_string(id++), "a", "aa", "aaa", "aaaa", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "b", "bb", "bbb", "bbbb", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "c", "cc", "ccc", "cccc", std::to_string(i), std::to_string(ts)});
        }
        sql_case.inputs_.push_back(input);
        // request table {0}
        hybridse::sqlcase::SqlCase::TableInfo request;
        request.columns_ = {"id int", "c1 string", "c2 string", "c3 string", "c4 string", "c6 double", "c7 timestamp"};
        request.indexs_ = {"index1:c1:c7"};
        request.rows_.push_back({std::to_string(id), "a", "bb", "ccc", "aaaa", "1.0", std::to_string(1590738991000)});
        sql_case.batch_request_ = request;
    }
    // table {1}
    {
        int id = 0;
        hybridse::sqlcase::SqlCase::TableInfo input;
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        input.columns_ = {"rid int", "x1 string", "x2 string", "x3 string", "x4 string", "x6 double", "x7 timestamp"};
        input.indexs_ = {"index1:x1:x7", "index2:x2:x7", "index3:x3:x7", "index4:x4:x7"};
        int64_t ts = 1590738991000;
        for (int i = 1; i < window_size; i++) {
            ts -= 1000;
            // prepare row {id, c1, c2, c3, c4, c5, c6, c7};
            input.rows_.push_back(
                {std::to_string(id++), "a", "aa", "aaa", "aaaa", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "b", "bb", "bbb", "bbbb", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "c", "cc", "ccc", "cccc", std::to_string(i), std::to_string(ts)});
        }
        sql_case.inputs_.push_back(input);
    }
}
static void BM_SimpleLastJoinTable2(benchmark::State& state) {  // NOLINT
    hybridse::sqlcase::SqlCase sql_case;
    sql_case.desc_ = "BM_SimpleLastJoin2Right";
    SimpleLastJoinNCaseData(sql_case, state.range(0));

    sql_case.sql_str_ = (R"(
    SELECT {0}.id, {0}.c1, {0}.c2, {0}.c3, {0}.c4, {0}.c7, {1}.x1, {1}.x7, {2}.x2, {2}.x7
FROM {0}
last join {1} order by {1}.x7 on {0}.c1 = {1}.x1 and {0}.c7 - {ts_diff} >= {1}.x7
last join {2} order by {2}.x7 on {0}.c2 = {2}.x2 and {0}.c7 - {ts_diff} >= {2}.x7;
)");
    absl::StrReplaceAll({{"{ts_diff}", std::to_string(state.range(0) * 1000 / 2)}}, &sql_case.sql_str_);
    BM_RequestQuery(state, sql_case, mc);
}
static void BM_SimpleLastJoinTable4(benchmark::State& state) {  // NOLINT
    hybridse::sqlcase::SqlCase sql_case;
    sql_case.desc_ = "BM_SimpleLastJoin3Table";
    SimpleLastJoinNCaseData(sql_case, state.range(0));
    sql_case.sql_str_ = R"(
    SELECT {0}.id, {0}.c1, {0}.c2, {0}.c3, {0}.c4, {0}.c7, {1}.x1, {1}.x7, {2}.x2, {2}.x7, {3}.x3, {3}.x7, {4}.x4, {4}.x7
FROM {0}
last join {1} order by {1}.x7 on {0}.c1 = {1}.x1 and {0}.c7 - {ts_diff} >= {1}.x7
last join {2} order by {2}.x7 on {0}.c2 = {2}.x2 and {0}.c7 - {ts_diff} >= {2}.x7
last join {3} order by {3}.x7 on {0}.c3 = {3}.x3 and {0}.c7 - {ts_diff} >= {3}.x7
last join {4} order by {4}.x7 on {0}.c4 = {4}.x4 and {0}.c7 - {ts_diff} >= {4}.x7;
)";
    absl::StrReplaceAll({{"{ts_diff}", std::to_string(state.range(0) * 1000 / 2)}}, &sql_case.sql_str_);
    BM_RequestQuery(state, sql_case, mc);
}

static void BM_SimpleWindowOutputLastJoinTable2(benchmark::State& state) {  // NOLINT
    hybridse::sqlcase::SqlCase sql_case;
    sql_case.desc_ = "BM_SimpleWindowOutputLastJoin4Table";
    SimpleWindowOutputLastJoinNCaseData(sql_case, state.range(0));
    sql_case.sql_str_ = R"(
select id, c1, c2, c3, c4, c6, c7, cur_hour, today
, w1_sum_c6, w1_max_c6, w1_min_c6, w1_avg_c6, w1_cnt_c6
, t1.rid as t1_rid, t2.rid as t2_rid
    from
    (
        select id, c1, c2, c3, c4, c6, c7, hour(c7) as cur_hour, day(c7) as today
, sum(c6) over w1 as w1_sum_c6
, max(c6) over w1 as w1_max_c6
, min(c6) over w1 as w1_min_c6
, avg(c6) over w1 as w1_avg_c6
, count(c6) over w1 as w1_cnt_c6
from {0}
window w1 as (PARTITION BY {0}.c1 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as w_out last join {1} as t1 order by t1.x7 on c1 = t1.x1 and c7 - {ts_diff}>= t1.x7
last join {1} as t2 order by t2.x7 on c2 = t2.x2 and c7 - {ts_diff} >= t2.x7
;
)";
    absl::StrReplaceAll({{"{ts_diff}", std::to_string(state.range(0) * 1000 / 2)}}, &sql_case.sql_str_);
    BM_RequestQuery(state, sql_case, mc);
}
static void BM_SimpleWindowOutputLastJoinTable4(benchmark::State& state) {  // NOLINT
    hybridse::sqlcase::SqlCase sql_case;
    sql_case.desc_ = "BM_SimpleWindowOutputLastJoin4Table";
    SimpleWindowOutputLastJoinNCaseData(sql_case, state.range(0));
    sql_case.sql_str_ = R"(
      select id, c1, c2, c3, c4, c6, c7, cur_hour, today
      , w1_sum_c6, w1_max_c6, w1_min_c6, w1_avg_c6, w1_cnt_c6
      , t1.rid as t1_rid, t2.rid as t2_rid, t3.rid as t3_rid, t4.rid as t4_rid
      from
      (
        select id, c1, c2, c3, c4, c6, c7, hour(c7) as cur_hour, day(c7) as today
        , sum(c6) over w1 as w1_sum_c6
        , max(c6) over w1 as w1_max_c6
        , min(c6) over w1 as w1_min_c6
        , avg(c6) over w1 as w1_avg_c6
        , count(c6) over w1 as w1_cnt_c6
        from {0}
        window w1 as (PARTITION BY {0}.c1 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
      ) as w_out last join {1} as t1 order by t1.x7 on c1 = t1.x1 and c7 - {ts_diff} >= t1.x7
        last join {1} as t2 order by t2.x7 on c2 = t2.x2 and c7 - {ts_diff} >= t2.x7
        last join {1} as t3 order by t3.x7 on c3 = t3.x3 and c7 - {ts_diff} >= t3.x7
        last join {1} as t4 order by t4.x7 on c4 = t4.x4 and c7 - {ts_diff} >= t4.x7;
)";
    absl::StrReplaceAll({{"{ts_diff}", std::to_string(state.range(0) * 1000 / 2)}}, &sql_case.sql_str_);
    BM_RequestQuery(state, sql_case, mc);
}

static void LastJoinNWindowOutputCase(hybridse::sqlcase::SqlCase& sql_case, int32_t window_size) {  // NOLINT
    sql_case.db_ = hybridse::sqlcase::SqlCase::GenRand("db");
    int request_id = 0;
    std::vector<std::string> columns = {"id int",    "c1 string", "c2 string",   "c3 string",
                                        "c4 string", "c6 double", "c7 timestamp"};
    std::vector<std::string> indexs = {"index1:c1:c7", "index2:c2:c7", "index3:c3:c7", "index4:c4:c7"};
    // table {0}
    {
        hybridse::sqlcase::SqlCase::TableInfo input;
        input.columns_ = columns;
        input.indexs_ = indexs;
        input.name_ = hybridse::sqlcase::SqlCase::GenRand("table");
        int id = 0;
        int64_t ts = 1590738991000;
        for (int i = 1; i < window_size; i++) {
            ts -= 1000;
            // prepare row {id, c1, c2, c3, c4, c5, c6, c7};
            input.rows_.push_back(
                {std::to_string(id++), "a", "aa", "aaa", "aaaa", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "b", "bb", "bbb", "bbbb", std::to_string(i), std::to_string(ts)});
            input.rows_.push_back(
                {std::to_string(id++), "c", "cc", "ccc", "cccc", std::to_string(i), std::to_string(ts)});
        }
        sql_case.inputs_.push_back(input);
        request_id = id;
    }
    // request table {0}
    {
        hybridse::sqlcase::SqlCase::TableInfo request;
        request.columns_ = columns;
        request.indexs_ = indexs;
        request.rows_.push_back(
            {std::to_string(request_id), "a", "bb", "ccc", "aaaa", "1.0", std::to_string(1590738991000 + 1000)});
        sql_case.batch_request_ = request;
    }
}
static void BM_LastJoin4WindowOutput(benchmark::State& state) {  // NOLINT
    hybridse::sqlcase::SqlCase sql_case;
    LastJoinNWindowOutputCase(sql_case, state.range(0));
    sql_case.desc_ = "BM_LastJoin4WindowOutput";
    sql_case.sql_str_ = R"(
select * from
(
select id as out1_id, c1, sum(c6) over w1 as w1_sum_c6, count(c6) over w1 as w1_cnt_c6 from {0}
window w1 as (PARTITION BY {0}.c1 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out1 last join
(
select id as out2_id, c2, sum(c6) over w2 as w2_sum_c6, count(c6) over w2 as w2_cnt_c6 from {0}
window w2 as (PARTITION BY {0}.c2 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out2 on out1_id=out2_id last join
(
select id as out3_id, c3, sum(c6) over w3 as w3_sum_c6, count(c6) over w3 as w3_cnt_c6 from {0}
window w3 as (PARTITION BY {0}.c3 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out3 on out1_id=out3_id last join
(
select id as out4_id, c4, sum(c6) over w4 as w4_sum_c6, count(c6) over w4 as w4_cnt_c6 from {0}
window w4 as (PARTITION BY {0}.c4 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out4 on out1_id=out4_id;
)";
    BM_RequestQuery(state, sql_case, mc);
}
static void BM_LastJoin8WindowOutput(benchmark::State& state) {  // NOLINT
    hybridse::sqlcase::SqlCase sql_case;
    LastJoinNWindowOutputCase(sql_case, state.range(0));
    sql_case.desc_ = "BM_LastJoin4WindowOutput";
    sql_case.sql_str_ = R"(
select * from
(
select id as out1_id, c1, sum(c6) over w1 as w1_sum_c6, count(c6) over w1 as w1_cnt_c6 from {0}
window w1 as (PARTITION BY {0}.c1 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out1 last join
(
select id as out2_id, c2, sum(c6) over w2 as w2_sum_c6, count(c6) over w2 as w2_cnt_c6 from {0}
window w2 as (PARTITION BY {0}.c2 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out2 on out1_id=out2_id last join
(
select id as out3_id, c3, sum(c6) over w3 as w3_sum_c6, count(c6) over w3 as w3_cnt_c6 from {0}
window w3 as (PARTITION BY {0}.c3 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out3 on out1_id=out3_id last join
(
select id as out4_id, c4, sum(c6) over w4 as w4_sum_c6, count(c6) over w4 as w4_cnt_c6 from {0}
window w4 as (PARTITION BY {0}.c4 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW)
) as out4 on out1_id=out4_id last join
(
select id as out5_id, c1, sum(c6) over w5 as w5_sum_c6, count(c6) over w5 as w5_cnt_c6 from {0}
window w5 as (PARTITION BY {0}.c1 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)
) as out5 on out1_id=out5_id last join
(
select id as out6_id, c2, sum(c6) over w6 as w6_sum_c6, count(c6) over w6 as w6_cnt_c6 from {0}
window w6 as (PARTITION BY {0}.c2 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)
) as out6 on out1_id=out6_id last join
(
select id as out7_id, c3, sum(c6) over w7 as w7_sum_c6, count(c6) over w7 as w7_cnt_c6 from {0}
window w7 as (PARTITION BY {0}.c3 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)
) as out7 on out1_id=out7_id last join
(
select id as out8_id, c4, sum(c6) over w8 as w8_sum_c6, count(c6) over w8 as w8_cnt_c6 from {0}
window w8 as (PARTITION BY {0}.c4 ORDER BY {0}.c7 ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)
) as out8 on out1_id=out8_id
;
)";
    BM_RequestQuery(state, sql_case, mc);
}

BENCHMARK(BM_SimpleLastJoinTable2)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleLastJoinTable4)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleWindowOutputLastJoinTable2)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleWindowOutputLastJoinTable4)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleRowWindow)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleRow4Window)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_LastJoin4WindowOutput)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_LastJoin8WindowOutput)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleQueryFunction);

BENCHMARK(BM_SimpleInsertFunction)->Args({10})->Args({100})->Args({1000})->Args({10000});

BENCHMARK(BM_InsertPlaceHolderFunction)->Args({10})->Args({100})->Args({1000})->Args({10000});

BENCHMARK(BM_InsertPlaceHolderBatchFunction)->Args({10})->Args({100})->Args({1000})->Args({10000});
BENCHMARK(BM_SimpleTableReaderSync)->Args({10})->Args({100})->Args({1000})->Args({2000})->Args({4000})->Args({10000});
BENCHMARK(BM_SimpleTableReaderAsync)->Args({10})->Args({100})->Args({1000})->Args({2000})->Args({4000})->Args({10000});
BENCHMARK(BM_SimpleTableReaderAsyncMulti)
    ->Args({10})
    ->Args({100})
    ->Args({1000})
    ->Args({2000})
    ->Args({4000})
    ->Args({10000});

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::base::SetupGlog(true);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    FLAGS_enable_distsql = hybridse::sqlcase::SqlCase::IsCluster();
    FLAGS_enable_localtablet = !hybridse::sqlcase::SqlCase::IsDisableLocalTablet();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::openmldb::sdk::MiniCluster mini_cluster(6181);
    mc = &mini_cluster;
    if (!hybridse::sqlcase::SqlCase::IsCluster()) {
        mini_cluster.SetUp(1);
    } else {
        mini_cluster.SetUp();
    }
    sleep(2);
    ::benchmark::RunSpecifiedBenchmarks();
    mini_cluster.Close();
}
