/*
 * mini_cluster_microbenchmark.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>
#include "benchmark/benchmark.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "sdk/base.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "vm/catalog.h"

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

::rtidb::sdk::MiniCluster* mc;

static void BM_SimpleQueryFunction(benchmark::State& state) {  // NOLINT
    ::rtidb::nameserver::TableInfo table_info;
    table_info.set_format_version(1);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    auto ns_client = mc->GetNsClient();
    std::string error;
    bool ok = ns_client->CreateDatabase(db, error);
    table_info.set_name(name);
    table_info.set_db(db);
    table_info.set_partition_num(1);
    RtiDBSchema* schema = table_info.mutable_column_desc_v1();
    auto col1 = schema->Add();
    col1->set_name("col1");
    col1->set_data_type(::rtidb::type::kVarchar);
    col1->set_type("string");
    auto col2 = schema->Add();
    col2->set_name("col2");
    col2->set_data_type(::rtidb::type::kBigInt);
    col2->set_type("int64");
    col2->set_is_ts_col(true);
    auto col3 = schema->Add();
    col3->set_name("col3");
    col3->set_data_type(::rtidb::type::kBigInt);
    col3->set_type("int64");
    col3->set_is_ts_col(false);
    auto col4 = schema->Add();
    col4->set_name("col4");
    col4->set_data_type(::rtidb::type::kBigInt);
    col4->set_type("int64");
    col4->set_is_ts_col(false);
    auto col5 = schema->Add();
    col5->set_name("col5");
    col5->set_data_type(::rtidb::type::kBigInt);
    col5->set_type("int64");
    col5->set_is_ts_col(false);

    RtiDBIndex* index = table_info.mutable_column_key();
    auto key1 = index->Add();
    key1->set_index_name("index0");
    key1->add_col_name("col1");
    key1->add_ts_name("col2");
    ok = ns_client->CreateTable(table_info, error);

    ::fesql::vm::Schema fe_schema;
    ::rtidb::catalog::SchemaAdapter::ConvertSchema(table_info.column_desc_v1(),
                                                   &fe_schema);
    ::fesql::codec::RowBuilder rb(fe_schema);
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
    ::rtidb::sdk::ClusterOptions option;
    option.zk_cluster = mc->GetZkCluster();
    option.zk_path = mc->GetZkPath();
    ::rtidb::sdk::ClusterSDK sdk(option);
    sdk.Init();
    std::vector<std::shared_ptr<::rtidb::catalog::TabletAccessor>> tablet;
    ok = sdk.GetTablet(db, name, &tablet);
    if (!ok || tablet.size() <= 0) return;
    uint32_t tid = sdk.GetTableId(db, name);
    {
        for (int32_t i = 0; i < 1000; i++) {
            ok = tablet[0]->GetClient()->Put(tid, 0, pk, ts + i, value, 1);
        }
    }
    std::string sql =
        "select col1, col2 + 1, col3, col4, col5 from " + name + " ;";
    ::fesql::sdk::Status status;
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) return;
    for (auto _ : state) {
        benchmark::DoNotOptimize(router->ExecuteSQL(db, sql, &status));
    }
}

static void GenerateInsertSQLSample(uint32_t size, std::string name,
                                    std::vector<std::string>* sample) {
    uint64_t time = 1589780888000l;
    for (uint64_t i = 0; i < size; ++i) {
        std::string insert_sql =
            "insert into " + name + " values('hello'," +
            std::to_string(time + i) + "," + std::to_string(i) + "," +
            std::to_string(2.7 + i) + "," + std::to_string(3.14 + i) + ");";
        sample->push_back(insert_sql);
    }
}

static void BM_SimpleInsertFunction(benchmark::State& state) {  // NOLINT
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
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
            benchmark::DoNotOptimize(
                router->ExecuteInsert(db, sample[i], &status));
        }
    }
}

static void BM_InsertPlaceHolderFunction(benchmark::State& state) {  // NOLINT
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
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
            std::shared_ptr<::rtidb::sdk::SQLInsertRow> row =
                router->GetInsertRow(db, insert, &status);
            if (row != nullptr) {
                row->Init(5);
                row->AppendString("hello");
                row->AppendInt64(i + time);
                row->AppendInt32(i);
                row->AppendFloat(3.14 + i);
                row->AppendDouble(2.7 + i);
                benchmark::DoNotOptimize(
                    router->ExecuteInsert(db, insert, row, &status));
            } else {
                std::cout << "get insert row failed" << std::endl;
            }
        }
    }
}

static void BM_InsertPlaceHolderBatchFunction(
    benchmark::State& state) {  // NOLINT
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
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
        std::shared_ptr<::rtidb::sdk::SQLInsertRows> rows =
            router->GetInsertRows(db, insert, &status);
        if (rows != nullptr) {
            for (int i = 0; i < state.range(0); ++i) {
                std::shared_ptr<::rtidb::sdk::SQLInsertRow> row =
                    rows->NewRow();
                row->Init(5);
                row->AppendString("hello");
                row->AppendInt64(i + time);
                row->AppendInt32(i);
                row->AppendFloat(3.14 + i);
                row->AppendDouble(2.7 + i);
            }
            benchmark::DoNotOptimize(
                router->ExecuteInsert(db, insert, rows, &status));
        } else {
            std::cout << "get insert row failed" << std::endl;
        }
    }
}

static void BM_SimpleRowWindow(benchmark::State& state) {  // NOLINT
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc->GetZkCluster();
    sql_opt.zk_path = mc->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (router == nullptr) {
        std::cout << "fail to init sql cluster router" << std::endl;
        return;
    }
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
    router->CreateDB(db, &status);
    std::string create = "create table " + name +
                         "(id int, c1 string, c6 double, c7 timestamp, index(key=(c1), ts=c7)) partitionnum=8;";
    router->ExecuteDDL(db, create, &status);
    if (status.msg != "ok") {
        std::cout << "fail to create table" << std::endl;
        return;
    }
    sleep(2);
    router->RefreshCatalog();
    std::vector<std::string> sample;
    std::string base_sql = "insert into " + name;
    sample.push_back(base_sql + " values(1, 'aa', 1.0, 1590738990000);");
    sample.push_back(base_sql + " values(2, 'aa', 2.0, 1590738991000);");
    sample.push_back(base_sql + " values(3, 'aa', 3.0, 1590738992000);");
    sample.push_back(base_sql + " values(4, 'aa', 4.0, 1590738993000);");
    sample.push_back(base_sql + " values(5, 'bb', 5.0, 1590738994000);");
    sample.push_back(base_sql + " values(6, 'bb', 6.0, 1590738995000);");
    sample.push_back(base_sql + " values(7, 'bb', 7.0, 1590738996000);");
    sample.push_back(base_sql + " values(8, 'bb', 8.0, 1590738997000);");
    sample.push_back(base_sql + " values(9, 'bb', 9.0, 1590738998000);");
    sample.push_back(base_sql + " values(10, 'cc', 1.0, 1590738993000);");
    sample.push_back(base_sql + " values(11, 'cc', 2.0, 1590738994000);");
    sample.push_back(base_sql + " values(12, 'cc', 3.0, 1590738995000);");
    sample.push_back(base_sql + " values(13, 'cc', 4.0, 1590738996000);");
    sample.push_back(base_sql + " values(14, 'cc', 5.0, 1590738997000);");
    sample.push_back(base_sql + " values(15, 'dd', 6.0, 1590738998000);");
    sample.push_back(base_sql + " values(16, 'dd', 7.0, 1590738999000);");
    for (const auto& sql : sample) {
        router->ExecuteInsert(db, sql, &status);
    }
    char sql[1000];
    int size = snprintf(sql, sizeof(sql), "SELECT id, c1, c6, c7,  min(c6) OVER w1 as w1_c6_min, count(id) "
                                         "OVER w1 as w1_cnt FROM %s WINDOW w1 AS (PARTITION BY %s.c1 "
                                         "ORDER BY %s.c7 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW);",
                                          name.c_str(), name.c_str(), name.c_str());
    std::string exe_sql(sql, size);
    auto request_row = router->GetRequestRow(db, exe_sql, &status);
    request_row->Init(2);
    request_row->AppendInt32(1);
    request_row->AppendString("aa");
    request_row->AppendDouble(1.0);
    request_row->AppendTimestamp(1590738993000l);
    request_row->Build();
    for (auto _ : state) {
        benchmark::DoNotOptimize(router->ExecuteSQL(db, exe_sql, request_row, &status));
    }
}

BENCHMARK(BM_SimpleRowWindow);

BENCHMARK(BM_SimpleQueryFunction);

BENCHMARK(BM_SimpleInsertFunction)
    ->Args({10})
    ->Args({100})
    ->Args({1000})
    ->Args({10000});

BENCHMARK(BM_InsertPlaceHolderFunction)
    ->Args({10})
    ->Args({100})
    ->Args({1000})
    ->Args({10000});

BENCHMARK(BM_InsertPlaceHolderBatchFunction)
    ->Args({10})
    ->Args({100})
    ->Args({1000})
    ->Args({10000});

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::rtidb::sdk::MiniCluster mini_cluster(6181);
    mc = &mini_cluster;
    mini_cluster.SetUp();
    sleep(2);
    ::benchmark::RunSpecifiedBenchmarks();
    mini_cluster.Close();
}
