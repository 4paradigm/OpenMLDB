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

#include "sdk/mini_cluster.h"
#include "benchmark/benchmark.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "sdk/base.h"
#include "vm/catalog.h"
#include "sdk/sql_router.h"

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

static void BM_SimpleQuery10_2Function(benchmark::State& state) { //NOLINT
    ::rtidb::sdk::MiniCluster mc(6181);
    mc.SetUp();
    ::rtidb::nameserver::TableInfo table_info;
    table_info.set_format_version(1);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    auto ns_client = mc.GetNsClient();
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
    ::rtidb::sdk::ClusterOptions option;
    option.zk_cluster = mc.GetZkCluster();
    option.zk_path = mc.GetZkPath();
    ::rtidb::sdk::ClusterSDK sdk(option);
    sdk.Init();
    std::vector<std::shared_ptr<::rtidb::client::TabletClient>> tablet;
    ok = sdk.GetTabletByTable(db, name, &tablet);
    uint32_t tid = sdk.GetTableId(db, name);
    {
        for (int32_t i = 0;  i < 10; i++)
        ok = tablet[0]->Put(tid, 0, pk, ts + i, value, 1);
    }
    std::string sql = "select col1, col2 + 1 from " + name + " ;";

    ::fesql::sdk::Status status;
    ::rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc.GetZkCluster();
    sql_opt.zk_path = mc.GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    for (auto _ : state) {
        int32_t i = -1;
        benchmark::DoNotOptimize(
                router->ExecuteSQL(db, sql, &status));
    }
    mc.Close();
}

BENCHMARK(BM_SimpleQuery10_2Function);
BENCHMARK_MAIN();
