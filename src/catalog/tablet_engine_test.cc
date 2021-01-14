/*
 * tablet_engine_test.cc
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
#include "catalog/schema_adapter.h"
#include "catalog/tablet_catalog.h"
#include "codec/fe_row_codec.h"
#include "codec/sdk_codec.h"
#include "storage/fe_table.h"
#include "storage/mem_table.h"
#include "storage/table.h"
#include "test/base_test.h"
#include "timer.h"  // NOLINT
#include "vm/engine.h"
namespace rtidb {
namespace catalog {
class TabletEngineTest : public rtidb::test::SQLCaseTest {
 public:
    TabletEngineTest() {}
    virtual ~TabletEngineTest() {}
    static void BatchModeCheck(fesql::sqlcase::SQLCase &sql_case);   // NOLINT
    static void RequestModeCheck(fesql::sqlcase::SQLCase &sql_case,  // NOLINT
                                 fesql::vm::EngineOptions options = fesql::vm::EngineOptions());
};
struct TestArgs {
    std::shared_ptr<::rtidb::storage::Table> table;
    ::rtidb::api::TableMeta meta;
    std::string row;
    std::string idx_name;
    std::string pk;
    uint64_t ts;
};
void StoreData(std::shared_ptr<TestArgs> args, std::shared_ptr<fesql::storage::Table> sql_table,
               const std::vector<fesql::codec::Row> &rows) {
    auto meta = args->meta;
    auto table = args->table;
    rtidb::codec::SDKCodec sdk_codec(meta);
    LOG(INFO) << "store data start: rows size " << rows.size() << ", index size: " << sql_table->GetIndexMap().size();
    fesql::codec::RowView row_view(sql_table->GetTableDef().columns());
    int column_size = sql_table->GetTableDef().columns_size();
    auto sql_schema = sql_table->GetTableDef().columns();
    uint64_t ts = ::baidu::common::timer::get_micros() / 1000;
    for (auto row : rows) {
        std::map<uint32_t, rtidb::codec::Dimension> dimensions;
        std::vector<uint64_t> ts_dimensions;
        std::vector<std::string> raw_data;
        row_view.Reset(row.buf());
        for (int i = 0; i < column_size; i++) {
            std::string key_str = sql_schema.Get(i).type() == fesql::type::kDate
                                      ? std::to_string(row_view.GetDateUnsafe(i))
                                      : row_view.GetAsString(i);
            if (key_str == "") {
                key_str = rtidb::codec::EMPTY_STRING;
            }
            raw_data.push_back(key_str);
        }
        ASSERT_EQ(0, sdk_codec.EncodeDimension(raw_data, 1, &dimensions));
        ASSERT_EQ(0, sdk_codec.EncodeTsDimension(raw_data, &ts_dimensions));

        rtidb::storage::Dimensions dims;
        rtidb::storage::TSDimensions ts_dims;

        auto iter = dimensions.find(0);
        for (auto dimension : iter->second) {
            auto dim = dims.Add();
            dim->set_key(dimension.first);
            dim->set_idx(dimension.second);
        }
        for (size_t i = 0; i < ts_dimensions.size(); i++) {
            auto ts_dim = ts_dims.Add();
            ts_dim->set_ts(ts_dimensions[i]);
            ts_dim->set_idx(i);
        }
        if (ts_dimensions.empty()) {
            ASSERT_TRUE(table->Put(ts, row.ToString(), dims));
            ts--;
        } else {
            ASSERT_TRUE(table->Put(dims, ts_dims, row.ToString()));
        }
    }
    LOG(INFO) << "store data done!";
}

std::shared_ptr<TestArgs> PrepareTableWithTableDef(const fesql::type::TableDef &table_def, const std::string &db_name,
                                                   const uint32_t tid) {
    std::shared_ptr<TestArgs> args = std::shared_ptr<TestArgs>(new TestArgs());
    args->meta.set_db(db_name);
    args->meta.set_name(table_def.name());
    args->meta.set_tid(tid);
    args->meta.set_pid(0);
    args->meta.set_seg_cnt(8);
    args->meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    args->meta.set_ttl_type(api::TTLType::kAbsoluteTime);
    args->meta.set_ttl(0);

    RtiDBIndex *index = args->meta.mutable_column_key();
    RtiDBSchema *schema = args->meta.mutable_column_desc();
    if (!SchemaAdapter::ConvertSchemaAndIndex(table_def.columns(), table_def.indexes(), schema, index)) {
        return std::shared_ptr<TestArgs>();
    }

    args->table = std::shared_ptr<::rtidb::storage::MemTable>(new ::rtidb::storage::MemTable(args->meta));
    args->table->Init();
    return args;
}

void TabletEngineTest::BatchModeCheck(fesql::sqlcase::SQLCase &sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();

    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());

    // Init catalog
    std::map<std::string, std::pair<std::shared_ptr<TestArgs>, std::shared_ptr<::fesql::storage::Table>>>
        name_table_map;
    for (int32_t i = 0; i < input_cnt; i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(AutoTableName(), i);
        }
        fesql::type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        LOG(INFO) << "input " << i << " index size " << table_def.indexes().size();
        std::shared_ptr<::fesql::storage::Table> sql_table(new ::fesql::storage::Table(i + 1, 1, table_def));

        auto args = PrepareTableWithTableDef(table_def, sql_case.db(), i + 1);
        if (!args) {
            FAIL() << "fail to prepare table";
        }
        ASSERT_TRUE(catalog->AddTable(args->meta, args->table));
        name_table_map.insert(std::make_pair(table_def.name(), std::make_pair(args, sql_table)));
    }

    // Init engine and run session
    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < input_cnt; ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        std::string tname = sql_case.inputs()[j].name_.empty() ? ("t" + std::to_string(j)) : sql_case.inputs()[j].name_;
        boost::replace_all(sql_str, placeholder, tname);
    }
    std::cout << sql_str << std::endl;

    fesql::base::Status get_status;
    fesql::vm::Engine engine(catalog);
    fesql::vm::BatchRunSession session;
    if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
        session.EnableDebug();
    }
    bool ok = engine.Get(sql_str, sql_case.db(), session, get_status);
    ASSERT_EQ(sql_case.expect().success_, ok);
    if (!sql_case.expect().success_) {
        return;
    }
    std::cout << "RUN IN MODE BATCH";
    fesql::vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    session.GetClusterJob().Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::vector<fesql::codec::Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        std::vector<fesql::codec::Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            name_table_map[input.name_].first->table->Init();
            name_table_map[input.name_].second->Init();
            StoreData(name_table_map[input.name_].first, name_table_map[input.name_].second, rows);
        } else {
            LOG(INFO) << "rows empty";
        }
    }

    // Check Output Schema
    std::vector<fesql::codec::Row> case_output_data;
    fesql::type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    std::vector<fesql::codec::Row> output;
    ASSERT_EQ(0, session.Run(output));
    CheckRows(schema, SortRows(schema, output, sql_case.expect().order_), case_output_data);
}

void TabletEngineTest::RequestModeCheck(fesql::sqlcase::SQLCase &sql_case,  // NOLINT
                                        fesql::vm::EngineOptions options) {
    int32_t input_cnt = sql_case.CountInputs();

    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    fesql::vm::Engine engine(catalog, options);

    catalog->SetLocalTablet(std::shared_ptr<fesql::vm::Tablet>(
        new fesql::vm::LocalTablet(&engine, std::shared_ptr<fesql::vm::CompileInfoCache>())));
    // Init catalog
    std::map<std::string, std::pair<std::shared_ptr<TestArgs>, std::shared_ptr<::fesql::storage::Table>>>
        name_table_map;
    for (int32_t i = 0; i < input_cnt; i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(AutoTableName(), i);
        }
        fesql::type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        LOG(INFO) << "input " << i << " index size " << table_def.indexes().size();
        std::shared_ptr<::fesql::storage::Table> sql_table(new ::fesql::storage::Table(i + 1, 1, table_def));

        auto args = PrepareTableWithTableDef(table_def, sql_case.db(), i + 1);
        if (!args) {
            FAIL() << "fail to prepare table";
        }
        catalog->AddTable(args->meta, args->table);
        name_table_map.insert(std::make_pair(table_def.name(), std::make_pair(args, sql_table)));
    }

    // Init engine and run session
    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < input_cnt; ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        std::string tname = sql_case.inputs()[j].name_.empty() ? ("t" + std::to_string(j)) : sql_case.inputs()[j].name_;
        boost::replace_all(sql_str, placeholder, tname);
    }
    std::cout << sql_str << std::endl;
    fesql::base::Status get_status;
    fesql::vm::RequestRunSession session;
    if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
        session.EnableDebug();
    }
    bool ok = engine.Get(sql_str, sql_case.db(), session, get_status);
    ASSERT_EQ(sql_case.expect().success_, ok);
    if (!sql_case.expect().success_) {
        return;
    }
    std::cout << "RUN IN MODE BATCH";
    fesql::vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    session.GetClusterJob().Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::vector<fesql::codec::Row> request_data;
    const std::string &request_name = session.GetRequestName();
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        if (input.name_ == request_name) {
            ASSERT_TRUE(sql_case.ExtractInputData(request_data, i));
            continue;
        }
        std::vector<fesql::codec::Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            name_table_map[input.name_].first->table->Init();
            name_table_map[input.name_].second->Init();
            StoreData(name_table_map[input.name_].first, name_table_map[input.name_].second, rows);
        } else {
            LOG(INFO) << "rows empty";
        }
    }

    // Check Output Schema
    std::vector<fesql::codec::Row> case_output_data;
    fesql::type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    DLOG(INFO) << "RUN IN MODE REQUEST";
    std::vector<fesql::codec::Row> output;

    auto request_table = name_table_map[request_name].first;
    auto request_sql_table = name_table_map[request_name].second;
    ASSERT_TRUE(request_table->table->Init());
    ASSERT_TRUE(request_sql_table->Init());
    for (auto in_row : request_data) {
        fesql::codec::Row out_row;
        int ret = session.Run(in_row, &out_row);
        ASSERT_EQ(0, ret);
        LOG(INFO) << "store request row into db"
                  << ", index size: " << request_sql_table->GetIndexMap().size();
        StoreData(request_table, request_sql_table, std::vector<fesql::codec::Row>{in_row});
        output.push_back(out_row);
    }
    CheckRows(schema, SortRows(schema, output, sql_case.expect().order_), case_output_data);
}

TEST_P(TabletEngineTest, batch_query_test) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-request-unsupport") &&
        !boost::contains(sql_case.mode(), "batch-unsupport")) {
        TabletEngineTest::BatchModeCheck(sql_case);
    }
}
TEST_P(TabletEngineTest, request_query_test) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-request-unsupport") &&
        !boost::contains(sql_case.mode(), "request-unsupport")) {
        TabletEngineTest::RequestModeCheck(sql_case);
    }
}
TEST_P(TabletEngineTest, cluster_request_query_test) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    fesql::vm::EngineOptions options;
    options.set_cluster_optimized(true);
    if (!boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-request-unsupport") &&
        !boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        TabletEngineTest::RequestModeCheck(sql_case, options);
    }
}
INSTANTIATE_TEST_SUITE_P(EngineConstQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/const_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineSimpleQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/simple_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineUdfQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/udf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineUdafQuery, TabletEngineTest,
                         testing::ValuesIn(rtidb::test::SQLCaseTest::InitCases("/cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineExtreamQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/extream_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineLastJoinQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/last_join_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineLastJoinWindowQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineWindowQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/window_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineWindowWithUnionQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/window_with_union_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineBatchGroupQuery, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/query/group_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestWindowRow, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestWindowRowRange, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_window_row_range.yaml")));

INSTANTIATE_TEST_SUITE_P(
    EngineTestWindowUnion, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_window_union.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestLastJoin, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_last_join.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestExpression, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_expression.yaml")));

INSTANTIATE_TEST_SUITE_P(
    EngineTestSelectSample, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_select_sample.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestSubSelect, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_sub_select.yaml")));

INSTANTIATE_TEST_SUITE_P(
    EngineTestUdafFunction, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_udaf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestUdfFunction, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_udf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestWhere, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_where.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestFZFunction, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_feature_zero_function.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestIndexOptimized, TabletEngineTest,
    testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/v1/test_index_optimized.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestErrorWindow, TabletEngineTest,
                         testing::ValuesIn(TabletEngineTest::InitCases("/cases/integration/error/error_window.yaml")));
INSTANTIATE_TEST_CASE_P(EngineTestDebugIssues, TabletEngineTest,
                        testing::ValuesIn(TabletEngineTest::InitCases("/cases/debug/issues_case.yaml")));

}  // namespace catalog
}  // namespace rtidb
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
