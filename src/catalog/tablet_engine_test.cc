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

#include <vector>

#include "base/fe_status.h"
#include "base/texttable.h"
#include "case/sql_case.h"
#include "catalog/schema_adapter.h"
#include "catalog/tablet_catalog.h"
#include "codec/fe_row_codec.h"
#include "codec/sdk_codec.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "proto/fe_common.pb.h"
#include "storage/fe_table.h"
#include "storage/mem_table.h"
#include "storage/table.h"
#include "vm/engine.h"
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20
namespace rtidb {
namespace catalog {

class TabletEngineTest
    : public ::testing::TestWithParam<fesql::sqlcase::SQLCase> {
 public:
    TabletEngineTest() {}
    virtual ~TabletEngineTest() {}
};

struct TestArgs {
    std::shared_ptr<::rtidb::storage::Table> table;
    ::rtidb::api::TableMeta meta;
    std::string row;
    std::string idx_name;
    std::string pk;
    uint64_t ts;
};

std::string FindRtidbDirPath(const std::string dirname) {
    boost::filesystem::path current_path(boost::filesystem::current_path());
    boost::filesystem::path fesql_path;

    if (current_path.filename() == dirname) {
        return current_path.string();
    }
    while (current_path.has_parent_path()) {
        current_path = current_path.parent_path();
        if (current_path.filename().string() == dirname) {
            break;
        }
    }
    if (current_path.filename().string() == dirname) {
        LOG(INFO) << "Dir Path is : " << current_path.string() << std::endl;
        return current_path.string();
    }
    return std::string();
}
std::vector<fesql::sqlcase::SQLCase> InitCases(std::string yaml_path);
std::vector<fesql::sqlcase::SQLCase> InitJavaSDKCases(std::string yaml_path);
void CheckSchema(const fesql::vm::Schema &schema,
                 const fesql::vm::Schema &exp_schema);
void CheckRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows,
               const std::vector<fesql::codec::Row> &exp_rows);
void PrintRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows);
void PrintSchema(const fesql::vm::Schema &schema);
void StoreData(std::shared_ptr<TestArgs> args,
               std::shared_ptr<fesql::storage::Table> sql_table,
               const std::vector<fesql::codec::Row> &rows);

void InitCases(std::string yaml_path,
               std::vector<fesql::sqlcase::SQLCase> &cases);  // NOLINT
void InitCases(std::string dir_path, std::string yaml_path,
               std::vector<fesql::sqlcase::SQLCase> &cases) {  // NOLINT
    if (!fesql::sqlcase::SQLCase::CreateSQLCasesFromYaml(dir_path, yaml_path,
                                                         cases)) {
        FAIL();
    }
}
std::vector<fesql::sqlcase::SQLCase> InitCases(std::string yaml_path) {
    std::vector<fesql::sqlcase::SQLCase> cases;
    InitCases(FindRtidbDirPath("rtidb") + "/fesql/", yaml_path, cases);
    return cases;
}

void CheckSchema(const fesql::vm::Schema &schema,
                 const fesql::vm::Schema &exp_schema) {
    LOG(INFO) << "expect schema:\n";
    PrintSchema(exp_schema);
    LOG(INFO) << "real schema:\n";
    PrintSchema(schema);
    ASSERT_EQ(schema.size(), exp_schema.size());
    for (int i = 0; i < schema.size(); i++) {
        ASSERT_EQ(schema.Get(i).name(), exp_schema.Get(i).name());
        ASSERT_EQ(schema.Get(i).type(), exp_schema.Get(i).type());
    }
}

void PrintSchema(const fesql::vm::Schema &schema) {
    std::ostringstream oss;
    fesql::codec::RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add ColumnName
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    // Add ColumnType
    t.endOfRow();
    for (int i = 0; i < schema.size(); i++) {
        t.add(fesql::sqlcase::SQLCase::TypeString(schema.Get(i).type()));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void PrintRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows) {
    std::ostringstream oss;
    fesql::codec::RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
    if (rows.empty()) {
        t.add("Empty set");
        t.endOfRow();
        return;
    }

    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            t.add(str);
            if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
                t.add("...");
                break;
            }
        }
        t.endOfRow();
        if (t.rows().size() >= MAX_DEBUG_LINES_CNT) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

const std::vector<fesql::codec::Row> SortRows(
    const fesql::vm::Schema &schema, const std::vector<fesql::codec::Row> &rows,
    const std::string &order_col) {
    DLOG(INFO) << "sort rows start";
    fesql::codec::RowView row_view(schema);
    int idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            idx = i;
            break;
        }
    }
    if (-1 == idx) {
        return rows;
    }

    if (schema.Get(idx).type() == fesql::type::kVarchar) {
        std::vector<std::pair<std::string, fesql::codec::Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(row_view.GetAsString(idx), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<std::string, fesql::codec::Row> &a,
                     std::pair<std::string, fesql::codec::Row> &b) {
                      return a.first < b.first;
                  });
        std::vector<fesql::codec::Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    } else {
        std::vector<std::pair<int64_t, fesql::codec::Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(
                boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<int64_t, fesql::codec::Row> &a,
                     std::pair<int64_t, fesql::codec::Row> &b) {
                      return a.first < b.first;
                  });
        std::vector<fesql::codec::Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    }
}
void CheckRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows,
               const std::vector<fesql::codec::Row> &exp_rows) {
    LOG(INFO) << "expect result:\n";
    PrintRows(schema, exp_rows);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, rows);
    ASSERT_EQ(rows.size(), exp_rows.size());
    fesql::codec::RowView row_view(schema);
    fesql::codec::RowView row_view_exp(schema);
    for (size_t row_index = 0; row_index < rows.size(); row_index++) {
        row_view.Reset(rows[row_index].buf());
        row_view_exp.Reset(exp_rows[row_index].buf());
        for (int i = 0; i < schema.size(); i++) {
            if (row_view_exp.IsNULL(i)) {
                ASSERT_TRUE(row_view.IsNULL(i)) << " At " << i;
                continue;
            }
            switch (schema.Get(i).type()) {
                case fesql::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i),
                              row_view_exp.GetInt32Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i),
                              row_view_exp.GetInt64Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i),
                              row_view_exp.GetInt16Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kFloat: {
                    ASSERT_FLOAT_EQ(row_view.GetFloatUnsafe(i),
                                    row_view_exp.GetFloatUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kDouble: {
                    ASSERT_DOUBLE_EQ(row_view.GetDoubleUnsafe(i),
                                     row_view_exp.GetDoubleUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i),
                              row_view_exp.GetStringUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i),
                              row_view_exp.GetDateUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i),
                              row_view_exp.GetTimestampUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i),
                              row_view_exp.GetBoolUnsafe(i))
                        << " At " << i;
                    break;
                }
                default: {
                    FAIL() << "Invalid Column Type";
                    break;
                }
            }
        }
    }
}
void StoreData(std::shared_ptr<TestArgs> args,
               std::shared_ptr<fesql::storage::Table> sql_table,
               const fesql::codec::Row &row) {}
void StoreData(std::shared_ptr<TestArgs> args,
               std::shared_ptr<fesql::storage::Table> sql_table,
               const std::vector<fesql::codec::Row> &rows) {
    auto meta = args->meta;
    auto table = args->table;
    rtidb::codec::SDKCodec sdk_codec(meta);
    LOG(INFO) << "store data start: rows size " << rows.size()
              << ", index size: " << sql_table->GetIndexMap().size();
    fesql::codec::RowView row_view(sql_table->GetTableDef().columns());
    int column_size = sql_table->GetTableDef().columns_size();
    auto sql_schema = sql_table->GetTableDef().columns();
    for (auto row : rows) {
        std::map<uint32_t, rtidb::codec::Dimension> dimensions;
        std::vector<uint64_t> ts_dimensions;
        std::vector<std::string> raw_data;
        row_view.Reset(row.buf());
        for (int i = 0; i < column_size; i++) {
            std::string key_str =
                sql_schema.Get(i).type() == fesql::type::kDate
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
        bool ok = table->Put(dims, ts_dims, row.ToString());
        ASSERT_TRUE(ok);
    }
    LOG(INFO) << "store data done!";
}
std::shared_ptr<TestArgs> PrepareTableWithTableDef(
    const fesql::type::TableDef &table_def, const uint32_t tid) {
    std::shared_ptr<TestArgs> args = std::shared_ptr<TestArgs>(new TestArgs());
    args->meta.set_name(table_def.name());
    args->meta.set_tid(tid);
    args->meta.set_pid(0);
    args->meta.set_seg_cnt(8);
    args->meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    args->meta.set_ttl_type(api::TTLType::kAbsoluteTime);
    args->meta.set_ttl(0);

    RtiDBIndex *index = args->meta.mutable_column_key();
    RtiDBSchema *schema = args->meta.mutable_column_desc();
    if (!SchemaAdapter::ConvertSchemaAndIndex(
            table_def.columns(), table_def.indexes(), schema, index)) {
        return std::shared_ptr<TestArgs>();
    }

    args->table = std::shared_ptr<::rtidb::storage::MemTable>(
        new ::rtidb::storage::MemTable(args->meta));
    args->table->Init();
    return args;
}

const std::string GenerateTableName(int32_t id) {
    return "auto_t" + std::to_string(id);
}

void BatchModeCheck(fesql::sqlcase::SQLCase &sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();

    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());

    // Init catalog
    std::map<std::string, std::pair<std::shared_ptr<TestArgs>,
                                    std::shared_ptr<::fesql::storage::Table>>>
        name_table_map;
    for (int32_t i = 0; i < input_cnt; i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(GenerateTableName(i), i);
        }
        fesql::type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        LOG(INFO) << "input " << i << " index size "
                  << table_def.indexes().size();
        std::shared_ptr<::fesql::storage::Table> sql_table(
            new ::fesql::storage::Table(i + 1, 1, table_def));

        auto args = PrepareTableWithTableDef(table_def, i + 1);
        if (!args) {
            FAIL() << "fail to prepare table";
        }
        std::shared_ptr<TabletTableHandler> handler(
            new TabletTableHandler(args->meta, sql_case.db(), args->table));
        ASSERT_TRUE(handler->Init());
        ASSERT_TRUE(catalog->AddTable(handler));
        name_table_map.insert(
            std::make_pair(table_def.name(), std::make_pair(args, sql_table)));
    }

    // Init engine and run session
    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < input_cnt; ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        std::string tname = sql_case.inputs()[j].name_.empty()
                                ? ("t" + std::to_string(j))
                                : sql_case.inputs()[j].name_;
        boost::replace_all(sql_str, placeholder, tname);
    }
    std::cout << sql_str << std::endl;

    fesql::base::Status get_status;
    fesql::vm::Engine engine(catalog);
    fesql::vm::BatchRunSession session;
    session.EnableDebug();
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

    if (!sql_case.batch_plan().empty()) {
        ASSERT_EQ(oss.str(), sql_case.batch_plan());
    }

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::vector<fesql::codec::Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        std::vector<fesql::codec::Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            name_table_map[input.name_].first->table->Init();
            name_table_map[input.name_].second->Init();
            StoreData(name_table_map[input.name_].first,
                      name_table_map[input.name_].second, rows);
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
    CheckRows(schema, SortRows(schema, output, sql_case.expect().order_),
              case_output_data);
}

void RequestModeCheck(fesql::sqlcase::SQLCase &sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();

    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());

    // Init catalog
    std::map<std::string, std::pair<std::shared_ptr<TestArgs>,
                                    std::shared_ptr<::fesql::storage::Table>>>
        name_table_map;
    for (int32_t i = 0; i < input_cnt; i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(GenerateTableName(i), i);
        }
        fesql::type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        LOG(INFO) << "input " << i << " index size "
                  << table_def.indexes().size();
        std::shared_ptr<::fesql::storage::Table> sql_table(
            new ::fesql::storage::Table(i + 1, 1, table_def));

        auto args = PrepareTableWithTableDef(table_def, i + 1);
        if (!args) {
            FAIL() << "fail to prepare table";
        }
        std::shared_ptr<TabletTableHandler> handler(
            new TabletTableHandler(args->meta, sql_case.db(), args->table));
        ASSERT_TRUE(handler->Init());
        ASSERT_TRUE(catalog->AddTable(handler));
        name_table_map.insert(
            std::make_pair(table_def.name(), std::make_pair(args, sql_table)));
    }

    // Init engine and run session
    std::string sql_str = sql_case.sql_str();
    for (int j = 0; j < input_cnt; ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        std::string tname = sql_case.inputs()[j].name_.empty()
                                ? ("t" + std::to_string(j))
                                : sql_case.inputs()[j].name_;
        boost::replace_all(sql_str, placeholder, tname);
    }
    std::cout << sql_str << std::endl;
    fesql::base::Status get_status;
    fesql::vm::Engine engine(catalog);
    fesql::vm::RequestRunSession session;
    session.EnableDebug();
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

    if (!sql_case.request_plan().empty()) {
        ASSERT_EQ(oss.str(), sql_case.request_plan());
    }

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
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
            StoreData(name_table_map[input.name_].first,
                      name_table_map[input.name_].second, rows);
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
                  << ", index size: "
                  << request_sql_table->GetIndexMap().size();
        StoreData(request_table, request_sql_table,
                  std::vector<fesql::codec::Row>{in_row});
        output.push_back(out_row);
    }
    CheckRows(schema, SortRows(schema, output, sql_case.expect().order_),
              case_output_data);
}

INSTANTIATE_TEST_CASE_P(
    EngineSimpleQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineUdfQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/udf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineUdafQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineExtreamQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/extream_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineLastJoinQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineLastJoinWindowQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestLastJoinWindowQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineWindowQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineWindowWithUnionQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_with_union_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchGroupQuery, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/query/group_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineTestWindowRow, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/integration/v1/test_window_row.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestWindowRowRange, TabletEngineTest,
    testing::ValuesIn(
        InitCases("/cases/integration/v1/test_window_row_range.yaml")));

INSTANTIATE_TEST_CASE_P(EngineTestWindowUnion, TabletEngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/test_window_union.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestLastJoin, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/integration/v1/test_last_join.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestExpression, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/integration/v1/test_expression.yaml")));

INSTANTIATE_TEST_CASE_P(EngineTestSelectSample, TabletEngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/test_select_sample.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineTestSubSelect, TabletEngineTest,
    testing::ValuesIn(InitCases("/cases/integration/v1/test_sub_select.yaml")));

INSTANTIATE_TEST_CASE_P(EngineTestUdafFunction, TabletEngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/integration/v1/test_udaf_function.yaml")));

TEST_P(TabletEngineTest, batch_query_test) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "batch-unsupport")) {
        BatchModeCheck(sql_case);
    }
}
TEST_P(TabletEngineTest, request_query_test) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "request-unsupport")) {
        RequestModeCheck(sql_case);
    }
}

}  // namespace catalog
}  // namespace rtidb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
