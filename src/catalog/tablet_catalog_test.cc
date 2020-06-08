/*
 * tablet_catalog_test.cc
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
#include "catalog/tablet_catalog.h"
#include "base/fe_status.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "gtest/gtest.h"
#include "proto/fe_common.pb.h"
#include "storage/mem_table.h"
#include "storage/table.h"
#include "vm/engine.h"

namespace rtidb {
namespace catalog {

class TabletCatalogTest : public ::testing::Test {};

struct TestArgs {
    std::shared_ptr<::rtidb::storage::Table> table;
    ::rtidb::api::TableMeta meta;
    std::string row;
    std::string idx_name;
    std::string pk;
    uint64_t ts;
};

TestArgs* PrepareTable(const std::string& tname) {
    TestArgs* args = new TestArgs();
    args->meta.set_name(tname);
    args->meta.set_tid(1);
    args->meta.set_pid(0);
    args->meta.set_seg_cnt(8);
    args->meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    RtiDBSchema* schema = args->meta.mutable_column_desc();
    auto col1 = schema->Add();
    col1->set_name("col1");
    col1->set_data_type(::rtidb::type::kVarchar);
    auto col2 = schema->Add();
    col2->set_name("col2");
    col2->set_data_type(::rtidb::type::kBigInt);

    RtiDBIndex* index = args->meta.mutable_column_key();
    auto key1 = index->Add();
    key1->set_index_name("index0");
    key1->add_col_name("col1");
    key1->add_ts_name("col2");
    args->idx_name = "index0";

    ::rtidb::storage::MemTable* table =
        new ::rtidb::storage::MemTable(args->meta);
    table->Init();
    ::fesql::vm::Schema fe_schema;
    SchemaAdapter::ConvertSchema(args->meta.column_desc(), &fe_schema);
    ::fesql::codec::RowBuilder rb(fe_schema);
    std::string pk = "pk1";
    args->pk = pk;
    uint32_t size = rb.CalTotalLength(pk.size());
    std::string value;
    value.resize(size);
    rb.SetBuffer(reinterpret_cast<int8_t*>(&(value[0])), size);
    rb.AppendString(pk.c_str(), pk.size());
    rb.AppendInt64(1589780888000l);
    table->Put(pk, 1589780888000l, value.c_str(), value.size());
    args->ts = 1589780888000l;
    std::shared_ptr<::rtidb::storage::MemTable> mtable(table);
    args->table = mtable;
    args->row = value;
    return args;
}

TEST_F(TabletCatalogTest, tablet_smoke_test) {
    TestArgs* args = PrepareTable("t1");
    TabletTableHandler handler(args->meta, "db1", args->table);
    ASSERT_TRUE(handler.Init());
    auto it = handler.GetIterator();
    if (!it) {
        ASSERT_TRUE(false);
    }
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    auto row = it->GetValue();
    ASSERT_EQ(row.ToString(), args->row);
    auto wit = handler.GetWindowIterator(args->idx_name + std::to_string(0));
    if (!wit) {
        ASSERT_TRUE(false);
    }
    wit->SeekToFirst();
    ASSERT_TRUE(wit->Valid());
    auto key = wit->GetKey();
    ASSERT_EQ(key.ToString(), args->pk);
    auto second_it = wit->GetValue();
    second_it->SeekToFirst();
    ASSERT_TRUE(second_it->Valid());
    ASSERT_EQ(args->ts, second_it->GetKey());
    ASSERT_EQ(args->row, second_it->GetValue().ToString());
    delete args;
}

TEST_F(TabletCatalogTest, sql_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs* args = PrepareTable("t1");
    std::shared_ptr<TabletTableHandler> handler(
        new TabletTableHandler(args->meta, "db1", args->table));
    ASSERT_TRUE(handler->Init());
    ASSERT_TRUE(catalog->AddTable(handler));
    ::fesql::vm::Engine engine(catalog);
    std::string sql = "select col1, col2 + 1 from t1;";
    ::fesql::vm::BatchRunSession session;
    session.EnableDebug();
    ::fesql::base::Status status;
    engine.Get(sql, "db1", session, status);
    if (status.code != ::fesql::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::fesql::common::kOk, status.code);
    std::vector<int8_t*> output;
    std::shared_ptr<::fesql::vm::TableHandler> result = session.Run();
    if (!result) {
        ASSERT_TRUE(false);
    }
    ::fesql::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(2, session.GetSchema().size());
    auto it = result->GetIterator();
    ASSERT_TRUE(it->Valid());
    const ::fesql::codec::Row& row = it->GetValue();
    rv.Reset(row.buf(), row.size());
    int64_t val = 0;
    ASSERT_EQ(0, rv.GetInt64(1, &val));
    int64_t exp = args->ts + 1;
    ASSERT_EQ(val, exp);
    char* data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
}

TEST_F(TabletCatalogTest, sql_last_join_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs* args = PrepareTable("t1");
    {
        std::shared_ptr<TabletTableHandler> handler(
            new TabletTableHandler(args->meta, "db1", args->table));
        ASSERT_TRUE(handler->Init());
        ASSERT_TRUE(catalog->AddTable(handler));
    }
    {
        TestArgs* args1 = PrepareTable("t2");
        std::shared_ptr<TabletTableHandler> handler(
            new TabletTableHandler(args1->meta, "db1", args1->table));
        ASSERT_TRUE(handler->Init());
        ASSERT_TRUE(catalog->AddTable(handler));
    }
    ::fesql::vm::Engine engine(catalog);
    std::string sql =
        "select t1.col1 as c1, t1.col2 as c2 , t2.col1 as c3, t2.col2 as c4 "
        "from t1 last join t2 order by t2.col2 "
        "on t1.col1 = t2.col1 and t1.col2 >= t2.col2;";
     ::fesql::vm::ExplainOutput explain;
    ::fesql::base::Status status;
    engine.Explain(sql, "db1", true, &explain, &status);
    std::cout << "logical_plan \n" << explain.logical_plan << std::endl;
    std::cout << "physical \n" << explain.physical_plan << std::endl;

    ::fesql::vm::BatchRunSession session;
    session.EnableDebug();
    engine.Get(sql, "db1", session, status);
    if (status.code != ::fesql::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::fesql::common::kOk, status.code);
    std::vector<int8_t*> output;
    std::shared_ptr<::fesql::vm::TableHandler> result = session.Run();
    if (!result) {
        ASSERT_TRUE(false);
    }
    ::fesql::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(4, session.GetSchema().size());
    auto it = result->GetIterator();
    ASSERT_TRUE(it->Valid());
    const ::fesql::codec::Row& row = it->GetValue();
    rv.Reset(row.buf(), row.size());
    char* data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
}

TEST_F(TabletCatalogTest, sql_last_join_smoke_test2) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs* args = PrepareTable("t1");
    {
        std::shared_ptr<TabletTableHandler> handler(
            new TabletTableHandler(args->meta, "db1", args->table));
        ASSERT_TRUE(handler->Init());
        ASSERT_TRUE(catalog->AddTable(handler));
    }
    {
        TestArgs* args1 = PrepareTable("t2");
        std::shared_ptr<TabletTableHandler> handler(
            new TabletTableHandler(args1->meta, "db1", args1->table));
        ASSERT_TRUE(handler->Init());
        ASSERT_TRUE(catalog->AddTable(handler));
    }

    ::fesql::vm::Engine engine(catalog);
    std::string sql =
        "select t1.col1 as c1, t1.col2 as c2 , t2.col1 as c3, t2.col2 as c4 "
        "from t1 last join t2 order by t2.col2 desc"
        " on t1.col1 = t2.col1 and t1.col2 = t2.col2;";
    ::fesql::vm::ExplainOutput explain;
    ::fesql::base::Status status;
    engine.Explain(sql, "db1", true, &explain, &status);
    std::cout << "logical_plan \n" << explain.logical_plan << std::endl;
    std::cout << "physical \n" << explain.physical_plan << std::endl;
    ::fesql::vm::BatchRunSession session;
    session.EnableDebug();
    engine.Get(sql, "db1", session, status);
    if (status.code != ::fesql::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::fesql::common::kOk, status.code);
    std::vector<int8_t*> output;
    std::shared_ptr<::fesql::vm::TableHandler> result = session.Run();
    if (!result) {
        ASSERT_TRUE(false);
    }
    ::fesql::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(4, session.GetSchema().size());
    auto it = result->GetIterator();
    ASSERT_TRUE(it->Valid());
    const ::fesql::codec::Row& row = it->GetValue();
    rv.Reset(row.buf(), row.size());
    char* data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
}

TEST_F(TabletCatalogTest, sql_window_smoke_500_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs* args = PrepareTable("t1");

    std::shared_ptr<TabletTableHandler> handler(
        new TabletTableHandler(args->meta, "db1", args->table));
    ASSERT_TRUE(handler->Init());
    ASSERT_TRUE(catalog->AddTable(handler));
    ::fesql::vm::Engine engine(catalog);
    std::stringstream ss;
    ss << "select ";
    for (uint32_t i = 0; i < 100; i++) {
        if (i > 0) ss << ",";
        ss << "col1 as col1" << i << ", col2 as col2" << i;
    }
    ss << " from t1 limit 1;";
    std::string sql = ss.str();
    ::fesql::vm::BatchRunSession session;
    session.EnableDebug();
    ::fesql::base::Status status;
    engine.Get(sql, "db1", session, status);
    if (status.code != ::fesql::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::fesql::common::kOk, status.code);
    std::vector<int8_t*> output;
    std::shared_ptr<::fesql::vm::TableHandler> result = session.Run();
    if (!result) {
        ASSERT_TRUE(false);
    }
    ::fesql::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(200, session.GetSchema().size());
}

TEST_F(TabletCatalogTest, sql_window_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs* args = PrepareTable("t1");

    std::shared_ptr<TabletTableHandler> handler(
        new TabletTableHandler(args->meta, "db1", args->table));
    ASSERT_TRUE(handler->Init());
    ASSERT_TRUE(catalog->AddTable(handler));
    ::fesql::vm::Engine engine(catalog);
    std::string sql =
        "select sum(col2) over w1, t1.col1, t1.col2 from t1 window w1 "
        "as(partition by t1.col1 order by t1.col2 ROWS BETWEEN 3 PRECEDING AND "
        "CURRENT ROW);";
    ::fesql::vm::BatchRunSession session;
    session.EnableDebug();
    ::fesql::base::Status status;
    engine.Get(sql, "db1", session, status);
    if (status.code != ::fesql::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::fesql::common::kOk, status.code);
    std::vector<int8_t*> output;
    std::shared_ptr<::fesql::vm::TableHandler> result = session.Run();
    if (!result) {
        ASSERT_TRUE(false);
    }
    ::fesql::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(3, session.GetSchema().size());
    auto it = result->GetIterator();
    ASSERT_TRUE(it->Valid());
    const ::fesql::codec::Row& row = it->GetValue();
    rv.Reset(row.buf(), row.size());
    int64_t val = 0;
    ASSERT_EQ(0, rv.GetInt64(0, &val));
    int64_t exp = args->ts;
    ASSERT_EQ(val, exp);
    char* data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(1, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
    ASSERT_EQ(0, rv.GetInt64(2, &val));
    ASSERT_EQ(val, exp);
}

}  // namespace catalog
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
