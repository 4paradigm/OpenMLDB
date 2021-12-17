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

#include "catalog/tablet_catalog.h"

#include <vector>

#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "codec/schema_codec.h"
#include "gtest/gtest.h"
#include "proto/fe_common.pb.h"
#include "schema/schema_adapter.h"
#include "storage/mem_table.h"
#include "storage/table.h"
#include "vm/engine.h"

namespace openmldb {
namespace catalog {

using ::openmldb::codec::SchemaCodec;

class TabletCatalogTest : public ::testing::Test {};

struct TestArgs {
    std::vector<std::shared_ptr<::openmldb::storage::Table>> tables;
    std::vector<::openmldb::api::TableMeta> meta;
    std::string row;
    std::string idx_name;
    std::string pk;
    uint64_t ts;
};

TestArgs *PrepareTable(const std::string &tname) {
    TestArgs *args = new TestArgs();
    ::openmldb::api::TableMeta meta;
    meta.set_name(tname);
    meta.set_db("db1");
    meta.set_tid(1);
    meta.set_pid(0);
    meta.set_seg_cnt(8);
    meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "col1", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "col2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(meta.add_column_key(), "index0", "col1", "col2", ::openmldb::type::kAbsoluteTime, 0, 0);
    args->idx_name = "index0";

    ::openmldb::storage::MemTable *table = new ::openmldb::storage::MemTable(meta);
    table->Init();
    ::hybridse::vm::Schema fe_schema;
    schema::SchemaAdapter::ConvertSchema(meta.column_desc(), &fe_schema);
    ::hybridse::codec::RowBuilder rb(fe_schema);
    std::string pk = "pk1";
    args->pk = pk;
    uint32_t size = rb.CalTotalLength(pk.size());
    std::string value;
    value.resize(size);
    rb.SetBuffer(reinterpret_cast<int8_t *>(&(value[0])), size);
    rb.AppendString(pk.c_str(), pk.size());
    rb.AppendInt64(1589780888000l);
    table->Put(pk, 1589780888000l, value.c_str(), value.size());
    args->ts = 1589780888000l;
    std::shared_ptr<::openmldb::storage::MemTable> mtable(table);
    args->tables.push_back(mtable);
    args->meta.push_back(meta);
    args->row = value;
    return args;
}

TestArgs *PrepareMultiPartitionTable(const std::string &tname, int partition_num) {
    TestArgs *args = new TestArgs();
    ::openmldb::api::TableMeta meta;
    meta.set_name(tname);
    meta.set_db("db1");
    meta.set_tid(1);
    meta.set_pid(0);
    meta.set_seg_cnt(8);
    meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "col1", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "col2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(meta.add_column_key(), "index0", "col1", "col2", ::openmldb::type::kAbsoluteTime, 0, 0);
    args->idx_name = "index0";
    for (int i = 0; i < partition_num; i++) {
        meta.add_table_partition();
    }

    for (int i = 0; i < partition_num; i++) {
        ::openmldb::api::TableMeta cur_meta(meta);
        cur_meta.set_pid(i);
        auto table = std::make_shared<::openmldb::storage::MemTable>(cur_meta);
        table->Init();
        args->tables.push_back(table);
        args->meta.push_back(cur_meta);
    }
    ::hybridse::vm::Schema fe_schema;
    schema::SchemaAdapter::ConvertSchema(meta.column_desc(), &fe_schema);
    ::hybridse::codec::RowBuilder rb(fe_schema);
    uint32_t base = 100;
    for (int i = 0; i < 100; i++) {
        std::string pk = "pk" + std::to_string(base + i);
        uint32_t size = rb.CalTotalLength(pk.size());
        uint32_t pid = 0;
        if (partition_num > 0) {
            pid = (uint32_t)(::openmldb::base::hash64(pk) % partition_num);
        }
        uint64_t ts = 1589780888000l;
        for (int j = 0; j < 5; j++) {
            std::string value;
            value.resize(size);
            rb.SetBuffer(reinterpret_cast<int8_t *>(&(value[0])), size);
            rb.AppendString(pk.c_str(), pk.size());
            rb.AppendInt64(ts + j);
            args->tables[pid]->Put(pk, ts + j, value.c_str(), value.size());
        }
    }
    return args;
}

TEST_F(TabletCatalogTest, tablet_smoke_test) {
    TestArgs *args = PrepareTable("t1");
    TabletTableHandler handler(args->meta[0], std::shared_ptr<hybridse::vm::Tablet>());
    ClientManager client_manager;
    ASSERT_TRUE(handler.Init(client_manager));
    handler.AddTable(args->tables[0]);
    auto it = handler.GetIterator();
    if (!it) {
        ASSERT_TRUE(false);
    }
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    auto row = it->GetValue();
    ASSERT_EQ(row.ToString(), args->row);
    auto wit = handler.GetWindowIterator(args->idx_name);
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

TEST_F(TabletCatalogTest, segment_handler_test) {
    TestArgs *args = PrepareTable("t1");
    auto handler = std::shared_ptr<TabletTableHandler>(
        new TabletTableHandler(args->meta[0], std::shared_ptr<hybridse::vm::Tablet>()));
    ClientManager client_manager;
    ASSERT_TRUE(handler->Init(client_manager));
    handler->AddTable(args->tables[0]);
    // Seek key not exist
    {
        auto partition = handler->GetPartition(args->idx_name);
        auto segment = partition->GetSegment(args->pk);
        auto iter = segment->GetIterator();
        if (!iter) {
            FAIL();
        }
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
    }
    delete args;
}

TEST_F(TabletCatalogTest, segment_handler_pk_not_exist_test) {
    TestArgs *args = PrepareTable("t1");
    auto handler = std::shared_ptr<TabletTableHandler>(
        new TabletTableHandler(args->meta[0], std::shared_ptr<hybridse::vm::Tablet>()));
    ClientManager client_manager;
    ASSERT_TRUE(handler->Init(client_manager));
    handler->AddTable(args->tables[0]);
    // Seek key not exist
    {
        auto partition = handler->GetPartition(args->idx_name);
        auto segment = partition->GetSegment("KEY_NOT_EXIST");
        auto iter = segment->GetIterator();
        if (iter) {
            FAIL();
        }
    }
    delete args;
}
TEST_F(TabletCatalogTest, sql_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs *args = PrepareTable("t1");
    ASSERT_TRUE(catalog->AddTable(args->meta[0], args->tables[0]));
    ::hybridse::vm::Engine engine(catalog);
    std::string sql = "select col1, col2 + 1 from t1;";
    ::hybridse::vm::BatchRunSession session;
    session.EnableDebug();
    ::hybridse::base::Status status;
    engine.Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::hybridse::common::kOk, status.code);
    std::vector<hybridse::codec::Row> outputs;
    if (0 != session.Run(outputs)) {
        ASSERT_TRUE(false);
    }
    ::hybridse::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(2, session.GetSchema().size());
    ASSERT_EQ(1u, outputs.size());
    const ::hybridse::codec::Row &row = outputs[0];
    rv.Reset(row.buf(), row.size());
    int64_t val = 0;
    ASSERT_EQ(0, rv.GetInt64(1, &val));
    int64_t exp = args->ts + 1;
    ASSERT_EQ(val, exp);
    const char *data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
}

TEST_F(TabletCatalogTest, sql_last_join_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs *args = PrepareTable("t1");
    ASSERT_TRUE(catalog->AddTable(args->meta[0], args->tables[0]));

    TestArgs *args1 = PrepareTable("t2");
    ASSERT_TRUE(catalog->AddTable(args1->meta[0], args1->tables[0]));

    ::hybridse::vm::Engine engine(catalog);
    std::string sql =
        "select t1.col1 as c1, t1.col2 as c2 , t2.col1 as c3, t2.col2 as c4 "
        "from t1 last join t2 order by t2.col2 "
        "on t1.col1 = t2.col1 and t1.col2 > t2.col2;";
    ::hybridse::vm::ExplainOutput explain;
    ::hybridse::base::Status status;
    engine.Explain(sql, "db1", ::hybridse::vm::kBatchMode, &explain, &status);
    std::cout << "logical_plan \n" << explain.logical_plan << std::endl;
    std::cout << "physical \n" << explain.physical_plan << std::endl;

    ::hybridse::vm::BatchRunSession session;
    session.EnableDebug();
    engine.Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::hybridse::common::kOk, status.code);
    std::vector<int8_t *> output;
    std::vector<hybridse::codec::Row> output_rows;
    ASSERT_EQ(0, session.Run(output_rows));
    ::hybridse::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(4, session.GetSchema().size());
    ASSERT_EQ(1u, output_rows.size());
    auto& row = output_rows[0];
    rv.Reset(row.buf(), row.size());
    ASSERT_EQ(args->pk, rv.GetStringUnsafe(0));
}

TEST_F(TabletCatalogTest, sql_last_join_smoke_test2) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs *args = PrepareTable("t1");
    ASSERT_TRUE(catalog->AddTable(args->meta[0], args->tables[0]));

    TestArgs *args1 = PrepareTable("t2");
    ASSERT_TRUE(catalog->AddTable(args1->meta[0], args1->tables[0]));

    ::hybridse::vm::Engine engine(catalog);
    std::string sql =
        "select t1.col1 as c1, t1.col2 as c2 , t2.col1 as c3, t2.col2 as c4 "
        "from t1 last join t2 order by t2.col2 desc"
        " on t1.col1 = t2.col1 and t1.col2 = t2.col2;";
    ::hybridse::vm::ExplainOutput explain;
    ::hybridse::base::Status status;
    engine.Explain(sql, "db1", ::hybridse::vm::kBatchMode, &explain, &status);
    std::cout << "logical_plan \n" << explain.logical_plan << std::endl;
    std::cout << "physical \n" << explain.physical_plan << std::endl;
    ::hybridse::vm::BatchRunSession session;
    session.EnableDebug();
    engine.Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::hybridse::common::kOk, status.code);
    std::vector<int8_t *> output;
    std::vector<hybridse::codec::Row> outputs;
    if (0 != session.Run(outputs)) {
        ASSERT_TRUE(false);
    }
    ::hybridse::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(4, session.GetSchema().size());
    ASSERT_EQ(1u, outputs.size());
    auto& row = outputs[0];
    rv.Reset(row.buf(), row.size());
    const char *data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
}

TEST_F(TabletCatalogTest, sql_window_smoke_500_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs *args = PrepareTable("t1");

    ASSERT_TRUE(catalog->AddTable(args->meta[0], args->tables[0]));
    ::hybridse::vm::Engine engine(catalog);
    std::stringstream ss;
    ss << "select ";
    for (uint32_t i = 0; i < 100; i++) {
        if (i > 0) ss << ",";
        ss << "col1 as col1" << i << ", col2 as col2" << i;
    }
    ss << " from t1 limit 1;";
    std::string sql = ss.str();
    ::hybridse::vm::BatchRunSession session;
    session.EnableDebug();
    ::hybridse::base::Status status;
    engine.Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::hybridse::common::kOk, status.code);
    std::vector<hybridse::codec::Row> outputs;
    if (0 != session.Run(outputs)) {
        ASSERT_TRUE(false);
    }
    ::hybridse::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(200, session.GetSchema().size());
}

TEST_F(TabletCatalogTest, sql_window_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs *args = PrepareTable("t1");

    ASSERT_TRUE(catalog->AddTable(args->meta[0], args->tables[0]));
    ::hybridse::vm::Engine engine(catalog);
    std::string sql =
        "select sum(col2) over w1, t1.col1, t1.col2 from t1 window w1 "
        "as(partition by t1.col1 order by t1.col2 ROWS BETWEEN 3 PRECEDING AND "
        "CURRENT ROW);";
    ::hybridse::vm::BatchRunSession session;
    session.EnableDebug();
    ::hybridse::base::Status status;
    engine.Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::hybridse::common::kOk, status.code);
    std::vector<hybridse::codec::Row> outputs;
    if (0 != session.Run(outputs)) {
        ASSERT_TRUE(false);
    }
    ::hybridse::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(3, session.GetSchema().size());
    ASSERT_EQ(1u, outputs.size());
    const ::hybridse::codec::Row &row = outputs[0];
    rv.Reset(row.buf(), row.size());
    int64_t val = 0;
    ASSERT_EQ(0, rv.GetInt64(0, &val));
    int64_t exp = args->ts;
    ASSERT_EQ(val, exp);
    const char *data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(1, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args->pk, pk);
    ASSERT_EQ(0, rv.GetInt64(2, &val));
    ASSERT_EQ(val, exp);
}

TEST_F(TabletCatalogTest, iterator_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    uint32_t pid_num = 8;
    TestArgs *args = PrepareMultiPartitionTable("t1", pid_num);
    for (uint32_t pid = 0; pid < pid_num; pid++) {
        ASSERT_TRUE(catalog->AddTable(args->meta[pid], args->tables[pid]));
    }
    auto handler = catalog->GetTable("db1", "t1");
    auto iterator = handler->GetWindowIterator("index0");
    int pk_cnt = 0;
    int record_num = 0;
    iterator->SeekToFirst();
    while (iterator->Valid()) {
        pk_cnt++;
        auto row_iterator = iterator->GetValue();
        row_iterator->SeekToFirst();
        while (row_iterator->Valid()) {
            record_num++;
            row_iterator->Next();
        }
        iterator->Next();
    }
    ASSERT_EQ(pk_cnt, 100);
    ASSERT_EQ(record_num, 500);

    auto full_iterator = handler->GetIterator();
    full_iterator->SeekToFirst();
    record_num = 0;
    while (full_iterator->Valid()) {
        record_num++;
        full_iterator->Next();
    }
    ASSERT_EQ(record_num, 500);
}
TEST_F(TabletCatalogTest, window_iterator_seek_test_discontinuous) {
    std::vector<std::shared_ptr<TabletCatalog>> catalog_vec;
    for (int i = 0; i < 2; i++) {
        std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
        ASSERT_TRUE(catalog->Init());
        catalog_vec.push_back(catalog);
    }
    uint32_t pid_num = 8;
    TestArgs *args = PrepareMultiPartitionTable("t1", pid_num);
    for (uint32_t pid = 0; pid < pid_num; pid++) {
        if (pid % 2 == 0) {
            ASSERT_TRUE(catalog_vec[0]->AddTable(args->meta[pid], args->tables[pid]));
        } else {
            ASSERT_TRUE(catalog_vec[1]->AddTable(args->meta[pid], args->tables[pid]));
        }
    }
    // WindowIterator Seek key Test
    {
        auto handler = catalog_vec[0]->GetTable("db1", "t1");
        auto iterator = handler->GetWindowIterator("index0");
        {
            int segment_cnt = 0;
            iterator->Seek("pk190");
            ASSERT_TRUE(iterator->Valid());
            auto row_iterator = iterator->GetValue();
            ASSERT_EQ("pk190", iterator->GetKey().ToString());
            row_iterator->SeekToFirst();
            while (row_iterator->Valid()) {
                segment_cnt++;
                row_iterator->Next();
            }
            ASSERT_EQ(5, segment_cnt);
        }
        {
            int segment_cnt = 0;
            iterator->Seek("pk195");
            auto row_iterator = iterator->GetValue();
            ASSERT_EQ("pk195", iterator->GetKey().ToString());
            row_iterator->SeekToFirst();
            while (row_iterator->Valid()) {
                segment_cnt++;
                row_iterator->Next();
            }
            ASSERT_EQ(5, segment_cnt);
        }
    }
    {
        auto handler = catalog_vec[1]->GetTable("db1", "t1");
        auto iterator = handler->GetWindowIterator("index0");
        int segment_cnt = 0;
        iterator->Seek("pk180");
        auto row_iterator = iterator->GetValue();
        ASSERT_EQ("pk180", iterator->GetKey().ToString());
        row_iterator->SeekToFirst();
        while (row_iterator->Valid()) {
            segment_cnt++;
            row_iterator->Next();
        }
        ASSERT_EQ(5, segment_cnt);
    }
}
TEST_F(TabletCatalogTest, iterator_test_discontinuous) {
    std::vector<std::shared_ptr<TabletCatalog>> catalog_vec;
    for (int i = 0; i < 2; i++) {
        std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
        ASSERT_TRUE(catalog->Init());
        catalog_vec.push_back(catalog);
    }
    uint32_t pid_num = 8;
    TestArgs *args = PrepareMultiPartitionTable("t1", pid_num);
    for (uint32_t pid = 0; pid < pid_num; pid++) {
        if (pid % 2 == 0) {
            ASSERT_TRUE(catalog_vec[0]->AddTable(args->meta[pid], args->tables[pid]));
        } else {
            ASSERT_TRUE(catalog_vec[1]->AddTable(args->meta[pid], args->tables[pid]));
        }
    }
    int pk_cnt = 0;
    int record_num = 0;
    int full_record_num = 0;
    for (int i = 0; i < 2; i++) {
        auto handler = catalog_vec[i]->GetTable("db1", "t1");
        auto iterator = handler->GetWindowIterator("index0");
        iterator->SeekToFirst();
        while (iterator->Valid()) {
            pk_cnt++;
            auto row_iterator = iterator->GetValue();
            row_iterator->SeekToFirst();
            while (row_iterator->Valid()) {
                record_num++;
                row_iterator->Next();
            }
            iterator->Next();
        }
        auto full_iterator = handler->GetIterator();
        full_iterator->SeekToFirst();
        while (full_iterator->Valid()) {
            full_record_num++;
            full_iterator->Next();
        }
    }
    ASSERT_EQ(pk_cnt, 100);
    ASSERT_EQ(record_num, 500);
    ASSERT_EQ(full_record_num, 500);
}

TEST_F(TabletCatalogTest, get_tablet) {
    auto local_tablet =
        std::make_shared<hybridse::vm::LocalTablet>(nullptr, std::shared_ptr<hybridse::vm::CompileInfoCache>());
    uint32_t pid_num = 8;
    TestArgs *args = PrepareMultiPartitionTable("t1", pid_num);
    auto handler = std::make_shared<TabletTableHandler>(args->meta[0], local_tablet);
    ClientManager client_manager;
    ASSERT_TRUE(handler->Init(client_manager));
    handler->AddTable(args->tables[0]);
    handler->AddTable(args->tables[3]);
    handler->AddTable(args->tables[7]);
    std::string pk = "key0";
    int pid = (uint32_t)(::openmldb::base::hash64(pk) % pid_num);
    ASSERT_EQ(pid, 7);
    auto tablet = handler->GetTablet("", pk);
    auto real_tablet = std::dynamic_pointer_cast<hybridse::vm::LocalTablet>(tablet);
    ASSERT_TRUE(real_tablet != nullptr);
    pk = "key1";
    pid = (uint32_t)(::openmldb::base::hash64(pk) % pid_num);
    ASSERT_EQ(pid, 6);
    tablet = handler->GetTablet("", pk);
    real_tablet = std::dynamic_pointer_cast<hybridse::vm::LocalTablet>(tablet);
    ASSERT_TRUE(real_tablet == nullptr);
    delete args;
}

}  // namespace catalog
}  // namespace openmldb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
