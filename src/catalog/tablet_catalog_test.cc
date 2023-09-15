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

#include <absl/strings/str_cat.h>
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

void PrintRow(const std::string& buf, const hybridse::codec::Schema& schema) {
    hybridse::codec::RowView row_view(schema);
    row_view.Reset(reinterpret_cast<const int8_t*>(buf.c_str()), buf.size());
    std::string out;
    for (int idx = 0; idx < schema.size(); idx++) {
        std::string str = row_view.GetAsString(idx);
        out += str + ", ";
    }
    LOG(INFO) << "row: " << out;
}

struct TestArgs {
    std::vector<std::shared_ptr<::openmldb::storage::Table>> tables;
    std::vector<::openmldb::api::TableMeta> meta;
    std::string row;
    std::string idx_name;
    std::string pk;
    uint64_t ts;
    std::vector<::hybridse::vm::AggrTableInfo> agg_infos;
};

TestArgs PrepareTable(const std::string &tname, int num_pk = 1, uint64_t num_ts = 1, bool add_null = false) {
    TestArgs args;
    ::openmldb::api::TableMeta meta;
    meta.set_name(tname);
    meta.set_db("db1");
    meta.set_tid(0);
    meta.set_pid(0);
    meta.set_seg_cnt(8);
    meta.add_table_partition();
    meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "col1", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "col2", ::openmldb::type::kBigInt);

    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "i16_col", ::openmldb::type::kSmallInt);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "i32_col", ::openmldb::type::kInt);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "i64_col", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "f_col", ::openmldb::type::kFloat);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "d_col", ::openmldb::type::kDouble);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "t_col", ::openmldb::type::kTimestamp);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "s_col", ::openmldb::type::kString);

    SchemaCodec::SetIndex(meta.add_column_key(), "index0", "col1", "col2", ::openmldb::type::kAbsoluteTime, 0, 0);
    args.idx_name = "index0";

    ::openmldb::storage::MemTable *table = new ::openmldb::storage::MemTable(meta);
    table->Init();
    ::hybridse::vm::Schema fe_schema;
    schema::SchemaAdapter::ConvertSchema(meta.column_desc(), &fe_schema);
    ::hybridse::codec::RowBuilder rb(fe_schema);
    std::string value;
    std::string pk;
    for (int i = 0; i < num_pk; i++) {
        for (uint64_t ts = 1; ts <= num_ts; ts++) {
            pk = "pk" + std::to_string(i);
            auto ts_str = std::to_string(ts);
            uint32_t size = rb.CalTotalLength(pk.size() + ts_str.size());
            value.resize(size);
            rb.SetBuffer(reinterpret_cast<int8_t *>(&(value[0])), size);
            rb.AppendString(pk.c_str(), pk.size());
            rb.AppendInt64(ts);

            // append null
            if (add_null && ts % 4 == 0) {
                rb.AppendNULL();
                rb.AppendNULL();
                rb.AppendNULL();
                rb.AppendNULL();
                rb.AppendNULL();
                rb.AppendNULL();
                rb.AppendNULL();
            } else {
                rb.AppendInt16(ts);
                rb.AppendInt32(ts);
                rb.AppendInt64(ts);
                rb.AppendFloat(ts);
                rb.AppendDouble(ts);
                rb.AppendTimestamp(ts);
                rb.AppendString(ts_str.c_str(), ts_str.size());
            }
            table->Put(pk, ts, value.c_str(), value.size());

            args.ts = ts;
        }
    }

    // if empty table, also have to construct a request row
    if (value.empty()) {
        uint64_t ts = 100;
        pk = "pk";
        auto ts_str = std::to_string(ts);
        uint32_t size = rb.CalTotalLength(pk.size() + ts_str.size());
        value.resize(size);
        rb.SetBuffer(reinterpret_cast<int8_t *>(&(value[0])), size);
        rb.AppendString(pk.c_str(), pk.size());
        rb.AppendInt64(ts);
        rb.AppendInt16(ts);
        rb.AppendInt32(ts);
        rb.AppendInt64(ts);
        rb.AppendFloat(ts);
        rb.AppendDouble(ts);
        rb.AppendTimestamp(ts);
        rb.AppendString(ts_str.c_str(), ts_str.size());
        args.ts = ts;
    }

    args.pk = pk;
    args.row = value;
    std::shared_ptr<::openmldb::storage::MemTable> mtable(table);
    args.tables.push_back(mtable);
    args.meta.push_back(meta);
    return args;
}

template <class T = int64_t>
TestArgs PrepareAggTable(const std::string &tname, int num_pk, uint64_t num_ts, int bucket_size,
                          int agg_col, bool add_null = false) {
    TestArgs args;
    ::openmldb::api::TableMeta meta;
    meta.set_name(tname);
    meta.set_db("aggr_db");
    meta.set_tid(2);
    meta.set_pid(0);
    meta.set_seg_cnt(8);
    meta.add_table_partition();
    meta.set_mode(::openmldb::api::TableMode::kTableLeader);

    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "key", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "ts_start", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "ts_end", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "num_rows", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "agg_val", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "binlog_offset", openmldb::type::DataType::kBigInt);
    SchemaCodec::SetIndex(meta.add_column_key(), "index0", "key", "ts_start", ::openmldb::type::kAbsoluteTime, 0, 0);

    ::openmldb::storage::MemTable *table = new ::openmldb::storage::MemTable(meta);
    table->Init();
    ::hybridse::vm::Schema fe_schema;
    schema::SchemaAdapter::ConvertSchema(meta.column_desc(), &fe_schema);
    ::hybridse::codec::RowBuilder rb(fe_schema);
    const int val_len = sizeof(T);
    for (int i = 0; i < num_pk; i++) {
        int count = 0;
        uint64_t ts_start = 0;
        T sum = 0;
        for (uint64_t ts = 1; ts <= num_ts; ts++) {
            if (add_null && ts % 4 == 0) {
                sum += 0;
            } else {
                sum += static_cast<T>(ts);
            }
            count++;

            if (ts && count % bucket_size == 0) {
                std::string value;
                std::string pk = "pk" + std::to_string(i);
                uint32_t size = rb.CalTotalLength(pk.size() + val_len);
                value.resize(size);
                rb.SetBuffer(reinterpret_cast<int8_t *>(&(value[0])), size);
                rb.AppendString(pk.c_str(), pk.size());
                rb.AppendTimestamp(ts_start);
                rb.AppendTimestamp(ts);
                rb.AppendInt32(count);
                rb.AppendString(reinterpret_cast<const char*>(&sum), val_len);
                rb.AppendInt64(i * num_ts + ts);
                table->Put(pk, ts_start, value.c_str(), value.size());

                ts_start = ts + 1;
                count = 0;
                sum = 0;
            }
        }
    }

    auto mtable = std::shared_ptr<::openmldb::storage::MemTable>(table);
    args.tables.push_back(mtable);
    args.meta.push_back(meta);
    return args;
}

TestArgs PrepareMultiPartitionTable(const std::string &tname, int partition_num) {
    TestArgs args;
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
    args.idx_name = "index0";
    for (int i = 0; i < partition_num; i++) {
        meta.add_table_partition();
    }

    for (int i = 0; i < partition_num; i++) {
        ::openmldb::api::TableMeta cur_meta(meta);
        cur_meta.set_pid(i);
        auto table = std::make_shared<::openmldb::storage::MemTable>(cur_meta);
        table->Init();
        args.tables.push_back(table);
        args.meta.push_back(cur_meta);
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
            args.tables[pid]->Put(pk, ts + j, value.c_str(), value.size());
        }
    }
    return args;
}

TEST_F(TabletCatalogTest, tablet_smoke_test) {
    TestArgs args = PrepareTable("t1");
    TabletTableHandler handler(args.meta[0], std::shared_ptr<hybridse::vm::Tablet>());
    ClientManager client_manager;
    ASSERT_TRUE(handler.Init(client_manager));
    handler.AddTable(args.tables[0]);
    auto it = handler.GetIterator();
    if (!it) {
        ASSERT_TRUE(false);
    }
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    auto row = it->GetValue();
    ASSERT_EQ(row.ToString(), args.row);
    auto wit = handler.GetWindowIterator(args.idx_name);
    if (!wit) {
        ASSERT_TRUE(false);
    }
    wit->SeekToFirst();
    ASSERT_TRUE(wit->Valid());
    auto key = wit->GetKey();
    ASSERT_EQ(key.ToString(), args.pk);
    auto second_it = wit->GetValue();
    second_it->SeekToFirst();
    ASSERT_TRUE(second_it->Valid());
    ASSERT_EQ(args.ts, second_it->GetKey());
    ASSERT_EQ(args.row, second_it->GetValue().ToString());
}

TEST_F(TabletCatalogTest, segment_handler_test) {
    TestArgs args = PrepareTable("t1");
    auto handler = std::shared_ptr<TabletTableHandler>(
        new TabletTableHandler(args.meta[0], std::shared_ptr<hybridse::vm::Tablet>()));
    ClientManager client_manager;
    ASSERT_TRUE(handler->Init(client_manager));
    handler->AddTable(args.tables[0]);
    // Seek key not exist
    {
        auto partition = handler->GetPartition(args.idx_name);
        auto segment = partition->GetSegment(args.pk);
        auto iter = segment->GetIterator();
        if (!iter) {
            FAIL();
        }
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
    }
}

TEST_F(TabletCatalogTest, add_drop_test) {
    TestArgs args = PrepareTable("t1");
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    ::openmldb::api::TableMeta meta = args.meta[0];
    meta.set_tid(2);
    ASSERT_TRUE(catalog->AddTable(meta, args.tables[0]));
    uint32_t tid = meta.tid();
    uint32_t pid = meta.pid();
    std::string db = meta.db();
    std::string name = meta.name();
    ASSERT_FALSE(catalog->DeleteTable(db, name, tid - 1, pid));
    ::openmldb::api::TableMeta new_meta = meta;
    new_meta.set_tid(1);
    ASSERT_FALSE(catalog->UpdateTableMeta(new_meta));
    ::openmldb::nameserver::TableInfo table_info;
    table_info.set_db(db);
    table_info.set_tid(1);
    table_info.set_name(name);
    bool index_updated = false;
    ASSERT_FALSE(catalog->UpdateTableInfo(table_info, &index_updated));
    ASSERT_TRUE(catalog->DeleteTable(db, name, tid, pid));
}

TEST_F(TabletCatalogTest, segment_handler_pk_not_exist_test) {
    TestArgs args = PrepareTable("t1");
    auto handler = std::shared_ptr<TabletTableHandler>(
        new TabletTableHandler(args.meta[0], std::shared_ptr<hybridse::vm::Tablet>()));
    ClientManager client_manager;
    ASSERT_TRUE(handler->Init(client_manager));
    handler->AddTable(args.tables[0]);
    // Seek key not exist
    {
        auto partition = handler->GetPartition(args.idx_name);
        auto segment = partition->GetSegment("KEY_NOT_EXIST");
        auto iter = segment->GetIterator();
        if (iter) {
            FAIL();
        }
    }
}

TEST_F(TabletCatalogTest, sql_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs args = PrepareTable("t1");
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));
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
    int64_t exp = args.ts + 1;
    ASSERT_EQ(val, exp);
    const char *data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args.pk, pk);
}

TEST_F(TabletCatalogTest, sql_last_join_smoke_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs args = PrepareTable("t1");
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args1 = PrepareTable("t2");
    ASSERT_TRUE(catalog->AddTable(args1.meta[0], args1.tables[0]));

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
    ASSERT_EQ(args.pk, rv.GetStringUnsafe(0));
}

TEST_F(TabletCatalogTest, sql_last_join_smoke_test2) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs args = PrepareTable("t1");
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args1 = PrepareTable("t2");
    ASSERT_TRUE(catalog->AddTable(args1.meta[0], args1.tables[0]));

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
    ASSERT_EQ(args.pk, pk);
}

TEST_F(TabletCatalogTest, sql_window_smoke_500_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    TestArgs args = PrepareTable("t1");

    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));
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
    TestArgs args = PrepareTable("t1");

    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));
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
    int64_t exp = args.ts;
    ASSERT_EQ(val, exp);
    const char *data = NULL;
    uint32_t data_size = 0;
    ASSERT_EQ(0, rv.GetString(1, &data, &data_size));
    std::string pk(data, data_size);
    ASSERT_EQ(args.pk, pk);
    ASSERT_EQ(0, rv.GetInt64(2, &val));
    ASSERT_EQ(val, exp);
}

TEST_F(TabletCatalogTest, iterator_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    uint32_t pid_num = 8;
    TestArgs args = PrepareMultiPartitionTable("t1", pid_num);
    for (uint32_t pid = 0; pid < pid_num; pid++) {
        ASSERT_TRUE(catalog->AddTable(args.meta[pid], args.tables[pid]));
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
    TestArgs args = PrepareMultiPartitionTable("t1", pid_num);
    for (uint32_t pid = 0; pid < pid_num; pid++) {
        if (pid % 2 == 0) {
            ASSERT_TRUE(catalog_vec[0]->AddTable(args.meta[pid], args.tables[pid]));
        } else {
            ASSERT_TRUE(catalog_vec[1]->AddTable(args.meta[pid], args.tables[pid]));
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
    TestArgs args = PrepareMultiPartitionTable("t1", pid_num);
    for (uint32_t pid = 0; pid < pid_num; pid++) {
        if (pid % 2 == 0) {
            ASSERT_TRUE(catalog_vec[0]->AddTable(args.meta[pid], args.tables[pid]));
        } else {
            ASSERT_TRUE(catalog_vec[1]->AddTable(args.meta[pid], args.tables[pid]));
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
    TestArgs args = PrepareMultiPartitionTable("t1", pid_num);
    auto handler = std::make_shared<TabletTableHandler>(args.meta[0], local_tablet);
    ClientManager client_manager;
    ASSERT_TRUE(handler->Init(client_manager));
    handler->AddTable(args.tables[0]);
    handler->AddTable(args.tables[3]);
    handler->AddTable(args.tables[7]);
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
}

TEST_F(TabletCatalogTest, aggr_table_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());

    std::vector<::hybridse::vm::AggrTableInfo> infos;
    ::hybridse::vm::AggrTableInfo info1 = {"aggr_t1", "aggr_db", "base_db", "base_t1",
                                           "sum", "col1", "col2", "col3", "1000"};
    infos.push_back(info1);
    ::hybridse::vm::AggrTableInfo info2 = {"aggr_t2", "aggr_db", "base_db", "base_t1",
                                           "sum", "col1", "col2", "col3", "1h"};
    infos.push_back(info2);
    ::hybridse::vm::AggrTableInfo info3 = {"aggr_t3", "aggr_db", "base_db", "base_t1",
                                           "avg", "col1", "col2,col4", "col3", "1h"};
    infos.push_back(info3);

    catalog->RefreshAggrTables(infos);
    auto res = catalog->GetAggrTables("base_db", "base_t1", "sum", "col1", "col2", "col3", "");
    ASSERT_EQ(2, res.size());
    ASSERT_EQ(info1, res[0]);
    ASSERT_EQ(info2, res[1]);

    res = catalog->GetAggrTables("base_db", "base_t1", "avg", "col1", "col2,col4", "col3", "");
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(info3, res[0]);

    res = catalog->GetAggrTables("base_db", "base_t1", "count", "col1", "col2,col4", "col3", "");
    ASSERT_EQ(0, res.size());
}

TEST_F(TabletCatalogTest, LongWindowSmokeTest) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 2, num_ts = 9, bucket_size = 2;

    TestArgs args = PrepareTable("t1", num_pk, num_ts);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args2 = PrepareAggTable("aggr_t1", num_pk, num_ts, bucket_size, 1);
    ASSERT_TRUE(catalog->AddTable(args2.meta[0], args2.tables[0]));

    ::hybridse::vm::AggrTableInfo info1 = {"aggr_t1", "aggr_db", "db1", "t1", "sum", "col2", "col1", "col2", "2", ""};

    catalog->RefreshAggrTables({info1});

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1";
    ::hybridse::vm::RequestRunSession session;
    session.EnableDebug();
    session.SetOptions(options);
    ::hybridse::base::Status status;
    hybridse::codec::Row output;
    const char *data = NULL;
    uint32_t data_size = 0;
    int64_t val = 0;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        int64_t exp = args.ts * 2 + (args.ts - 1) + (args.ts - 2);
        ASSERT_EQ(val, exp);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 2s PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        int64_t exp = args.ts;
        for (uint64_t i = 0; i <= args.ts; i++) {
            exp += i;
        }
        ASSERT_EQ(val, exp);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        int64_t exp = args.ts * 2 + (args.ts - 1) + (args.ts - 2);
        ASSERT_EQ(val, exp);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        int64_t exp = args.ts * 2 + (args.ts - 1);
        ASSERT_EQ(val, exp);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 10 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        int64_t exp = args.ts;
        for (uint64_t i = 0; i <= args.ts; i++) {
            exp += i;
        }
        ASSERT_EQ(val, exp);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }
}

TEST_F(TabletCatalogTest, long_window_thread_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 2, num_ts = 9, bucket_size = 2;

    TestArgs args = PrepareTable("t1", num_pk, num_ts);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args2 = PrepareAggTable("aggr_t1", num_pk, num_ts, bucket_size, 1);
    ASSERT_TRUE(catalog->AddTable(args2.meta[0], args2.tables[0]));

    ::hybridse::vm::AggrTableInfo info1 = {"aggr_t1", "aggr_db", "db1", "t1",
                                           "sum", "col2", "col1", "col2", "2"};
    catalog->RefreshAggrTables({info1});

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1";
    ::hybridse::vm::RequestRunSession session;
    session.EnableDebug();
    session.SetOptions(options);
    ::hybridse::base::Status status;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));

    std::string sql =
        "SELECT col1, sum(col2) OVER w1 FROM t1 "
        "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW);";
    engine.Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(::hybridse::common::kOk, status.code);

    volatile bool stop = false;
    auto run_and_check = [&]() {
        while (!stop) {
            ::hybridse::codec::RowView rv(session.GetSchema());
            hybridse::codec::Row output;
            const char *data = NULL;
            uint32_t data_size = 0;
            int64_t val = 0;
            ASSERT_EQ(0, session.Run(request_row, &output));
            ASSERT_EQ(2, session.GetSchema().size());
            rv.Reset(output.buf(), output.size());
            ASSERT_EQ(0, rv.GetInt64(1, &val));
            int64_t exp = args.ts * 2 + (args.ts - 1) + (args.ts - 2);
            ASSERT_EQ(val, exp);
            ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
            std::string pk(data, data_size);
            ASSERT_EQ(args.pk, pk);
        }
    };

    int iter = 10;
    for (int i = 0; i < iter; i++) {
        std::thread t1(run_and_check);
        std::thread t2(run_and_check);
        std::thread t3(run_and_check);
        std::thread t4(run_and_check);
        sleep(2);
        stop = true;
        t1.join();
        t2.join();
        t3.join();
        t4.join();
    }
}

TEST_F(TabletCatalogTest, long_window_equal_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 2, num_ts = 10000, bucket_size = 10;

    TestArgs args = PrepareTable("t1", num_pk, num_ts);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args2 = PrepareAggTable("aggr_t1", num_pk, num_ts, bucket_size, 1);
    ASSERT_TRUE(catalog->AddTable(args2.meta[0], args2.tables[0]));

    ::hybridse::vm::AggrTableInfo info1 = {"aggr_t1", "aggr_db", "db1", "t1",
                                           "sum", "col2", "col1", "col2", "2"};
    catalog->RefreshAggrTables({info1});

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1";
    ::hybridse::vm::RequestRunSession session_lw;
    session_lw.EnableDebug();
    session_lw.SetOptions(options);
    ::hybridse::vm::RequestRunSession session;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));

    std::vector<std::string> units = {"", "s"};
    std::vector<std::string> excludes = {"", "EXCLUDE CURRENT_TIME"};

    for (const auto& exclude : excludes) {
        for (const auto &unit : units) {
            for (int i = 1; i < 10; i++) {
                std::string sql = absl::StrCat(
                    "SELECT col1, sum(col2) OVER w1 FROM t1 "
                    "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN ",
                    i, unit, " PRECEDING AND CURRENT ROW ", exclude, ");");

                ::hybridse::base::Status status;
                engine.Get(sql, "db1", session, status);
                if (status.code != ::hybridse::common::kOk) {
                    std::cout << status.msg << std::endl;
                }
                ASSERT_EQ(::hybridse::common::kOk, status.code);
                hybridse::codec::Row output;
                ASSERT_EQ(0, session.Run(request_row, &output));

                engine.Get(sql, "db1", session_lw, status);
                if (status.code != ::hybridse::common::kOk) {
                    std::cout << status.msg << std::endl;
                }
                ASSERT_EQ(::hybridse::common::kOk, status.code);
                hybridse::codec::Row output_lw;
                ASSERT_EQ(0, session_lw.Run(request_row, &output_lw));

                ::hybridse::codec::RowView rv(session.GetSchema());
                ::hybridse::codec::RowView rv_lw(session_lw.GetSchema());
                ASSERT_EQ(2, session.GetSchema().size());
                ASSERT_EQ(2, session_lw.GetSchema().size());
                rv.Reset(output.buf(), output.size());
                rv_lw.Reset(output.buf(), output.size());
                int64_t val = 0, val_lw = 0;
                ASSERT_EQ(0, rv.GetInt64(1, &val));
                ASSERT_EQ(0, rv.GetInt64(1, &val_lw));
                ASSERT_EQ(val, val_lw);
                const char *data = NULL;
                uint32_t data_size = 0;
                ASSERT_EQ(0, rv_lw.GetString(0, &data, &data_size));
                std::string pk(data, data_size);
                ASSERT_EQ(args.pk, pk);
            }
        }
    }
}

template <class T>
void CheckAggResult(::hybridse::vm::Engine* engine, ::hybridse::vm::RequestRunSession session,
                    const hybridse::codec::Row &request_row, const std::string &col, T exp) {
    std::string sql =
        absl::StrCat("SELECT col1, sum(", col, ") OVER w1 FROM t1 "
        "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 8 PRECEDING AND CURRENT ROW);");
    ::hybridse::base::Status status;
    hybridse::codec::Row output;
    engine->Get(sql, "db1", session, status);
    if (status.code != ::hybridse::common::kOk) {
        LOG(ERROR) << "status = " << status.msg;
    }
    ::hybridse::codec::RowView rv(session.GetSchema());
    ASSERT_EQ(::hybridse::common::kOk, status.code);
    ASSERT_EQ(0, session.Run(request_row, &output));
    ASSERT_EQ(2, session.GetSchema().size());
    rv.Reset(output.buf(), output.size());
    T val = 0;
    ASSERT_EQ(0, rv.GetValue(output.buf(), 1, session.GetSchema().Get(1).type(), &val));
    ASSERT_EQ(val, exp);
}

TestArgs PrepareMultipleAggCatalog(TabletCatalog* catalog, int num_pk, int num_ts, int bucket_size, bool add_null) {
    TestArgs args;
    std::vector<std::string> agg_cols = {"i16_col", "i32_col", "i64_col", "f_col", "d_col", "t_col"};
    std::vector<::hybridse::vm::AggrTableInfo> infos;
    for (int i = 0; i < static_cast<int>(agg_cols.size()); i++) {
        const auto& agg_col = agg_cols[i];
        if (agg_col == "i16_col" || agg_col == "i32_col" || agg_col == "i64_col" || agg_col == "t_col") {
            args = PrepareAggTable(absl::StrCat("aggr_t1_", agg_col), num_pk, num_ts, bucket_size, i + 2, add_null);
        } else if (agg_col == "f_col") {
            args =
                PrepareAggTable<float>(absl::StrCat("aggr_t1_", agg_col), num_pk, num_ts, bucket_size, i + 2, add_null);
        } else if (agg_col == "d_col") {
            args = PrepareAggTable<double>(absl::StrCat("aggr_t1_", agg_col), num_pk, num_ts, bucket_size, i + 2,
                                           add_null);
        }
        catalog->AddTable(args.meta[0], args.tables[0]);

        ::hybridse::vm::AggrTableInfo info = {
            absl::StrCat("aggr_t1_", agg_col), "aggr_db", "db1", "t1", "sum", agg_col, "col1", "col2", "2"};
        infos.push_back(info);
    }
    catalog->RefreshAggrTables(infos);
    return args;
}

TEST_F(TabletCatalogTest, long_window_nullvalue_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 2, num_ts = 9, bucket_size = 2;

    TestArgs args = PrepareTable("t1", num_pk, num_ts, true);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));
    TestArgs args2 = PrepareMultipleAggCatalog(catalog.get(), num_pk, num_ts, bucket_size, true);

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1";
    ::hybridse::vm::RequestRunSession session;
    session.EnableDebug();
    session.SetOptions(options);
    ::hybridse::base::Status status;
    hybridse::codec::Row output;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));
    int64_t exp = args.ts % 4 == 0 ? 0 : args.ts;
    for (uint64_t i = args.ts > 8 ? args.ts - 8 : 0; i <= args.ts; i++) {
        if (i % 4 != 0) {
            exp += i;
        }
    }

    CheckAggResult<int16_t>(&engine, session, request_row, "i16_col", exp);
    CheckAggResult<int32_t>(&engine, session, request_row, "i32_col", exp);
    CheckAggResult<int64_t>(&engine, session, request_row, "i64_col", exp);
    CheckAggResult<float>(&engine, session, request_row, "f_col", exp);
    CheckAggResult<double>(&engine, session, request_row, "d_col", exp);
    CheckAggResult<int64_t>(&engine, session, request_row, "t_col", exp);
}

TEST_F(TabletCatalogTest, long_window_multi_windows_test) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 2, num_ts = 9, bucket_size = 2;

    TestArgs args = PrepareTable("t1", num_pk, num_ts, false);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args2 = PrepareMultipleAggCatalog(catalog.get(), num_pk, num_ts, bucket_size, false);

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1,w2";
    ::hybridse::vm::RequestRunSession session;
    session.EnableDebug();
    session.SetOptions(options);
    ::hybridse::base::Status status;
    hybridse::codec::Row output;
    const char *data = NULL;
    uint32_t data_size = 0;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));
    {
        std::string sql =
            "SELECT col1, sum(i32_col) OVER w1 as s0, sum(f_col) OVER w2, sum(i32_col) OVER w1 as s1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), "
            "w2 AS (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(4, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);

        int32_t i32_val = 0;
        ASSERT_EQ(0, rv.GetInt32(1, &i32_val));
        int32_t exp = args.ts * 2 + (args.ts - 1) + (args.ts - 2);
        ASSERT_EQ(i32_val, exp);
        ASSERT_EQ(0, rv.GetInt32(3, &i32_val));
        ASSERT_EQ(i32_val, exp);

        float f_val = 0;
        ASSERT_EQ(0, rv.GetFloat(2, &f_val));
        float exp2 = args.ts * 2 + (args.ts - 1) + (args.ts - 2);
        ASSERT_EQ(f_val, exp2);
    }
}

TEST_F(TabletCatalogTest, long_window_empty_agg) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 2, num_ts = 9, bucket_size = 2;

    TestArgs args = PrepareTable("t1", num_pk, num_ts);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args2 = PrepareAggTable("aggr_t1", 0, 0, bucket_size, 1);
    ASSERT_TRUE(catalog->AddTable(args2.meta[0], args2.tables[0]));

    ::hybridse::vm::AggrTableInfo info1 = {"aggr_t1", "aggr_db", "db1", "t1",
                                           "sum", "col2", "col1", "col2", "2"};
    catalog->RefreshAggrTables({info1});

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1";
    ::hybridse::vm::RequestRunSession session;
    session.EnableDebug();
    session.SetOptions(options);
    ::hybridse::base::Status status;
    hybridse::codec::Row output;
    const char *data = NULL;
    uint32_t data_size = 0;
    int64_t val = 0;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        int64_t exp = args.ts * 2 + (args.ts - 1) + (args.ts - 2);
        ASSERT_EQ(val, exp);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }
}

TEST_F(TabletCatalogTest, long_window_empty) {
    std::shared_ptr<TabletCatalog> catalog(new TabletCatalog());
    ASSERT_TRUE(catalog->Init());
    int num_pk = 0, num_ts = 0, bucket_size = 2;

    TestArgs args = PrepareTable("t1", num_pk, num_ts);
    ASSERT_TRUE(catalog->AddTable(args.meta[0], args.tables[0]));

    TestArgs args2 = PrepareAggTable("aggr_t1", 0, 0, bucket_size, 1);
    ASSERT_TRUE(catalog->AddTable(args2.meta[0], args2.tables[0]));

    ::hybridse::vm::AggrTableInfo info1 = {"aggr_t1", "aggr_db", "db1", "t1",
                                           "sum", "col2", "col1", "col2", "2"};
    catalog->RefreshAggrTables({info1});

    ::hybridse::vm::Engine engine(catalog);
    auto options = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*options)[::hybridse::vm::LONG_WINDOWS] = "w1";
    ::hybridse::vm::RequestRunSession session;
    session.EnableDebug();
    session.SetOptions(options);
    ::hybridse::base::Status status;
    hybridse::codec::Row output;
    const char *data = NULL;
    uint32_t data_size = 0;
    int64_t val;
    ::hybridse::codec::Row request_row(::hybridse::base::RefCountedSlice::Create(args.row.c_str(), args.row.size()));

    {
        std::string sql =
            "SELECT col1, sum(col2) OVER w1 FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1 ORDER BY col2 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW);";
        engine.Get(sql, "db1", session, status);
        ::hybridse::codec::RowView rv(session.GetSchema());
        if (status.code != ::hybridse::common::kOk) {
            std::cout << status.msg << std::endl;
        }
        ASSERT_EQ(::hybridse::common::kOk, status.code);
        ASSERT_EQ(0, session.Run(request_row, &output));
        ASSERT_EQ(2, session.GetSchema().size());
        rv.Reset(output.buf(), output.size());
        ASSERT_EQ(0, rv.GetInt64(1, &val));
        ASSERT_EQ(args.ts, val);
        ASSERT_EQ(0, rv.GetString(0, &data, &data_size));
        std::string pk(data, data_size);
        ASSERT_EQ(args.pk, pk);
    }
}

}  // namespace catalog
}  // namespace openmldb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
