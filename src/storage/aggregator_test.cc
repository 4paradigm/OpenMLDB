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

#include <map>
#include <utility>
#include "gtest/gtest.h"

#include "base/file_util.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "storage/aggregator.h"
#include "storage/mem_table.h"
namespace openmldb {
namespace storage {

using ::openmldb::codec::SchemaCodec;

uint32_t counter = 10;

class AggregatorTest : public ::testing::Test {
 public:
    AggregatorTest() {}
    ~AggregatorTest() {}
};
std::string GenRand() { return std::to_string(rand() % 10000000 + 1); }  // NOLINT
void AddDefaultAggregatorBaseSchema(::openmldb::api::TableMeta* table_meta) {
    table_meta->set_name("t0");
    table_meta->set_pid(0);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "id1", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "id2", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_col", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col3", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col4", openmldb::type::DataType::kSmallInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col5", openmldb::type::DataType::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col6", openmldb::type::DataType::kFloat);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col7", openmldb::type::DataType::kDouble);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col8", openmldb::type::DataType::kDate);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col9", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col_null", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "low_card", openmldb::type::DataType::kInt);

    SchemaCodec::SetIndex(table_meta->add_column_key(), "idx", "id1|id2", "ts_col", ::openmldb::type::kAbsoluteTime, 0,
                          0);
    return;
}

void AddDefaultAggregatorSchema(::openmldb::api::TableMeta* table_meta) {
    table_meta->set_name("pre_aggr_1");
    table_meta->set_pid(0);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "key", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_start", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_end", openmldb::type::DataType::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "num_rows", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "agg_val", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "binlog_offset", openmldb::type::DataType::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "filter_key", openmldb::type::DataType::kString);

    SchemaCodec::SetIndex(table_meta->add_column_key(), "key", "key", "ts_start", ::openmldb::type::kAbsoluteTime, 0,
                          0);
}

bool UpdateAggr(std::shared_ptr<Aggregator> aggr, codec::RowBuilder* row_builder) {
    std::string encoded_row;
    auto window_size = aggr->GetWindowSize();
    std::string str1("abc");
    std::string str2("hello");
    for (int i = 0; i <= 100; i++) {
        std::string str = i % 2 == 0 ? str1 : str2;
        uint32_t row_size = row_builder->CalTotalLength(6 + str.size());
        encoded_row.resize(row_size);
        row_builder->SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder->AppendString("id1", 3);
        row_builder->AppendString("id2", 3);
        row_builder->AppendTimestamp(static_cast<int64_t>(i) * window_size / 2);
        row_builder->AppendInt32(i);
        row_builder->AppendInt16(i);
        row_builder->AppendInt64(i);
        row_builder->AppendFloat(static_cast<float>(i));
        row_builder->AppendDouble(static_cast<double>(i));
        row_builder->AppendDate(i);
        row_builder->AppendString(str.c_str(), str.size());
        row_builder->AppendNULL();
        row_builder->AppendInt32(i % 2);
        bool ok = aggr->Update("id1|id2", encoded_row, i);
        if (!ok) {
            return false;
        }
    }
    return true;
}

bool UpdateMinAggr(std::shared_ptr<Aggregator> aggr, codec::RowBuilder* row_builder,
                    std::shared_ptr<Table> base_table, int *delete_index) {
    std::string encoded_row;
    std::string delete_row;
    auto window_size = aggr->GetWindowSize();
    *delete_index = 80;
    std::string str1("abc");
    std::string str2("hello");
    for (int i = 0; i <= 100; i++) {
        std::string str = i % 2 == 0 ? str1 : str2;
        uint32_t row_size = row_builder->CalTotalLength(6 + str.size());
        encoded_row.resize(row_size);
        row_builder->SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder->AppendString("id1", 3);
        row_builder->AppendString("id2", 3);
        row_builder->AppendTimestamp(static_cast<int64_t>(i) * window_size / 2);
        row_builder->AppendInt32(i);
        row_builder->AppendInt16(i);
        row_builder->AppendInt64(i);
        row_builder->AppendFloat(static_cast<float>(i));
        row_builder->AppendDouble(static_cast<double>(i));
        row_builder->AppendDate(i);
        row_builder->AppendString(str.c_str(), str.size());
        row_builder->AppendNULL();
        row_builder->AppendInt32(i % 2);
        Dimensions dimensions;
        auto dimension = dimensions.Add();
        dimension->set_idx(0);
        dimension->set_key("id1|id2");
        if (i == *delete_index) {
            delete_row = encoded_row;
        }
        bool ok = base_table->Put(static_cast<int64_t>(i) * window_size / 2, encoded_row, dimensions);
        if (!ok) {
            return false;
        }
        ok = aggr->Update("id1|id2", encoded_row, i);
        if (!ok) {
            return false;
        }
    }
    bool ok = base_table->Delete("id1|id2", 0, (*delete_index) * window_size / 2);
    if (!ok) {
        PDLOG(ERROR, "base table delete failed");
        return false;
    }
    ok = aggr->Update("id1|id2", delete_row, 101, false, true);
    if (!ok) {
        PDLOG(ERROR, "aggr deletes one record failed");
        return false;
    }
    return true;
}

bool UpdateMaxAggr(std::shared_ptr<Aggregator> aggr, codec::RowBuilder* row_builder,
                    std::shared_ptr<Table> base_table, int *delete_index) {
    std::string encoded_row;
    std::string delete_row;
    *delete_index = 81;
    auto window_size = aggr->GetWindowSize();
    std::string str1("abc");
    std::string str2("hello");
    for (int i = 0; i <= 100; i++) {
        std::string str = i % 2 == 0 ? str1 : str2;
        uint32_t row_size = row_builder->CalTotalLength(6 + str.size());
        encoded_row.resize(row_size);
        row_builder->SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder->AppendString("id1", 3);
        row_builder->AppendString("id2", 3);
        row_builder->AppendTimestamp(static_cast<int64_t>(i) * window_size / 2);
        row_builder->AppendInt32(i);
        row_builder->AppendInt16(i);
        row_builder->AppendInt64(i);
        row_builder->AppendFloat(static_cast<float>(i));
        row_builder->AppendDouble(static_cast<double>(i));
        row_builder->AppendDate(i);
        row_builder->AppendString(str.c_str(), str.size());
        row_builder->AppendNULL();
        row_builder->AppendInt32(i % 2);
        Dimensions dimensions;
        auto dimension = dimensions.Add();
        dimension->set_idx(0);
        dimension->set_key("id1|id2");
        if (i == *delete_index) {
            delete_row = encoded_row;
        }
        bool ok = base_table->Put(static_cast<int64_t>(i) * window_size / 2, encoded_row, dimensions);
        if (!ok) {
            return false;
        }
        ok = aggr->Update("id1|id2", encoded_row, i);
        if (!ok) {
            return false;
        }
    }
    bool ok = base_table->Delete("id1|id2", 0, *delete_index * window_size / 2);
    if (!ok) {
        PDLOG(ERROR, "base table deletes failed");
        return false;
    }
    ok = aggr->Update("id1|id2", delete_row, 101, false, true);
    if (!ok) {
        PDLOG(ERROR, "aggr deletes one record failed");
        return false;
    }
    return true;
}


bool DeleteAndUpdateAggr(std::shared_ptr<Aggregator> aggr, codec::RowBuilder* row_builder,
                         int *delete_index) {
    std::string encoded_row;
    std::string delete_row;
    auto window_size = aggr->GetWindowSize();
    std::string str1("abc");
    std::string str2("hello");
    *delete_index = 80;
    for (int i = 0; i <= 100; i++) {
        std::string str = i % 2 == 0 ? str1 : str2;
        uint32_t row_size = row_builder->CalTotalLength(6 + str.size());
        encoded_row.resize(row_size);
        row_builder->SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder->AppendString("id1", 3);
        row_builder->AppendString("id2", 3);
        row_builder->AppendTimestamp(static_cast<int64_t>(i) * window_size / 2);
        row_builder->AppendInt32(i);
        row_builder->AppendInt16(i);
        row_builder->AppendInt64(i);
        row_builder->AppendFloat(static_cast<float>(i));
        row_builder->AppendDouble(static_cast<double>(i));
        row_builder->AppendDate(i);
        row_builder->AppendString(str.c_str(), str.size());
        row_builder->AppendNULL();
        row_builder->AppendInt32(i % 2);
        bool ok = aggr->Update("id1|id2", encoded_row, i);
        if (!ok) {
            return false;
        }
        if (i == *delete_index) {
            delete_row = encoded_row;
        }
    }
    bool ok = aggr->Update("id1|id2", delete_row, 101, false, true);
    if (!ok) {
        return false;
    }
    return true;
}

bool GetUpdatedResult(const uint32_t& id, const std::string& aggr_col, const std::string& aggr_type,
                      const std::string& bucket_size, std::shared_ptr<Aggregator>& aggregator,  // NOLINT
                      std::shared_ptr<Table>& table, AggrBuffer** buffer) {                     // NOLINT
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id + 1);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();

    // replicator
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, aggr_col, aggr_type,
                                 "ts_col", bucket_size, "low_card");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, nullptr);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    UpdateAggr(aggr, &row_builder);
    std::string key = "id1|id2";
    aggregator = aggr;
    aggr->GetAggrBuffer("id1|id2", buffer);
    table = aggr_table;
    ::openmldb::base::RemoveDirRecursive(folder);
    return true;
}

bool GetMinUpdatedResult(const uint32_t& id, const std::string& aggr_col, const std::string& aggr_type,
                         const std::string& bucket_size, std::shared_ptr<Aggregator>& aggregator,  // NOLINT
                         std::shared_ptr<Table>& table, AggrBuffer** buffer, int *delete_index) {  // NOLINT
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id + 1);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> base_table = std::make_shared<MemTable>(base_table_meta);
    base_table->Init();
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();

    // replicator
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, aggr_col, aggr_type,
                                 "ts_col", bucket_size, "low_card");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, base_table);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    UpdateMinAggr(aggr, &row_builder, base_table, delete_index);
    std::string key = "id1|id2";
    aggregator = aggr;
    aggr->GetAggrBuffer("id1|id2", buffer);
    table = aggr_table;
    ::openmldb::base::RemoveDirRecursive(folder);
    return true;
}


bool GetMaxUpdatedResult(const uint32_t& id, const std::string& aggr_col, const std::string& aggr_type,
                         const std::string& bucket_size, std::shared_ptr<Aggregator>& aggregator,  // NOLINT
                         std::shared_ptr<Table>& table, AggrBuffer** buffer, int *delete_index) {  // NOLINT
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id + 1);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> base_table = std::make_shared<MemTable>(base_table_meta);
    base_table->Init();
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();

    // replicator
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, aggr_col, aggr_type,
                                 "ts_col", bucket_size, "low_card");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, base_table);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    UpdateMaxAggr(aggr, &row_builder, base_table, delete_index);
    std::string key = "id1|id2";
    aggregator = aggr;
    aggr->GetAggrBuffer("id1|id2", buffer);
    table = aggr_table;
    ::openmldb::base::RemoveDirRecursive(folder);
    return true;
}

bool GetDeleteAndUpdatedResult(const uint32_t& id, const std::string& aggr_col, const std::string& aggr_type,
                               const std::string& bucket_size, std::shared_ptr<Aggregator>& aggregator,  // NOLINT
                               std::shared_ptr<Table>& table, AggrBuffer** buffer, int *delete_index) {  // NOLINT
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id + 1);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();

    // replicator
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, aggr_col, aggr_type,
                                 "ts_col", bucket_size, "low_card");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, nullptr);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    DeleteAndUpdateAggr(aggr, &row_builder, delete_index);
    std::string key = "id1|id2";
    aggregator = aggr;
    aggr->GetAggrBuffer("id1|id2", buffer);
    table = aggr_table;
    ::openmldb::base::RemoveDirRecursive(folder);
    return true;
}

template <typename T>
void CheckSumAggrResult(std::shared_ptr<Table> aggr_table, DataType data_type, std::shared_ptr<Aggregator> aggr,
                        int32_t expect_null = 0) {
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    int64_t window_size = aggr->GetWindowSize();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        int64_t ts_start, ts_end;
        origin_row_view.GetTimestamp(1, &ts_start);
        origin_row_view.GetTimestamp(2, &ts_end);
        ASSERT_EQ(ts_start, i * window_size);
        ASSERT_EQ(ts_end, i * window_size + window_size - 1);
        int num_rows;
        origin_row_view.GetInt32(3, &num_rows);
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        T origin_val = *(reinterpret_cast<T*>(ch));
        if (expect_null) {
            ASSERT_EQ(origin_val, 0);
        } else {
            ASSERT_EQ(origin_val, static_cast<T>(i * 4 + 1));
        }
        it->Next();
    }
    return;
}

template <typename T>
void CheckSumAggrResultAfterDelete(std::shared_ptr<Table> aggr_table, DataType data_type,
                                   std::shared_ptr<Aggregator> aggr, int delete_index,
                                   int32_t expect_null = 0) {
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    int64_t window_size = aggr->GetWindowSize();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        int64_t ts_start, ts_end;
        origin_row_view.GetTimestamp(1, &ts_start);
        origin_row_view.GetTimestamp(2, &ts_end);
        ASSERT_EQ(ts_start, i * window_size);
        ASSERT_EQ(ts_end, i * window_size + window_size - 1);
        int num_rows;
        origin_row_view.GetInt32(3, &num_rows);
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        T origin_val = *(reinterpret_cast<T*>(ch));
        if (expect_null) {
            ASSERT_EQ(origin_val, 0);
        } else {
            if (i == delete_index / 2) {
                ASSERT_EQ(origin_val, static_cast<T>(i * 2 + 1));
            } else {
                ASSERT_EQ(origin_val, static_cast<T>(i * 4 + 1));
            }
        }
        it->Next();
    }
    return;
}

template <typename T>
void CheckMinAggrResult(std::shared_ptr<Table> aggr_table, DataType data_type, int32_t expect_null = 0) {
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        auto is_null = origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(is_null, expect_null);
        if (is_null == 0) {
            T origin_val = *reinterpret_cast<T*>(ch);
            ASSERT_EQ(origin_val, static_cast<T>(i * 2));
        }
        it->Next();
    }
    return;
}

template <typename T>
void CheckMinAggrResultAfterDelete(std::shared_ptr<Table> aggr_table, DataType data_type,
                                   int delete_index, int32_t expect_null = 0) {
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    int i = 50 - 1;
    while (it->Valid()) {
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        auto is_null = origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(is_null, expect_null);
        if (is_null == 0) {
            T origin_val = *reinterpret_cast<T*>(ch);
            if (i == delete_index / 2) {
                ASSERT_EQ(origin_val, static_cast<T>(i * 2 + 1));
            } else {
                ASSERT_EQ(origin_val, static_cast<T>(i * 2));
            }
        }
        it->Next();
        i--;
    }
    ASSERT_EQ(i, -1);
    return;
}


template <typename T>
void CheckMaxAggrResultAfterDelete(std::shared_ptr<Table> aggr_table, DataType data_type,
                                   int delete_index, int32_t expect_null = 0) {
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    int i = 50 - 1;
    while (it->Valid()) {
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        auto is_null = origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(is_null, expect_null);
        if (is_null == 0) {
            T origin_val = *reinterpret_cast<T*>(ch);
            if (i == delete_index / 2) {
                ASSERT_EQ(origin_val, static_cast<T>(i * 2));
            } else {
                ASSERT_EQ(origin_val, static_cast<T>(i * 2 + 1));
            }
        }
        it->Next();
        i--;
    }
    ASSERT_EQ(i, -1);
    return;
}

template <typename T>
void CheckMaxAggrResult(std::shared_ptr<Table> aggr_table, DataType data_type, int32_t expect_null = 0) {
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        auto is_null = origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(is_null, expect_null);
        if (is_null == 0) {
            T origin_val = *reinterpret_cast<T*>(ch);
            ASSERT_EQ(origin_val, static_cast<T>(i * 2 + 1));
        }
        it->Next();
    }
    return;
}

void CheckCountAggrResult(std::shared_ptr<Table> aggr_table, DataType data_type, int64_t count) {
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        int64_t origin_val = *reinterpret_cast<int64_t*>(ch);
        ASSERT_EQ(origin_val, count);
        it->Next();
    }
    return;
}

template <typename T>
void CheckAvgAggrResult(std::shared_ptr<Table> aggr_table, DataType data_type, int32_t expect_null = 0) {
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        T origin_val = *(reinterpret_cast<T*>(ch));
        if (expect_null) {
            ASSERT_EQ(origin_val, static_cast<T>(0));
        } else {
            ASSERT_EQ(origin_val, static_cast<T>(i * 4 + 1));
        }
        int64_t cnt = *(reinterpret_cast<int64_t*>(ch + sizeof(T)));
        if (expect_null) {
            ASSERT_EQ(cnt, 0);
        } else {
            ASSERT_EQ(cnt, 2);
        }
        it->Next();
    }
    return;
}

void CheckCountWhereAggrResult(std::shared_ptr<Table> aggr_table, std::shared_ptr<Aggregator> aggr, int64_t count) {
    // there are 101 aggr update, we have 2 filter val
    // every window have one aggr_val
    // there are 99 records in aggr table at the end.
    ASSERT_EQ(aggr_table->GetRecordCnt(), 99);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 98; i >= 0; --i) {
        ASSERT_EQ("id1|id2", it->GetPK());
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        ASSERT_EQ(i / 2 * aggr->GetWindowSize(), it->GetKey());
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        int64_t origin_val = *reinterpret_cast<int64_t*>(ch);
        ASSERT_EQ(origin_val, count);
        int64_t ts_start, ts_end;
        int num_rows;
        origin_row_view.GetString(0, &ch, &ch_length);
        std::string key = std::string(ch, ch_length);
        origin_row_view.GetTimestamp(1, &ts_start);
        origin_row_view.GetTimestamp(2, &ts_end);
        origin_row_view.GetInt32(3, &num_rows);
        origin_row_view.GetString(6, &ch, &ch_length);
        std::string fk = std::string(ch, ch_length);
        ASSERT_EQ(i / 2 * aggr->GetWindowSize(), ts_start);
        ASSERT_EQ(i / 2 * aggr->GetWindowSize() + aggr->GetWindowSize() - 1, ts_end);
        DLOG(INFO) << key << "|" << fk << "[" << ts_start << ", " << ts_end << "]: num_rows = " << num_rows
                   << ", val = " << origin_val;
        it->Next();
    }
    return;
}

TEST_F(AggregatorTest, CreateAggregator) {
    // rows_num window type
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    {
        uint32_t id = counter++;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum",
                                     "ts_col", "1000");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsNum);
        ASSERT_EQ(aggr->GetWindowSize(), 1000);
    }
    // rows_range window type
    {
        uint32_t id = counter++;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum",
                                     "ts_col", "1d");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsRange);
        ASSERT_EQ(aggr->GetWindowSize(), 86400000);
    }
    {
        uint32_t id = counter++;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum",
                                     "ts_col", "2s");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsRange);
        ASSERT_EQ(aggr->GetWindowSize(), 2000);
    }
    {
        uint32_t id = counter++;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum",
                                     "ts_col", "3m");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsRange);
        ASSERT_EQ(aggr->GetWindowSize(), 3 * 60 * 1000);
    }
    {
        uint32_t id = counter++;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum",
                                     "ts_col", "100h");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsRange);
        ASSERT_EQ(aggr->GetWindowSize(), 100 * 60 * 60 * 1000);
    }
    ::openmldb::base::RemoveDirRecursive(folder);
}

TEST_F(AggregatorTest, SumAggregatorUpdate) {
    // rows_num window type
    {
        std::map<std::string, std::string> map;
        std::string folder = "/tmp/" + GenRand() + "/";
        uint32_t id = counter++;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::map<std::string, uint32_t> mapping;
        mapping.insert(std::make_pair("idx", 0));
        std::shared_ptr<Table> aggr_table =
            std::make_shared<MemTable>("t", id, 0, 8, mapping, 0, ::openmldb::type::kAbsoluteTime);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr =
            CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum", "ts_col", "2");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        codec::RowBuilder row_builder(base_table_meta.column_desc());
        ASSERT_TRUE(UpdateAggr(aggr, &row_builder));
        std::string key = "id1|id2";
        ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
        auto it = aggr_table->NewTraverseIterator(0);
        it->SeekToFirst();
        for (int i = 50 - 1; i >= 0; --i) {
            ASSERT_TRUE(it->Valid());
            auto tmp_val = it->GetValue();
            std::string origin_data = tmp_val.ToString();
            codec::RowView origin_row_view(aggr_table_meta.column_desc(),
                                           reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                           origin_data.size());
            char* ch = NULL;
            uint32_t ch_length = 0;
            origin_row_view.GetString(4, &ch, &ch_length);
            int32_t val = *reinterpret_cast<int32_t*>(ch);
            ASSERT_EQ(val, i * 4 + 1);
            it->Next();
        }
        AggrBuffer* last_buffer;
        auto ok = aggr->GetAggrBuffer(key, &last_buffer);
        ASSERT_TRUE(ok);
        ASSERT_EQ(last_buffer->aggr_cnt_, 1);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
        ASSERT_EQ(last_buffer->binlog_offset_, 100);
        ::openmldb::base::RemoveDirRecursive(folder);
    }
    // rows_range window type
    {
        std::shared_ptr<Aggregator> aggregator;
        AggrBuffer* last_buffer;
        std::shared_ptr<Table> aggr_table;
        ASSERT_TRUE(GetUpdatedResult(counter, "col3", "sum", "1s", aggregator, aggr_table, &last_buffer));
        CheckSumAggrResult<int64_t>(aggr_table, DataType::kInt, aggregator);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
        counter += 2;
        ASSERT_TRUE(GetUpdatedResult(counter, "col4", "sum", "1m", aggregator, aggr_table, &last_buffer));
        CheckSumAggrResult<int64_t>(aggr_table, DataType::kSmallInt, aggregator);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
        counter += 2;
        ASSERT_TRUE(GetUpdatedResult(counter, "col5", "sum", "2h", aggregator, aggr_table, &last_buffer));
        CheckSumAggrResult<int64_t>(aggr_table, DataType::kBigInt, aggregator);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
        counter += 2;
        ASSERT_TRUE(GetUpdatedResult(counter, "col6", "sum", "3h", aggregator, aggr_table, &last_buffer));
        CheckSumAggrResult<float>(aggr_table, DataType::kFloat, aggregator);
        ASSERT_EQ(last_buffer->aggr_val_.vfloat, static_cast<float>(100));
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
        counter += 2;
        ASSERT_TRUE(GetUpdatedResult(counter, "col7", "sum", "1d", aggregator, aggr_table, &last_buffer));
        CheckSumAggrResult<double>(aggr_table, DataType::kDouble, aggregator);
        ASSERT_EQ(last_buffer->aggr_val_.vdouble, static_cast<double>(100));
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
        counter += 2;
        ASSERT_TRUE(GetUpdatedResult(counter, "col_null", "sum", "1d", aggregator, aggr_table, &last_buffer));
        CheckSumAggrResult<int64_t>(aggr_table, DataType::kInt, aggregator, 1);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, static_cast<int64_t>(0));
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(0));
    }
}

TEST_F(AggregatorTest, SumAggregatorDelete) {
    // rows_num window type
    {
        std::map<std::string, std::string> map;
        std::string folder = "/tmp/" + GenRand() + "/";
        uint32_t id = counter++;
        int delete_index = 0;
        ::openmldb::api::TableMeta base_table_meta;
        base_table_meta.set_tid(id);
        AddDefaultAggregatorBaseSchema(&base_table_meta);
        id = counter++;
        ::openmldb::api::TableMeta aggr_table_meta;
        aggr_table_meta.set_tid(id);
        AddDefaultAggregatorSchema(&aggr_table_meta);
        std::map<std::string, uint32_t> mapping;
        mapping.insert(std::make_pair("idx", 0));
        std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
        aggr_table->Init();
        std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
            aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
        replicator->Init();
        auto aggr =
            CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum", "ts_col", "2");
        std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
            base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
        base_replicator->Init();
        aggr->Init(base_replicator, nullptr);
        codec::RowBuilder row_builder(base_table_meta.column_desc());
        ASSERT_TRUE(DeleteAndUpdateAggr(aggr, &row_builder, &delete_index));
        std::string key = "id1|id2";
        auto it = aggr_table->NewTraverseIterator(0);
        it->SeekToFirst();
        for (int i = 50 - 1; i >= 0; --i) {
            ASSERT_TRUE(it->Valid());
            auto tmp_val = it->GetValue();
            std::string origin_data = tmp_val.ToString();
            codec::RowView origin_row_view(aggr_table_meta.column_desc(),
                                           reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                           origin_data.size());
            char* ch = NULL;
            uint32_t ch_length = 0;
            origin_row_view.GetString(4, &ch, &ch_length);
            int32_t val = *reinterpret_cast<int32_t*>(ch);
            if (i == delete_index / 2) {
                ASSERT_EQ(val, i * 2 + 1);
            } else {
                ASSERT_EQ(val, i * 4 + 1);
            }
            it->Next();
        }
        AggrBuffer* last_buffer;
        auto ok = aggr->GetAggrBuffer(key, &last_buffer);
        ASSERT_TRUE(ok);
        ASSERT_EQ(last_buffer->aggr_cnt_, 1);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
        ASSERT_EQ(last_buffer->binlog_offset_, 100);
        ::openmldb::base::RemoveDirRecursive(folder);
    }
    // rows_range window type
    {
        int delete_index = 0;
        std::shared_ptr<Aggregator> aggregator;
        AggrBuffer* last_buffer;
        std::shared_ptr<Table> aggr_table;
        ASSERT_TRUE(GetDeleteAndUpdatedResult(counter, "col3", "sum", "1s", aggregator,
                                              aggr_table, &last_buffer, &delete_index));
        CheckSumAggrResultAfterDelete<int64_t>(aggr_table, DataType::kInt, aggregator, delete_index);
        ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
        ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    }
}

TEST_F(AggregatorTest, MinAggregatorUpdate) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    ASSERT_TRUE(GetUpdatedResult(counter, "col3", "MIN", "1s", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<int32_t>(aggr_table, DataType::kInt);
    ASSERT_EQ(last_buffer->aggr_val_.vsmallint, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col4", "min", "1m", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<int16_t>(aggr_table, DataType::kSmallInt);
    ASSERT_EQ(last_buffer->aggr_val_.vint, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col5", "min", "2h", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<int64_t>(aggr_table, DataType::kBigInt);
    ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col6", "min", "3h", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<float>(aggr_table, DataType::kFloat);
    ASSERT_EQ(last_buffer->aggr_val_.vfloat, static_cast<float>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col7", "min", "1d", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<double>(aggr_table, DataType::kDouble);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, static_cast<double>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col8", "min", "2d", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<int32_t>(aggr_table, DataType::kDate);
    ASSERT_EQ(last_buffer->aggr_val_.vint, static_cast<int32_t>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    ASSERT_TRUE(GetUpdatedResult(counter, "col_null", "min", "2d", aggregator, aggr_table, &last_buffer));
    CheckMinAggrResult<int32_t>(aggr_table, DataType::kInt, 1);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(0));
    ASSERT_TRUE(GetUpdatedResult(counter, "col9", "min", "2d", aggregator, aggr_table, &last_buffer));
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(strcmp(ch, "abc"), 0);
        it->Next();
    }
    ASSERT_EQ(strncmp(last_buffer->aggr_val_.vstring.data, "abc", 3), 0);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
}

TEST_F(AggregatorTest, PeakAggregatorDelete) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    int delete_index;
    ASSERT_TRUE(GetMinUpdatedResult(counter, "col3", "MIN", "1s", aggregator,
                                    aggr_table, &last_buffer, &delete_index));
    CheckMinAggrResultAfterDelete<int32_t>(aggr_table, DataType::kInt, delete_index);
    counter += 2;
    ASSERT_TRUE(GetMaxUpdatedResult(counter, "col4", "MAX", "1m", aggregator,
                                    aggr_table, &last_buffer, &delete_index));
    CheckMaxAggrResultAfterDelete<int16_t>(aggr_table, DataType::kSmallInt, delete_index);

}

TEST_F(AggregatorTest, MaxAggregatorUpdate) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    ASSERT_TRUE(GetUpdatedResult(counter, "col3", "MAX", "1s", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<int32_t>(aggr_table, DataType::kInt);
    ASSERT_EQ(last_buffer->aggr_val_.vsmallint, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col4", "Max", "1m", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<int16_t>(aggr_table, DataType::kSmallInt);
    ASSERT_EQ(last_buffer->aggr_val_.vint, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col5", "max", "2h", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<int64_t>(aggr_table, DataType::kBigInt);
    ASSERT_EQ(last_buffer->aggr_val_.vlong, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col6", "max", "3h", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<float>(aggr_table, DataType::kFloat);
    ASSERT_EQ(last_buffer->aggr_val_.vfloat, static_cast<float>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col7", "max", "1d", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<double>(aggr_table, DataType::kDouble);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, static_cast<double>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col8", "max", "2d", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<int32_t>(aggr_table, DataType::kDate);
    ASSERT_EQ(last_buffer->aggr_val_.vint, static_cast<int32_t>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    ASSERT_TRUE(GetUpdatedResult(counter, "col_null", "max", "2d", aggregator, aggr_table, &last_buffer));
    CheckMaxAggrResult<int32_t>(aggr_table, DataType::kInt, 1);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(0));
    ASSERT_TRUE(GetUpdatedResult(counter, "col9", "max", "2d", aggregator, aggr_table, &last_buffer));
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    auto it = aggr_table->NewTraverseIterator(0);
    it->SeekToFirst();
    for (int i = 50 - 1; i >= 0; --i) {
        ASSERT_TRUE(it->Valid());
        auto tmp_val = it->GetValue();
        std::string origin_data = tmp_val.ToString();
        codec::RowView origin_row_view(aggr_table->GetTableMeta()->column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(strcmp(ch, "hello"), 0);
        it->Next();
    }
    ASSERT_EQ(strncmp(last_buffer->aggr_val_.vstring.data, "abc", 3), 0);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
}

TEST_F(AggregatorTest, CountAggregatorUpdate) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    ASSERT_TRUE(GetUpdatedResult(counter, "col3", "count", "1s", aggregator, aggr_table, &last_buffer));
    CheckCountAggrResult(aggr_table, DataType::kInt, 2);
    ASSERT_EQ(last_buffer->non_null_cnt_, 1);
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col_null", "COUNT", "1m", aggregator, aggr_table, &last_buffer));
    CheckCountAggrResult(aggr_table, DataType::kInt, 0);
    ASSERT_EQ(last_buffer->non_null_cnt_, 0);
}

TEST_F(AggregatorTest, AvgAggregatorUpdate) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    ASSERT_TRUE(GetUpdatedResult(counter, "col3", "AVG", "1s", aggregator, aggr_table, &last_buffer));
    CheckAvgAggrResult<double>(aggr_table, DataType::kInt);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col4", "Avg", "1m", aggregator, aggr_table, &last_buffer));
    CheckAvgAggrResult<double>(aggr_table, DataType::kSmallInt);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col5", "avg", "2h", aggregator, aggr_table, &last_buffer));
    CheckAvgAggrResult<double>(aggr_table, DataType::kBigInt);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col6", "avg", "3h", aggregator, aggr_table, &last_buffer));
    CheckAvgAggrResult<double>(aggr_table, DataType::kFloat);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, static_cast<double>(100));
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    counter += 2;
    ASSERT_TRUE(GetUpdatedResult(counter, "col7", "avg", "1d", aggregator, aggr_table, &last_buffer));
    CheckAvgAggrResult<double>(aggr_table, DataType::kDouble);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, 100);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(1));
    ASSERT_TRUE(GetUpdatedResult(counter, "col_null", "avg", "1d", aggregator, aggr_table, &last_buffer));
    CheckAvgAggrResult<double>(aggr_table, DataType::kInt, 1);
    ASSERT_EQ(last_buffer->aggr_val_.vdouble, 0);
    ASSERT_EQ(last_buffer->non_null_cnt_, static_cast<int64_t>(0));
}

TEST_F(AggregatorTest, CountWhereAggregatorUpdate) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    GetUpdatedResult(counter, "col3", "count_where", "1s", aggregator, aggr_table, &last_buffer);
    CheckCountWhereAggrResult(aggr_table, aggregator, 1);
    ASSERT_TRUE(aggregator->GetAggrBuffer("id1|id2", "0", &last_buffer));
    ASSERT_EQ(last_buffer->non_null_cnt_, 1);
    ASSERT_TRUE(aggregator->GetAggrBuffer("id1|id2", "1", &last_buffer));
    ASSERT_EQ(last_buffer->non_null_cnt_, 1);
    counter += 2;
    GetUpdatedResult(counter, "col_null", "count_WHERE", "1m", aggregator, aggr_table, &last_buffer);
    CheckCountWhereAggrResult(aggr_table, aggregator, 0);
    ASSERT_TRUE(aggregator->GetAggrBuffer("id1|id2", "0", &last_buffer));
    ASSERT_EQ(last_buffer->non_null_cnt_, 0);
    ASSERT_TRUE(aggregator->GetAggrBuffer("id1|id2", "1", &last_buffer));
    ASSERT_EQ(last_buffer->non_null_cnt_, 0);
}

TEST_F(AggregatorTest, OutOfOrder) {
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    uint32_t id = counter++;
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    id = counter++;
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr =
        CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "sum", "ts_col", "1s");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, nullptr);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    std::string encoded_row;
    uint32_t row_size = row_builder.CalTotalLength(6);
    encoded_row.resize(row_size);
    ASSERT_TRUE(UpdateAggr(aggr, &row_builder));
    std::string key = "id1|id2";
    ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
    // out of order update
    row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
    row_builder.AppendString("id1", 3);
    row_builder.AppendString("id2", 3);
    row_builder.AppendTimestamp(static_cast<int64_t>(25) * 1000);
    row_builder.AppendInt32(100);
    row_builder.AppendInt16(100);
    row_builder.AppendInt64(100);
    row_builder.AppendFloat(static_cast<float>(4));
    row_builder.AppendDouble(static_cast<double>(5));
    row_builder.AppendDate(100);
    row_builder.AppendString("abc", 3);
    row_builder.AppendNULL();
    bool ok = aggr->Update(key, encoded_row, 101);
    ASSERT_TRUE(ok);
    ASSERT_EQ(aggr_table->GetRecordCnt(), 51);
    auto it = aggr_table->NewTraverseIterator(0);
    it->Seek(key, 25 * 1000 + 100);
    ASSERT_TRUE(it->Valid());

    // the updated agg val
    auto val = it->GetValue();
    std::string origin_data = val.ToString();
    codec::RowView origin_row_view(aggr_table_meta.column_desc(),
                                   reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                   origin_data.size());
    int32_t origin_cnt = 0;
    char* ch = NULL;
    uint32_t ch_length = 0;
    origin_row_view.GetInt32(3, &origin_cnt);
    origin_row_view.GetString(4, &ch, &ch_length);
    ASSERT_EQ(origin_cnt, 3);
    int32_t update_val = *reinterpret_cast<int32_t*>(ch);
    ASSERT_EQ(update_val, 201);

    // the old agg val
    it->Next();
    {
        auto val = it->GetValue();
        std::string origin_data = val.ToString();
        codec::RowView origin_row_view(aggr_table_meta.column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                       origin_data.size());
        int32_t origin_cnt = 0;
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetInt32(3, &origin_cnt);
        origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(origin_cnt, 2);
        int32_t update_val = *reinterpret_cast<int32_t*>(ch);
        ASSERT_EQ(update_val, 101);
    }
    ::openmldb::base::RemoveDirRecursive(folder);
}

TEST_F(AggregatorTest, OutOfOrderCountWhere) {
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    uint32_t id = counter++;
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    id = counter++;
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "count_where",
                                 "ts_col", "1s", "low_card");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, nullptr);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    ASSERT_TRUE(UpdateAggr(aggr, &row_builder));
    ASSERT_EQ(aggr_table->GetRecordCnt(), 99);
    std::string encoded_row;
    uint32_t row_size = row_builder.CalTotalLength(9);
    encoded_row.resize(row_size);
    std::string key = "id1|id2";
    // out of order update
    row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
    row_builder.AppendString("id1", 3);
    row_builder.AppendString("id2", 3);
    row_builder.AppendTimestamp(static_cast<int64_t>(25) * 1000);
    row_builder.AppendInt32(100);
    row_builder.AppendInt16(100);
    row_builder.AppendInt64(100);
    row_builder.AppendFloat(static_cast<float>(4));
    row_builder.AppendDouble(static_cast<double>(5));
    row_builder.AppendDate(100);
    row_builder.AppendString("abc", 3);
    row_builder.AppendNULL();
    row_builder.AppendInt32(0);
    bool ok = aggr->Update(key, encoded_row, 101);
    ASSERT_TRUE(ok);
    ASSERT_EQ(aggr_table->GetRecordCnt(), 100);
    auto it = aggr_table->NewTraverseIterator(0);
    it->Seek(key, 25 * 1000 + 100);
    ASSERT_TRUE(it->Valid());

    // the updated agg val
    auto val = it->GetValue();
    codec::RowView origin_row_view(aggr_table_meta.column_desc(),
                                   reinterpret_cast<int8_t*>(const_cast<char*>(val.data())),
                                   val.size());
    int32_t origin_cnt = 0;
    char* ch = NULL;
    uint32_t ch_length = 0;
    origin_row_view.GetInt32(3, &origin_cnt);
    origin_row_view.GetString(4, &ch, &ch_length);
    ASSERT_EQ(origin_cnt, 2);
    int64_t update_val = *reinterpret_cast<int64_t*>(ch);
    ASSERT_EQ(update_val, 2);

    // the old agg val
    it->Next();
    {
        auto val = it->GetValue();
        codec::RowView origin_row_view(aggr_table_meta.column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(val.data())),
                                       val.size());
        int32_t origin_cnt = 0;
        char* ch = NULL;
        uint32_t ch_length = 0;
        origin_row_view.GetInt32(3, &origin_cnt);
        origin_row_view.GetString(4, &ch, &ch_length);
        ASSERT_EQ(origin_cnt, 1);
        int64_t update_val = *reinterpret_cast<int64_t*>(ch);
        ASSERT_EQ(update_val, 1);
    }
    it->SeekToFirst();
    while (it->Valid()) {
        ASSERT_EQ(key, it->GetPK());
        auto val = it->GetValue();
        codec::RowView row_view(aggr_table_meta.column_desc(),
                                       reinterpret_cast<int8_t*>(const_cast<char*>(val.data())),
                                       val.size());
        std::string pk, fk;
        int64_t ts_start, ts_end;
        int num_rows;
        row_view.GetStrValue(0, &pk);
        row_view.GetStrValue(6, &fk);
        row_view.GetTimestamp(1, &ts_start);
        row_view.GetTimestamp(2, &ts_end);
        row_view.GetInt32(3, &num_rows);
        char* ch = NULL;
        uint32_t ch_length = 0;
        row_view.GetString(4, &ch, &ch_length);
        int64_t update_val = *reinterpret_cast<int64_t*>(ch);
        DLOG(INFO) << pk << "|" << fk << " [" << ts_start << ", " << ts_end << "]"
                   << ", num_rows: " << num_rows << ", update_val:" << update_val;
        it->Next();
    }
    ::openmldb::base::RemoveDirRecursive(folder);
}

TEST_F(AggregatorTest, AlignedCountWhere) {
    std::map<std::string, std::string> map;
    std::string folder = "/tmp/" + GenRand() + "/";
    uint32_t id = counter++;
    ::openmldb::api::TableMeta base_table_meta;
    base_table_meta.set_tid(id);
    AddDefaultAggregatorBaseSchema(&base_table_meta);
    id = counter++;
    ::openmldb::api::TableMeta aggr_table_meta;
    aggr_table_meta.set_tid(id);
    AddDefaultAggregatorSchema(&aggr_table_meta);
    std::shared_ptr<Table> aggr_table = std::make_shared<MemTable>(aggr_table_meta);
    aggr_table->Init();
    std::shared_ptr<LogReplicator> replicator = std::make_shared<LogReplicator>(
        aggr_table->GetId(), aggr_table->GetPid(), folder, map, ::openmldb::replica::kLeaderNode);
    replicator->Init();
    auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, replicator, 0, "col3", "count_where",
                                 "ts_col", "1s", "low_card");
    std::shared_ptr<LogReplicator> base_replicator = std::make_shared<LogReplicator>(
        base_table_meta.tid(), base_table_meta.pid(), folder, map, ::openmldb::replica::kLeaderNode);
    base_replicator->Init();
    aggr->Init(base_replicator, nullptr);
    codec::RowBuilder row_builder(base_table_meta.column_desc());
    ASSERT_TRUE(UpdateAggr(aggr, &row_builder));
    ASSERT_EQ(aggr_table->GetRecordCnt(), 99);
    std::string encoded_row;
    uint32_t row_size = row_builder.CalTotalLength(9);
    encoded_row.resize(row_size);
    std::string key = "id1|id2";

    // curr batch range cannot cover the new row
    {
        int64_t cur_ts = 200;
        row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder.AppendString("id1", 3);
        row_builder.AppendString("id2", 3);
        row_builder.AppendTimestamp(cur_ts * 1000 + 500);
        row_builder.AppendInt32(cur_ts);
        row_builder.AppendInt16(cur_ts);
        row_builder.AppendInt64(cur_ts);
        row_builder.AppendFloat(static_cast<float>(cur_ts));
        row_builder.AppendDouble(static_cast<double>(cur_ts));
        row_builder.AppendDate(cur_ts);
        row_builder.AppendString("abc", 3);
        row_builder.AppendNULL();
        row_builder.AppendInt32(0);
        bool ok = aggr->Update(key, encoded_row, 101);
        ASSERT_TRUE(ok);
        ASSERT_EQ(aggr_table->GetRecordCnt(), 100);
        AggrBuffer* last_buffer;
        aggr->GetAggrBuffer(key, "0", &last_buffer);
        // the curr buffer will be [cur_ts * 1000, cur_ts * 1000 + window_size - 1]
        ASSERT_EQ(cur_ts * 1000, last_buffer->ts_begin_);
        ASSERT_EQ(cur_ts * 1000 + aggr->GetWindowSize() - 1, last_buffer->ts_end_);

        auto it = aggr_table->NewTraverseIterator(0);
        it->SeekToFirst();
        ASSERT_TRUE(it->Valid());
        // the batch range persistent in table will be
        // [0, 999] with filter key 0, [0, 999] with filter key 1,
        // [1000, 1999] with filter key 0, [1000, 1999] with filter key 1,
        // ..., [48000, 48999] with filter key 0, [48000, 48999] with filter key 1,
        // [49000, 49999] with filter key 0, [50000, 50999] with filter key 0
        cur_ts = 50;
        while (it->Valid()) {
            ASSERT_EQ(key, it->GetPK());
            auto val = it->GetValue();
            codec::RowView row_view(aggr_table_meta.column_desc(),
                                    reinterpret_cast<int8_t*>(const_cast<char*>(val.data())), val.size());
            std::string pk, fk;
            int64_t ts_start, ts_end;
            int num_rows;
            row_view.GetStrValue(0, &pk);
            row_view.GetStrValue(6, &fk);
            row_view.GetTimestamp(1, &ts_start);
            row_view.GetTimestamp(2, &ts_end);
            row_view.GetInt32(3, &num_rows);
            char* ch = NULL;
            uint32_t ch_length = 0;
            row_view.GetString(4, &ch, &ch_length);
            int64_t update_val = *reinterpret_cast<int64_t*>(ch);
            DLOG(INFO) << pk << "|" << fk << " [" << ts_start << ", " << ts_end << "]"
                       << ", num_rows: " << num_rows << ", update_val:" << update_val;
            ASSERT_EQ(cur_ts * 1000, ts_start);
            ASSERT_EQ(cur_ts * 1000 + aggr->GetWindowSize() - 1, ts_end);
            if (cur_ts == 50 || cur_ts == 49) {
                cur_ts--;
            } else if (fk == "0") {
                cur_ts--;
            }
            it->Next();
        }
    }

    // there is no aggr entries with this filter key
    {
        int cur_ts = 40;
        row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder.AppendString("id1", 3);
        row_builder.AppendString("id2", 3);
        row_builder.AppendTimestamp(cur_ts * 1000 + 500);
        row_builder.AppendInt32(cur_ts);
        row_builder.AppendInt16(cur_ts);
        row_builder.AppendInt64(cur_ts);
        row_builder.AppendFloat(static_cast<float>(cur_ts));
        row_builder.AppendDouble(static_cast<double>(cur_ts));
        row_builder.AppendDate(cur_ts);
        row_builder.AppendString("abc", 3);
        row_builder.AppendNULL();
        // filter key 2 not exists previously
        row_builder.AppendInt32(2);
        bool ok = aggr->Update(key, encoded_row, 101);
        ASSERT_TRUE(ok);
        ASSERT_EQ(aggr_table->GetRecordCnt(), 100);
        AggrBuffer* last_buffer;
        aggr->GetAggrBuffer(key, "2", &last_buffer);
        // the curr buffer will be [cur_ts * 1000, cur_ts * 1000 + window_size - 1]
        ASSERT_EQ(cur_ts * 1000, last_buffer->ts_begin_);
        ASSERT_EQ(cur_ts * 1000 + aggr->GetWindowSize() - 1, last_buffer->ts_end_);

        auto it = aggr_table->NewTraverseIterator(0);
        it->SeekToFirst();
        ASSERT_TRUE(it->Valid());
        // the batch range persistent in table will be
        // [0, 999] with filter key 0, [0, 999] with filter key 1,
        // [1000, 1999] with filter key 0, [1000, 1999] with filter key 1,
        // ..., [48000, 48999] with filter key 0, [48000, 48999] with filter key 1,
        // [49000, 49999] with filter key 0, [50000, 50999] with filter key 0
        cur_ts = 50;
        while (it->Valid()) {
            ASSERT_EQ(key, it->GetPK());
            auto val = it->GetValue();
            codec::RowView row_view(aggr_table_meta.column_desc(),
                                    reinterpret_cast<int8_t*>(const_cast<char*>(val.data())), val.size());
            std::string pk, fk;
            int64_t ts_start, ts_end;
            int num_rows;
            row_view.GetStrValue(0, &pk);
            row_view.GetStrValue(6, &fk);
            row_view.GetTimestamp(1, &ts_start);
            row_view.GetTimestamp(2, &ts_end);
            row_view.GetInt32(3, &num_rows);
            ASSERT_EQ(cur_ts * 1000, ts_start);
            ASSERT_EQ(cur_ts * 1000 + aggr->GetWindowSize() - 1, ts_end);
            if (cur_ts == 50 || cur_ts == 49) {
                cur_ts--;
            } else if (fk == "0") {
                cur_ts--;
            }
            it->Next();
        }
    }

    // filter key is empty
    {
        int cur_ts = 25;
        row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
        row_builder.AppendString("id1", 3);
        row_builder.AppendString("id2", 3);
        row_builder.AppendTimestamp(cur_ts * 1000 + 500);
        row_builder.AppendInt32(cur_ts);
        row_builder.AppendInt16(cur_ts);
        row_builder.AppendInt64(cur_ts);
        row_builder.AppendFloat(static_cast<float>(cur_ts));
        row_builder.AppendDouble(static_cast<double>(cur_ts));
        row_builder.AppendDate(cur_ts);
        row_builder.AppendString("abc", 3);
        row_builder.AppendNULL();
        // filter key is null
        row_builder.AppendNULL();
        bool ok = aggr->Update(key, encoded_row, 101);
        ASSERT_TRUE(ok);
        ASSERT_EQ(aggr_table->GetRecordCnt(), 100);
        AggrBuffer* last_buffer;
        aggr->GetAggrBuffer(key, &last_buffer);
        // the curr buffer will be [cur_ts * 1000, cur_ts * 1000 + window_size - 1]
        ASSERT_EQ(cur_ts * 1000, last_buffer->ts_begin_);
        ASSERT_EQ(cur_ts * 1000 + aggr->GetWindowSize() - 1, last_buffer->ts_end_);

        auto it = aggr_table->NewTraverseIterator(0);
        it->SeekToFirst();
        ASSERT_TRUE(it->Valid());
        // the batch range persistent in table will be
        // [0, 999] with filter key 0, [0, 999] with filter key 1,
        // [1000, 1999] with filter key 0, [1000, 1999] with filter key 1,
        // ..., [48000, 48999] with filter key 0, [48000, 48999] with filter key 1,
        // [49000, 49999] with filter key 0, [50000, 50999] with filter key 0
        cur_ts = 50;
        while (it->Valid()) {
            ASSERT_EQ(key, it->GetPK());
            auto val = it->GetValue();
            codec::RowView row_view(aggr_table_meta.column_desc(),
                                    reinterpret_cast<int8_t*>(const_cast<char*>(val.data())), val.size());
            std::string pk, fk;
            int64_t ts_start, ts_end;
            row_view.GetStrValue(0, &pk);
            row_view.GetStrValue(6, &fk);
            row_view.GetTimestamp(1, &ts_start);
            row_view.GetTimestamp(2, &ts_end);
            ASSERT_EQ(cur_ts * 1000, ts_start);
            ASSERT_EQ(cur_ts * 1000 + aggr->GetWindowSize() - 1, ts_end);
            if (cur_ts == 50 || cur_ts == 49) {
                cur_ts--;
            } else if (fk == "0") {
                cur_ts--;
            }
            it->Next();
        }
    }
    ::openmldb::base::RemoveDirRecursive(folder);
}

TEST_F(AggregatorTest, FlushAll) {
    std::shared_ptr<Aggregator> aggregator;
    AggrBuffer* last_buffer;
    std::shared_ptr<Table> aggr_table;
    ASSERT_TRUE(GetUpdatedResult(counter, "col3", "sum", "1s", aggregator, aggr_table, &last_buffer));
    aggregator->FlushAll();
    ASSERT_TRUE(aggregator->GetAggrBuffer("id1|id2", &last_buffer));
    ASSERT_EQ(aggr_table->GetRecordCnt(), 51);
    ASSERT_EQ(last_buffer->aggr_cnt_, 1);
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}