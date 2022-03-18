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

#include "storage/aggregator.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
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

void AddDefaultAggregatorBaseSchema(::openmldb::api::TableMeta* table_meta) {
    table_meta->set_name("t0");
    table_meta->set_pid(0);
    table_meta->set_mode(::openmldb::api::TableMode::kTableLeader);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "id1", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "id2", openmldb::type::DataType::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts_col", openmldb::type::DataType::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col3", openmldb::type::DataType::kInt);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "col4", openmldb::type::DataType::kDouble);

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

    SchemaCodec::SetIndex(table_meta->add_column_key(), "key", "key", "ts_start", ::openmldb::type::kAbsoluteTime, 0,
                          0);
}

TEST_F(AggregatorTest, CreateAggregator) {
    // rows_num window type
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
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, 0, "col3", "sum", "ts_col", "1000");
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
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, 0, "col3", "sum", "ts_col", "1d");
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsRange);
        ASSERT_EQ(aggr->GetWindowSize(), 86400000);
    }
}

TEST_F(AggregatorTest, SumAggregatorUpdate) {
    // rows_num window type
    {
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
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, 0, "col3", "sum", "ts_col", "2");
        codec::RowBuilder row_builder(base_table_meta.column_desc());
        std::string encoded_row;
        uint32_t row_size = row_builder.CalTotalLength(6);
        encoded_row.resize(row_size);
        std::string key = "id1|id2";
        for (int i = 0; i < 100; i++) {
            row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
            row_builder.AppendString("id1", 3);
            row_builder.AppendString("id2", 3);
            row_builder.AppendInt64(i);
            row_builder.AppendInt32(i);
            row_builder.AppendDouble(static_cast<double>(i));
            bool ok = aggr->Update(key, encoded_row, i);
            ASSERT_TRUE(ok);
        }
        ASSERT_EQ(aggr_table->GetRecordCnt(), 50);
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
        auto aggr = CreateAggregator(base_table_meta, aggr_table_meta, aggr_table, 0, "col3", "sum", "ts_col", "1d");
        ASSERT_TRUE(aggr != nullptr);
        ASSERT_EQ(aggr->GetAggrType(), AggrType::kSum);
        ASSERT_EQ(aggr->GetWindowType(), WindowType::kRowsRange);
        ASSERT_EQ(aggr->GetWindowSize(), 86400000);
    }
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
