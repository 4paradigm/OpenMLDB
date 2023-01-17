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

#include "base/ddl_parser.h"

#include "codec/schema_codec.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace openmldb::base {

std::ostream& operator<<(std::ostream& os, IndexMap& index_map) {
    for (auto& indexes : index_map) {
        os << " {" << indexes.first << "[";
        for (auto& ck : indexes.second) {
            os << ck.ShortDebugString() << ", ";
        }
        os << "]} ";
    }
    return os;
}

class DDLParserTest : public ::testing::Test {
 public:
    void SetUp() override {
        db.set_name("DDLParserTest");
        ASSERT_TRUE(AddTableToDB(
            &db, "behaviourTable",
            {"itemId",    "string", "reqId",  "string",  "tags",   "string", "instanceKey", "string", "eventTime",
             "timestamp", "ip",     "string", "browser", "string", "query",  "string",      "mcuid",  "string",
             "weight",    "double", "page",   "int",     "rank",   "int",    "_i_rank",     "string"}));
        ASSERT_TRUE(AddTableToDB(
            &db, "behaviourTable2",
            {"itemId",    "string", "reqId",  "string",  "tags",   "string", "instanceKey", "string", "eventTime",
             "timestamp", "ip",     "string", "browser", "string", "query",  "string",      "mcuid",  "string",
             "weight",    "double", "page",   "int",     "rank",   "int",    "_i_rank",     "string"}));
        ASSERT_TRUE(AddTableToDB(
            &db, "adinfo",
            {"id", "string", "ingestionTime", "timestamp", "brandName", "string", "name", "string", "brandId", "int"}));
        ASSERT_TRUE(AddTableToDB(&db, "feedbackTable",
                                 {"itemId", "string", "reqId", "string", "instanceKey", "string", "eventTime",
                                  "timestamp", "ingestionTime", "timestamp", "actionValue", "int"}));

        ASSERT_TRUE(AddTableToDB(&db, "t1",
                                 {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                                  "col5", "int64", "col6", "string"}));
        ASSERT_TRUE(AddTableToDB(&db, "t2",
                                 {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                                  "col5", "int64", "col6", "string"}));
    }

    static bool AddColumnToTable(const std::string& col_name, const std::string& col_type,
                                 hybridse::type::TableDef* table) {
        // copy to trim
        auto name = col_name;
        auto type = col_type;
        boost::trim(name);
        boost::trim(type);
        auto col = table->add_columns();
        col->set_name(name);
        auto t = codec::DATA_TYPE_MAP.find(type);
        if (t == codec::DATA_TYPE_MAP.end()) {
            return false;
        }
        col->set_type(codec::SchemaCodec::ConvertType(t->second));
        return true;
    }

    // , , {name, type, name, type, ...}
    static bool AddTableToDB(::hybridse::type::Database* db, const std::string& table_name,
                             std::initializer_list<std::string> cols_def) {
        auto table = db->add_tables();
        table->set_name(table_name);
        auto array = std::data(cols_def);
        for (std::size_t i = 0; i < cols_def.size(); i += 2) {
            auto name = array[i];
            auto type = array[i + 1];
            EXPECT_TRUE(AddColumnToTable(name, type, table));
        }
        return true;
    }
    // , , "col:type,col:type,..."
    static bool AddTableToDB(::hybridse::type::Database* db, const std::string& table_name, const std::string& cols_def,
                             const std::string& col_sep, const std::string& name_type_sep) {
        auto table = db->add_tables();
        table->set_name(table_name);
        std::vector<std::string> cols;
        boost::split(cols, cols_def, boost::is_any_of(col_sep));
        for (auto col : cols) {
            // name: type
            std::vector<std::string> vec;
            boost::trim(col);
            boost::split(vec, col, boost::is_any_of(name_type_sep));
            EXPECT_EQ(vec.size(), 2);

            auto name = vec[0];
            auto type = vec[1];
            EXPECT_TRUE(AddColumnToTable(name, type, table));
        }
        return true;
    }

    // can't handle duplicate table names
    static int GetTableIdxInDB(::hybridse::type::Database* db, const std::string& table_name) {
        for (int i = 0; i < db->tables_size(); ++i) {
            if (db->tables(i).name() == table_name) {
                return i;
            }
        }
        return -1;
    }

    void AddIndexToDB(const base::IndexMap& index_map, ::hybridse::type::Database* db) {
        for (auto& table_indexes : index_map) {
            auto& table = table_indexes.first;
            auto idx = GetTableIdxInDB(db, table);
            ASSERT_NE(idx, -1);
            auto table_def = db->mutable_tables(idx);
            auto& indexes = table_indexes.second;
            for (auto& column_key : indexes) {
                auto index_def = table_def->add_indexes();
                index_def->set_name("parsed_index_" + std::to_string(index_id++));
                index_def->mutable_first_keys()->CopyFrom(column_key.col_name());
                if (column_key.has_ts_name() && !column_key.ts_name().empty()) {
                    index_def->set_second_key(column_key.ts_name());
                    index_def->set_ts_offset(0);
                }
            }
        }
    }
    void ClearAllIndex() {
        for (auto& table : *db.mutable_tables()) {
            table.clear_indexes();
        }
    }

 protected:
    ::hybridse::type::Database db;
    int index_id = 0;
};

// create procedure: only inner plan will be sql compiled.
TEST_F(DDLParserTest, createSpExtractIndexes) {
    std::string query =
        "SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (UNION behaviourTable2 "
        "PARTITION BY itemId ORDER BY "
        "eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

    auto query_map = DDLParser::ExtractIndexes(query, db);

    auto sp_map = DDLParser::ExtractIndexes("create procedure sp1() begin " + query + " end;", db);

    ASSERT_EQ(query_map.size(), sp_map.size());
    LOG(INFO) << "query indexes " << query_map;
    LOG(INFO) << "sp indexes " << sp_map;
}

TEST_F(DDLParserTest, joinExtract) {
    {
        // last join
        auto sql =
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col2 "
            "and t2.col5 >= t1.col5;";

        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;

        // so add index on t2 (key=col2, ts=col5)
        AddIndexToDB(index_map, &db);

        // TODO(hw): check data provider type
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }

    {
        ClearAllIndex();
        // left join
        auto sql = "SELECT t1.col1, t1.col2, t2.col1, t2.col2 FROM t1 left join t2 on t1.col1 = t2.col2;";

        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        // the added index only has key, no ts
        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }
}

TEST_F(DDLParserTest, emptyIndexes) {
    {
        // request data provider, won't get indexes.
        auto sql =
            "SELECT sum(col1) as col1sum FROM (select col1, col2, "
            "col3 from t1) where col1 = 10 and col2 = 20 group by col2, col1;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_TRUE(index_map.empty());
    }
}

TEST_F(DDLParserTest, windowExtractIndexes) {
    {
        // abs 3d
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3d "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }

    {
        ClearAllIndex();
        // 0 < abs < 1min -> 1min
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_EQ(index_map.size(), 1);
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kAbsoluteTime);
        ASSERT_EQ(ttl.abs_ttl(), 1);
        ASSERT_EQ(ttl.lat_ttl(), 0);

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }

    {
        ClearAllIndex();
        // abs 0 -> 1min start, only UNBOUNDED means never gc
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 0s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kAbsoluteTime);
        ASSERT_EQ(ttl.abs_ttl(), 1);
        ASSERT_EQ(ttl.lat_ttl(), 0);

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }

    {
        ClearAllIndex();
        // UNBOUNDED abs -> abs ttl 0
        auto sql =
            "SELECT sum(col3) OVER w1 "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kAbsoluteTime);
        ASSERT_EQ(ttl.abs_ttl(), 0);
        ASSERT_EQ(ttl.lat_ttl(), 0);
    }

    {
        ClearAllIndex();
        // x open preceding abs -> x - 1ms, doesn't matter
        auto sql =
            "SELECT sum(col3) OVER w1 as w1_col3_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3m OPEN PRECEDING AND CURRENT ROW)";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kAbsoluteTime);
        ASSERT_EQ(ttl.abs_ttl(), 3);
        ASSERT_EQ(ttl.lat_ttl(), 0);
    }

    {
        ClearAllIndex();
        // latest
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kLatestTime);
        ASSERT_EQ(ttl.abs_ttl(), 0);
        ASSERT_EQ(ttl.lat_ttl(), 3);

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }

    {
        ClearAllIndex();
        // latest 0, only UNBOUNDED means never gc
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS BETWEEN 0 "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kLatestTime);
        ASSERT_EQ(ttl.abs_ttl(), 0);
        ASSERT_EQ(ttl.lat_ttl(), 1);

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::Explain(sql, db);
    }

    {
        ClearAllIndex();
        // UNBOUNDED latest -> lat ttl 0
        auto sql =
            "SELECT sum(col3) OVER w1 as w1_col3_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        auto index = index_map.begin()->second;
        ASSERT_EQ(index.size(), 1);
        auto ttl = index.begin()->ttl();
        ASSERT_EQ(ttl.ttl_type(), type::TTLType::kLatestTime);
        ASSERT_EQ(ttl.abs_ttl(), 0);
        ASSERT_EQ(ttl.lat_ttl(), 0);
    }

    {
        ClearAllIndex();
        // no order by
        auto sql = "SELECT sum(col1) as col1sum FROM t1 group by col2, col1;";
        // GROUP_BY node
        auto index_map = DDLParser::ExtractIndexesForBatch(sql, db);
        LOG(INFO) << "result for batch: " << index_map;

        // REQUEST_UNION node, this will use index(key=col1,no ts)
        index_map = DDLParser::ExtractIndexes(sql, db);
        LOG(INFO) << "result: " << index_map;
    }
}

TEST_F(DDLParserTest, renameColumns) {
    AddTableToDB(&db, "tt1", {"col1", "string", "col2", "timestamp", "col3", "double"});
    AddTableToDB(&db, "tt2", {"c1", "string", "c2", "timestamp", "c3", "double"});
    auto sql =
        "select col1, col2, col3, sum(col3) over w1 from tt1 window "
        "w1 as (union (select c1 as col1, c2 as col2, c3 as col3 from tt2)  partition by col1 order by col2 rows "
        "between 1000 preceding and current row);";
    auto index_map = DDLParser::ExtractIndexes(sql, db);
    ASSERT_FALSE(index_map.empty());
    LOG(INFO) << index_map;
}

TEST_F(DDLParserTest, mergeNode) {
    AddTableToDB(&db, "t1", "id:int, pk1:string, col1:int32, std_ts:timestamp", ",", ":");
    auto sql =
        "SELECT id, pk1, col1, std_ts,\n"
        "      sum(col1) OVER (PARTITION BY pk1 ORDER BY std_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as "
        "w1_col1_sum,\n"
        "      sum(col1) OVER w2 as w2_col1_sum,\n"
        "      sum(col1) OVER (PARTITION BY pk1 ORDER BY std_ts ROWS_RANGE BETWEEN 30s PRECEDING AND CURRENT ROW) as "
        "w3_col1_sum\n"
        "      FROM t1\n"
        "      WINDOW w2 AS (PARTITION BY pk1 ORDER BY std_ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    auto index_map = DDLParser::ExtractIndexes(sql, db);
    LOG(INFO) << index_map;
    ASSERT_EQ(index_map.size(), 1);
    auto index = index_map.begin()->second;
    ASSERT_EQ(index.size(), 1);
    auto ttl = index.begin()->ttl();
    ASSERT_EQ(ttl.ttl_type(), type::TTLType::kAbsAndLat);
    ASSERT_EQ(ttl.abs_ttl(), 1);
    ASSERT_EQ(ttl.lat_ttl(), 2);
}

TEST_F(DDLParserTest, twoTable) {
    AddTableToDB(&db, "t1", "col0 string, col1 int32, col2 int16, col3 float, col4 double, col5 int64, col6 string",
                 ",", " ");
    AddTableToDB(&db, "t2", "str0 string, str1 string, col3 float, col4 double, col2 int16, col1 int32, col5 int64",
                 ",", " ");
    auto sql =
        "SELECT t1.col1 as id, t1.col2 as t1_col2, t1.col5 as t1_col5, sum(t1.col1) OVER w1 as w1_col1_sum, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, sum(t2.col4) OVER w1 as w1_t2_col4_sum, sum(t2.col2) OVER w1 as "
        "w1_t2_col2_sum, sum(t1.col5) OVER w1 as w1_col5_sum, str1 as t2_str1 FROM t1 last join t2 order by t2.col5 on "
        "t1.col1=t2.col1 and t1.col5 = t2.col5 WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col5 ROWS_RANGE BETWEEN "
        "3 PRECEDING AND CURRENT ROW) limit 10;";
    auto index_map = DDLParser::ExtractIndexes(sql, db);
    LOG(INFO) << index_map;
    ASSERT_EQ(index_map.size(), 2);
    auto t1_index = index_map.find("t1");
    auto t2_index = index_map.find("t2");
    ASSERT_TRUE(t1_index != index_map.end() && t2_index != index_map.end());
    ASSERT_TRUE(t1_index->second.size() == 1 && t2_index->second.size() == 1);
    auto ttl1 = t1_index->second.begin()->ttl();
    auto ttl2 = t2_index->second.begin()->ttl();
    ASSERT_EQ(ttl1.ttl_type(), type::TTLType::kAbsoluteTime);
    ASSERT_EQ(ttl1.abs_ttl(), 1);
    ASSERT_EQ(ttl2.ttl_type(), type::TTLType::kLatestTime);
    ASSERT_EQ(ttl2.abs_ttl(), 1);
}

TEST_F(DDLParserTest, getOutputSchema) {
    std::string query =
        "SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (UNION behaviourTable2 "
        "PARTITION BY itemId ORDER BY "
        "eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

    auto output_schema = DDLParser::GetOutputSchema(query, db);
    ASSERT_EQ(output_schema->GetColumnCnt(), 1);
    ASSERT_EQ(output_schema->GetColumnName(0), "w1_rank_sum");
    ASSERT_EQ(output_schema->GetColumnType(0), hybridse::sdk::DataType::kTypeInt32);
}

TEST_F(DDLParserTest, extractLongWindow) {
    {
        // normal case
        std::string query =
            "SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 "
            "WINDOW w1 AS (PARTITION BY c1 ORDER BY c6 "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w1"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 1);
        ASSERT_EQ(window_infos[0].window_name_, "w1");
        ASSERT_EQ(window_infos[0].aggr_func_, "sum");
        ASSERT_EQ(window_infos[0].aggr_col_, "c3");
        ASSERT_EQ(window_infos[0].partition_col_, "c1");
        ASSERT_EQ(window_infos[0].order_col_, "c6");
        ASSERT_EQ(window_infos[0].bucket_size_, "1000");
    }

    {
        // no long window
        std::string query =
            "SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 "
            "WINDOW w1 AS (PARTITION BY c1 ORDER BY c6 "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w2"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 0);
    }

    {
        // multi long windows
        std::string query =
            "SELECT id, sum(c1) over w1 as m1, max(c2) over w1 as m2, min(c3) over w1 as m3, "
            "avg(c4) over w2 as m4, sum(c5) over w2 as m5, sum(c6) over w2 as m6 "
            "FROM table1 "
            "WINDOW w1 AS (PARTITION BY k1 ORDER BY k3 ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW), "
            "w2 AS (PARTITION BY k2 ORDER BY k4 ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW) ";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w1"] = "1d";
        window_map["w2"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 6);
        ASSERT_EQ(window_infos[0].window_name_, "w1");
        ASSERT_EQ(window_infos[0].aggr_func_, "sum");
        ASSERT_EQ(window_infos[0].aggr_col_, "c1");
        ASSERT_EQ(window_infos[0].partition_col_, "k1");
        ASSERT_EQ(window_infos[0].order_col_, "k3");
        ASSERT_EQ(window_infos[0].bucket_size_, "1d");

        ASSERT_EQ(window_infos[1].window_name_, "w1");
        ASSERT_EQ(window_infos[1].aggr_func_, "max");
        ASSERT_EQ(window_infos[1].aggr_col_, "c2");
        ASSERT_EQ(window_infos[1].partition_col_, "k1");
        ASSERT_EQ(window_infos[1].order_col_, "k3");
        ASSERT_EQ(window_infos[1].bucket_size_, "1d");

        ASSERT_EQ(window_infos[2].window_name_, "w1");
        ASSERT_EQ(window_infos[2].aggr_func_, "min");
        ASSERT_EQ(window_infos[2].aggr_col_, "c3");
        ASSERT_EQ(window_infos[2].partition_col_, "k1");
        ASSERT_EQ(window_infos[2].order_col_, "k3");
        ASSERT_EQ(window_infos[2].bucket_size_, "1d");

        ASSERT_EQ(window_infos[3].window_name_, "w2");
        ASSERT_EQ(window_infos[3].aggr_func_, "avg");
        ASSERT_EQ(window_infos[3].aggr_col_, "c4");
        ASSERT_EQ(window_infos[3].partition_col_, "k2");
        ASSERT_EQ(window_infos[3].order_col_, "k4");
        ASSERT_EQ(window_infos[3].bucket_size_, "1000");

        ASSERT_EQ(window_infos[4].window_name_, "w2");
        ASSERT_EQ(window_infos[4].aggr_func_, "sum");
        ASSERT_EQ(window_infos[4].aggr_col_, "c5");
        ASSERT_EQ(window_infos[4].partition_col_, "k2");
        ASSERT_EQ(window_infos[4].order_col_, "k4");
        ASSERT_EQ(window_infos[4].bucket_size_, "1000");

        ASSERT_EQ(window_infos[5].window_name_, "w2");
        ASSERT_EQ(window_infos[5].aggr_func_, "sum");
        ASSERT_EQ(window_infos[5].aggr_col_, "c6");
        ASSERT_EQ(window_infos[5].partition_col_, "k2");
        ASSERT_EQ(window_infos[5].order_col_, "k4");
        ASSERT_EQ(window_infos[5].bucket_size_, "1000");
    }

    {
        // multi long windows
        std::string query =
            "SELECT id, sum(c1) over w1 as m1, sum(c2) over w1 as m2, sum(c3) over w1 as m3, "
            "sum(c4) over w2 as m4, sum(c5) over w2 as m5, sum(c6) over w2 as m6 "
            "FROM table1 "
            "WINDOW w1 AS (PARTITION BY k1 ORDER BY k3 ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW), "
            "w2 AS (PARTITION BY k2,k3 ORDER BY k4 ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW) ";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w2"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 3);

        ASSERT_EQ(window_infos[0].window_name_, "w2");
        ASSERT_EQ(window_infos[0].aggr_func_, "sum");
        ASSERT_EQ(window_infos[0].aggr_col_, "c4");
        ASSERT_EQ(window_infos[0].partition_col_, "k2,k3");
        ASSERT_EQ(window_infos[0].order_col_, "k4");
        ASSERT_EQ(window_infos[0].bucket_size_, "1000");

        ASSERT_EQ(window_infos[1].window_name_, "w2");
        ASSERT_EQ(window_infos[1].aggr_func_, "sum");
        ASSERT_EQ(window_infos[1].aggr_col_, "c5");
        ASSERT_EQ(window_infos[1].partition_col_, "k2,k3");
        ASSERT_EQ(window_infos[1].order_col_, "k4");
        ASSERT_EQ(window_infos[1].bucket_size_, "1000");

        ASSERT_EQ(window_infos[2].window_name_, "w2");
        ASSERT_EQ(window_infos[2].aggr_func_, "sum");
        ASSERT_EQ(window_infos[2].aggr_col_, "c6");
        ASSERT_EQ(window_infos[2].partition_col_, "k2,k3");
        ASSERT_EQ(window_infos[2].order_col_, "k4");
        ASSERT_EQ(window_infos[2].bucket_size_, "1000");
    }

    {
        // anonymous window
        auto query =
            "SELECT id, pk1, col1, std_ts,\n"
            "      sum(col1) OVER (PARTITION BY pk1 ORDER BY std_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as "
            "w1_col1_sum,\n"
            "      sum(col1) OVER w2 as w2_col1_sum,\n"
            "      sum(col1) OVER (PARTITION BY pk1 ORDER BY std_ts"
            " ROWS_RANGE BETWEEN 30s PRECEDING AND CURRENT ROW) as "
            "w3_col1_sum\n"
            "      FROM t1\n"
            "      WINDOW w2 AS (PARTITION BY pk1 ORDER BY std_ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w2"] = "1d";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 1);

        ASSERT_EQ(window_infos[0].window_name_, "w2");
        ASSERT_EQ(window_infos[0].aggr_func_, "sum");
        ASSERT_EQ(window_infos[0].aggr_col_, "col1");
        ASSERT_EQ(window_infos[0].partition_col_, "pk1");
        ASSERT_EQ(window_infos[0].order_col_, "std_ts");
        ASSERT_EQ(window_infos[0].bucket_size_, "1d");
    }

    {
        // with limit
        std::string query =
            "SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 "
            "WINDOW w1 AS (PARTITION BY c1 ORDER BY c6 "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) limit 10;";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w1"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 1);
        ASSERT_EQ(window_infos[0].window_name_, "w1");
        ASSERT_EQ(window_infos[0].aggr_func_, "sum");
        ASSERT_EQ(window_infos[0].aggr_col_, "c3");
        ASSERT_EQ(window_infos[0].partition_col_, "c1");
        ASSERT_EQ(window_infos[0].order_col_, "c6");
        ASSERT_EQ(window_infos[0].bucket_size_, "1000");
    }

    {
        // xxx_where 1
        std::string query =
            "SELECT c1, c2, count_where(c3, 2=c1) OVER w1 AS w1_c3_sum FROM demo_table1 "
            "WINDOW w1 AS (PARTITION BY c1 ORDER BY c6 "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w1"] = "1s";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 1);
        ASSERT_EQ(window_infos[0].window_name_, "w1");
        ASSERT_EQ(window_infos[0].aggr_func_, "count_where");
        ASSERT_EQ(window_infos[0].aggr_col_, "c3");
        ASSERT_EQ(window_infos[0].partition_col_, "c1");
        ASSERT_EQ(window_infos[0].order_col_, "c6");
        ASSERT_EQ(window_infos[0].bucket_size_, "1s");
        ASSERT_EQ(window_infos[0].filter_col_, "c1");
    }

    {
        // xxx_where 2
        std::string query =
            "SELECT c1, c2, count_where(c3, c1=2) OVER w1 AS w1_c3_sum FROM demo_table1 "
            "WINDOW w1 AS (PARTITION BY c1 ORDER BY c6 "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w1"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(extract_status.IsOK());
        ASSERT_EQ(window_infos.size(), 1);
        ASSERT_EQ(window_infos[0].window_name_, "w1");
        ASSERT_EQ(window_infos[0].aggr_func_, "count_where");
        ASSERT_EQ(window_infos[0].aggr_col_, "c3");
        ASSERT_EQ(window_infos[0].partition_col_, "c1");
        ASSERT_EQ(window_infos[0].order_col_, "c6");
        ASSERT_EQ(window_infos[0].bucket_size_, "1000");
        ASSERT_EQ(window_infos[0].filter_col_, "c1");
    }

    {
        // xxx_where unsupported
        std::string query =
            "SELECT c1, c2, count_where(c3, c1+c2=2) OVER w1 AS w1_c3_sum FROM demo_table1 "
            "WINDOW w1 AS (PARTITION BY c1 ORDER BY c6 "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

        std::unordered_map<std::string, std::string> window_map;
        window_map["w1"] = "1000";
        openmldb::base::LongWindowInfos window_infos;
        auto extract_status = DDLParser::ExtractLongWindowInfos(query, window_map, &window_infos);
        ASSERT_TRUE(!extract_status.IsOK());
    }
}

TEST_F(DDLParserTest, validateSQL) {
    std::string query = "SWLECT 1;";
    auto ret = DDLParser::ValidateSQLInBatch(query, db);
    ASSERT_FALSE(ret.empty());
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0];

    query = "SELECT * from not_exist_table;";
    ret = DDLParser::ValidateSQLInBatch(query, db);
    ASSERT_FALSE(ret.empty());
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0];

    query = "SELECT foo(col1) from t1;";
    ret = DDLParser::ValidateSQLInBatch(query, db);
    ASSERT_FALSE(ret.empty());
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0] << "\n" << ret[1];

    query = "SELECT * FROM t1;";
    ret = DDLParser::ValidateSQLInBatch(query, db);
    ASSERT_TRUE(ret.empty());

    query = "SELECT foo(col1) from t1;";
    ret = DDLParser::ValidateSQLInRequest(query, db);
    ASSERT_FALSE(ret.empty());
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0] << "\n" << ret[1];

    query =
        "SELECT count(col1) over w1 from t1 window w1 as(partition by col0 order by col1 rows between unbounded "
        "preceding and current row);";
    ret = DDLParser::ValidateSQLInRequest(query, db);
    ASSERT_TRUE(ret.empty());
}
}  // namespace openmldb::base

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
