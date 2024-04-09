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

#include <utility>

#include "absl/cleanup/cleanup.h"
#include "codec/schema_codec.h"
#include "glog/logging.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"

namespace openmldb::base {

// IndexMap helpers
std::ostream& operator<<(std::ostream& os, const IndexMap& index_map) {
    for (auto& indexes : index_map) {
        os << " {" << indexes.first << "[";
        for (auto& ck : indexes.second) {
            os << ck.ShortDebugString() << ", ";
        }
        os << "]} ";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const std::map<std::string, std::vector<std::string>>& readable_map) {
    for (auto& table : readable_map) {
        os << " {" << table.first << "[";
        for (auto& index : table.second) {
            os << index << ", ";
        }
        os << "]} ";
    }
    return os;
}

void CheckEqual(const IndexMap& map1, const IndexMap& map2) {
    ASSERT_EQ(map1.size(), map2.size()) << "map size not equal";
    for (const auto& [key, value] : map1) {
        auto it = map2.find(key);
        if (it == map2.end()) {
            FAIL() << "can't find key " << key << " in map2";
        }
        // check vector equal
        ASSERT_TRUE(std::equal(value.begin(), value.end(), it->second.begin(),
                               [](const common::ColumnKey& x, const common::ColumnKey& y) {
                                   return google::protobuf::util::MessageDifferencer::Equals(x, y);
                               }))
            << "key " << key << " not equal";
    }
}

void StrToTTLType(const std::string& ttl_type, type::TTLType* type) {
    if (ttl_type == "abs") {
        *type = type::TTLType::kAbsoluteTime;
    } else if (ttl_type == "lat") {
        *type = type::TTLType::kLatestTime;
    } else if (ttl_type == "abs&lat") {
        *type = type::TTLType::kAbsAndLat;
    } else if (ttl_type == "abs||lat") {
        *type = type::TTLType::kAbsOrLat;
    } else {
        FAIL() << "unknown ttl type " << ttl_type;
    }
}

common::ColumnKey ParseIndex(const std::string& index_str) {
    common::ColumnKey key;
    std::vector<std::string> vec;
    boost::split(vec, index_str, boost::is_any_of(";"));
    if (vec.size() != 3) {
        LOG(WARNING) << "invalid index str " << index_str;
        return {};
    }
    std::vector<std::string> keys;
    boost::split(keys, vec[0], boost::is_any_of(","));
    for (auto& k : keys) {
        key.add_col_name(k);
    }
    if (!vec[1].empty()) {
        key.set_ts_name(vec[1]);
    }
    // parse ttl in vec[2]
    std::vector<std::string> ttl_parts;
    boost::split(ttl_parts, vec[2], boost::is_any_of(","));
    if (ttl_parts.size() != 3) {
        LOG(WARNING) << "invalid ttl str " << vec[2];
        return {};
    }
    auto ttl = key.mutable_ttl();
    type::TTLType type;
    StrToTTLType(ttl_parts[0], &type);
    ttl->set_ttl_type(type);
    ttl->set_abs_ttl(std::stoull(ttl_parts[1]));
    ttl->set_lat_ttl(std::stoull(ttl_parts[2]));
    return key;
}

// <table, [index1, index2, ...]>
// a human readable string for one index: key1,key2,...;ts;<ttl>. (ts is optional and only one, if no ts, it should be
// key;;<ttl>) <ttl>: type,abs_value,lat_value, e.g. abs,10,0 lat,0,20 abs&lat,10,20 abs||lat,10,20
void CheckEqual(const IndexMap& map, std::map<std::string, std::vector<std::string>>&& readable_map) {
    auto error_message = [](const IndexMap& map, const std::map<std::string, std::vector<std::string>>& readable) {
        std::stringstream ss;
        ss << map << " vs " << readable;
        return ss.str();
    };

    ASSERT_EQ(map.size(), readable_map.size()) << "map size not equal" << error_message(map, readable_map);
    for (const auto& [key, value] : map) {
        auto it = readable_map.find(key);
        if (it == readable_map.end()) {
            FAIL() << "can't find key " << key << " in expected map. " << error_message(map, readable_map);
        }
        // check vector equal
        auto& index_list = it->second;
        ASSERT_EQ(value.size(), index_list.size())
            << "key " << key << " size not equal. " << error_message(map, readable_map);
        for (size_t i = 0; i < value.size(); i++) {
            // fix ColumnKey(e.g. abs type ttl, lat default value is 0, it's ok to set it to 0)
            auto fixed = value[i];
            if (!fixed.ttl().has_abs_ttl()) {
                fixed.mutable_ttl()->set_abs_ttl(0);
            }
            if (!fixed.ttl().has_lat_ttl()) {
                fixed.mutable_ttl()->set_lat_ttl(0);
            }
            auto expected = ParseIndex(index_list[i]);
            ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(fixed, expected))
                << fixed.ShortDebugString() << " != " << expected.ShortDebugString();
        }
    }
}

class DDLParserTest : public ::testing::Test {
 public:
    void SetUp() override {
        db.set_name(DB_NAME);
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
        ASSERT_TRUE(AddTableToDB(&db, "t3",
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

    std::shared_ptr<::hybridse::vm::SimpleCatalog> BuildSingleDBCatalog(const ::hybridse::type::Database& db) {
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        catalog->AddDatabase(db);
        return catalog;
    }

    // db2 is a copy of db, named with db.name() + "2"
    std::shared_ptr<::hybridse::vm::SimpleCatalog> BuildTwoDBCatalog(const ::hybridse::type::Database& db) {
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        catalog->AddDatabase(db);
        auto db2 = db;
        db2.set_name(db.name() + "2");
        catalog->AddDatabase(db2);
        return catalog;
    }

    // single db index map is simple for test, so if input is single db, we just check the index map of db
    IndexMap ExtractIndexesWithSingleDB(const std::string& sql, const ::hybridse::type::Database& db) {
        auto catalog = BuildSingleDBCatalog(db);
        auto index_map = DDLParser::ExtractIndexes(sql, db.name(), catalog);
        if (index_map.empty()) {
            return {};
        }
        return index_map.begin()->second;
    }

    bool EnsurePass(const std::string& sql) {
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        catalog->AddDatabase(db);
        return DDLParser::ValidateSQLInRequest(sql, db.name(), catalog).empty();
    }

 protected:
    std::string DB_NAME = "DDLParserTest";
    ::hybridse::type::Database db;
    int index_id = 0;
};

TEST_F(DDLParserTest, ttlMerge) {
    // values: {abs ttl, lat ttl}
    auto test_func = [](type::TTLType old_type, std::initializer_list<uint64_t> old_values, type::TTLType new_type,
                        std::initializer_list<uint64_t> new_values, bool should_update, type::TTLType expect_type,
                        std::initializer_list<uint64_t> expect_values) {
        common::TTLSt old_ttl, new_ttl, result;
        ASSERT_TRUE(old_values.size() == 2 && new_values.size() == 2 && expect_values.size() == 2);
        old_ttl.set_ttl_type(old_type);
        old_ttl.set_abs_ttl(*old_values.begin());
        old_ttl.set_lat_ttl(*(old_values.begin() + 1));

        new_ttl.set_ttl_type(new_type);
        new_ttl.set_abs_ttl(*new_values.begin());
        new_ttl.set_lat_ttl(*(new_values.begin() + 1));
        {
            SCOPED_TRACE(
                absl::StrCat("old ttl[", old_ttl.ShortDebugString(), "], new ttl[", new_ttl.ShortDebugString(), "]"));
            ASSERT_EQ(TTLMerge(old_ttl, new_ttl, &result), should_update);
            ASSERT_TRUE(result.ttl_type() == expect_type)
                << "expect " << expect_type << ", but got " << result.ttl_type();
            ASSERT_EQ(result.abs_ttl(), *expect_values.begin());
            ASSERT_EQ(result.lat_ttl(), *(expect_values.begin() + 1));
        }
    };

    auto test_same_type = [&](type::TTLType type, std::initializer_list<uint64_t> old_values,
                              std::initializer_list<uint64_t> new_values, bool should_update,
                              std::initializer_list<uint64_t> expect_values) {
        test_func(type, old_values, type, new_values, should_update, type, expect_values);
    };
    // same type
    test_same_type(type::TTLType::kAbsoluteTime, {1, 0}, {2, 0}, true, {2, 0});
    test_same_type(type::TTLType::kAbsoluteTime, {1, 0}, {0, 0}, true, {0, 0});
    test_same_type(type::TTLType::kAbsoluteTime, {0, 0}, {22, 0}, false, {0, 0});
    test_same_type(type::TTLType::kAbsAndLat, {10, 20}, {20, 30}, true, {20, 30});
    test_same_type(type::TTLType::kAbsAndLat, {10, 20}, {5, 30}, true, {10, 30});
    test_same_type(type::TTLType::kAbsOrLat, {10, 20}, {20, 30}, true, {20, 30});
    test_same_type(type::TTLType::kAbsOrLat, {10, 20}, {5, 30}, true, {10, 30});

    // just old ttl after merged, but we do std cvt in merge, result will be changed
    test_func(type::TTLType::kAbsOrLat, {0, 0}, type::TTLType::kAbsOrLat, {1, 2}, true, type::TTLType::kAbsoluteTime,
              {0, 0});
    test_func(type::TTLType::kAbsAndLat, {0, 10}, type::TTLType::kAbsOrLat, {1, 2}, true, type::TTLType::kAbsoluteTime,
              {0, 0});

    // different type
    // abs + lat
    test_func(type::TTLType::kAbsoluteTime, {1, 0}, type::TTLType::kLatestTime, {0, 2}, true, type::TTLType::kAbsAndLat,
              {1, 2});
    test_func(type::TTLType::kLatestTime, {0, 3}, type::TTLType::kAbsoluteTime, {4, 0}, true, type::TTLType::kAbsAndLat,
              {4, 3});

    // abs + complex type
    test_func(type::TTLType::kAbsoluteTime, {10, 0}, type::TTLType::kAbsAndLat, {5, 6}, true, type::TTLType::kAbsAndLat,
              {10, 6});
    test_func(type::TTLType::kAbsoluteTime, {10, 0}, type::TTLType::kAbsAndLat, {15, 6}, true,
              type::TTLType::kAbsAndLat, {15, 6});
    test_func(type::TTLType::kAbsoluteTime, {10, 0}, type::TTLType::kAbsOrLat, {7, 8}, false,
              type::TTLType::kAbsoluteTime, {10, 0});
    test_func(type::TTLType::kAbsoluteTime, {10, 0}, type::TTLType::kAbsOrLat, {17, 8}, true,
              type::TTLType::kAbsoluteTime, {17, 0});
    // swap
    test_func(type::TTLType::kAbsAndLat, {5, 6}, type::TTLType::kAbsoluteTime, {10, 0}, true, type::TTLType::kAbsAndLat,
              {10, 6});
    test_func(type::TTLType::kAbsAndLat, {15, 6}, type::TTLType::kAbsoluteTime, {10, 0}, false,
              type::TTLType::kAbsAndLat, {15, 6});
    test_func(type::TTLType::kAbsOrLat, {7, 8}, type::TTLType::kAbsoluteTime, {10, 0}, true,
              type::TTLType::kAbsoluteTime, {10, 0});
    test_func(type::TTLType::kAbsOrLat, {17, 8}, type::TTLType::kAbsoluteTime, {10, 0}, true,
              type::TTLType::kAbsoluteTime, {17, 0});
    // lat + complex type
    test_func(type::TTLType::kLatestTime, {0, 11}, type::TTLType::kAbsAndLat, {12, 6}, true, type::TTLType::kAbsAndLat,
              {12, 11});
    test_func(type::TTLType::kLatestTime, {0, 11}, type::TTLType::kAbsOrLat, {14, 15}, true, type::TTLType::kLatestTime,
              {0, 15});
    // swap
    test_func(type::TTLType::kAbsAndLat, {12, 6}, type::TTLType::kLatestTime, {0, 11}, true, type::TTLType::kAbsAndLat,
              {12, 11});
    test_func(type::TTLType::kAbsOrLat, {14, 15}, type::TTLType::kLatestTime, {0, 11}, true, type::TTLType::kLatestTime,
              {0, 15});

    // and + or
    test_func(type::TTLType::kAbsAndLat, {10, 20}, type::TTLType::kAbsOrLat, {30, 40}, true, type::TTLType::kAbsAndLat,
              {30, 40});
    test_func(type::TTLType::kAbsOrLat, {10, 20}, type::TTLType::kAbsAndLat, {30, 40}, true, type::TTLType::kAbsAndLat,
              {30, 40});
    test_func(type::TTLType::kAbsAndLat, {0, 20}, type::TTLType::kAbsOrLat, {20, 30}, true,
              type::TTLType::kAbsoluteTime, {0, 0});
}

// create procedure: only inner plan will be sql compiled.
TEST_F(DDLParserTest, createSpExtractIndexes) {
    std::string query =
        "SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (UNION behaviourTable2 "
        "PARTITION BY itemId ORDER BY "
        "eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

    auto query_map = ExtractIndexesWithSingleDB(query, db);

    auto sp_map = ExtractIndexesWithSingleDB("create procedure sp1() begin " + query + " end;", db);

    ASSERT_EQ(query_map.size(), sp_map.size());
    CheckEqual(query_map, sp_map);
}

TEST_F(DDLParserTest, joinExtract) {
    {
        // last join
        auto sql =
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col2 "
            "and t2.col5 >= t1.col5;";

        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // t2[col_name: "col2" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]
        CheckEqual(index_map, {{"t2", {"col2;col5;lat,0,1"}}});

        // so add index on t2 (key=col2, ts=col5)
        AddIndexToDB(index_map, &db);

        // TODO(hw): check data provider type
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }

    {
        ClearAllIndex();
        // left join
        auto sql = "SELECT t1.col1, t1.col2, t2.col1, t2.col2 FROM t1 left join t2 on t1.col1 = t2.col2;";

        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t2[col_name: "col2" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]}
        CheckEqual(index_map, {{"t2", {"col2;;abs,0,0"}}});
        // the added index only has key, no ts
        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }
}

TEST_F(DDLParserTest, complexJoin) {
    {
        // last join t2 must have a simple equal condition
        auto sql =
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 order by t2.col5 on abs(t1.col1) = "
            "t2.col1 "
            "and t2.col5 >= t1.col5;";

        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t2[col_name: "col1" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]}
        CheckEqual(index_map, {{"t2", {"col1;col5;lat,0,1"}}});

        // so add index on t2 (key=col2, ts=col5)
        AddIndexToDB(index_map, &db);

        // TODO(hw): check data provider type
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }

    {
        ClearAllIndex();
        // no simple equal condition, won't extract index
        auto sql =
            "SELECT t1.col1, t1.col2, t2.col1, t2.col2 FROM t1 left join t2 on timestamp(int64(t1.col6)) = "
            "timestamp(int64(t2.col6));";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        ASSERT_TRUE(index_map.empty());
        // must have a simple equal condition
        sql =
            "SELECT t1.col1, t1.col2, t2.col1, t2.col2 FROM t1 left join t2 on timestamp(int64(t1.col6)) = "
            "timestamp(int64(t2.col6)) and t1.col1 = t2.col2;";
        index_map = ExtractIndexesWithSingleDB(sql, db);
        // index is on t2.col2 {t2[col_name: "col2" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]}
        CheckEqual(index_map, {{"t2", {"col2;;abs,0,0"}}});

        // the added index only has key, no ts
        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }
}

TEST_F(DDLParserTest, multiJoin) {
    AddTableToDB(&db, "main", "id string, c1 int64, c2 int64", ",", " ");
    AddTableToDB(&db, "t1", "c1 int64, c2 int64", ",", " ");
    auto sql =
        "select * from (select id as merge_id_0, c1 from main) as out0 last join (select id as merge_id_1, sum(c1) "
        "over w1 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current "
        "row)) as out1 on out0.merge_id_0 = out1.merge_id_1 last join (select id as merge_id_2, t1.c2 from main last "
        "join t1 on main.c1==t1.c1) as out2 on out0.merge_id_0 = out2.merge_id_2 last join (select id as merge_id_3, "
        "t1.c2 from main last join t1 order by t1.c2 on main.c1==t1.c1) as out3 on out0.merge_id_0 = out3.merge_id_3 "
        "last join (select id as merge_id_4, sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from "
        "t1) partition by c1 order by c2 rows between unbounded preceding and current row)) as out4 on out0.merge_id_0 "
        "= out4.merge_id_4;";
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    // {main[col_name: "c1" ts_name: "c2" ttl { ttl_type: kLatestTime abs_ttl: 0 lat_ttl: 0 }, ]}
    // {t1[col_name: "c1" ttl { ttl_type: kLatestTime lat_ttl: 1 }, col_name: "c1" ts_name: "c2" ttl { ttl_type:
    // kLatestTime abs_ttl: 0 lat_ttl: 0 }, ]}
    CheckEqual(index_map, {{"main", {"c1;c2;lat,0,0"}}, {"t1", {"c1;;lat,0,1", "c1;c2;lat,0,0"}}});
}

TEST_F(DDLParserTest, emptyIndexes) {
    {
        // invalid sql in online serving(agg on table), won't get indexes.
        auto sql =
            "SELECT sum(col1) as col1sum FROM (select col1, col2, "
            "col3 from t1) where col1 = 10 and col2 = 20 group by col2, col1;";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
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
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t1[col_name: "col1" ts_name: "col5" ttl { ttl_type: kAbsoluteTime abs_ttl: 4320 }, ]}
        CheckEqual(index_map, {{"t1", {"col1;col5;abs,4320,0"}}});

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }

    {
        ClearAllIndex();
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // 0 < abs < 1min -> 1min, abs ttl value is in min, be careful
        //  {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kAbsoluteTime abs_ttl: 1 }, ]}
        CheckEqual(index_map, {{"t1", {"col2;col5;abs,1,0"}}});

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }

    {
        ClearAllIndex();
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 0s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // abs 0 -> 1min start, 0 == UNBOUNDED means never gc
        // {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kAbsoluteTime abs_ttl: 1 }, ]}
        CheckEqual(index_map, {{"t1", {"col2;col5;abs,1,0"}}});

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }

    {
        ClearAllIndex();
        // UNBOUNDED abs -> abs ttl 0
        auto sql =
            "SELECT sum(col3) OVER w1 "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        CheckEqual(index_map, {{"t1", {"col2;col5;abs,0,0"}}});
    }

    {
        ClearAllIndex();
        // x open preceding abs -> x - 1ms, doesn't matter
        auto sql =
            "SELECT sum(col3) OVER w1 as w1_col3_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3m OPEN PRECEDING AND CURRENT ROW)";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kAbsoluteTime abs_ttl: 3 }, ]}
        CheckEqual(index_map, {{"t1", {"col2;col5;abs,3,0"}}});
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
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 3 }, ]}
        CheckEqual(index_map, {{"t1", {"col2;col5;lat,0,3"}}});

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
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
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]}
        CheckEqual(index_map, {{"t1", {"col2;col5;lat,0,1"}}});

        AddIndexToDB(index_map, &db);
        LOG(INFO) << "after add index:\n" << DDLParser::PhysicalPlan(sql, db);
    }

    {
        ClearAllIndex();
        // UNBOUNDED latest -> lat ttl 0
        auto sql =
            "SELECT sum(col3) OVER w1 as w1_col3_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        // {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 0 }, ]}
        CheckEqual(index_map, {{"t1", {"col2;col5;lat,0,0"}}});
    }

    // test extract index for batch mode
    // {
    //     ClearAllIndex();
    //     // no order by
    //     auto sql = "SELECT sum(col1) as col1sum FROM t1 group by col2, col1;";
    //     // GROUP_BY node
    //     auto index_map = ExtractIndexesWithSingleDBForBatch(sql, db);
    //     // {t1[col_name: "col1" col_name: "col2" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]}
    //     CheckEqual(index_map, {{"t1", {"col1,col2;;lat,0,1"}}});

    //     // REQUEST_UNION node, this will use index(key=col1,no ts)
    //     index_map = ExtractIndexesWithSingleDB(sql, db);
    //     LOG(INFO) << "result: " << index_map;
    // }
}

TEST_F(DDLParserTest, renameColumns) {
    AddTableToDB(&db, "tt1", {"col1", "string", "col2", "timestamp", "col3", "double"});
    AddTableToDB(&db, "tt2", {"c1", "string", "c2", "timestamp", "c3", "double"});
    auto sql =
        "select col1, col2, col3, sum(col3) over w1 from tt1 window "
        "w1 as (union (select c1 as col1, c2 as col2, c3 as col3 from tt2)  partition by col1 order by col2 rows "
        "between 1000 preceding and current row);";
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    // {tt1[col_name: "col1" ts_name: "col2" ttl { ttl_type: kLatestTime lat_ttl: 1000 }, ]}  {tt2[col_name: "c1"
    // ts_name: "c2" ttl { ttl_type: kLatestTime lat_ttl: 1000 }, ]}
    CheckEqual(index_map, {{"tt1", {"col1;col2;lat,0,1000"}}, {"tt2", {"c1;c2;lat,0,1000"}}});
}

TEST_F(DDLParserTest, lastJoinOverLastJoin) {
    absl::Cleanup clean = [&]() { ClearAllIndex(); };
    auto sql =
        R"(select * from t1 last join (select * from t2 last join feedbackTable on t2.col1 = feedbackTable.actionValue) tx on t1.col0 = tx.col0)";
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    CheckEqual(index_map, {{"t2", {"col0;;lat,0,1"}}, {"feedbackTable", {"actionValue;;lat,0,1"}}});

    AddIndexToDB(index_map, &db);
    EXPECT_TRUE(EnsurePass(sql));
}

TEST_F(DDLParserTest, lastJoinWindow) {
    absl::Cleanup clean = [&]() { ClearAllIndex(); };
    auto sql =
        R"(select * from t1 last join (
            select *, count(col0) over w as agg from t2
            window w as (partition by col1 order by col5 rows between 3 preceding and current row))
          tx on t1.col0 = tx.col0)";
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    std::map<std::string, std::vector<std::string>> expect = {{"t2", {"col0;;lat,0,1", "col1;col5;lat,0,3"}}};
    CheckEqual(index_map, std::forward<std::map<std::string, std::vector<std::string>>>(expect));

    AddIndexToDB(index_map, &db);
    EXPECT_TRUE(EnsurePass(sql));
}

TEST_F(DDLParserTest, lastJoinUnion) {
    absl::Cleanup clean = [&]() { ClearAllIndex(); };
    auto sql =
        R"(select * from t1 last join (
           select * from t2 union all select * from t3
           ) tx on t1.col0 = tx.col0)";
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    std::map<std::string, std::vector<std::string>> expect = {{"t2", {"col0;;lat,0,1"}}, {"t3", {"col0;;lat,0,1"}}};
    CheckEqual(index_map, std::forward<std::map<std::string, std::vector<std::string>>>(expect));

    AddIndexToDB(index_map, &db);
    EXPECT_TRUE(EnsurePass(sql));
}

TEST_F(DDLParserTest, windowWithoutOrderBy) {
    {
        absl::Cleanup clean = [&]() { ClearAllIndex(); };
        auto sql =
            R"(select *, count(col0) over w as agg from t1
          window w as (partition by col1 rows between unbounded preceding and current row))";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        std::map<std::string, std::vector<std::string>> expect = {{"t1", {"col1;;lat,0,0"}}};
        CheckEqual(index_map, std::forward<std::map<std::string, std::vector<std::string>>>(expect));

        AddIndexToDB(index_map, &db);
        ASSERT_TRUE(EnsurePass(sql));
    }
    {
        absl::Cleanup clean = [&]() { ClearAllIndex(); };
        auto sql =
            R"(select *, count(col0) over w as agg from t1
          window w as (partition by col1 rows between 4 preceding and current row))";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        std::map<std::string, std::vector<std::string>> expect = {{"t1", {"col1;;lat,0,4"}}};
        CheckEqual(index_map, std::forward<std::map<std::string, std::vector<std::string>>>(expect));

        AddIndexToDB(index_map, &db);
        ASSERT_TRUE(EnsurePass(sql));
    }
    {
        absl::Cleanup clean = [&]() { ClearAllIndex(); };
        auto sql =
            R"(select *, count(col0) over w as agg from t1
          window w as (partition by col1 rows_range between unbounded preceding and current row))";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        std::map<std::string, std::vector<std::string>> expect = {{"t1", {"col1;;abs,0,0"}}};
        CheckEqual(index_map, std::forward<std::map<std::string, std::vector<std::string>>>(expect));

        AddIndexToDB(index_map, &db);
        ASSERT_TRUE(EnsurePass(sql));
    }
    {
        absl::Cleanup clean = [&]() { ClearAllIndex(); };
        // invalid SQL
        auto sql =
            R"(select *, count(col0) over w as agg from t1
          window w as (partition by col1 rows_range between 4 preceding and current row))";
        auto index_map = ExtractIndexesWithSingleDB(sql, db);
        ASSERT_TRUE(index_map.empty());
    }
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
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    // {t1[col_name: "pk1" ts_name: "std_ts" ttl { ttl_type: kAbsAndLat abs_ttl: 1 lat_ttl: 2 }, ]}
    CheckEqual(index_map, {{"t1", {"pk1;std_ts;abs&lat,1,2"}}});
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
    auto index_map = ExtractIndexesWithSingleDB(sql, db);
    // {t1[col_name: "col2" ts_name: "col5" ttl { ttl_type: kAbsoluteTime abs_ttl: 1 }, ]}
    // {t2[col_name: "col1" col_name: "col5" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]}
    CheckEqual(index_map, {{"t1", {"col2;col5;abs,1,0"}}, {"t2", {"col1,col5;col5;lat,0,1"}}});
}

TEST_F(DDLParserTest, multiDBExtractIndexes) {
    auto catalog = BuildTwoDBCatalog(db);
    // t2 from db2
    auto sql = std::string("SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join ") + DB_NAME +
               "2.t2 t2 order by t2.col5 on t1.col1 = t2.col2 "
               "and t2.col5 >= t1.col5;";
    auto index_map = DDLParser::ExtractIndexes(sql, DB_NAME, catalog);
    // last join, only t2 needs index
    ASSERT_EQ(index_map.size(), 1);
    // t2[col_name: "col2" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]
    CheckEqual(index_map[DB_NAME + "2"], {{"t2", {"col2;col5;lat,0,1"}}});

    // all tables are <db>.<table> style, using db can be empty
    sql = std::string("SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM ") + DB_NAME + ".t1 t1 last join " + DB_NAME +
          "2.t2 t2 order by t2.col5 on t1.col1 = t2.col2 "
          "and t2.col5 >= t1.col5;";
    index_map = DDLParser::ExtractIndexes(sql, "", catalog);
    // last join, only t2 needs index
    ASSERT_EQ(index_map.size(), 1);
    // t2[col_name: "col2" ts_name: "col5" ttl { ttl_type: kLatestTime lat_ttl: 1 }, ]
    CheckEqual(index_map[DB_NAME + "2"], {{"t2", {"col2;col5;lat,0,1"}}});

    // explain in multi db, needs index
    // create a new catalog
    catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
    catalog->AddDatabase(db);
    auto db2 = db;
    db2.set_name(db.name() + "2");
    AddIndexToDB(index_map[DB_NAME + "2"], &db2);
    catalog->AddDatabase(db2);
    hybridse::vm::ExplainOutput output;
    ASSERT_TRUE(DDLParser::Explain(sql, "", catalog, &output));
}

TEST_F(DDLParserTest, getOutputSchema) {
    std::string query =
        "SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (UNION behaviourTable2 "
        "PARTITION BY itemId ORDER BY "
        "eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

    auto output_schema = DDLParser::GetOutputSchema(query, db.name(), BuildSingleDBCatalog(db));
    ASSERT_EQ(output_schema->GetColumnCnt(), 1);
    ASSERT_EQ(output_schema->GetColumnName(0), "w1_rank_sum");
    ASSERT_EQ(output_schema->GetColumnType(0), hybridse::sdk::DataType::kTypeInt32);

    // no used db, can't query by <table> style(use first db in java sdk wrapper)
    ASSERT_FALSE(DDLParser::GetOutputSchema(query, "", BuildSingleDBCatalog(db)));
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
    auto catalog = BuildSingleDBCatalog(db);

    std::string query = "SWLECT 1;";
    auto ret = DDLParser::ValidateSQLInBatch(query, db.name(), catalog);
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0];

    query = "SELECT * from not_exist_table;";
    ret = DDLParser::ValidateSQLInBatch(query, db.name(), catalog);
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0];

    query = "SELECT foo(col1) from t1;";
    ret = DDLParser::ValidateSQLInBatch(query, db.name(), catalog);
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0] << "\n" << ret[1];

    query = "SELECT * FROM t1;";
    ret = DDLParser::ValidateSQLInBatch(query, db.name(), catalog);
    ASSERT_TRUE(ret.empty());

    query = "SELECT foo(col1) from t1;";
    ret = DDLParser::ValidateSQLInRequest(query, db.name(), catalog);
    ASSERT_EQ(ret.size(), 2);
    LOG(INFO) << ret[0] << "\n" << ret[1];

    query =
        "SELECT count(col1) over w1 from t1 window w1 as(partition by col0 order by col1 rows between unbounded "
        "preceding and current row);";
    ret = DDLParser::ValidateSQLInRequest(query, db.name(), catalog);
    ASSERT_TRUE(ret.empty());

    // no used db, can't do <table> query(use first db is the logic in java sdk wrapper)
    query = "SELECT * FROM t1;";
    ret = DDLParser::ValidateSQLInRequest(query, "", catalog);
    ASSERT_EQ(ret.size(), 2);
    ASSERT_STREQ(ret[0].c_str(), "Fail to transform data_provider op: table .t1 not exist!");
    ASSERT_TRUE(DDLParser::ValidateSQLInRequest("SELECT * FROM " + db.name() + ".t1;", "", catalog).empty());
}
}  // namespace openmldb::base

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
