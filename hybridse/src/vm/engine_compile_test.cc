/*
 * Copyright 2021 4Paradigm
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

#include "absl/strings/str_join.h"
#include "case/case_data_mock.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "testing/engine_test_base.h"
#include "udf/openmldb_udf.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace hybridse {
namespace vm {
using hybridse::sqlcase::CaseDataMock;
class EngineCompileTest : public ::testing::TestWithParam<SqlCase> {
 public:
    EngineCompileTest() {}
    virtual ~EngineCompileTest() {}
};
TEST_F(EngineCompileTest, EngineLRUCacheTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    AddTable(db, table_def);

    // table t2
    hybridse::type::TableDef table_def2;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def2);
    table_def2.set_name("t2");
    AddTable(db, table_def2);
    catalog->AddDatabase(db);

    // Simple Engine
    EngineOptions options;
    options.SetCompileOnly(true);
    options.SetMaxSqlCacheSize(1);
    Engine engine(catalog, options);

    std::string sql = "select col1, col2 from t1;";
    std::string sql2 = "select col1, col2 as cl2 from t1;";
    {
        base::Status get_status;
        BatchRunSession bsession1;
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession1, get_status)) << get_status;
        ASSERT_EQ(get_status.code, common::kOk);
        BatchRunSession bsession2;
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_EQ(bsession1.GetCompileInfo().get(), bsession2.GetCompileInfo().get());
        BatchRunSession bsession3;
        ASSERT_TRUE(engine.Get(sql2, "simple_db", bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(), bsession2.GetCompileInfo().get());
    }
}


TEST_F(EngineCompileTest, EngineWithParameterizedLRUCacheTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index0");
    index->add_first_keys("col0");
    index->set_second_key("col5");
    AddTable(db, table_def);

    catalog->AddDatabase(db);

    // Simple Engine
    EngineOptions options;
    options.SetCompileOnly(true);
    options.SetMaxSqlCacheSize(1);
    Engine engine(catalog, options);

    hybridse::codec::Schema parameter_schema;
    {
        auto column = parameter_schema.Add();
        column->set_type(hybridse::type::kVarchar);
    }
    {
        auto column = parameter_schema.Add();
        column->set_type(hybridse::type::kInt64);
    }

    hybridse::codec::Schema parameter_schema2;
    {
        auto column = parameter_schema2.Add();
        column->set_type(hybridse::type::kInt64);
    }
    {
        auto column = parameter_schema2.Add();
        column->set_type(hybridse::type::kInt64);
    }
    std::string sql = "select col1, col2 from t1 where col0=? and col5<?;";
    std::string sql2 = "select col1, col2 as cl2 from t1 where col0=? and col5<?;";
    {
        base::Status get_status;
        BatchRunSession bsession1;
        bsession1.SetParameterSchema(parameter_schema);
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession1, get_status)) << get_status;
        ASSERT_EQ(get_status.code, common::kOk);
        BatchRunSession bsession2;
        bsession2.SetParameterSchema(parameter_schema);
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_EQ(bsession1.GetCompileInfo().get(), bsession2.GetCompileInfo().get());
        BatchRunSession bsession3;
        bsession3.SetParameterSchema(parameter_schema);
        ASSERT_TRUE(engine.Get(sql2, "simple_db", bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(), bsession3.GetCompileInfo().get());

        BatchRunSession bsession4;
        bsession4.SetParameterSchema(parameter_schema2);
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession4, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(), bsession4.GetCompileInfo().get());
    }
}


TEST_F(EngineCompileTest, EngineEmptyDefaultDBLRUCacheTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    {
        // database simple_db
        hybridse::type::Database db;
        db.set_name("db1");

        // table t1
        hybridse::type::TableDef table_def;
        sqlcase::CaseSchemaMock::BuildTableDef(table_def);
        table_def.set_name("t1");
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
        AddTable(db, table_def);
        catalog->AddDatabase(db);
    }
    {
        // database simple_db
        hybridse::type::Database db;
        db.set_name("db2");
        // table t2
        hybridse::type::TableDef table_def2;
        sqlcase::CaseSchemaMock::BuildTableDef(table_def2);
        table_def2.set_name("t2");
        AddTable(db, table_def2);
        catalog->AddDatabase(db);
    }

    EngineOptions options;
    Engine engine(catalog, options);
    std::string sql =
        "select db1.t1.col1, db1.t1.col2,db2.t2.col3,db2.t2.col4 from db1.t1 last join db2.t2 ORDER BY db2.t2.col5 "
        "on db1.t1.col1=db2.t2.col1;";
    std::string sql2 =
        "select db1.t1.col1, db1.t1.col3, db2.t2.col3,db2.t2.col4 from db1.t1 last join db2.t2 ORDER BY db2.t2.col5 "
        "on db1.t1.col1=db2.t2.col1;";
    {
        base::Status get_status;
        BatchRunSession bsession1;
        ASSERT_TRUE(engine.Get(sql, "", bsession1, get_status)) << get_status;
        ASSERT_EQ(get_status.code, common::kOk);
        BatchRunSession bsession2;
        ASSERT_TRUE(engine.Get(sql, "", bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_EQ(bsession1.GetCompileInfo().get(), bsession2.GetCompileInfo().get());

        // cache compile info is different under different default db
        BatchRunSession bsession3;
        ASSERT_TRUE(engine.Get(sql, "default_db", bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(), bsession3.GetCompileInfo().get());

        // cache compile info is different under different default db
        BatchRunSession bsession4;
        ASSERT_TRUE(engine.Get(sql2, "", bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(), bsession4.GetCompileInfo().get());
    }
}
TEST_F(EngineCompileTest, EngineCompileOnlyTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    AddTable(db, table_def);

    // table t2
    hybridse::type::TableDef table_def2;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def2);
    table_def2.set_name("t2");
    AddTable(db, table_def2);

    catalog->AddDatabase(db);

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 last join t2 "
            "order by t2.col5 on t1.col1 = t2.col2;"};
        EngineOptions options;
        options.SetCompileOnly(true);
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_TRUE(engine.Get(sqlstr, "simple_db", session, get_status));
        }
    }

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 inner join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;"};
        EngineOptions options;
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_FALSE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }
}

using CaseType = std::pair<std::string, std::set<std::pair<std::string, std::string>>>;

class GetDependentTablesTest : public ::testing::TestWithParam<CaseType> {
 public:
    GetDependentTablesTest() {}
    ~GetDependentTablesTest() {}

    void SetUp() override {
        auto catalog = BuildSimpleCatalog();

        // database simple_db
        hybridse::type::Database db;
        db.set_name("simple_db");

        {
            // table t1
            hybridse::type::TableDef table_def;
            sqlcase::CaseSchemaMock::BuildTableDef(table_def);
            table_def.set_name("t1");
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index12");
            index->add_first_keys("col1");
            index->add_first_keys("col2");
            index->set_second_key("col5");
            AddTable(db, table_def);
        }

        {
            // table t2
            hybridse::type::TableDef table_def2;
            sqlcase::CaseSchemaMock::BuildTableDef(table_def2);
            table_def2.set_name("t2");
            AddTable(db, table_def2);
        }

        {
            // table t3
            hybridse::type::TableDef table_def;
            sqlcase::CaseSchemaMock::BuildTableDef(table_def);
            table_def.set_name("t3");
            AddTable(db, table_def);
        }

        catalog->AddDatabase(db);

        {
            hybridse::type::Database db;
            db.set_name("db1");
            hybridse::type::TableDef table_def;
            sqlcase::CaseSchemaMock::BuildTableDef(table_def);
            table_def.set_name("t1");
            AddTable(db, table_def);
            catalog->AddDatabase(db);
        }

        {
            hybridse::type::Database db;
            db.set_name("db2");
            hybridse::type::TableDef table_def;
            sqlcase::CaseSchemaMock::BuildTableDef(table_def);
            table_def.set_name("t2");
            AddTable(db, table_def);
            catalog->AddDatabase(db);
        }

        base::Status get_status;
        EngineOptions options;
        engine = std::make_shared<Engine>(catalog, options);
    }

    static std::vector<CaseType> GetCases() {
        return std::vector<CaseType>{
            {"SELECT col1, col2 from t1;", {{"simple_db", "t1"}}},
            {R"(SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1
                 last join t2 order by t2.col5 on t1.col1 = t2.col2;)",
             {{"simple_db", "t1"}, {"simple_db", "t2"}}},

            {R"(SELECT t1.COL1, t1.COL2, db2.t2.COL1, db2.t2.COL2 FROM t1
last join db2.t2 order by db2.t2.col5 on t1.col1 = db2.t2.col2;)",
             {{"simple_db", "t1"}, {"db2", "t2"}}},

            {R"(SELECT db1.t1.COL1, db1.t1.COL2, db2.t2.COL1, db2.t2.COL2 FROM db1.t1
last join db2.t2 order by db2.t2.col5 on db1.t1.col1 = db2.t2.col2;)",
             {{"db1", "t1"}, {"db2", "t2"}}},

            {R"(SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1
last join t2 order by t2.col5 on t1.col1 = t2.col2;)",
             {{"simple_db", "t1"}, {"simple_db", "t2"}}},

            {
                R"(SELECT t1.col1 as id, t1.col2 as t1_col2, t1.col5 as t1_col5,      count(t1.col1) OVER w1 as w1_col1_sum, sum(t1.col3) OVER w1 as w1_col3_sum,
      sum(t2.col4) OVER w1 as w1_t2_col4_sum, sum(t2.col2) OVER w1 as w1_t2_col2_sum,
      sum(t1.col5) OVER w1 as w1_col5_sum,
                            FROM t1
      last join t2 order by t2.col5 on t1.col1=t2.col1 and t1.col5 = t2.col5
      WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;)",
                {{"simple_db", "t1"}, {"simple_db", "t2"}}},

            {
                R"(SELECT
                col2, col5,
                sum(col1) OVER w1 as w1_col1_sum,
                sum(col3) OVER w1 as w1_col3_sum,
                sum(col4) OVER w1 as w1_col4_sum,
                sum(col2) OVER w1 as w1_col2_sum,
                sum(col5) OVER w1 as w1_col5_sum,
                count(col1) OVER w1 as w1_col1_cnt, col1,
                col6 as col6 FROM t1
               WINDOW w1 AS (UNION t2 PARTITION BY t1.col2 ORDER BY t1.col5 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)
               limit 10;)",
                {{"simple_db", "t1"}, {"simple_db", "t2"}}},

            {
                R"(SELECT
                t1.col1 as id,
                t1.col2 as t1_col2,
                t1.col5 as t1_col5,
                count(t1.col1) OVER w1 as w1_col1_sum,
                sum(t1.col3) OVER w1 as w1_col3_sum,
                sum(t2.col4) OVER w1 as w1_t2_col4_sum,
                sum(t2.col2) OVER w1 as w1_t2_col2_sum,
                sum(t1.col5) OVER w1 as w1_col5_sum,
                FROM t1 last join t2 order by t2.col5
                on t1.col1=t2.col1 and t1.col5 = t2.col5
                WINDOW w1 AS (
                    UNION (select *, * from t3)
                    PARTITION BY t1.col2 ORDER BY t1.col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)
               limit 10;)",
                {
                    {"simple_db", "t1"},
                    {"simple_db", "t2"},
                    {"simple_db", "t3"},
                }},
            {"SELECT substr(\"hello world\", 3, 6);", {}},
            {R"(with
                    t3 as (select concat(col0, 'S') as col0, col1, col5 from t1),
                    t4 as (select col0, col1, col5 from t2)
                SELECT t3.col1 as id, t3.col0, t4.col0,
                FROM t3 last join t4 order by t4.col5 on t3.col1=t4.col1
                )",
             {{"simple_db", "t1"}, {"simple_db", "t2"}}},
        };
    }

    std::shared_ptr<Engine> engine;
};

INSTANTIATE_TEST_SUITE_P(Func, GetDependentTablesTest, ::testing::ValuesIn(GetDependentTablesTest::GetCases()));

TEST_P(GetDependentTablesTest, UnitTest) {
    auto& pair = GetParam();
    std::string sqlstr = pair.first;
    boost::to_lower(sqlstr);
    std::set<std::pair<std::string, std::string>> tables;
    base::Status get_status;
    ASSERT_TRUE(engine->GetDependentTables(sqlstr, "simple_db", &tables, get_status)) << get_status;
    ASSERT_TRUE(absl::c_equal(pair.second, tables))
        << "expect: [" << absl::StrJoin(pair.second, ",", absl::PairFormatter("=")) << "], but get ["
        << absl::StrJoin(tables, ",", absl::PairFormatter("=")) << "]";
}

TEST_F(GetDependentTablesTest, FailTest) {
    base::Status get_status;
    ASSERT_FALSE(engine->GetDependentTables("select * from t1;", "simple_db", nullptr, get_status));
}

TEST_F(EngineCompileTest, RouterTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");

    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    index = table_def.add_indexes();
    index->set_name("index2");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    AddTable(db, table_def);
    catalog->AddDatabase(db);
    {
        std::string sql =
            "select col2, sum(col1) over w1 from t1 \n"
            "window w1 as (partition by col2 \n"
            "order by col5 rows between 3 preceding and current row);";
        EngineOptions options;
        options.SetCompileOnly(true);
        Engine engine(catalog, options);
        ExplainOutput explain_output;
        codec::Schema empty_parameter_schema;
        base::Status status;
        ASSERT_TRUE(engine.Explain(sql, "simple_db", kBatchRequestMode,
                                   empty_parameter_schema, &explain_output, &status));
        ASSERT_EQ(explain_output.router.GetMainTable(), "t1");
        ASSERT_EQ(explain_output.router.GetRouterCol(), "col2");
    }
}

TEST_F(EngineCompileTest, ExplainBatchRequestTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");

    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    index = table_def.add_indexes();
    index->set_name("index2");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    AddTable(db, table_def);
    catalog->AddDatabase(db);

    std::set<size_t> common_column_indices({2, 3, 5});
    std::string sql =
        "select col0, col1, col2, sum(col1) over w1, \n"
        "sum(col2) over w1, sum(col5) over w1 from t1 \n"
        "window w1 as (partition by col2 \n"
        "order by col5 rows between 3 preceding and current row);";
    EngineOptions options;
    options.SetCompileOnly(true);
    Engine engine(catalog, options);
    ExplainOutput explain_output;
    base::Status status;
    ASSERT_TRUE(engine.Explain(sql, "simple_db", kBatchRequestMode,
                               common_column_indices, &explain_output,
                               &status));
    ASSERT_TRUE(status.isOK()) << status;
    auto& output_schema = explain_output.output_schema;
    ASSERT_EQ(false, output_schema.Get(0).is_constant());
    ASSERT_EQ(false, output_schema.Get(1).is_constant());
    ASSERT_EQ(true, output_schema.Get(2).is_constant());
    ASSERT_EQ(false, output_schema.Get(3).is_constant());
    ASSERT_EQ(true, output_schema.Get(4).is_constant());
    ASSERT_EQ(true, output_schema.Get(5).is_constant());
}
TEST_F(EngineCompileTest, MockRequestExplainTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");
    AddTable(db, table_def);
    catalog->AddDatabase(db);

    std::string sql =
        "select col0, col1, col2, sum(col1) over w1, \n"
        "sum(col2) over w1, sum(col5) over w1 from t1 \n"
        "window w1 as (partition by col2 \n"
        "order by col5 rows between 3 preceding and current row);";
    EngineOptions options;
    options.SetCompileOnly(true);
    Engine engine(catalog, options);
    ExplainOutput explain_output;
    base::Status status;
    ASSERT_TRUE(engine.Explain(sql, "simple_db", kMockRequestMode,
                               &explain_output,
                               &status));
    ASSERT_TRUE(status.isOK()) << status;
}

TEST_F(EngineCompileTest, MockRequestCompileTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    hybridse::type::Database db;
    db.set_name("simple_db");

    // table t1
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");
    AddTable(db, table_def);
    catalog->AddDatabase(db);

    std::string sql =
        "select col0, col1, col2, sum(col1) over w1, \n"
        "sum(col2) over w1, sum(col5) over w1 from t1 \n"
        "window w1 as (partition by col2 \n"
        "order by col5 rows between 3 preceding and current row);";
    EngineOptions options;
    Engine engine(catalog, options);
    base::Status status;
    vm::MockRequestRunSession session;
    ASSERT_TRUE(engine.Get(sql, "simple_db", session, status)) << status;

    ASSERT_TRUE(status.isOK()) << status;
    ASSERT_EQ(7u, session.GetRequestSchema().size());
    ASSERT_EQ("t1", session.GetRequestName());
    ASSERT_EQ("simple_db", session.GetRequestDbName());
}
TEST_F(EngineCompileTest, EngineCompileWithoutDefaultDBTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    {
        // database simple_db
        hybridse::type::Database db;
        db.set_name("db1");

        // table t1
        hybridse::type::TableDef table_def;
        sqlcase::CaseSchemaMock::BuildTableDef(table_def);
        table_def.set_name("t1");
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
        AddTable(db, table_def);
        catalog->AddDatabase(db);
    }
    {
        // database simple_db
        hybridse::type::Database db;
        db.set_name("db2");
        // table t2
        hybridse::type::TableDef table_def2;
        sqlcase::CaseSchemaMock::BuildTableDef(table_def2);
        table_def2.set_name("t2");
        AddTable(db, table_def2);
        catalog->AddDatabase(db);
    }

    {
        std::vector<std::string> sql_str_list = {
            "select db1.t1.col1, db1.t1.col2,db2.t2.col3,db2.t2.col4 from db1.t1 last join db2.t2 ORDER BY db2.t2.col5 "
            "on db1.t1.col1=db2.t2.col1;"};
        EngineOptions options;
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_TRUE(engine.Get(sqlstr, "", session, get_status)) << get_status;
        }
    }
}

void cut2(::openmldb::base::UDFContext* ctx, ::openmldb::base::StringRef* input,
        ::openmldb::base::StringRef* output, bool* is_null) {
    if (input == nullptr || output == nullptr) {
        *is_null = true;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    char *buffer = ctx->pool->Alloc(size);
    memcpy(buffer, input->data_, size);
    output->size_ = size;
    output->data_ = buffer;
    *is_null = false;
}

TEST_F(EngineCompileTest, ExternalFunctionTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();
    hybridse::type::Database db;
    db.set_name("simple_db");
    hybridse::type::TableDef table_def;
    sqlcase::CaseSchemaMock::BuildTableDef(table_def);
    table_def.set_name("t1");
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->set_second_key("col5");
    AddTable(db, table_def);

    catalog->AddDatabase(db);

    std::string sql = "select myfun(col1 + 1) from t1;";
    EngineOptions options;
    Engine engine(catalog, options);
    ASSERT_TRUE(engine.RegisterExternalFunction("myfun", node::kInt64, false,
                {node::kInt32}, false, false, "").isOK());
    ASSERT_FALSE(engine.RegisterExternalFunction("myfun", node::kInt64, false,
                {node::kInt32}, false, false, "").isOK());
    ASSERT_FALSE(engine.RegisterExternalFunction("lcase", node::kInt64, false,
                {node::kInt32}, false, false, "").isOK());
    base::Status get_status;
    BatchRunSession session;
    ASSERT_TRUE(engine.Get(sql, "simple_db", session, get_status));
    ASSERT_TRUE(engine.RemoveExternalFunction("myfun", {node::kInt32}, "").isOK());
    std::string sql1 = "select myfun(col1 + 2) from t1;";
    ASSERT_FALSE(engine.Get(sql1, "simple_db", session, get_status));

    ASSERT_TRUE(engine.RegisterExternalFunction("cut2", node::kVarchar, false,
                {node::kVarchar}, false, false, "").isOK());
    std::string sql2 = "select cut2(col0) from t1;";
    ASSERT_TRUE(engine.Get(sql2, "simple_db", session, get_status));
}

TEST_F(EngineCompileTest, DetermineEngineMode) {
    EXPECT_EQ(vm::kRequestMode, Engine::TryDetermineEngineMode(R"(SELECT * from t1
CONFIG (execute_mode = 'request', values = [12]))",
                                    kBatchMode));

    EXPECT_EQ(vm::kBatchRequestMode, Engine::TryDetermineEngineMode(R"(SELECT * from t1
CONFIG (execute_mode = 'request', values = [(12), (12)]))",
                                    kBatchMode));

    EXPECT_EQ(vm::kBatchRequestMode, Engine::TryDetermineEngineMode(R"(SELECT * from t1
CONFIG (execute_mode = 'batchrequest', values = [(12)]))",
                                    kBatchMode));

    // no option, default
    EXPECT_EQ(vm::kRequestMode, Engine::TryDetermineEngineMode(R"(SELECT * FROM t1)", kRequestMode));
    // on error, default
    EXPECT_EQ(vm::kBatchMode, Engine::TryDetermineEngineMode(R"(SELECT)", kBatchMode));
}

}  // namespace vm
}  // namespace hybridse

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    // ::hybridse::vm::CoreAPI::EnableSignalTraceback();
    return RUN_ALL_TESTS();
}
