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

#include "case/case_data_mock.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "testing/engine_test_base.h"

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
    options.set_compile_only(true);
    options.set_max_sql_cache_size(1);
    Engine engine(catalog, options);

    std::string sql = "select col1, col2 from t1;";
    std::string sql2 = "select col1, col2 as cl2 from t1;";
    {
        base::Status get_status;
        BatchRunSession bsession1;
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession1, get_status))
            << get_status;
        ASSERT_EQ(get_status.code, common::kOk);
        BatchRunSession bsession2;
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_EQ(bsession1.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
        BatchRunSession bsession3;
        ASSERT_TRUE(engine.Get(sql2, "simple_db", bsession3, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_TRUE(engine.Get(sql, "simple_db", bsession2, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        ASSERT_NE(bsession1.GetCompileInfo().get(),
                  bsession2.GetCompileInfo().get());
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
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 last join t2 "
            "order by t2.col5 on t1.col1 = t2.col2;"};
        EngineOptions options;
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
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
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;"};
        EngineOptions options;
        options.set_performance_sensitive(false);
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

TEST_F(EngineCompileTest, EngineGetDependentTableTest) {
    {
        std::vector<std::pair<std::string, std::set<std::string>>> pairs;
        pairs.push_back(std::make_pair("SELECT col1, col2 from t1;",
                                       std::set<std::string>({"t1"})));
        pairs.push_back(
            std::make_pair("SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 "
                           "last join t2 "
                           "order by t2.col5 on t1.col1 = t2.col2;",
                           std::set<std::string>({"t1", "t2"})));

        pairs.push_back(
            std::make_pair("SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 "
                           "last join t2 "
                           "order by t2.col5 on t1.col1 = t2.col2;",
                           std::set<std::string>({"t1", "t2"})));
        pairs.push_back(std::make_pair(
            "SELECT t1.col1 as id, t1.col2 as t1_col2, t1.col5 as t1_col5,\n"
            "      test_sum(t1.col1) OVER w1 as w1_col1_sum, sum(t1.col3) OVER "
            "w1 as w1_col3_sum,\n"
            "      sum(t2.col4) OVER w1 as w1_t2_col4_sum, sum(t2.col2) OVER "
            "w1 as w1_t2_col2_sum,\n"
            "      sum(t1.col5) OVER w1 as w1_col5_sum,\n"
            "      str1 as t2_str1 FROM t1\n"
            "      last join t2 order by t2.col5 on t1.col1=t2.col1 and "
            "t1.col5 = t2.col5\n"
            "      WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col5 "
            "ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            std::set<std::string>({"t1", "t2"})));

        for (auto pair : pairs) {
            base::Status get_status;
            EngineOptions options;
            Engine engine(std::shared_ptr<Catalog>(), options);
            std::string sqlstr = pair.first;
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            std::set<std::string> tables;
            ASSERT_TRUE(engine.GetDependentTables(
                sqlstr, "simple_db", kBatchMode, &tables, get_status));
            ASSERT_EQ(tables, pair.second);
        }

        for (auto pair : pairs) {
            base::Status get_status;
            EngineOptions options;
            Engine engine(std::shared_ptr<Catalog>(), options);
            std::string sqlstr = pair.first;
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            std::set<std::string> tables;
            ASSERT_TRUE(engine.GetDependentTables(sqlstr, "db", kRequestMode,
                                                  &tables, get_status));
            ASSERT_EQ(tables, pair.second);
        }
    }

    // const select
    {
        std::vector<std::pair<std::string, std::set<std::string>>> pairs;
        pairs.push_back(std::make_pair("SELECT substr(\"hello world\", 3, 6);",
                                       std::set<std::string>()));
        for (auto pair : pairs) {
            base::Status get_status;
            EngineOptions options;
            Engine engine(std::shared_ptr<Catalog>(), options);
            std::string sqlstr = pair.first;
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            std::set<std::string> tables;
            ASSERT_TRUE(engine.GetDependentTables(
                sqlstr, "simple_db", kBatchMode, &tables, get_status));
            ASSERT_EQ(tables, pair.second);
        }
    }
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
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        Engine engine(catalog, options);
        ExplainOutput explain_output;
        base::Status status;
        ASSERT_TRUE(engine.Explain(sql, "simple_db", kBatchRequestMode,
                                   &explain_output, &status));
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
    options.set_compile_only(true);
    options.set_performance_sensitive(false);
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

}  // namespace vm
}  // namespace hybridse

int main(int argc, char** argv) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    // ::hybridse::vm::CoreAPI::EnableSignalTraceback();
    return RUN_ALL_TESTS();
}
