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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "node/node_manager.h"
#include "plan/plan_api.h"
#include "testing/test_base.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"
#include "vm/transform.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace hybridse {
namespace vm {

using hybridse::sqlcase::SqlCase;
const std::vector<std::string> FILTERS({"physical-plan-unsupport", "zetasql-unsupport", "plan-unsupport",
                                        "parser-unsupport", "request-unsupport"});
class TransformRequestModeTest : public ::testing::TestWithParam<SqlCase> {
 public:
    TransformRequestModeTest() {}
    ~TransformRequestModeTest() {}
    ::hybridse::node::NodeManager manager;
};

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(TransformRequestModeTest);
void PhysicalPlanCheck(const std::shared_ptr<Catalog>& catalog, std::string sql, std::string exp,
                       const std::vector<passes::PhysicalPlanPassType>& extra_passes = {},
                       const std::unordered_map<std::string, std::string>* options = nullptr) {
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");

    boost::to_lower(sql);
    std::cout << sql << std::endl;

    ::hybridse::node::NodeManager manager;
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &manager, base_status, false, false, false,
                                                            options))
            << base_status;
        std::cout.flush();
    }
    std::stringstream logical_plan_ss;
    plan_trees[0]->Print(logical_plan_ss, "\t");
    LOG(WARNING) << "Logical plan: " << logical_plan_ss.str();

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    RequestModeTransformer transform(&manager, "db", catalog, nullptr, m.get(), lib, {}, false, false, false, true,
                                     options);

    for (auto pass : extra_passes) {
        transform.AddPass(pass);
    }
    transform.AddDefaultPasses();

    PhysicalOpNode* physical_plan = nullptr;
    auto s = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    ASSERT_TRUE(s.isOK()) << s;
    //    m->print(::llvm::errs(), NULL);
    std::ostringstream oss;
    physical_plan->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;
    std::ostringstream ss;
    PrintAllSchema(ss, physical_plan);
    LOG(INFO) << "schema:\n" << ss.str() << std::endl;
    ASSERT_EQ(oss.str(), exp);
}
INSTANTIATE_TEST_SUITE_P(SqlSimpleQueryParse, TransformRequestModeTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/simple_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(SqlFeatureSignatureQueryParse, TransformRequestModeTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/feature_signature_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(SqlWindowQueryParse, TransformRequestModeTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/window_query.yaml", FILTERS)));
INSTANTIATE_TEST_SUITE_P(SqlJoinPlan, TransformRequestModeTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/plan/join_query.yaml", FILTERS)));

void CheckTransformPhysicalPlan(const SqlCase& sql_case, bool is_cluster_optimized, node::NodeManager* nm) {
    std::string sqlstr = sql_case.sql_str();
    LOG(INFO) << sqlstr;
    const hybridse::base::Status exp_status(::hybridse::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

    hybridse::type::TableDef table_def;
    hybridse::type::TableDef table_def2;
    hybridse::type::TableDef table_def3;
    hybridse::type::TableDef table_def4;
    hybridse::type::TableDef table_def5;
    hybridse::type::TableDef table_def6;

    BuildTableDef(table_def);
    table_def.set_name("t1");
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index0");
        index->add_first_keys("col0");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index2");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }

    BuildTableDef(table_def2);
    BuildTableDef(table_def3);
    BuildTableDef(table_def4);
    BuildTableDef(table_def5);
    BuildTableDef(table_def6);
    table_def2.set_name("t2");
    {
        ::hybridse::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index2_t2");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    table_def3.set_name("t3");
    table_def2.set_name("t2");
    {
        ::hybridse::type::IndexDef* index = table_def3.add_indexes();
        index->set_name("index1_t3");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    table_def4.set_name("t4");
    table_def5.set_name("t5");
    table_def6.set_name("t6");

    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    AddTable(db, table_def2);
    AddTable(db, table_def3);
    AddTable(db, table_def4);
    AddTable(db, table_def5);
    AddTable(db, table_def6);

    {
        hybridse::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tb");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index1_tb");
            index->add_first_keys("c1");
            index->set_second_key("c5");
        }
        AddTable(db, table_def);
    }
    {
        hybridse::type::TableDef table_def;
        BuildTableA(table_def);
        table_def.set_name("tc");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index1_tc");
            index->add_first_keys("c1");
            index->set_second_key("c5");
        }
        AddTable(db, table_def);
    }
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index2_table2");
            index->add_first_keys("col2");
            index->set_second_key("col5");
        }
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    {
        if (plan::PlanAPI::CreatePlanTreeFromScript(sqlstr, plan_trees, nm, base_status, false, is_cluster_optimized)) {
            LOG(INFO) << "\n" << *(plan_trees[0]) << std::endl;
        } else {
            LOG(INFO) << base_status.str();
            EXPECT_EQ(false, sql_case.expect().success_);
            if (sql_case.expect().code_ != -1) {
                EXPECT_EQ(sql_case.expect().code_, base_status.code);
            }
            if (!sql_case.expect().msg_.empty()) {
                EXPECT_EQ(sql_case.expect().msg_, base_status.msg);
            }
            return;
        }
        ASSERT_EQ(0, base_status.code);
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();

    auto parameter_types = sql_case.ExtractParameterTypes();
    RequestModeTransformer transform(nm, "db", catalog, &parameter_types, m.get(), lib, {}, false, false, false);
    PhysicalOpNode* physical_plan = nullptr;
    transform.AddDefaultPasses();
    Status status = transform.TransformPhysicalPlan(plan_trees, &physical_plan);
    EXPECT_EQ(sql_case.expect().success_, status.isOK()) << status;
    if (status.isOK()) {
        std::ostringstream oss;
        physical_plan->Print(oss, "");
        std::cout << "physical plan:\n" << sqlstr << "\n" << oss.str() << std::endl;
        std::ostringstream ss;
        PrintSchema(ss, *physical_plan->GetOutputSchema());
        std::cout << "schema:\n" << ss.str() << std::endl;
    }
    if (!sql_case.expect().success_) {
        if (sql_case.expect().code_ != -1) {
            EXPECT_EQ(sql_case.expect().code_, status.code);
        }
        if (!sql_case.expect().msg_.empty()) {
            EXPECT_EQ(sql_case.expect().msg_, status.msg);
        }
    }
}

TEST_P(TransformRequestModeTest, TransformPhysicalPlan) { CheckTransformPhysicalPlan(GetParam(), false, &manager); }
TEST_P(TransformRequestModeTest, ClusterTransformPhysicalPlan) {
    CheckTransformPhysicalPlan(GetParam(), true, &manager);
}
class TransformRequestModePassOptimizedTest : public ::testing::TestWithParam<std::pair<std::string, std::string>> {
 public:
    TransformRequestModePassOptimizedTest() {}
    ~TransformRequestModePassOptimizedTest() {}
};

INSTANTIATE_TEST_SUITE_P(
    SortOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(std::make_pair("SELECT "
                                   "col1, "
                                   "sum(col3) OVER w1 as w1_col3_sum, "
                                   "sum(col2) OVER w1 as w1_col2_sum "
                                   "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE "
                                   "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
                                   "LIMIT(limit=10, optimized)\n"
                                   "  PROJECT(type=Aggregation, limit=10)\n"
                                   "    REQUEST_UNION(partition_keys=(), orders=(ASC), "
                                   "range=(col5, 3 PRECEDING, 0 CURRENT), index_keys=(col1))\n"
                                   "      DATA_PROVIDER(request=t1)\n"
                                   "      DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
                    std::make_pair("SELECT "
                                   "col1, "
                                   "sum(col3) OVER w1 as w1_col3_sum, "
                                   "sum(col2) OVER w1 as w1_col2_sum "
                                   "FROM t1 WINDOW w1 AS (PARTITION BY col2, col1 ORDER BY col5 "
                                   "ROWS_RANGE "
                                   "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
                                   "LIMIT(limit=10, optimized)\n"
                                   "  PROJECT(type=Aggregation, limit=10)\n"
                                   "    REQUEST_UNION(partition_keys=(), orders=(ASC), "
                                   "range=(col5, 3 PRECEDING, 0 CURRENT), index_keys=(col1,col2))\n"
                                   "      DATA_PROVIDER(request=t1)\n"
                                   "      DATA_PROVIDER(type=Partition, table=t1, index=index12)")));

INSTANTIATE_TEST_SUITE_P(
    JoinFilterOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(std::make_pair("SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join "
                                   "t2 order by t2.col5 on "
                                   " t1.col1 = t2.col1 and t2.col5 >= t1.col5;",
                                   "SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))\n"
                                   "  REQUEST_JOIN(type=LastJoin, right_sort=(ASC), "
                                   "condition=t2.col5 >= t1.col5, "
                                   "left_keys=(), right_keys=(), index_keys=(t1.col1))\n"
                                   "    DATA_PROVIDER(request=t1)\n"
                                   "    DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)"),
                    std::make_pair("SELECT "
                                   "t2.col1, "
                                   "sum(t1.col3) OVER w1 as w1_col3_sum, "
                                   "sum(t1.col2) OVER w1 as w1_col2_sum "
                                   "FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col1 "
                                   "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 "
                                   "ROWS_RANGE BETWEEN 3 "
                                   "PRECEDING AND CURRENT ROW) limit 10;",
                                   "LIMIT(limit=10, optimized)\n"
                                   "  PROJECT(type=Aggregation, limit=10)\n"
                                   "    JOIN(type=LastJoin, right_sort=(ASC), condition=, "
                                   "left_keys=(), "
                                   "right_keys=(), index_keys=(t1.col1))\n"
                                   "      REQUEST_UNION(partition_keys=(), orders=(ASC), "
                                   "range=(t1.col5, 3 PRECEDING, 0 CURRENT), index_keys=(t1.col1))\n"
                                   "        DATA_PROVIDER(request=t1)\n"
                                   "        DATA_PROVIDER(type=Partition, table=t1, index=index1)\n"
                                   "      DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)"),
                    std::make_pair("SELECT "
                                   "t2.col1, "
                                   "sum(t1.col3) OVER w1 as w1_col3_sum, "
                                   "sum(t1.col2) OVER w1 as w1_col2_sum "
                                   "FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col1 "
                                   "WINDOW w1 AS (PARTITION BY t1.col1, t1.col2 ORDER BY t1.col5 "
                                   "ROWS_RANGE "
                                   "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
                                   "LIMIT(limit=10, optimized)\n"
                                   "  PROJECT(type=Aggregation, limit=10)\n"
                                   "    JOIN(type=LastJoin, right_sort=(ASC), condition=, "
                                   "left_keys=(), "
                                   "right_keys=(), index_keys=(t1.col1))\n"
                                   "      REQUEST_UNION(partition_keys=(), orders=(ASC), "
                                   "range=(t1.col5, 3 PRECEDING, 0 CURRENT), index_keys=(t1.col1,t1.col2))\n"
                                   "        DATA_PROVIDER(request=t1)\n"
                                   "        DATA_PROVIDER(type=Partition, table=t1, index=index12)\n"
                                   "      DATA_PROVIDER(type=Partition, table=t2, index=index1_t2)")));

INSTANTIATE_TEST_SUITE_P(
    RequestWindowUnionOptimized, TransformRequestModePassOptimizedTest,
    testing::Values(
        // 0
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE "
            "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), "
            "index_keys=(col1))\n"
            "      +-UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), index_keys=(col1))\n"
            "          RENAME(name=t1)\n"
            "            DATA_PROVIDER(type=Partition, table=t3, index=index1_t3)\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, index=index1)"),
        // 1
        std::make_pair("SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
                       "      WINDOW w1 AS (UNION t3 PARTITION BY col1,col2 ORDER BY col5 "
                       "ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
                       "LIMIT(limit=10, optimized)\n"
                       "  PROJECT(type=Aggregation, limit=10)\n"
                       "    REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), "
                       "index_keys=(col1,col2))\n"
                       "      +-UNION(partition_keys=(col1), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), "
                       "index_keys=(col2))\n"
                       "          RENAME(name=t1)\n"
                       "            DATA_PROVIDER(type=Partition, table=t3, "
                       "index=index2_t3)\n"
                       "      DATA_PROVIDER(request=t1)\n"
                       "      DATA_PROVIDER(type=Partition, table=t1, index=index12)"),
        std::make_pair(
            "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1\n"
            "      WINDOW w1 AS (UNION t3 PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
            "LIMIT(limit=10, optimized)\n"
            "  PROJECT(type=Aggregation, limit=10)\n"
            "    REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), "
            "index_keys=(col1))\n"
            "      +-UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), index_keys=(col1))\n"
            "          RENAME(name=t1)\n"
            "            DATA_PROVIDER(type=Partition, table=t3, index=index1_t3)\n"
            "      DATA_PROVIDER(request=t1)\n"
            "      DATA_PROVIDER(type=Partition, table=t1, index=index1)")));

TEST_P(TransformRequestModePassOptimizedTest, PassPassOptimizedTest) {
    auto in_out = GetParam();
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    {
        hybridse::type::TableDef table_def2;
        BuildTableDef(table_def2);
        table_def2.set_name("t2");
        ::hybridse::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
        AddTable(db, table_def2);
    }

    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_name("t3");
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index1_t3");
            index->add_first_keys("col1");
            index->set_second_key("col5");
        }
        {
            ::hybridse::type::IndexDef* index = table_def.add_indexes();
            index->set_name("index2_t3");
            index->add_first_keys("col2");
            index->set_second_key("col5");
        }
        AddTable(db, table_def);
    }
    auto catalog = BuildSimpleCatalog(db);
    hybridse::type::Database db2;
    db2.set_name("db2");
    {
        hybridse::type::TableDef table_def;
        BuildTableDef(table_def);
        table_def.set_catalog("db2");
        table_def.set_name("table2");
        AddTable(db2, table_def);
    }
    catalog->AddDatabase(db2);
    PhysicalPlanCheck(catalog, in_out.first, in_out.second);
}

TEST_F(TransformRequestModePassOptimizedTest, SplitAggregationOptimizedTest) {
    const std::string sql =
        "SELECT col1, sum(col2) OVER w1, col2+1, add(col2, col1), count(col2) OVER w1, "
        "sum(col2) over w2 as w1_col2_sum , sum(col2) over w3 FROM t1\n"
        "WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 3m PRECEDING AND CURRENT ROW),"
        "w2 AS (PARTITION BY col1,col2 ORDER BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW),"
        "w3 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW);";

    const std::string expected =
        "SIMPLE_PROJECT(sources=(col1, sum(col2)over w1, col2 + 1, add(col2, col1), count(col2)over w1, w1_col2_sum, "
        "sum(col2)over w3))\n"
        "  REQUEST_JOIN(type=kJoinTypeConcat)\n"
        "    REQUEST_JOIN(type=kJoinTypeConcat)\n"
        "      REQUEST_JOIN(type=kJoinTypeConcat)\n"
        "        PROJECT(type=RowProject)\n"
        "          DATA_PROVIDER(request=t1)\n"
        "        SIMPLE_PROJECT(sources=(sum(col2)over w1, count(col2)over w1))\n"
        "          REQUEST_JOIN(type=kJoinTypeConcat)\n"
        "            PROJECT(type=Aggregation)\n"
        "              REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 180000 PRECEDING, 0 CURRENT), "
        "index_keys=(col1))\n"
        "                DATA_PROVIDER(request=t1)\n"
        "                DATA_PROVIDER(type=Partition, table=t1, index=index1)\n"
        "            PROJECT(type=Aggregation)\n"
        "              REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 180000 PRECEDING, 0 CURRENT), "
        "index_keys=(col1))\n"
        "                DATA_PROVIDER(request=t1)\n"
        "                DATA_PROVIDER(type=Partition, table=t1, index=index1)\n"
        "      PROJECT(type=Aggregation)\n"
        "        REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), "
        "index_keys=(col1,col2))\n"
        "          DATA_PROVIDER(request=t1)\n"
        "          DATA_PROVIDER(type=Partition, table=t1, index=index12)\n"
        "    PROJECT(type=Aggregation)\n"
        "      REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), "
        "index_keys=(col1))\n"
        "        DATA_PROVIDER(request=t1)\n"
        "        DATA_PROVIDER(type=Partition, table=t1, index=index1)";

    std::shared_ptr<SimpleCatalog> catalog(new SimpleCatalog(true));
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    catalog->AddDatabase(db);

    {
        hybridse::type::TableDef table_def;
        BuildAggTableDef(table_def, "aggr_t1", "aggr_db");
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("key");
        index->set_second_key("ts_start");
        hybridse::type::Database db;
        db.set_name("aggr_db");
        AddTable(db, table_def);
        catalog->AddDatabase(db);
    }

    std::unordered_map<std::string, std::string> options;
    options[LONG_WINDOWS] = "w1:1000, w2";
    std::vector<passes::PhysicalPlanPassType> extra_passes = {passes::kPassSplitAggregationOptimized};
    PhysicalPlanCheck(catalog, sql, expected, extra_passes, &options);
}

TEST_F(TransformRequestModePassOptimizedTest, LongWindowOptimizedTest) {
    // five long window agg applied
    const std::string sql =
        R"(SELECT
            col1,
            sum(col2) OVER w1,
            col2+1,
            add(col2, col1),
            count(col2) OVER w1,
            sum(col2) over w2 as w1_col2_sum ,
            sum(col2) over w3,
            count_where(col0, col1 > 1) over w1 as cw1,
            count_where(*, col5 = 0) over w1 as cw2,
        FROM t1
        WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 3m PRECEDING AND CURRENT ROW),
               w2 AS (PARTITION BY col1,col2 ORDER BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW),
               w3 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW);)";

    const std::string expected =
        R"(SIMPLE_PROJECT(sources=(col1, sum(col2)over w1, col2 + 1, add(col2, col1), count(col2)over w1, w1_col2_sum, sum(col2)over w3, cw1, cw2))
  REQUEST_JOIN(type=kJoinTypeConcat)
    REQUEST_JOIN(type=kJoinTypeConcat)
      REQUEST_JOIN(type=kJoinTypeConcat)
        PROJECT(type=RowProject)
          DATA_PROVIDER(request=t1)
        SIMPLE_PROJECT(sources=(sum(col2)over w1, count(col2)over w1, cw1, cw2))
          REQUEST_JOIN(type=kJoinTypeConcat)
            REQUEST_JOIN(type=kJoinTypeConcat)
              REQUEST_JOIN(type=kJoinTypeConcat)
                PROJECT(type=ReduceAggregation: sum(col2)over w1 (range[180000 PRECEDING,0 CURRENT]))
                  REQUEST_AGG_UNION(partition_keys=(), orders=(ASC), range=(col5, 180000 PRECEDING, 0 CURRENT), index_keys=(col1))
                    DATA_PROVIDER(request=t1)
                    DATA_PROVIDER(type=Partition, table=t1, index=index1)
                    DATA_PROVIDER(type=Partition, table=aggr_t1, index=index1_t2)
                PROJECT(type=ReduceAggregation: count(col2)over w1 (range[180000 PRECEDING,0 CURRENT]))
                  REQUEST_AGG_UNION(partition_keys=(), orders=(ASC), range=(col5, 180000 PRECEDING, 0 CURRENT), index_keys=(col1))
                    DATA_PROVIDER(request=t1)
                    DATA_PROVIDER(type=Partition, table=t1, index=index1)
                    DATA_PROVIDER(type=Partition, table=aggr_t1, index=index1_t2)
              PROJECT(type=ReduceAggregation: count_where(col0, col1 > 1)over w1 (range[180000 PRECEDING,0 CURRENT]))
                REQUEST_AGG_UNION(partition_keys=(), orders=(ASC), range=(col5, 180000 PRECEDING, 0 CURRENT), index_keys=(col1))
                  DATA_PROVIDER(request=t1)
                  DATA_PROVIDER(type=Partition, table=t1, index=index1)
                  DATA_PROVIDER(type=Partition, table=aggr_t1, index=index1_t2)
            PROJECT(type=ReduceAggregation: count_where(*, col5 = 0)over w1 (range[180000 PRECEDING,0 CURRENT]))
              REQUEST_AGG_UNION(partition_keys=(), orders=(ASC), range=(col5, 180000 PRECEDING, 0 CURRENT), index_keys=(col1))
                DATA_PROVIDER(request=t1)
                DATA_PROVIDER(type=Partition, table=t1, index=index1)
                DATA_PROVIDER(type=Partition, table=aggr_t1, index=index1_t2)
      PROJECT(type=ReduceAggregation: sum(col2)over w2 (range[3 PRECEDING,0 CURRENT]))
        REQUEST_AGG_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), index_keys=(col1,col2))
          DATA_PROVIDER(request=t1)
          DATA_PROVIDER(type=Partition, table=t1, index=index12)
          DATA_PROVIDER(type=Partition, table=aggr_t1, index=index1_t2)
    PROJECT(type=Aggregation)
      REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, 3 PRECEDING, 0 CURRENT), index_keys=(col1))
        DATA_PROVIDER(request=t1)
        DATA_PROVIDER(type=Partition, table=t1, index=index1))";

    std::shared_ptr<SimpleCatalog> catalog(new SimpleCatalog(true));
    hybridse::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    hybridse::type::Database db;
    db.set_name("db");
    AddTable(db, table_def);
    catalog->AddDatabase(db);

    {
        hybridse::type::TableDef table_def;
        BuildAggTableDef(table_def, "aggr_t1", "aggr_db");
        ::hybridse::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("key");
        index->set_second_key("ts_start");
        hybridse::type::Database db;
        db.set_name("aggr_db");
        AddTable(db, table_def);
        catalog->AddDatabase(db);
    }

    std::unordered_map<std::string, std::string> options;
    options[LONG_WINDOWS] = "w1:1000, w2";
    std::vector<passes::PhysicalPlanPassType> extra_passes = {passes::kPassSplitAggregationOptimized,
                                                              passes::kPassLongWindowOptimized};
    PhysicalPlanCheck(catalog, sql, expected, extra_passes, &options);
}

}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
