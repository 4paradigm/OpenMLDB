/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform_request_mode_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/status.h"
#include "boost/algorithm/string.hpp"
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
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/udf.h"
#include "vm/test_base.h"
#include "vm/transform.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {
class TransformRequestModeTest : public ::testing::TestWithParam<std::string> {
 public:
    TransformRequestModeTest() {}
    ~TransformRequestModeTest() {}
};

void Physical_Plan_Check(const std::shared_ptr<tablet::TabletCatalog>& catalog,
                         std::string sql, std::string exp) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");

    boost::to_lower(sql);
    std::cout << sql << std::endl;

    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager, false);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sql, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::ostringstream oss;
            oss << *(plan_trees[0]);
            LOG(INFO) << "logical plan:\n" << oss.str();
            //            std::cout << "logical plan:\n" << oss.str() <<
            //            std::endl;
        } else {
            std::cout << base_status.msg;
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    RequestModeransformer transform(&manager, "db", catalog, m.get());
    transform.AddDefaultPasses();
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(transform.TransformPhysicalPlan(plan_trees, &physical_plan,
                                                base_status));
    //    m->print(::llvm::errs(), NULL);
    std::ostringstream oss;
    physical_plan->Print(oss, "");
    std::cout << "physical plan:\n" << sql << "\n" << oss.str() << std::endl;
    std::ostringstream ss;
    PrintSchema(ss, physical_plan->output_schema_);
    std::cout << "schema:\n" << ss.str() << std::endl;
    ASSERT_EQ(oss.str(), exp);
}

INSTANTIATE_TEST_CASE_P(
    SqlSimpleProjectPlanner, TransformRequestModeTest,
    testing::Values(
        "SELECT COL1 as c1 FROM t1;", "SELECT t1.COL1 c1 FROM t1 limit 10;",
        "SELECT COL1 as c1, col2  FROM t1;",
        "SELECT col1-col2 as col1_2, *, col1+col2 as col12 FROM t1 limit 10;",
        "SELECT *, col1+col2 as col12 FROM t1 limit 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlWindowProjectPlanner, TransformRequestModeTest,
    testing::Values(
        "SELECT COL1, COL2, `COL5`, AVG(COL3) OVER w, SUM(COL3) OVER w FROM "
        "t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `COL5` ROWS BETWEEN UNBOUNDED PRECEDING AND "
        "CURRENT ROW);",
        "SELECT COL1, SUM(col4) OVER w as w_amt_sum FROM t1 \n"
        "WINDOW w AS (PARTITION BY COL2\n"
        "              ORDER BY `col5` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING);",
        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col5 ORDER BY `col5` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlWherePlan, TransformRequestModeTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL1+COL2;",
        "SELECT COL1 FROM t1 where COL1;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 where COL1 > 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlLikePlan, TransformRequestModeTest,
    testing::Values("SELECT COL1 FROM t1 where COL like \"%abc\";",
                    "SELECT COL1 FROM t1 where COL1 like '%123';",
                    "SELECT COL1 FROM t1 where COL not like \"%abc\";",
                    "SELECT COL1 FROM t1 where COL1 not like '%123';",
                    "SELECT COL1 FROM t1 where COL1 not like 10;",
                    "SELECT COL1 FROM t1 where COL1 like 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlInPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT COL1 FROM t1 where COL in (1, 2, 3, 4, 5);",
        "SELECT COL1 FROM t1 where COL1 in (\"abc\", \"xyz\", \"test\");",
        "SELECT COL1 FROM t1 where COL1 not in (1,2,3,4,5);"));

INSTANTIATE_TEST_CASE_P(
    SqlGroupPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT distinct sum(COL1) as col1sum FROM t1 where col2 > 10 group "
        "by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10;",
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
        "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
        "SELECT sum(COL1) FROM t1 group by COL1+COL2;",
        "SELECT sum(COL1) FROM t1 group by COL1;",
        "SELECT sum(COL1) FROM t1 group by COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT sum(COL1) FROM t1 group by COL1, COL2;",
        "SELECT sum(COL1) FROM t1 group by COL1;"));

INSTANTIATE_TEST_CASE_P(
    SqlHavingPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT COL1 FROM t1 having COL1+COL2;",
        "SELECT COL1 FROM t1 having COL1;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20 or COL1 =0;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20;",
        "SELECT COL1 FROM t1 HAVING COL1 > 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlOrderPlan, TransformRequestModeTest,
    testing::Values("SELECT COL1 FROM t1 order by COL1 + COL2 - COL3;",
                    "SELECT COL1 FROM t1 order by COL1, COL2, COL3;",
                    "SELECT COL1 FROM t1 order by COL1, COL2;",
                    "SELECT COL1 FROM t1 order by COL1;"));

INSTANTIATE_TEST_CASE_P(
    SqlWhereGroupHavingOrderPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1+COL2 limit 10;",
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 order by COL1 limit 10;",
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0 limit 10;",
        "SELECT sum(COL1) as col1sum FROM t1 where col2 > 10 group by COL1, "
        "COL2 having col1sum > 0;",
        "SELECT sum(COL1) as col1sum FROM t1 group by COL1, COL2 having "
        "sum(COL1) > 0;",
        "SELECT sum(COL1) as col1sum FROM t1 group by COL1, COL2 having "
        "col1sum > 0;"));

INSTANTIATE_TEST_CASE_P(
    SqlJoinPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT * FROM t1 full join t2 on t1.col1 = t2.col2;",
        "SELECT * FROM t1 right join t2 on t1.col1 = t2.col2;",
        "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col2;",
        "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 on "
        "t1.col1 = t2.col2 and t2.col5 >= t1.col5;"));

INSTANTIATE_TEST_CASE_P(
    SqlLeftJoinWindowPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT "
        "t1.col1, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "sum(t2.col2) OVER w1 as w1_col2_sum "
        "FROM t1 left join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "SELECT "
        "t1.col1, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "sum(t2.col2) OVER w1 as w1_col2_sum "
        "FROM t1 left join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY t1.col1, t2.col2 ORDER BY t1.col5 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;"));
INSTANTIATE_TEST_CASE_P(
    SqlUnionPlan, TransformRequestModeTest,
    testing::Values("SELECT * FROM t1 UNION SELECT max(col1), min(col2), "
                    "sum(col3), avg(col4), col5 FROM t1;",
                    "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col2 UNION "
                    "SELECT * FROM t1 inner join t2 on t1.col1 = t2.col3 UNION "
                    "SELECT * FROM t1 inner join t2 on t2.col1 = t2.col4;"));
INSTANTIATE_TEST_CASE_P(
    SqlDistinctPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT distinct COL1 FROM t1 HAVING COL1 > 10 and COL2 = 20;",
        "SELECT DISTINCT sum(COL1) as col1sum, * FROM t1 group by COL1,COL2;",
        "SELECT DISTINCT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col5 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        //        "SELECT DISTINCT COUNT(*) FROM t1;",
        "SELECT distinct COL1 FROM t1 where COL1+COL2;",
        "SELECT DISTINCT COL1 FROM t1 where COL1 > 10;"));

INSTANTIATE_TEST_CASE_P(
    SqlSubQueryPlan, TransformRequestModeTest,
    testing::Values(
        "SELECT * FROM t1 WHERE COL1 > (select avg(COL1) from t1) limit 10;",
        "select * from (select * from t1 where col1>0);",
        "select * from \n"
        "    (select * from t1 where col1 = 7) s\n"
        "left join \n"
        "    (select * from t1 where col2 = 2) t\n"
        "on s.col3 = t.col3\n"
        "union\n"
        "select distinct * from \n"
        "    (select distinct * from t1 where col1 = 7) s\n"
        "right join \n"
        "    (select distinct * from t1 where col2 = 2) t\n"
        "on s.col3 = t.col3;",
        "select * from \n"
        "    (select * from t1 where col1 = 7) s\n"
        "left join \n"
        "    (select * from t1 where col2 = 2) t\n"
        "on s.col3 = t.col3\n"
        "union\n"
        "select distinct * from \n"
        "    (select  * from t1 where col1 = 7) s\n"
        "right join \n"
        "    (select distinct * from t1 where col2 = 2) t\n"
        "on s.col3 = t.col3;"));

TEST_P(TransformRequestModeTest, transform_physical_plan) {
    std::string sqlstr = GetParam();
    LOG(INFO) << sqlstr;

    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");
    boost::to_lower(sqlstr);
    LOG(INFO) << sqlstr;
    std::cout << sqlstr << std::endl;

    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;
    fesql::type::TableDef table_def3;
    fesql::type::TableDef table_def4;
    fesql::type::TableDef table_def5;
    fesql::type::TableDef table_def6;

    BuildTableDef(table_def);
    BuildTableDef(table_def2);
    BuildTableDef(table_def3);
    BuildTableDef(table_def4);
    BuildTableDef(table_def5);
    BuildTableDef(table_def6);

    table_def.set_name("t1");
    table_def2.set_name("t2");
    table_def3.set_name("t3");
    table_def4.set_name("t4");
    table_def5.set_name("t5");
    table_def6.set_name("t6");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    std::shared_ptr<::fesql::storage::Table> table3(
        new ::fesql::storage::Table(3, 1, table_def3));
    std::shared_ptr<::fesql::storage::Table> table4(
        new ::fesql::storage::Table(4, 1, table_def4));
    std::shared_ptr<::fesql::storage::Table> table5(
        new ::fesql::storage::Table(5, 1, table_def5));
    std::shared_ptr<::fesql::storage::Table> table6(
        new ::fesql::storage::Table(6, 1, table_def6));

    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);
    AddTable(catalog, table_def3, table3);
    AddTable(catalog, table_def4, table4);
    AddTable(catalog, table_def5, table5);
    AddTable(catalog, table_def6, table6);

    fesql::type::TableDef request_def;
    BuildTableDef(request_def);
    request_def.set_name("t1");
    request_def.set_catalog("request");
    std::shared_ptr<::fesql::storage::Table> request(
        new ::fesql::storage::Table(1, 1, request_def));
    AddTable(catalog, request_def, request);

    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager, false);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sqlstr, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) ==
            0) {
            std::ostringstream oss;
            oss << *(plan_trees[0]) << std::endl;
            std::cout << "logical plan:\n" << oss.str() << std::endl;
        } else {
            std::cout << base_status.msg;
        }

        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    RequestModeransformer transform(&manager, "db", catalog, m.get());
    PhysicalOpNode* physical_plan = nullptr;
    ASSERT_TRUE(transform.TransformPhysicalPlan(plan_trees, &physical_plan,
                                                base_status));
    std::ostringstream oss;
    physical_plan->Print(oss, "");
    std::cout << "physical plan:\n" << sqlstr << "\n" << oss.str() << std::endl;
    std::ostringstream ss;
    PrintSchema(ss, physical_plan->output_schema_);
    std::cout << "schema:\n" << ss.str() << std::endl;
    //    m->print(::llvm::errs(), NULL);
}

TEST_F(TransformRequestModeTest, pass_group_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col1;",
        "PROJECT(type=Aggregation)\n"
        "  REQUEST_UNION(groups=(), orders=, keys=, start=-1, end=-1)\n"
        "    DATA_PROVIDER(request=t1)\n"
        "    INDEX_SEEK(keys=(col1))\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index1)"));
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2;",
        "PROJECT(type=Aggregation)\n"
        "  REQUEST_UNION(groups=(), orders=, keys=, start=-1, end=-1)\n"
        "    DATA_PROVIDER(request=t1)\n"
        "    INDEX_SEEK(keys=(col1,col2))\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col1, col2, col3;",
        "PROJECT(type=Aggregation)\n"
        "  REQUEST_UNION(groups=(col3), orders=, keys=, start=-1, end=-1)\n"
        "    DATA_PROVIDER(request=t1)\n"
        "    INDEX_SEEK(keys=(col1,col2))\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    in_outs.push_back(std::make_pair(
        "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;",
        "PROJECT(type=Aggregation)\n"
        "  REQUEST_UNION(groups=(col3), orders=, keys=, start=-1, end=-1)\n"
        "    DATA_PROVIDER(request=t1)\n"
        "    INDEX_SEEK(keys=(col1,col2))\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    auto catalog = BuildCommonCatalog(table_def, table);

    fesql::type::TableDef request_def;
    BuildTableDef(request_def);
    request_def.set_name("t1");
    request_def.set_catalog("request");
    std::shared_ptr<::fesql::storage::Table> request(
        new ::fesql::storage::Table(1, 1, request_def));
    AddTable(catalog, request_def, request);

    for (auto in_out : in_outs) {
        Physical_Plan_Check(catalog, in_out.first, in_out.second);
    }
}

TEST_F(TransformRequestModeTest, pass_sort_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=Aggregation, limit=10)\n"
        "    REQUEST_UNION(groups=(), orders=() ASC, keys=(col5) ASC, "
        "start=-3, end=0)\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      INDEX_SEEK(keys=(col1))\n"
        "        DATA_PROVIDER(request=t1)\n"
        "        DATA_PROVIDER(type=IndexScan, table=t1, index=index1)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2, col1 ORDER BY col5 ROWS "
        "BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=Aggregation, limit=10)\n"
        "    REQUEST_UNION(groups=(), orders=() ASC, keys=(col5) ASC, "
        "start=-3, end=0)\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      INDEX_SEEK(keys=(col1,col2))\n"
        "        DATA_PROVIDER(request=t1)\n"
        "        DATA_PROVIDER(type=IndexScan, table=t1, index=index12)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "col1+col2 as col12, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "*, "
        "sum(col2) OVER w1 as w1_col2_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col3 ORDER BY col5 ROWS BETWEEN 3"
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=Aggregation, limit=10)\n"
        "    REQUEST_UNION(groups=(col3), orders=(col5) ASC, keys=(col5) "
        "ASC, start=-3, end=0)\n"
        "      DATA_PROVIDER(request=t1)\n"
        "      DATA_PROVIDER(table=t1)"));

    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    auto catalog = BuildCommonCatalog(table_def, table);

    fesql::type::TableDef request_def;
    BuildTableDef(request_def);
    request_def.set_name("t1");
    request_def.set_catalog("request");
    std::shared_ptr<::fesql::storage::Table> request(
        new ::fesql::storage::Table(1, 1, request_def));
    AddTable(catalog, request_def, request);

    for (auto in_out : in_outs) {
        Physical_Plan_Check(catalog, in_out.first, in_out.second);
    }
}

TEST_F(TransformRequestModeTest, pass_join_optimized_test) {
    std::vector<std::pair<std::string, std::string>> in_outs;
    in_outs.push_back(std::make_pair(
        "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 on "
        "t1.col1 = t2.col2 and t2.col5 >= t1.col5;",
        "PROJECT(type=RowProject)\n"
        "  REQUEST_JOIN(type=LastJoin, condition=t2.col5 >= t1.col5)\n"
        "    DATA_PROVIDER(request=t1)\n"
        "    GROUP_BY(groups=(t2.col2))\n"
        "      DATA_PROVIDER(table=t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 on "
        "t1.col1 = t2.col1 and t2.col5 >= t1.col5;",
        "PROJECT(type=RowProject)\n"
        "  REQUEST_JOIN(type=LastJoin, condition=t2.col5 >= t1.col5)\n"
        "    DATA_PROVIDER(request=t1)\n"
        "    DATA_PROVIDER(type=IndexScan, table=t2, index=index1_t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "t2.col1, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "sum(t1.col2) OVER w1 as w1_col2_sum "
        "FROM t1 last join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY t1.col1 ORDER BY t1.col5 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=Aggregation, limit=10)\n"
        "    JOIN(type=LastJoin, condition=, key=(t1.col1))\n"
        "      REQUEST_UNION(groups=(), orders=() ASC, keys=(t1.col5) ASC, "
        "start=-3, end=0)\n"
        "        DATA_PROVIDER(request=t1)\n"
        "        INDEX_SEEK(keys=(t1.col1))\n"
        "          DATA_PROVIDER(request=t1)\n"
        "          DATA_PROVIDER(type=IndexScan, table=t1, index=index1)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t2, index=index1_t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "t2.col1, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "sum(t1.col2) OVER w1 as w1_col2_sum "
        "FROM t1 last join t2 on t1.col2 = t2.col2 "
        "WINDOW w1 AS (PARTITION BY t1.col1, t1.col2 ORDER BY t1.col5 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=Aggregation, limit=10)\n"
        "    JOIN(type=LastJoin, condition=, key=(t1.col2))\n"
        "      REQUEST_UNION(groups=(), orders=() ASC, keys=(t1.col5) ASC, "
        "start=-3, end=0)\n"
        "        DATA_PROVIDER(request=t1)\n"
        "        INDEX_SEEK(keys=(t1.col1,t1.col2))\n"
        "          DATA_PROVIDER(request=t1)\n"
        "          DATA_PROVIDER(type=IndexScan, table=t1, index=index12)\n"
        "      GROUP_BY(groups=(t2.col2))\n"
        "        DATA_PROVIDER(table=t2)"));
    in_outs.push_back(std::make_pair(
        "SELECT "
        "t2.col1, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "sum(t1.col2) OVER w1 as w1_col2_sum "
        "FROM t1 last join t2 on t1.col1 = t2.col1 "
        "WINDOW w1 AS (PARTITION BY t1.col1, t1.col2 ORDER BY t1.col5 ROWS "
        "BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;",
        "LIMIT(limit=10, optimized)\n"
        "  PROJECT(type=Aggregation, limit=10)\n"
        "    JOIN(type=LastJoin, condition=, key=(t1.col1))\n"
        "      REQUEST_UNION(groups=(), orders=() ASC, keys=(t1.col5) ASC, "
        "start=-3, end=0)\n"
        "        DATA_PROVIDER(request=t1)\n"
        "        INDEX_SEEK(keys=(t1.col1,t1.col2))\n"
        "          DATA_PROVIDER(request=t1)\n"
        "          DATA_PROVIDER(type=IndexScan, table=t1, index=index12)\n"
        "      DATA_PROVIDER(type=IndexScan, table=t2, index=index1_t2)"));

    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    table_def.set_name("t1");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index12");
        index->add_first_keys("col1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }
    auto catalog = BuildCommonCatalog(table_def, table);
    {
        fesql::type::TableDef table_def2;
        BuildTableDef(table_def2);
        table_def2.set_name("t2");
        ::fesql::type::IndexDef* index = table_def2.add_indexes();
        index->set_name("index1_t2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
        std::shared_ptr<::fesql::storage::Table> table2(
            new ::fesql::storage::Table(1, 1, table_def2));
        AddTable(catalog, table_def2, table2);
    }

    {
        fesql::type::TableDef request_def;
        BuildTableDef(request_def);
        request_def.set_name("t1");
        request_def.set_catalog("request");
        std::shared_ptr<::fesql::storage::Table> request(
            new ::fesql::storage::Table(1, 1, request_def));
        AddTable(catalog, request_def, request);
    }

    for (auto in_out : in_outs) {
        Physical_Plan_Check(catalog, in_out.first, in_out.second);
    }
}

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    //    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
