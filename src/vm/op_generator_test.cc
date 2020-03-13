/*
 * op_generator_test.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "vm/op_generator.h"
#include <udf/udf.h>
#include <memory>
#include <string>
#include <utility>
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "plan/planner.h"
#include "tablet/tablet_catalog.h"
#include "vm/test_base.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {

void AssertOpGen(const fesql::type::TableDef& table_def,
                 OpVector* op,  // NOLINT
                 const std::string& sql, const Status& exp_status) {
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    auto catalog = BuildCommonCatalog(table_def, table);
    OpGenerator generator(catalog);
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    ::fesql::node::NodeManager manager;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    {
        ::fesql::plan::SimplePlanner planner(&manager);
        ::fesql::parser::FeSQLParser parser;
        ::fesql::node::NodePointVector parser_trees;
        parser.parse(sql, parser_trees, &manager, base_status);
        ASSERT_EQ(0, base_status.code);
        planner.CreatePlanTree(parser_trees, plan_trees, base_status);
        std::cout << base_status.msg;
        ASSERT_EQ(0, base_status.code);
        std::cout.flush();
    }
    ::fesql::udf::RegisterUDFToModule(m.get());

    generator.Gen(plan_trees, "db", m.get(), op, base_status);
    std::cout << base_status.msg << std::endl;
    ASSERT_EQ(base_status.code, exp_status.code);
    ASSERT_EQ(base_status.msg, exp_status.msg);
    m->print(::llvm::errs(), NULL);
}
void BuildTableDef(::fesql::type::TableDef& table_def) {  // NOLINT
    table_def.set_name("t1");
    table_def.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table_def.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col15");
    }
}
class OpGeneratorTest : public ::testing::Test {
 public:
    OpGeneratorTest() { InitTable(); }
    ~OpGeneratorTest() {}
    void InitTable() {}

 protected:
};

TEST_F(OpGeneratorTest, test_normal) {
    fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col15");
    OpVector op;
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    "
        "return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 10;";
    const Status exp_status(::fesql::common::kOk, "ok");
    AssertOpGen(table_def, &op, sql, exp_status);
    ASSERT_EQ(3u, op.ops.size());

    {
        ASSERT_EQ(0u, op.ops[0]->idx);
        ASSERT_EQ(kOpScan, op.ops[0]->type);
    }

    ASSERT_EQ(1u, op.ops[1]->idx);
    ASSERT_EQ(kOpProject, op.ops[1]->type);

    {
        ASSERT_EQ(2u, op.ops[2]->idx);
        ASSERT_EQ(kOpLimit, op.ops[2]->type);
        LimitOp* limit_op = dynamic_cast<LimitOp*>(op.ops[2]);
        ASSERT_EQ(10u, limit_op->limit);
    }
}

TEST_F(OpGeneratorTest, test_windowp_project) {
    {
        fesql::type::TableDef table_def;
        BuildTableDef(table_def);
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col2");
        index->set_second_key("col15");
        OpVector op;
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col2\n"
            "              ORDER BY col15 RANGE BETWEEN 2d PRECEDING AND "
            "1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kOk, "ok");
        AssertOpGen(table_def, &op, sql, exp_status);
        ASSERT_EQ(3u, op.ops.size());
        {
            ASSERT_EQ(0u, op.ops[0]->idx);
            ASSERT_EQ(kOpScan, op.ops[0]->type);
        }

        ASSERT_EQ(1u, op.ops[1]->idx);
        ASSERT_EQ(kOpProject, op.ops[1]->type);
        ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op.ops[1]);
        ASSERT_TRUE(project_op->window_agg);
        ASSERT_EQ(::fesql::type::kInt16, project_op->w.keys.cbegin()->type);
        ASSERT_EQ(1u, project_op->w.keys.cbegin()->pos);

        ASSERT_EQ(::fesql::type::kInt64, project_op->w.order.type);
        ASSERT_EQ(4u, project_op->w.order.pos);
        ASSERT_TRUE(project_op->w.is_range_between);
        ASSERT_EQ(-86400000 * 2, project_op->w.start_offset);
        ASSERT_EQ(-1000, project_op->w.end_offset);
        {
            ASSERT_EQ(2u, op.ops[2]->idx);
            ASSERT_EQ(kOpLimit, op.ops[2]->type);
            LimitOp* limit_op = dynamic_cast<LimitOp*>(op.ops[2]);
            ASSERT_EQ(10u, limit_op->limit);
        }
    }
}
//
// TEST_F(OpGeneratorTest, test_multi_windowp_project) {
//    {
//        fesql::type::TableDef table_def;
//        BuildTableDef(table_def);
//        {
//            ::fesql::type::IndexDef* index = table_def.add_indexes();
//            index->set_name("index1");
//            index->add_first_keys("col2");
//            index->set_second_key("col15");
//        }
//        {
//            ::fesql::type::IndexDef* index = table_def.add_indexes();
//            index->set_name("index2");
//            index->add_first_keys("col1");
//            index->set_second_key("col15");
//        }
//
//        OpVector op;
//        const std::string sql =
//            "SELECT "
//            "sum(col1) OVER w1 as w1_col1_sum, "
//            "sum(col2) OVER w2 as w2_col2_sum, "
//            "sum(col3) OVER w1 as w1_col3_sum, "
//            "sum(col4) OVER w2 as w2_col4_sum, "
//            "sum(col1) OVER w2 as w2_col1_sum, "
//            "sum(col2) OVER w1 as w1_col2_sum, "
//            "sum(col3) OVER w2 as w2_col3_sum, "
//            "sum(col4) OVER w2 as w2_col4_sum "
//            "FROM t1 "
//            "WINDOW "
//            "w1 AS (PARTITION BY col2 ORDER BY col15 RANGE BETWEEN 1d "
//            "PRECEDING "
//            "AND 1s PRECEDING), "
//            "w2 AS (PARTITION BY col1 ORDER BY col15 RANGE BETWEEN 2d "
//            "PRECEDING AND 1s PRECEDING) "
//            "limit 10;";
//        const Status exp_status(::fesql::common::kOk, "ok");
//        AssertOpGen(table_def, &op, sql, exp_status);
//        ASSERT_EQ(4, op.ops.size());
//        ASSERT_EQ(kOpScan, op.ops[0]->type);
//        ASSERT_EQ(kOpProject, op.ops[1]->type);
//        ASSERT_EQ(kOpProject, op.ops[2]->type);
//        ASSERT_EQ(kOpMerge, op.ops[3]->type);
//        {
//            ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op.ops[1]);
//            ASSERT_TRUE(project_op->window_agg);
//            ASSERT_EQ("index1", project_op->w.index_name);
//            ASSERT_EQ(1L, project_op->w.keys.size());
//            ASSERT_EQ(::fesql::type::kInt16,
//            project_op->w.keys.cbegin()->type); ASSERT_EQ(1,
//            project_op->w.keys.cbegin()->pos);
//            ASSERT_EQ(::fesql::type::kInt64, project_op->w.order.type);
//            ASSERT_EQ(4, project_op->w.order.pos);
//            ASSERT_TRUE(project_op->w.is_range_between);
//            ASSERT_EQ(-86400000, project_op->w.start_offset);
//            ASSERT_EQ(-1000, project_op->w.end_offset);
//        }
//
//        {
//            ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op.ops[2]);
//            ASSERT_TRUE(project_op->window_agg);
//            ASSERT_EQ("index2", project_op->w.index_name);
//            ASSERT_EQ(::fesql::type::kInt32,
//            project_op->w.keys.cbegin()->type); ASSERT_EQ(0,
//            project_op->w.keys.cbegin()->pos);
//            ASSERT_EQ(::fesql::type::kInt64, project_op->w.order.type);
//            ASSERT_EQ(4, project_op->w.order.pos);
//            ASSERT_TRUE(project_op->w.is_range_between);
//            ASSERT_EQ(-86400000 * 2, project_op->w.start_offset);
//            ASSERT_EQ(-1000, project_op->w.end_offset);
//        }
//
//        {
//            MergeOp* merge_op = reinterpret_cast<MergeOp*>(op.ops[3]);
//            ASSERT_EQ(nullptr, merge_op->fn);
//            ASSERT_EQ(2u, merge_op->children.size());
//            ASSERT_EQ(kOpProject, merge_op->children[0]->type);
//            ASSERT_EQ(kOpProject, merge_op->children[1]->type);
//            ASSERT_EQ(merge_op->output_schema.Get(0).name(), "w1_col1_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(1).name(), "w2_col2_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(2).name(), "w1_col3_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(3).name(), "w2_col4_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(4).name(), "w2_col1_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(5).name(), "w1_col2_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(6).name(), "w2_col3_sum");
//            ASSERT_EQ(merge_op->output_schema.Get(7).name(), "w2_col4_sum");
//        }
//    }
//}

TEST_F(OpGeneratorTest, test_op_generator_error) {
    ::fesql::type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col15");
    {
        OpVector op;
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1\n"
            "              ORDER BY col15 RANGE BETWEEN 2d PRECEDING AND "
            "1s "
            "PRECEDING) limit 10;";
        const Status exp_status(
            ::fesql::common::kIndexNotFound,
            "fail to generate project operator: index is not match window");
        AssertOpGen(table_def, &op, sql, exp_status);
    }

    {
        OpVector op;
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col2\n"
            "              ORDER BY col1 RANGE BETWEEN 2d PRECEDING AND 1s "
            "PRECEDING) limit 10;";
        const Status exp_status(
            ::fesql::common::kIndexNotFound,
            "fail to generate project operator: index is not match window");
        AssertOpGen(table_def, &op, sql, exp_status);
    }

    {
        OpVector op;
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col222\n"
            "              ORDER BY col15 RANGE BETWEEN 2d PRECEDING AND "
            "1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kColumnNotFound,
                                "key column col222 is not exist in table t1");
        AssertOpGen(table_def, &op, sql, exp_status);
    }

    {
        OpVector op;
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col2\n"
            "              ORDER BY col555 RANGE BETWEEN 2d PRECEDING AND "
            "1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kColumnNotFound,
                                "ts column col555 is not exist in table t1");
        AssertOpGen(table_def, &op, sql, exp_status);
    }
}



}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
