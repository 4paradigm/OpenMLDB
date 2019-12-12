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
#include "vm/table_mgr.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {

class TableMgrImpl : public TableMgr {
 public:
    explicit TableMgrImpl(std::shared_ptr<TableStatus> status)
        : status_(status) {}
    ~TableMgrImpl() {}
    std::shared_ptr<TableStatus> GetTableDef(const std::string&,
                                             const std::string&) {
        return status_;
    }
    std::shared_ptr<TableStatus> GetTableDef(const std::string&,
                                             const uint32_t) {
        return status_;
    }

 private:
    std::shared_ptr<TableStatus> status_;
};

class OpGeneratorTest : public ::testing::Test {
 public:
    OpGeneratorTest() { InitTable(); }
    ~OpGeneratorTest() {}
    void InitTable() {
        table_def.set_name("t1");
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
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col2");
        index->set_second_key("col15");
    }

 protected:
    fesql::type::TableDef table_def;
};

TEST_F(OpGeneratorTest, test_normal) {
    std::shared_ptr<TableStatus> status(new TableStatus());
    status->table_def = table_def;
    TableMgrImpl table_mgr(status);
    OpGenerator generator(&table_mgr);
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 10;";

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
        ASSERT_EQ(0, base_status.code);
    }

    ASSERT_EQ(2, plan_trees.size());

    OpVector op;
    bool ok = generator.Gen(plan_trees, "db", m.get(), &op, base_status);
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL);
    ASSERT_EQ(2, op.ops.size());
}

TEST_F(OpGeneratorTest, test_windowp_project) {
    std::shared_ptr<TableStatus> status(new TableStatus());
    status->table_def = table_def;
    std::unique_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, status->table_def));
    ASSERT_TRUE(table->Init());
    status->table = std::move(table);
    TableMgrImpl table_mgr(status);
    OpGenerator generator(&table_mgr);
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_op_generator", *ctx);
    const std::string sql =
        "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
        "WINDOW w1 AS (PARTITION BY col2\n"
        "              ORDER BY col15 RANGE BETWEEN 2d PRECEDING AND 1s "
        "PRECEDING) limit 10;";

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
        ASSERT_EQ(0, base_status.code);
    }

    ASSERT_EQ(1, plan_trees.size());

    ::fesql::udf::RegisterUDFToModule(m.get());
    OpVector op;
    bool ok = generator.Gen(plan_trees, "db", m.get(), &op, base_status);
    if (!ok) {
        std::cout << base_status.msg << std::endl;
    }
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL);
    ASSERT_EQ(2, op.ops.size());
    ASSERT_EQ(0, op.ops[0]->idx);
    ASSERT_EQ(kOpScan, op.ops[0]->type);

    ASSERT_EQ(1, op.ops[1]->idx);
    ASSERT_EQ(kOpProject, op.ops[1]->type);
    ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op.ops[1]);
    ASSERT_TRUE(project_op->window_agg);
    ASSERT_EQ(::fesql::type::kInt16, project_op->w.keys.cbegin()->first);
    ASSERT_EQ(1, project_op->w.keys.cbegin()->second);

    ASSERT_EQ(::fesql::type::kInt64, project_op->w.order.first);
    ASSERT_EQ(4, project_op->w.order.second);
    ASSERT_TRUE(project_op->w.is_range_between);
    ASSERT_EQ(-86400000 * 2, project_op->w.start_offset);
    ASSERT_EQ(-1000, project_op->w.end_offset);
}

void AssertOpGenError(TableMgrImpl *table_mrg_ptr, const std::string &sql, const Status &exp_status) {
    OpGenerator generator(table_mrg_ptr);
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
        ASSERT_EQ(0, base_status.code);
        ASSERT_EQ(1, plan_trees.size());
    }
    ::fesql::udf::RegisterUDFToModule(m.get());
    OpVector op;
    bool ok = generator.Gen(plan_trees, "db", m.get(), &op, base_status);
    ASSERT_FALSE(ok);
    std::cout << base_status.msg << std::endl;
    ASSERT_EQ(base_status.code, exp_status.code);
    ASSERT_EQ(base_status.msg, exp_status.msg);
}

TEST_F(OpGeneratorTest, test_op_generator_error) {
    std::shared_ptr<TableStatus> status(new TableStatus());
    status->table_def = table_def;
    std::unique_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, status->table_def));
    ASSERT_TRUE(table->Init());
    status->table = std::move(table);
    TableMgrImpl table_mgr(status);
    {
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col1\n"
            "              ORDER BY col15 RANGE BETWEEN 2d PRECEDING AND 1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kIndexNotFound,"fail to generate project operator: index is not match window");
        AssertOpGenError(&table_mgr, sql, exp_status);
    }

    {
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col2\n"
            "              ORDER BY col1 RANGE BETWEEN 2d PRECEDING AND 1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kIndexNotFound,"fail to generate project operator: index is not match window");
        AssertOpGenError(&table_mgr, sql, exp_status);
    }

    {
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col222\n"
            "              ORDER BY col15 RANGE BETWEEN 2d PRECEDING AND 1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kColumnNotFound,"key column col222 is not exist in table t1");
        AssertOpGenError(&table_mgr, sql, exp_status);
    }

    {
        const std::string sql =
            "SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
            "WINDOW w1 AS (PARTITION BY col2\n"
            "              ORDER BY col555 RANGE BETWEEN 2d PRECEDING AND 1s "
            "PRECEDING) limit 10;";
        const Status exp_status(::fesql::common::kColumnNotFound,"ts column col555 is not exist in table t1");
        AssertOpGenError(&table_mgr, sql, exp_status);
    }
}



// TODO(chenjing): multi window merge

// TEST_F(OpGeneratorTest, test_multi_windowp_project) {
//    std::shared_ptr<TableStatus> status(new TableStatus());
//    status->table_def = table_def;
//    TableMgrImpl table_mgr(status);
//    OpGenerator generator(&table_mgr);
//    auto ctx = llvm::make_unique<LLVMContext>();
//    auto m = make_unique<Module>("test_op_generator", *ctx);
//    const std::string sql =
//        "SELECT sum(col1) OVER w1 as w1_col1_sum, sum(col1) OVER w2 as "
//        "w2_col1_sum FROM t1 "
//        "WINDOW "
//        "w1 AS (PARTITION BY col2 ORDER BY `TS` RANGE BETWEEN 1d PRECEDING AND
//        " "1s PRECEDING), " "w2 AS (PARTITION BY col3 ORDER BY `TS` RANGE
//        BETWEEN 2d PRECEDING AND " "1s PRECEDING) " "limit 10;";
//
//    std::cout << sql;
//    ::fesql::node::NodeManager manager;
//    ::fesql::node::PlanNodeList plan_trees;
//    ::fesql::base::Status base_status;
//    {
//        ::fesql::plan::SimplePlanner planner(&manager);
//        ::fesql::parser::FeSQLParser parser;
//        ::fesql::node::NodePointVector parser_trees;
//        parser.parse(sql, parser_trees, &manager, base_status);
//        ASSERT_EQ(0, base_status.code);
//        planner.CreatePlanTree(parser_trees, plan_trees, base_status);
//        ASSERT_EQ(0, base_status.code);
//    }
//
//    ASSERT_EQ(1, plan_trees.size());
//
//    RegisterUDFToModule(m.get());
//    OpVector op;
//    bool ok = generator.Gen(plan_trees, "db", m.get(), &op, base_status);
//    ASSERT_TRUE(ok);
//    m->print(::llvm::errs(), NULL);
//    ASSERT_EQ(4, op.ops.size());
//    ASSERT_EQ(kOpScan, op.ops[0]->type);
//    ASSERT_EQ(kOpProject, op.ops[1]->type);
//    ASSERT_EQ(kOpProject, op.ops[2]->type);
//    ASSERT_EQ(kOpMerge, op.ops[3]->type);
//    {
//        ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op.ops[1]);
//        ASSERT_TRUE(project_op->window_agg);
//        ASSERT_EQ(std::vector<std::string>({"col2"}), project_op->w.keys);
//        ASSERT_EQ(std::vector<std::string>({"TS"}), project_op->w.orders);
//        ASSERT_TRUE(project_op->w.is_range_between);
//        ASSERT_EQ(-86400000, project_op->w.start_offset);
//        ASSERT_EQ(-1000, project_op->w.end_offset);
//    }
//    {
//        ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op.ops[2]);
//        ASSERT_TRUE(project_op->window_agg);
//        ASSERT_EQ(std::vector<std::string>({"col3"}), project_op->w.keys);
//        ASSERT_EQ(std::vector<std::string>({"TS"}), project_op->w.orders);
//        ASSERT_TRUE(project_op->w.is_range_between);
//        ASSERT_EQ(-86400000 * 2, project_op->w.start_offset);
//        ASSERT_EQ(-1000, project_op->w.end_offset);
//    }
//}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
