/*
 * fn_let_ir_builder_test.cc
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

#include "codegen/fn_let_ir_builder.h"
#include <memory>
#include <string>
#include <vector>
#include "codegen/fn_ir_builder.h"
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
#include "parser/parser.h"
#include "plan/planner.h"
#include "storage/codec.h"
#include "storage/window.h"
#include "udf/udf.h"
#include "vm/jit.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

static node::NodeManager manager;

/// Check E. If it's in a success state then return the contained value. If
/// it's in a failure state log the error(s) and exit.
template <typename T>
T FeCheck(::llvm::Expected<T>&& E) {
    if (E.takeError()) {
        // NOLINT
    }
    return std::move(*E);
}
node::ProjectListNode* GetPlanNodeList(node::PlanNodeList trees) {
    ::fesql::node::ProjectPlanNode* plan_node = nullptr;
    auto node = trees[0]->GetChildren();
    while (!node.empty()) {
        if (node[0]->GetType() == node::kPlanTypeProject) {
            plan_node = dynamic_cast<fesql::node::ProjectPlanNode*>(node[0]);
            break;
        }
        node = node[0]->GetChildren();
    }

    ::fesql::node::ProjectListNode* pp_node_ptr =
        dynamic_cast<fesql::node::ProjectListNode*>(
            plan_node->project_list_vec_[0]);
    return pp_node_ptr;
}
class FnLetIRBuilderTest : public ::testing::Test {
 public:
    FnLetIRBuilderTest() { GetSchema(table_); }
    ~FnLetIRBuilderTest() {}
    void GetSchema(::fesql::type::TableDef& table) {  // NOLINT
        table.set_name("t1");
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kInt32);
            column->set_name("col1");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kInt16);
            column->set_name("col2");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kFloat);
            column->set_name("col3");
        }
        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kDouble);
            column->set_name("col4");
        }

        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kInt64);
            column->set_name("col5");
        }

        {
            ::fesql::type::ColumnDef* column = table.add_columns();
            column->set_type(::fesql::type::kVarchar);
            column->set_name("col6");
        }
    }
    void BuildBuf(int8_t** buf, uint32_t* size) {
        storage::RowBuilder builder(table_.columns());
        uint32_t total_size = builder.CalTotalLength(1);
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendInt32(32);
        builder.AppendInt16(16);
        builder.AppendFloat(2.1f);
        builder.AppendDouble(3.1);
        builder.AppendInt64(64);
        builder.AppendString("1", 1);
        *buf = ptr;
        *size = total_size;
    }

 protected:
    fesql::type::TableDef table_;
};

void AddFunc(const std::string& fn, ::llvm::Module* m) {
    ::fesql::node::NodePointVector trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::base::Status status;
    int ret = parser.parse(fn, trees, &manager, status);
    ASSERT_EQ(0, ret);
    FnIRBuilder fn_ir_builder(m);
    for (node::SQLNode* node : trees) {
        LOG(INFO) << "Add Func: " << *node;
        bool ok =
            fn_ir_builder.Build(dynamic_cast<node::FnNodeFnDef*>(node), status);
        ASSERT_TRUE(ok);
    }
}

TEST_F(FnLetIRBuilderTest, test_primary) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret =
        parser.parse("SELECT col1, col6, 1.0, \"hello\"  FROM t1 limit 10;",
                     list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNodeList trees;
    ret = planner.CreatePlanTree(list, trees, status);
    ASSERT_EQ(0, ret);
    std::cout << *(trees[0]);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(trees);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(4, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));
    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();
    int8_t* buf = NULL;
    uint32_t size = 0;
    BuildBuf(&buf, &size);
    int8_t* output = NULL;
    int32_t ret2 = decode(buf, nullptr, size, &output);
    ASSERT_EQ(ret2, 0);
    uint32_t out_size = *reinterpret_cast<uint32_t*>(output + 2);
    ASSERT_EQ(out_size, 27u);
    ASSERT_EQ(32u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(1.0, *reinterpret_cast<double*>(output + 11));
    ASSERT_EQ(21u, *reinterpret_cast<uint8_t*>(output + 19));
    ASSERT_EQ(22u, *reinterpret_cast<uint8_t*>(output + 20));
    std::string str(reinterpret_cast<char*>(output + 21), 1);
    ASSERT_EQ("1", str);
    std::string str2(reinterpret_cast<char*>(output + 22), 5);
    ASSERT_EQ("hello", str2);
    free(buf);
}

TEST_F(FnLetIRBuilderTest, test_udf) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend";
    AddFunc(test, m.get());
    int ret = parser.parse("SELECT test(col1,col1), col6 FROM t1 limit 10;",
                           list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNodeList trees;
    ret = planner.CreatePlanTree(list, trees, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(trees);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(2, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));
    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();
    int8_t* buf = NULL;
    uint32_t size = 0;
    BuildBuf(&buf, &size);
    int8_t* output = NULL;
    int32_t ret2 = decode(buf, nullptr, size, &output);
    ASSERT_EQ(ret2, 0);
    uint32_t out_size = *reinterpret_cast<uint32_t*>(output + 2);
    ASSERT_EQ(out_size, 13u);
    ASSERT_EQ(65u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(12u, *reinterpret_cast<uint8_t*>(output + 11));
    std::string str(reinterpret_cast<char*>(output + 12), 1);
    ASSERT_EQ("1", str);
    free(buf);
}

TEST_F(FnLetIRBuilderTest, test_simple_project) {
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret =
        parser.parse("SELECT col1 FROM t1 limit 10;", list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size);
    int8_t* output = NULL;
    int32_t ret2 = decode(ptr, nullptr, size, &output);
    ASSERT_EQ(ret2, 0);
    ASSERT_EQ(11u, *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(32u, *reinterpret_cast<uint32_t*>(output + 7));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_extern_udf_project) {
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse("SELECT inc_int32(col1) FROM t1 limit 10;", list,
                           &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::fesql::udf::RegisterUDFToModule(m.get());
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();

    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size);
    int8_t* output = NULL;
    int32_t ret2 = decode(ptr, nullptr, size, &output);
    ASSERT_EQ(ret2, 0);
    ASSERT_EQ(11u, *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(33u, *reinterpret_cast<uint32_t*>(output + 7));
    free(ptr);
}

void BuildWindow(std::vector<fesql::storage::Row>& rows,  // NOLINT
                 int8_t** buf) {
    ::fesql::type::TableDef table;
    table.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }

    {
        storage::RowBuilder builder(table.columns());
        std::string str = "1";
        uint32_t total_size = builder.CalTotalLength(str.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendInt32(1);
        builder.AppendInt16(2);
        builder.AppendFloat(3.1f);
        builder.AppendDouble(4.1);
        builder.AppendInt64(5);
        builder.AppendString(str.c_str(), 1);
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "22";
        uint32_t total_size = builder.CalTotalLength(str.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendInt32(11);
        builder.AppendInt16(22);
        builder.AppendFloat(33.1f);
        builder.AppendDouble(44.1);
        builder.AppendInt64(55);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "333";
        uint32_t total_size = builder.CalTotalLength(str.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendInt32(111);
        builder.AppendInt16(222);
        builder.AppendFloat(333.1f);
        builder.AppendDouble(444.1);
        builder.AppendInt64(555);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "4444";
        uint32_t total_size = builder.CalTotalLength(str.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendInt32(1111);
        builder.AppendInt16(2222);
        builder.AppendFloat(3333.1f);
        builder.AppendDouble(4444.1);
        builder.AppendInt64(5555);
        builder.AppendString("4444", str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        uint32_t total_size = builder.CalTotalLength(str.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendInt32(11111);
        builder.AppendInt16(22222);
        builder.AppendFloat(33333.1f);
        builder.AppendDouble(44444.1);
        builder.AppendInt64(55555);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }

    ::fesql::storage::WindowImpl* w = new ::fesql::storage::WindowImpl(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
}

TEST_F(FnLetIRBuilderTest, test_extern_agg_sum_project) {
    std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum , "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum,  "
        "sum(col2) OVER w1 as w1_col2_sum,  "
        "sum(col5) OVER w1 as w1_col5_sum  "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING) limit 10;";

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse(sql, list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::fesql::udf::RegisterUDFToModule(m.get());
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(5, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();

    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> window;
    BuildWindow(window, &ptr);
    LOG(INFO) << "input ptr " << ptr;

    int8_t* output = NULL;
    int32_t ret2 = decode(window.back().buf, ptr, 0, &output);
    ASSERT_EQ(0, ret2);
    ASSERT_EQ(1u + 11u + 111u + 1111u + 11111u,
              *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
              *reinterpret_cast<float*>(output + 7 + 4));
    ASSERT_EQ(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
              *reinterpret_cast<double*>(output + 7 + 4 + 4));
    ASSERT_EQ(2 + 22 + 222 + 2222 + 22222,
              *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 8));
    ASSERT_EQ(5L + 55L + 555L + 5555L + 55555L,
              *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_simple_window_project_mix) {
    std::string sql =
        "SELECT "
        "col1, "
        "sum(col1) OVER w1 as w1_col1_sum , "
        "col3, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "col4, "
        "sum(col4) OVER w1 as w1_col4_sum,  "
        "sum(col2) OVER w1 as w1_col2_sum,  "
        "sum(col5) OVER w1 as w1_col5_sum  "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING) limit 10;";

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse(sql, list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::fesql::udf::RegisterUDFToModule(m.get());
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(8, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();

    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> window;
    BuildWindow(window, &ptr);
    LOG(INFO) << "input ptr " << ptr;

    int8_t* output = NULL;
    int32_t ret2 = decode(window.back().buf, ptr, 0, &output);
    ASSERT_EQ(0, ret2);
    ASSERT_EQ(11111u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(1u + 11u + 111u + 1111u + 11111u,
              *reinterpret_cast<uint32_t*>(output + 7 + 4));
    ASSERT_EQ(33333.1f, *reinterpret_cast<float*>(output + 7 + 4 + 4));
    ASSERT_EQ(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
              *reinterpret_cast<float*>(output + 7 + 4 + 4 + 4));
    ASSERT_EQ(44444.1, *reinterpret_cast<double*>(output + 7 + 4 + 4 + 4 + 4));
    ASSERT_EQ(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
              *reinterpret_cast<double*>(output + 7 + 4 + 4 + 4 + 4 + 8));
    ASSERT_EQ(2 + 22 + 222 + 2222 + 22222,
              *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 4 + 4 + 8 + 8));
    ASSERT_EQ(
        5L + 55L + 555L + 5555L + 55555L,
        *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 4 + 4 + 8 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_extern_agg_min_project) {
    std::string sql =
        "SELECT "
        "min(col1) OVER w1 as w1_col1_min , "
        "min(col3) OVER w1 as w1_col3_min, "
        "min(col4) OVER w1 as w1_col4_min,  "
        "min(col2) OVER w1 as w1_col2_min,  "
        "min(col5) OVER w1 as w1_col5_min  "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING) limit 10;";
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse(sql, list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::fesql::udf::RegisterUDFToModule(m.get());
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(5, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();

    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> window;
    BuildWindow(window, &ptr);
    int8_t* output = NULL;
    int32_t ret2 = decode(window.back().buf, ptr, 0, &output);
    ASSERT_EQ(ret2, 0);
    ASSERT_EQ(7u + 4u + 4u + 8u + 2u + 8u,
              *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(1u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(3.1f, *reinterpret_cast<float*>(output + 7 + 4));
    ASSERT_EQ(4.1, *reinterpret_cast<double*>(output + 7 + 4 + 4));
    ASSERT_EQ(2, *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 8));
    ASSERT_EQ(5L, *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 8 + 2));
    free(ptr);
}
TEST_F(FnLetIRBuilderTest, test_extern_agg_max_project) {
    std::string sql =
        "SELECT "
        "max(col1) OVER w1 as w1_col1_max , "
        "max(col3) OVER w1 as w1_col3_max, "
        "max(col4) OVER w1 as w1_col4_max,  "
        "max(col2) OVER w1 as w1_col2_max,  "
        "max(col5) OVER w1 as w1_col5_max  "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING) limit 10;";

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse(sql, list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::fesql::udf::RegisterUDFToModule(m.get());
    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(5, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();

    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> window;
    BuildWindow(window, &ptr);
    int8_t* output = NULL;
    int32_t ret2 = decode(window.back().buf, ptr, 0, &output);
    ASSERT_EQ(ret2, 0);
    ASSERT_EQ(7u + 4u + 4u + 8u + 2u + 8u,
              *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(11111u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(33333.1f, *reinterpret_cast<float*>(output + 7 + 4));
    ASSERT_EQ(44444.1, *reinterpret_cast<double*>(output + 7 + 4 + 4));
    ASSERT_EQ(22222, *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 8));
    ASSERT_EQ(55555L, *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_col_at_udf) {
    std::string udf_str =
        "%%fun\n"
        "def test_at(col:list<float>, pos:i32):float\n"
        "\treturn col[pos]\n"
        "end\n"
        "def test_at(col:list<int>, pos:i32):i32\n"
        "\treturn col[pos]\n"
        "end\n"
        "def test_add(x:i32, y:i32):i32\n"
        "\treturn x+y\n"
        "end\n"
        "def count_list(col:list<float>, pos:i32):i32\n"
        "\tquery_value=test_at(col,pos)\n"
        "\tcnt = 0\n"
        "\tfor x in col\n"
        "\t\tif query_value >= x\n"
        "\t\t\tcnt += 1\n"
        "\treturn cnt\n"
        "end\n";
    std::string sql =
        "SELECT "
        "test_at(col3,0) OVER w1 as col3_at_0, "
        "test_at(col1,1) OVER w1 as col1_at_1, "
        "count_list(col3,2) OVER w1 as col3_at_1 "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
        "FOLLOWING) limit 10;";
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    AddFunc(udf_str, m.get());

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;

    int ret = parser.parse(sql, list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create an LLJIT instance.
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".

    RowFnLetIRBuilder ir_builder(table_.columns(), m.get(), false);
    vm::Schema schema;
    bool ok = ir_builder.Build("test_at_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    LOG(INFO) << "fn let ir build ok";
    ASSERT_EQ(3, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_at_fn"));

    int32_t (*decode)(int8_t*, int8_t*, int32_t, int8_t**) = (int32_t(*)(
        int8_t*, int8_t*, int32_t, int8_t**))load_fn_jit.getAddress();

    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> window;
    BuildWindow(window, &ptr);
    int8_t* output = NULL;
    int32_t ret2 = decode(window.back().buf, ptr, 0, &output);
    ASSERT_EQ(ret2, 0);
    //    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8, *reinterpret_cast<uint32_t*>(output +
    //    2));
    ASSERT_EQ(7u + 4u + 4u + 4u, *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(3.1f, *reinterpret_cast<float*>(output + 7));
    ASSERT_EQ(11, *reinterpret_cast<int32_t*>(output + 7 + 4));
    ASSERT_EQ(3, *reinterpret_cast<int32_t*>(output + 7 + 4 + 4));
    free(ptr);
}
}  // namespace codegen
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
