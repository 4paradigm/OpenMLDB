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

#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "gtest/gtest.h"

#include "parser/parser.h"
#include "plan/planner.h"

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

using namespace llvm;
using namespace llvm::orc;

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

typedef struct {
    std::string col;
    ::fesql::type::Type type;
    union {
        int16_t vsmallint;
        int vint;         /* machine integer */
        int64_t vlong;    /* machine integer */
        const char* vstr; /* string */
        float vfloat;
        double vdouble;
    } val_;
    //    ::std::string value_str;
} Param;
class WindowFnLetIRBuilderTest : public ::testing::TestWithParam<Param> {
 public:
    WindowFnLetIRBuilderTest() {}
    ~WindowFnLetIRBuilderTest() {}
};

INSTANTIATE_TEST_CASE_P(
    GetColWithColName, WindowFnLetIRBuilderTest,
    ::testing::Values(Param{"col1", ::fesql::type::kInt32, {.vint = 1}},
                      Param{"col2", ::fesql::type::kInt16, {.vsmallint= 2}},
                      Param{"col3", ::fesql::type::kFloat, {.vfloat = 3.1f}},
                      Param{"col4", ::fesql::type::kDouble, {.vdouble = 4.1}},
                      Param{"col5", ::fesql::type::kInt64, {.vlong = 5}}));

TEST_P(WindowFnLetIRBuilderTest, test_get_col) {
    ::fesql::type::TableDef table;
    // prepare schema
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

    // prepare row buf
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *((int32_t*)(ptr + 2)) = 1;
    *((int16_t*)(ptr + 2 + 4)) = 2;
    *((float*)(ptr + 2 + 4 + 2)) = 3.1f;
    *((double*)(ptr + 2 + 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 5;

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_get_col", *ctx);
    RowFnLetIRBuilder ir_builder(&table, m.get());

    Param param = GetParam();
    // Create the get_col1_fn function entry and insert this entry into module
    // M.  The function will have a return type of "int" and take two arguments.
    {
        ::fesql::type::Type col_type;
        const std::string fn_name = "test_get_" + param.col + "_fn";
        bool ok = ir_builder.Build(fn_name, param.col, col_type);
        ASSERT_TRUE(ok);
        ASSERT_EQ(param.type, col_type);
        m->print(::llvm::errs(), NULL);
        auto J = ExitOnErr(LLJITBuilder().create());
        ExitOnErr(J->addIRModule(
            std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
        auto load_fn_jit = ExitOnErr(J->lookup(fn_name));
        int32_t (*decode)(int8_t*, int8_t*) =
            (int32_t(*)(int8_t*, int8_t*))load_fn_jit.getAddress();
        std::cout << decode << std::endl;
        switch (param.type) {
            case ::fesql::type::kInt16: {
                int16_t i = 0;
                int32_t ret2 = decode(ptr, (int8_t*)&i);
                ASSERT_EQ(ret2, 0u);
                ASSERT_EQ(i, param.val_.vsmallint);
                break;
            }
            case ::fesql::type::kInt32: {
                int32_t i = 0;
                int32_t ret2 = decode(ptr, (int8_t*)&i);
                ASSERT_EQ(ret2, 0u);
                ASSERT_EQ(i, param.val_.vint);
                break;
            }
            case ::fesql::type::kInt64: {
                int64_t i = 0;
                int32_t ret2 = decode(ptr, (int8_t*)&i);
                ASSERT_EQ(ret2, 0u);
                ASSERT_EQ(i, param.val_.vlong);
                break;
            }
            case ::fesql::type::kFloat: {
                float i = 0;
                int32_t ret2 = decode(ptr, (int8_t*)&i);
                ASSERT_EQ(ret2, 0u);
                ASSERT_EQ(i, param.val_.vfloat);
                break;
            }
        }
    }
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
