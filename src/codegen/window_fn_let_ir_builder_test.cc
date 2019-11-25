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

#include "base/window.cc"
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
union UnionData {
    int16_t vsmallint;
    int vint;         /* machine integer */
    int64_t vlong;    /* machine integer */
    const char* vstr; /* string */
    float vfloat;
    double vdouble;
};
typedef struct {
    std::string col;
    ::fesql::type::Type type;
    std::vector<UnionData> vals;
} Param;

void GetSchema(::fesql::type::TableDef& table) {
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
}
class WindowFnLetIRBuilderTest : public ::testing::TestWithParam<Param> {
 public:
    WindowFnLetIRBuilderTest() { GetSchema(table); }
    ~WindowFnLetIRBuilderTest() {}

    template <class T>
    void Check(base::WindowIteratorImpl& impl,
               int32_t (*get_filed)(int8_t*, int8_t*), ::fesql::type::Type type,
               std::vector<UnionData> vals) {
        base::ColumnIteratorImpl<T>* column_iter =
            new base::ColumnIteratorImpl<T>(impl, get_filed);
        ASSERT_TRUE(nullptr != column_iter);
        for (auto val : vals) {
            ASSERT_TRUE(column_iter->Valid());
            switch (type) {
                case ::fesql::type::kInt16: {
                    ASSERT_EQ(val.vsmallint, column_iter->Next());
                    break;
                }
                case ::fesql::type::kInt32: {
                    ASSERT_EQ(val.vint, column_iter->Next());
                    break;
                }
                case ::fesql::type::kInt64: {
                    ASSERT_EQ(val.vlong, column_iter->Next());
                    break;
                }
                case ::fesql::type::kFloat: {
                    ASSERT_EQ(val.vfloat, column_iter->Next());
                    break;
                }
                case ::fesql::type::kDouble: {
                    ASSERT_EQ(val.vdouble, column_iter->Next());
                    break;
                }
            }
        }
    }

 protected:
    ::fesql::type::TableDef table;
};

INSTANTIATE_TEST_CASE_P(
    GetColWithColName, WindowFnLetIRBuilderTest,
    ::testing::Values(
        Param{
            "col1", ::fesql::type::kInt32,
            std::vector<UnionData>({{.vint = 1}, {.vint = 11}, {.vint = 111}})},
        Param{"col2", ::fesql::type::kInt16,
              std::vector<UnionData>(
                  {{.vsmallint = 2}, {.vsmallint = 22}, {.vsmallint = 222}})},
        Param{"col3", ::fesql::type::kFloat,
              std::vector<UnionData>(
                  {{.vfloat = 3.1f}, {.vfloat = 33.1f}, {.vfloat = 333.1f}})},
        Param{"col4", ::fesql::type::kDouble,
              std::vector<UnionData>(
                  {{.vdouble = 4.1}, {.vdouble = 44.1}, {.vdouble = 444.1}})},
        Param{"col5", ::fesql::type::kInt64,
              std::vector<UnionData>(
                  {{.vlong = 5}, {.vlong = 55}, {.vlong = 555}})}));

TEST_P(WindowFnLetIRBuilderTest, WindowIteratorImplTest) {
    // prepare row buf
    std::vector<base::Row> rows;
    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *((int32_t*)(ptr + 2)) = 1;
        *((int16_t*)(ptr + 2 + 4)) = 2;
        *((float*)(ptr + 2 + 4 + 2)) = 3.1f;
        *((double*)(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        rows.push_back(base::Row{.buf = ptr});
    }

    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *((int32_t*)(ptr + 2)) = 11;
        *((int16_t*)(ptr + 2 + 4)) = 22;
        *((float*)(ptr + 2 + 4 + 2)) = 33.1f;
        *((double*)(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        rows.push_back(base::Row{.buf = ptr});
    }

    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *((int32_t*)(ptr + 2)) = 111;
        *((int16_t*)(ptr + 2 + 4)) = 222;
        *((float*)(ptr + 2 + 4 + 2)) = 333.1f;
        *((double*)(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        rows.push_back(base::Row{.buf = ptr});
    }

    base::WindowIteratorImpl impl(rows);
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    impl.reset();

    Param param = GetParam();
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_get_col", *ctx);
    RowFnLetIRBuilder ir_builder(&table, m.get());

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
        case ::fesql::type::kInt32: {
            Check<int32_t>(impl, decode, param.type, param.vals);
            break;
        }
        case ::fesql::type::kInt64: {
            Check<int64_t>(impl, decode, param.type, param.vals);
            break;
        }
        case ::fesql::type::kInt16: {
            Check<int16_t>(impl, decode, param.type, param.vals);
            break;
        }
        case ::fesql::type::kFloat: {
            Check<float>(impl, decode, param.type, param.vals);
            break;
        }
        case ::fesql::type::kDouble: {
            Check<double>(impl, decode, param.type, param.vals);
            break;
        }
        default: {
            FAIL();
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
